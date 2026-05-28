from __future__ import annotations

import asyncio
import functools
import sys
import uuid

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.database import get_db
from db.models import Settings, Analysis, Trade, DailyStat, Post, PriceAlert

app = FastAPI(
    title="AI 코인 투자 어시스턴트",
    description="GPT-4o + Bybit 기반 자동 암호화폐 투자 어시스턴트",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Schemas ───────────────────────────────────────────────────────────────────
# API 요청/응답의 데이터 형식을 정의하는 Pydantic 모델.
# FastAPI가 이 클래스를 기반으로 자동으로 타입 검증 및 직렬화를 처리한다.

# POST /analyze 요청 바디: 분석할 게시글 내용과 채널 정보
class AnalyzeRequest(BaseModel):
    content: str
    channel_id: str = "manual"
    published_at: datetime = Field(default_factory=datetime.utcnow)


# POST /analyze 응답: GPT 분석 결과 (매매 신호, 요약, 시나리오)
class AnalyzeResponse(BaseModel):
    analysis_id: int
    signal_type: str
    summary: Optional[str]
    invalidation: Optional[str]
    scenario_json: Any


# GET /history 응답 항목: 분석 이력 목록의 개별 항목
class HistoryItem(BaseModel):
    id: int
    post_id: int
    signal_type: str
    summary: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True  # SQLAlchemy 모델을 바로 직렬화할 수 있게 허용


# GET /trades 응답 항목: 매매 내역 목록의 개별 항목
class TradeItem(BaseModel):
    id: int
    analysis_id: int
    symbol: str
    side: str
    qty: Decimal
    price: Optional[Decimal]
    status: str
    mode: str
    executed_at: Optional[datetime]

    class Config:
        from_attributes = True


# PATCH /settings 요청 바디: 변경할 설정 항목만 선택적으로 전송 (None이면 변경 안 함)
class SettingsPatch(BaseModel):
    mode: Optional[str] = Field(None, pattern="^(AUTO|SEMI_AUTO|MANUAL)$")
    max_trade_amount_krw: Optional[int] = Field(None, gt=0)
    daily_loss_limit_krw: Optional[int] = Field(None, gt=0)
    stop_loss_pct: Optional[Decimal] = Field(None, gt=0, le=1)


# GET /settings 응답: 현재 설정 전체 조회
class SettingsResponse(BaseModel):
    id: int
    mode: str
    max_trade_amount_krw: int
    daily_loss_limit_krw: int
    stop_loss_pct: Decimal
    is_halted: bool
    updated_at: datetime

    class Config:
        from_attributes = True


# GET /status 응답: 운영 상태 + 오늘의 매매 통계 요약
class StatusResponse(BaseModel):
    mode: str
    is_halted: bool
    max_trade_amount_krw: int
    daily_loss_limit_krw: int
    stop_loss_pct: Decimal
    today_total_trades: int
    today_realized_pnl_krw: int
    crawler_running: bool


# ── Analysis ──────────────────────────────────────────────────────────────────

@app.post("/analyze", response_model=AnalyzeResponse, tags=["Analysis"])
async def analyze(body: AnalyzeRequest, db: AsyncSession = Depends(get_db)):
    from analysis.gpt_analyzer import (
        _analyze_with_gpt,
        _save_analysis_sync,
        _save_market_context_sync,
        _create_price_alerts_sync,
    )

    loop = asyncio.get_event_loop()

    # posts 테이블에 수동 입력 게시글 저장
    post = Post(
        channel_id=body.channel_id,
        post_id=f"manual_{uuid.uuid4().hex[:12]}",
        content=body.content,
        post_type="text",
        image_urls=[],
        published_at=body.published_at,
    )
    db.add(post)
    await db.flush()
    post_db_id = post.id

    # GPT 분석
    try:
        analyses, market_indicators = await _analyze_with_gpt(body.content)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"GPT 분석 실패: {e}")

    if market_indicators:
        ctx_fn = functools.partial(_save_market_context_sync, post_db_id, market_indicators)
        await loop.run_in_executor(None, ctx_fn)

    first_id: int | None = None
    first_result: dict | None = None

    for result in analyses:
        if result["signal_type"] == "SKIP":
            continue

        save_fn = functools.partial(
            _save_analysis_sync,
            post_db_id,
            result["signal_type"], result["coin_symbol"], result["timeframe"],
            result["is_reference_only"], result["youtuber_zone_low"], result["youtuber_zone_high"],
            result["entry_price_1"], result["entry_price_2"], result["entry_price_3"], result["entry_price_4"],
            result["absolute_stop"], result["stop_loss_price"], result["take_profit_price"],
            result["short_entry_price"], result["short_stop_loss"], result["risk_reward_ratio"],
            result["current_rsi"], result["rsi_signal"], result["volume_signal"], result["fib_level"],
            result["summary"], result["invalidation"], result["scenario"], result["raw"],
        )
        analysis_id = await loop.run_in_executor(None, save_fn)

        if result["coin_symbol"] and not result["is_reference_only"]:
            alerts_fn = functools.partial(
                _create_price_alerts_sync,
                analysis_id, result["coin_symbol"],
                result["entry_price_1"], result["entry_price_2"], result["entry_price_3"],
                result["stop_loss_price"], result["take_profit_price"],
            )
            await loop.run_in_executor(None, alerts_fn)

        if first_id is None:
            first_id = analysis_id
            first_result = result

    await db.commit()

    if first_id is None or first_result is None:
        raise HTTPException(status_code=422, detail="분석 결과 없음 (SKIP 또는 빈 응답)")

    return AnalyzeResponse(
        analysis_id=first_id,
        signal_type=first_result["signal_type"],
        summary=first_result.get("summary"),
        invalidation=first_result.get("invalidation"),
        scenario_json=first_result.get("scenario"),
    )


@app.get("/history", response_model=list[HistoryItem], tags=["Analysis"])
async def get_history(limit: int = Query(default=5, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Analysis).order_by(Analysis.created_at.desc()).limit(limit)
    )
    return result.scalars().all()


@app.get("/history/{analysis_id}", response_model=AnalyzeResponse, tags=["Analysis"])
async def get_history_detail(analysis_id: int, db: AsyncSession = Depends(get_db)):
    analysis = await db.get(Analysis, analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    return analysis


# ── Trading ───────────────────────────────────────────────────────────────────

@app.get("/trades", response_model=list[TradeItem], tags=["Trading"])
async def get_trades(
    limit: int = Query(default=20, ge=1, le=200),
    status: Optional[str] = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Trade).order_by(Trade.executed_at.desc()).limit(limit)
    if status:
        stmt = stmt.where(Trade.status == status)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/trades/{trade_id}", response_model=TradeItem, tags=["Trading"])
async def get_trade_detail(trade_id: int, db: AsyncSession = Depends(get_db)):
    trade = await db.get(Trade, trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    return trade


@app.post("/trades/{analysis_id}/execute", response_model=TradeItem, tags=["Trading"])
async def execute_trade(analysis_id: int, db: AsyncSession = Depends(get_db)):
    from trading.trade_executor import _execute_alert

    analysis = await db.get(Analysis, analysis_id)
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")
    if analysis.signal_type not in ("BUY", "SELL"):
        raise HTTPException(status_code=422, detail=f"실행 불가 신호: {analysis.signal_type}")

    settings = await db.get(Settings, 1)
    mode = settings.mode if settings else "MANUAL"

    # ENTRY_2(중립형) 우선 → 없으면 다른 ENTRY 알림
    stmt = (
        select(PriceAlert)
        .where(PriceAlert.analysis_id == analysis_id)
        .where(PriceAlert.status.in_(["PENDING", "PENDING_SLOT"]))
        .where(PriceAlert.alert_type.like("ENTRY_%"))
        .order_by(PriceAlert.alert_type)
    )
    result = await db.execute(stmt)
    alerts = result.scalars().all()
    alert_row = next((a for a in alerts if a.alert_type == "ENTRY_2"), None) or (alerts[0] if alerts else None)

    if alert_row is None:
        # 기존 알림 없으면 entry_price_2 기준으로 생성
        if not analysis.entry_price_2:
            raise HTTPException(status_code=422, detail="진입가(entry_price_2) 없음 — 실행 불가")
        alert_row = PriceAlert(
            analysis_id=analysis_id,
            coin_symbol=analysis.coin_symbol,
            target_price=analysis.entry_price_2,
            alert_type="ENTRY_2",
            status="TRIGGERED",
        )
        db.add(alert_row)
        await db.flush()
    else:
        alert_row.status = "TRIGGERED"

    await db.commit()
    await db.refresh(alert_row)

    alert = {
        "id":              alert_row.id,
        "analysis_id":     analysis_id,
        "coin_symbol":     analysis.coin_symbol,
        "target_price":    float(alert_row.target_price),
        "alert_type":      alert_row.alert_type,
        "signal_type":     analysis.signal_type,
        "stop_loss_price": float(analysis.stop_loss_price) if analysis.stop_loss_price else None,
        "take_profit":     float(analysis.take_profit_price) if analysis.take_profit_price else None,
        "absolute_stop":   float(analysis.absolute_stop) if analysis.absolute_stop else None,
    }

    await _execute_alert(alert, mode)

    trade_result = await db.execute(
        select(Trade)
        .where(Trade.analysis_id == analysis_id)
        .order_by(Trade.executed_at.desc())
        .limit(1)
    )
    trade = trade_result.scalar_one_or_none()
    if not trade:
        raise HTTPException(status_code=500, detail="주문 실패 — trade 레코드 없음")
    return trade


# ── System ────────────────────────────────────────────────────────────────────

@app.get("/status", response_model=StatusResponse, tags=["System"])
async def get_status(db: AsyncSession = Depends(get_db)):
    settings = await db.get(Settings, 1)
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")

    today = datetime.now(timezone.utc).date()
    result = await db.execute(select(DailyStat).where(DailyStat.date == today))
    daily = result.scalar_one_or_none()

    return StatusResponse(
        mode=settings.mode,
        is_halted=settings.is_halted,
        max_trade_amount_krw=settings.max_trade_amount_krw,
        daily_loss_limit_krw=settings.daily_loss_limit_krw,
        stop_loss_pct=settings.stop_loss_pct,
        today_total_trades=daily.total_trades if daily else 0,
        today_realized_pnl_krw=daily.realized_pnl_krw if daily else 0,
        crawler_running=False,
    )


@app.get("/settings", response_model=SettingsResponse, tags=["System"])
async def get_settings(db: AsyncSession = Depends(get_db)):
    settings = await db.get(Settings, 1)
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")
    return settings


@app.patch("/settings", response_model=SettingsResponse, tags=["System"])
async def update_settings(body: SettingsPatch, db: AsyncSession = Depends(get_db)):
    settings = await db.get(Settings, 1)
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(settings, field, value)
    settings.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(settings)
    return settings


@app.post("/settings/resume", response_model=SettingsResponse, tags=["System"])
async def resume_trading(db: AsyncSession = Depends(get_db)):
    settings = await db.get(Settings, 1)
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")
    settings.is_halted = False
    settings.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(settings)
    return settings


@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
