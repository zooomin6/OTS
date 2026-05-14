from __future__ import annotations

import asyncio
import sys

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
from db.models import Settings, Analysis, Trade, DailyStat

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

class AnalyzeRequest(BaseModel):
    content: str
    channel_id: str = "manual"
    published_at: datetime = Field(default_factory=datetime.utcnow)


class AnalyzeResponse(BaseModel):
    analysis_id: int
    signal_type: str
    summary: Optional[str]
    invalidation: Optional[str]
    scenario_json: Any


class HistoryItem(BaseModel):
    id: int
    post_id: int
    signal_type: str
    summary: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


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


class SettingsPatch(BaseModel):
    mode: Optional[str] = Field(None, pattern="^(FULL_AUTO|SEMI_AUTO)$")
    max_trade_amount_krw: Optional[int] = Field(None, gt=0)
    daily_loss_limit_krw: Optional[int] = Field(None, gt=0)
    stop_loss_pct: Optional[Decimal] = Field(None, gt=0, le=1)


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
    # TODO: gpt_analyzer.analyze_post() → analyses INSERT → telegram 발송
    raise HTTPException(status_code=501, detail="Not implemented yet")


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
    # TODO: risk_manager.check() → bybit_client.place_order() → trades INSERT
    raise HTTPException(status_code=501, detail="Not implemented yet")


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
    return settings


@app.post("/settings/resume", response_model=SettingsResponse, tags=["System"])
async def resume_trading(db: AsyncSession = Depends(get_db)):
    settings = await db.get(Settings, 1)
    if not settings:
        raise HTTPException(status_code=404, detail="Settings not found")
    settings.is_halted = False
    settings.updated_at = datetime.now(timezone.utc)
    return settings


@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
