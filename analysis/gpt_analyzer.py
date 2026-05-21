"""
GPT-4o 투자 시나리오 분석기.
Kafka post.new 컨슘 → DB에서 게시글 조회 → GPT-4o 분석 → analyses 저장 → Telegram 발송.
"""
from __future__ import annotations

import asyncio
import functools
import json
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC        = os.environ.get("KAFKA_TOPIC_POST_NEW", "post.new")
KAFKA_GROUP        = "analysis-group"
DATABASE_URL       = os.environ["DATABASE_URL"]
OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

# GPT-4o에게 줄 역할과 응답 형식 지시
SYSTEM_PROMPT = """\
당신은 국내 코인 투자 분석 어시스턴트입니다.
유튜브 멤버십 투자 게시글(텍스트 + 이미지 차트)을 분석하여 반드시 아래 JSON 형식만 반환하세요. 설명 텍스트 없이 JSON만.

{
  "signal_type": "BUY" | "SELL" | "HOLD",
  "coin_symbol": "BTC" | "ETH" | "SOL" | "XRP" | 기타심볼 | null,
  "timeframe": "MONTHLY" | "WEEKLY" | "DAILY" | "HOURLY" | null,
  "youtuber_zone_low": 유튜버가 제시한 매수/매도구간 하단 (숫자) | null,
  "youtuber_zone_high": 유튜버가 제시한 매수/매도구간 상단 (숫자) | null,
  "entry_price_1": 안정형 진입가 (숫자) | null,
  "entry_price_2": 중립형 진입가 (숫자) | null,
  "entry_price_3": 공격형 진입가 (숫자) | null,
  "entry_price_4": 초공격형 진입가 — 마지막 매수/매도 (숫자) | null,
  "entry_ratio_1": null,
  "entry_ratio_2": null,
  "entry_ratio_3": null,
  "absolute_stop": 마지노선 — 이 아래면 시즌 종료 수준 (숫자) | null,
  "stop_loss_price": 손절가 (숫자) | null,
  "take_profit_price": 1차 목표 익절가 (숫자) | null,
  "take_profit_price_2": 2차 목표 익절가 (숫자) | null,
  "short_entry_price": SELL 신호 시 숏 진입 추천가 (숫자) | null,
  "short_stop_loss": SELL 신호 시 숏 손절가 (숫자) | null,
  "risk_reward_ratio": R:R 비율 (소수, 예: 2.5) | null,
  "current_rsi": 게시글에 언급된 RSI 현재값 (숫자, 예: 43.37) | null,
  "rsi_signal": "OVERSOLD" | "NEUTRAL" | "OVERBOUGHT" | null,
  "volume_signal": "HIGH" | "NORMAL" | "LOW" | null,
  "fib_level": 가장 가까운 피보나치 레벨 (예: 0.618) | null,
  "summary": "핵심 투자 내용 2~3문장 요약",
  "invalidation": "이 분석이 무효화되는 조건",
  "scenario": [
    {"step": 1, "action": "액션 설명", "condition": "진입·청산 조건", "target_price": null}
  ]
}

## 분석 원칙

### 1. 유튜버 신호 우선
- 유튜버가 제시한 가격들을 정확히 추출하세요. 텍스트에 명시된 숫자를 최우선으로 사용.
- 이미지 차트에 표시된 수치가 있으면 텍스트와 함께 참고.
- signal_type은 유튜버의 방향성(BUY/SELL/HOLD)을 따릅니다.

### 2. 차트 시간 단위 (timeframe) 추출
- 게시글/차트에서 "월봉", "주봉", "일봉", "시봉/시간봉/1H/4H" 키워드를 찾아 판단.
- MONTHLY(월봉), WEEKLY(주봉): 참고용 분석 — 자동매매 주문 없음.
- DAILY(일봉), HOURLY(시간봉): 자동매매 실행 대상.
- 명확히 알 수 없으면 null.

### 3. 성향별 단일 진입가 배정

**BUY 신호일 때** — 각 성향의 사람은 자신의 레벨 하나에서만 매수합니다:
- **entry_price_1 (안정형)**: 유튜버 레벨 중 가장 높은 가격. 일찍 진입, 리스크 최소.
- **entry_price_2 (중립형)**: 유튜버 레벨 중 중간 가격.
- **entry_price_3 (공격형)**: 유튜버 레벨 중 하단 가격. 깊은 하락 기다림.
- **entry_price_4 (초공격형)**: 유튜버가 "마지막 매수" 또는 최저 레벨로 명시한 가격. 명시 없으면 null.

**SELL 신호일 때** — 방향이 반전됩니다 (숏은 높은 가격이 더 안전):
- **entry_price_1 (안정형)**: 유튜버 레벨 중 가장 낮은 가격. 충분히 내린 후 숏.
- **entry_price_2 (중립형)**: 유튜버 레벨 중 중간 가격.
- **entry_price_3 (공격형)**: 유튜버 레벨 중 가장 높은 가격. 일찍 숏 진입.
- **entry_price_4 (초공격형)**: 유튜버가 "가장 공격적 숏 자리"로 명시한 가격. 명시 없으면 null.

레벨이 4개보다 적으면 있는 것만 채우고 나머지는 null.
레벨이 1개뿐이면 entry_price_1에만 채우고 나머지 null.

### 4. 숏 진입가 (SELL 신호 전용)
- GPT가 숏 포지션 진입이 적절하다고 판단할 때만 short_entry_price를 채웁니다.
- 단순 롱 청산 알림에 그칠 경우 short_entry_price = null.
- short_stop_loss = short_entry_price × 1.03 (숏 손절은 진입가 위 +3%).
- 유튜버가 명시한 값 있으면 그 값 우선.

### 5. 마지노선 (absolute_stop) 추출
- 유튜버가 "시즌 종료", "추세선 붕괴", "절대 지지선" 등으로 표현한 가격.
- stop_loss_price와 다름: 여기 도달하면 단순 손절이 아니라 시장 방향 자체가 바뀐 것.
- 없으면 null.

### 6. 손절가 자동 계산
- 유튜버가 명시한 손절가 있으면 그대로 사용.
- BUY 신호 — 없으면: stop_loss_price = youtuber_zone_low × 0.97 (구간 하단 -3%)
- SELL 신호 — 없으면: stop_loss_price = youtuber_zone_high × 1.03 (구간 상단 +3%)

### 7. R:R 비율 계산
- risk_reward_ratio = (take_profit_price - entry_price_2) / (entry_price_2 - stop_loss_price)
- entry_price_2 기준 (중립형 기준값). 계산 불가능하면 null.

### 8. 기술적 지표
- current_rsi: 유튜버가 텍스트에서 언급한 RSI 수치 (예: "rsi 43.37" → 43.37).
- rsi_signal: current_rsi 기준 30 이하 → OVERSOLD, 70 이상 → OVERBOUGHT, 나머지 → NEUTRAL.
- 거래량: 최근 캔들 거래량이 평균 대비 높으면 HIGH, 낮으면 LOW. 판단 불가 → null.
- 피보나치: 차트의 주요 되돌림 레벨 중 entry_price_2와 가장 가까운 값.

### 9. 무효화 조건
- BUY 신호: "종가 기준 {zone_low 또는 absolute_stop} 하향 이탈" 형식으로 반드시 포함.
- SELL 신호: "종가 기준 {zone_high} 상향 돌파" 형식으로 반드시 포함.
"""


# ── DB ───────────────────────────────────────────────────────

def _db_connect():
    import psycopg2
    from urllib.parse import urlparse
    url = (DATABASE_URL
           .replace("postgresql+asyncpg://", "postgresql://")
           .replace("postgresql+psycopg://",  "postgresql://"))
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        dbname=p.path.lstrip("/"),
        options="-c client_encoding=UTF8",
    )


def _fetch_post_sync(post_db_id: int) -> dict | None:
    """DB id로 게시글을 조회한다. image_urls도 함께 반환해 Vision 분석에 활용한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, content, channel_id, image_urls, post_type FROM posts WHERE id = %s",
                (post_db_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id":         row[0],
                "content":    row[1],
                "channel_id": row[2],
                "image_urls": row[3] or [],   # JSONB → Python 리스트
                "post_type":  row[4],
            }
    finally:
        conn.close()


def _calc_expires_at(timeframe: str | None) -> str | None:
    """timeframe 기반으로 expires_at 문자열을 계산한다 (DB NOW() 기준 상대값)."""
    if timeframe == "DAILY":
        return "NOW() + INTERVAL '5 days'"
    if timeframe == "HOURLY":
        return "NOW() + INTERVAL '24 hours'"
    return None  # MONTHLY / WEEKLY / None → 만료 없음


def _save_analysis_sync(
    post_db_id: int,
    signal_type: str,
    coin_symbol: str | None,
    timeframe: str | None,
    is_reference_only: bool,
    youtuber_zone_low: float | None,
    youtuber_zone_high: float | None,
    entry_price_1: float | None,
    entry_price_2: float | None,
    entry_price_3: float | None,
    entry_price_4: float | None,
    entry_ratio_1: int | None,
    entry_ratio_2: int | None,
    entry_ratio_3: int | None,
    absolute_stop: float | None,
    stop_loss_price: float | None,
    take_profit_price: float | None,
    short_entry_price: float | None,
    short_stop_loss: float | None,
    risk_reward_ratio: float | None,
    current_rsi: float | None,
    rsi_signal: str | None,
    volume_signal: str | None,
    fib_level: float | None,
    summary: str,
    invalidation: str,
    scenario_json: list,
    raw_response: str,
) -> int:
    """analyses 테이블에 저장하고 새 analysis id를 반환한다."""
    expires_expr = _calc_expires_at(timeframe)

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO analyses (
                    post_id, signal_type, coin_symbol,
                    timeframe, is_reference_only,
                    youtuber_zone_low, youtuber_zone_high,
                    entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                    entry_ratio_1, entry_ratio_2, entry_ratio_3,
                    absolute_stop, stop_loss_price, take_profit_price,
                    short_entry_price, short_stop_loss,
                    risk_reward_ratio, current_rsi, rsi_signal, volume_signal, fib_level,
                    summary, invalidation, scenario_json, raw_response,
                    expires_at
                )
                VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s::jsonb, %s,
                    {expires_expr if expires_expr else 'NULL'}
                )
                RETURNING id
                """,
                (
                    post_db_id, signal_type, coin_symbol,
                    timeframe, is_reference_only,
                    youtuber_zone_low, youtuber_zone_high,
                    entry_price_1, entry_price_2, entry_price_3, entry_price_4,
                    entry_ratio_1, entry_ratio_2, entry_ratio_3,
                    absolute_stop, stop_loss_price, take_profit_price,
                    short_entry_price, short_stop_loss,
                    risk_reward_ratio, current_rsi, rsi_signal, volume_signal, fib_level,
                    summary, invalidation,
                    json.dumps(scenario_json, ensure_ascii=False),
                    raw_response,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return row[0]
    finally:
        conn.close()


def _create_price_alerts_sync(
    analysis_id: int,
    coin_symbol: str,
    entry_price_1: float | None,
    entry_price_2: float | None,
    entry_price_3: float | None,
    stop_loss_price: float | None,
    take_profit_price: float | None,
) -> None:
    """
    분석 결과에서 추출된 가격 수치를 price_alerts 테이블에 등록한다.
    Price Monitor 서비스가 이 테이블을 보고 바이빗 실시간 가격과 비교해 알림을 발송한다.
    """
    alerts = []
    if entry_price_1:
        alerts.append(("ENTRY_1",     entry_price_1))
    if entry_price_2:
        alerts.append(("ENTRY_2",     entry_price_2))
    if entry_price_3:
        alerts.append(("ENTRY_3",     entry_price_3))
    if stop_loss_price:
        alerts.append(("STOP_LOSS",   stop_loss_price))
    if take_profit_price:
        alerts.append(("TAKE_PROFIT", take_profit_price))

    if not alerts:
        return

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            for alert_type, price in alerts:
                cur.execute(
                    """
                    INSERT INTO price_alerts (analysis_id, coin_symbol, target_price, alert_type)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (analysis_id, coin_symbol, price, alert_type),
                )
        conn.commit()
        print(f"[analyzer] 가격 알림 등록: {len(alerts)}개 ({coin_symbol})")
    finally:
        conn.close()


# ── GPT-4o ───────────────────────────────────────────────────

async def _analyze_with_gpt(content: str, image_urls: list[str] | None = None) -> dict:
    """
    게시글 텍스트와 이미지를 GPT-4o로 분석하고 파싱된 결과를 반환한다.

    이미지가 있으면 Vision 기능으로 차트에서 직접 수치를 읽는다.
    이미지는 최대 5장까지만 전송 (비용·토큰 제한).
    """
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)

    # 텍스트 + 이미지를 하나의 메시지로 구성
    # 이미지가 없으면 단순 텍스트 메시지, 있으면 멀티모달 메시지
    if image_urls:
        user_content: list = [{"type": "text", "text": content}]
        for url in image_urls[:5]:  # 최대 5장 (비용 절감)
            user_content.append({
                "type": "image_url",
                "image_url": {"url": url, "detail": "high"},  # high: 차트 수치를 정확히 읽기 위해 고해상도 모드
            })
    else:
        user_content = content  # type: ignore[assignment]

    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_content},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
    )
    raw = response.choices[0].message.content
    parsed = json.loads(raw)
    timeframe = parsed.get("timeframe")
    if timeframe:
        timeframe = timeframe.upper()
        if timeframe not in ("MONTHLY", "WEEKLY", "DAILY", "HOURLY"):
            timeframe = None

    return {
        "signal_type":        parsed.get("signal_type", "HOLD").upper(),
        "coin_symbol":        parsed.get("coin_symbol"),
        "timeframe":          timeframe,
        "is_reference_only":  timeframe in ("MONTHLY", "WEEKLY"),
        "youtuber_zone_low":  parsed.get("youtuber_zone_low"),
        "youtuber_zone_high": parsed.get("youtuber_zone_high"),
        "entry_price_1":      parsed.get("entry_price_1"),
        "entry_price_2":      parsed.get("entry_price_2"),
        "entry_price_3":      parsed.get("entry_price_3"),
        "entry_price_4":      parsed.get("entry_price_4"),
        "entry_ratio_1":      parsed.get("entry_ratio_1"),
        "entry_ratio_2":      parsed.get("entry_ratio_2"),
        "entry_ratio_3":      parsed.get("entry_ratio_3"),
        "absolute_stop":      parsed.get("absolute_stop"),
        "stop_loss_price":    parsed.get("stop_loss_price"),
        "take_profit_price":  parsed.get("take_profit_price"),
        "take_profit_price_2":parsed.get("take_profit_price_2"),
        "short_entry_price":  parsed.get("short_entry_price"),
        "short_stop_loss":    parsed.get("short_stop_loss"),
        "risk_reward_ratio":  parsed.get("risk_reward_ratio"),
        "current_rsi":        parsed.get("current_rsi"),
        "rsi_signal":         parsed.get("rsi_signal"),
        "volume_signal":      parsed.get("volume_signal"),
        "fib_level":          parsed.get("fib_level"),
        "summary":            parsed.get("summary", ""),
        "invalidation":       parsed.get("invalidation", ""),
        "scenario":           parsed.get("scenario", []),
        "raw":                raw,
    }


# ── Telegram ─────────────────────────────────────────────────

async def _send_telegram(
    analysis_id: int,
    result: dict,
    content_preview: str,
) -> None:
    """분석 결과를 Telegram으로 알린다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    import httpx

    signal_type = result["signal_type"]
    EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}
    emoji = EMOJI.get(signal_type, "⚪")

    coin      = result.get("coin_symbol") or "?"
    zone_low  = result.get("youtuber_zone_low")
    zone_high = result.get("youtuber_zone_high")
    e1 = result.get("entry_price_1")   # 안정형
    e2 = result.get("entry_price_2")   # 중립형
    e3 = result.get("entry_price_3")   # 공격형
    e4 = result.get("entry_price_4")   # 초공격형
    abs_stop = result.get("absolute_stop")
    sl  = result.get("stop_loss_price")
    tp  = result.get("take_profit_price")
    rr  = result.get("risk_reward_ratio")
    cur_rsi = result.get("current_rsi")
    rsi = result.get("rsi_signal")
    vol = result.get("volume_signal")
    fib = result.get("fib_level")

    def _fmt(v):
        if v is None:
            return "-"
        return f"{v:,.0f}" if v >= 1000 else f"{v:,.4f}"

    lines = [f"{emoji} *새 투자 신호 — {signal_type} ({coin})*\n"]

    if zone_low and zone_high:
        lines.append(f"📌 유튜버 구간: {_fmt(zone_low)} ~ {_fmt(zone_high)}\n")

    # 성향별 진입가 표
    entry_rows = [
        ("안정형", e1), ("중립형", e2), ("공격형", e3), ("초공격형", e4),
    ]
    active = [(label, price) for label, price in entry_rows if price]
    if active:
        entry_lines = ["🎯 *성향별 진입가*"]
        for label, price in active:
            entry_lines.append(f"  {label}: {_fmt(price)}")
        lines.append("\n".join(entry_lines) + "\n")

    if sl or tp:
        lines.append(
            f"🛡 손절: {_fmt(sl)}  |  🏆 목표: {_fmt(tp)}"
            + (f"  (R:R {rr:.1f})" if rr else "")
            + "\n"
        )

    if abs_stop:
        lines.append(f"⛔ 마지노선: {_fmt(abs_stop)} (이탈 시 시즌 종료)\n")

    tech_parts = []
    if cur_rsi:
        tech_parts.append(f"RSI {cur_rsi} ({rsi or '?'})")
    elif rsi:
        tech_parts.append(f"RSI={rsi}")
    if vol:
        tech_parts.append(f"거래량={vol}")
    if fib:
        tech_parts.append(f"Fib {fib}")
    if tech_parts:
        lines.append("📊 기술지표: " + " | ".join(tech_parts) + "\n")

    lines.append(f"\n*요약*\n{result['summary']}\n")
    lines.append(f"*무효 조건*\n{result['invalidation']}\n")
    lines.append(f"\n원문: {content_preview[:80]}...")
    lines.append(f"\n분석 ID: \\#{analysis_id}")

    text = "\n".join(lines)

    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id":    TELEGRAM_CHAT_ID,
                    "text":       text,
                    "parse_mode": "Markdown",
                },
                timeout=10,
            )
        except Exception as e:
            print(f"[analyzer] Telegram 발송 실패: {e}")


# ── 파이프라인 ────────────────────────────────────────────────

async def _process(msg_value: bytes) -> None:
    """Kafka 메시지 1건: 게시글 조회 → GPT 분석 → DB 저장 → 가격알림 등록 → Telegram."""
    data = json.loads(msg_value)
    post_db_id  = data["post_id"]
    # Kafka 메시지에 image_urls가 있으면 활용, 없으면 DB에서 가져온 값 사용
    kafka_images = data.get("image_urls", [])

    loop = asyncio.get_event_loop()

    # 게시글 조회 (image_urls, post_type 포함)
    post = await loop.run_in_executor(None, _fetch_post_sync, post_db_id)
    if not post:
        print(f"[analyzer] 게시글 없음: id={post_db_id}")
        return

    # Kafka 메시지의 이미지가 있으면 우선 사용, 없으면 DB에서 가져온 것 사용
    image_urls = kafka_images or post.get("image_urls", [])
    print(f"[analyzer] 분석 시작: post_id={post_db_id}, 이미지 {len(image_urls)}개")

    # GPT-4o 분석 (텍스트 + 이미지 Vision)
    result = await _analyze_with_gpt(post["content"], image_urls=image_urls)
    print(f"[analyzer] 신호: {result['signal_type']} | 코인: {result['coin_symbol']}")

    # DB 저장
    save_fn = functools.partial(
        _save_analysis_sync,
        post_db_id,
        result["signal_type"],
        result["coin_symbol"],
        result["timeframe"],
        result["is_reference_only"],
        result["youtuber_zone_low"],
        result["youtuber_zone_high"],
        result["entry_price_1"],
        result["entry_price_2"],
        result["entry_price_3"],
        result["entry_price_4"],
        result["entry_ratio_1"],
        result["entry_ratio_2"],
        result["entry_ratio_3"],
        result["absolute_stop"],
        result["stop_loss_price"],
        result["take_profit_price"],
        result["short_entry_price"],
        result["short_stop_loss"],
        result["risk_reward_ratio"],
        result["current_rsi"],
        result["rsi_signal"],
        result["volume_signal"],
        result["fib_level"],
        result["summary"],
        result["invalidation"],
        result["scenario"],
        result["raw"],
    )
    analysis_id = await loop.run_in_executor(None, save_fn)
    print(f"[analyzer] 저장 완료: analysis_id={analysis_id}")

    # MONTHLY/WEEKLY 참고용 분석은 가격 알림 생성 안 함
    if result["coin_symbol"] and not result["is_reference_only"]:
        alerts_fn = functools.partial(
            _create_price_alerts_sync,
            analysis_id,
            result["coin_symbol"],
            result["entry_price_1"],
            result["entry_price_2"],
            result["entry_price_3"],
            result["stop_loss_price"],
            result["take_profit_price"],
        )
        await loop.run_in_executor(None, alerts_fn)

    # Telegram 알림
    await _send_telegram(analysis_id, result, post["content"])


# ── 실행 루프 ─────────────────────────────────────────────────

async def run_consumer() -> None:
    """Kafka post.new 토픽을 구독하며 분석을 실행한다."""
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print(f"[analyzer] 시작 — {KAFKA_TOPIC} 구독 중")
    try:
        async for msg in consumer:
            try:
                await _process(msg.value)
            except Exception as e:
                print(f"[analyzer] 처리 에러: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(run_consumer())
