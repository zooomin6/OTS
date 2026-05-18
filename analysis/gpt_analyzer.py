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
당신은 국내 주식·코인 투자 분석 어시스턴트입니다.
유튜브 멤버십 투자 게시글(텍스트 + 이미지)을 읽고 반드시 아래 JSON 형식만 반환하세요. 설명 텍스트 없이 JSON만.

{
  "signal_type": "BUY" | "SELL" | "HOLD",
  "coin_symbol": "BTC" | "ETH" | "SOL" | "XRP" | 기타심볼 | null,
  "entry_price_1": 1차 매수 목표가 (숫자) | null,
  "entry_price_2": 2차 매수 목표가 (숫자) | null,
  "stop_loss_price": 손절가 (숫자) | null,
  "take_profit_price": 목표 익절가 (숫자) | null,
  "summary": "핵심 투자 내용 2~3문장 요약",
  "invalidation": "이 분석이 무효화되는 조건",
  "scenario": [
    {"step": 1, "action": "액션 설명", "condition": "진입·청산 조건", "target_price": null}
  ]
}

- signal_type: 게시글의 핵심 방향 (BUY=매수/강세, SELL=매도/약세, HOLD=관망/중립)
- coin_symbol: 언급된 코인/주식 심볼. 이미지의 차트에서도 확인할 것. 없으면 null
- entry_price_1/2: 1차·2차 매수 구간 또는 목표가. 이미지에 표시된 수치 우선. 없으면 null
- stop_loss_price: 손절 기준가. 없으면 null
- take_profit_price: 익절 목표가. 없으면 null
- scenario: 단계별 시나리오 최대 3단계, target_price는 언급 없으면 null
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


def _save_analysis_sync(
    post_db_id: int,
    signal_type: str,
    coin_symbol: str | None,
    entry_price_1: float | None,
    entry_price_2: float | None,
    stop_loss_price: float | None,
    take_profit_price: float | None,
    summary: str,
    invalidation: str,
    scenario_json: list,
    raw_response: str,
) -> int:
    """analyses 테이블에 저장하고 새 analysis id를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analyses (
                    post_id, signal_type, coin_symbol,
                    entry_price_1, entry_price_2, stop_loss_price, take_profit_price,
                    summary, invalidation, scenario_json, raw_response
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id
                """,
                (
                    post_db_id,
                    signal_type,
                    coin_symbol,
                    entry_price_1,
                    entry_price_2,
                    stop_loss_price,
                    take_profit_price,
                    summary,
                    invalidation,
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
    return {
        "signal_type":      parsed.get("signal_type", "HOLD").upper(),
        "coin_symbol":      parsed.get("coin_symbol"),
        "entry_price_1":    parsed.get("entry_price_1"),
        "entry_price_2":    parsed.get("entry_price_2"),
        "stop_loss_price":  parsed.get("stop_loss_price"),
        "take_profit_price":parsed.get("take_profit_price"),
        "summary":          parsed.get("summary", ""),
        "invalidation":     parsed.get("invalidation", ""),
        "scenario":         parsed.get("scenario", []),
        "raw":              raw,
    }


# ── Telegram ─────────────────────────────────────────────────

async def _send_telegram(
    analysis_id: int,
    signal_type: str,
    summary: str,
    content_preview: str,
) -> None:
    """분석 결과를 Telegram으로 알린다."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    import httpx

    EMOJI = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}
    emoji = EMOJI.get(signal_type, "⚪")

    text = (
        f"{emoji} *새 투자 신호 — {signal_type}*\n\n"
        f"*요약*\n{summary}\n\n"
        f"*원문 미리보기*\n{content_preview[:120]}\n\n"
        f"분석 ID: \\#{analysis_id}"
    )

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
        result["entry_price_1"],
        result["entry_price_2"],
        result["stop_loss_price"],
        result["take_profit_price"],
        result["summary"],
        result["invalidation"],
        result["scenario"],
        result["raw"],
    )
    analysis_id = await loop.run_in_executor(None, save_fn)
    print(f"[analyzer] 저장 완료: analysis_id={analysis_id}")

    # 코인과 가격 정보가 있으면 price_alerts 자동 등록
    if result["coin_symbol"]:
        alerts_fn = functools.partial(
            _create_price_alerts_sync,
            analysis_id,
            result["coin_symbol"],
            result["entry_price_1"],
            result["entry_price_2"],
            result["stop_loss_price"],
            result["take_profit_price"],
        )
        await loop.run_in_executor(None, alerts_fn)

    # Telegram 알림
    await _send_telegram(
        analysis_id,
        result["signal_type"],
        result["summary"],
        post["content"],
    )


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
