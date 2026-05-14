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
유튜브 멤버십 투자 게시글을 읽고 반드시 아래 JSON 형식만 반환하세요. 설명 텍스트 없이 JSON만.

{
  "signal_type": "BUY" | "SELL" | "HOLD",
  "summary": "핵심 투자 내용 2~3문장 요약",
  "invalidation": "이 분석이 무효화되는 조건",
  "scenario": [
    {"step": 1, "action": "액션 설명", "condition": "진입·청산 조건", "target_price": null}
  ]
}

- signal_type: 게시글의 핵심 방향 (BUY=매수/강세, SELL=매도/약세, HOLD=관망/중립)
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
    """DB id로 게시글을 조회한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, content, channel_id FROM posts WHERE id = %s",
                (post_db_id,),
            )
            row = cur.fetchone()
            return {"id": row[0], "content": row[1], "channel_id": row[2]} if row else None
    finally:
        conn.close()


def _save_analysis_sync(
    post_db_id: int,
    signal_type: str,
    summary: str,
    invalidation: str,
    scenario_json: list,
    raw_response: str,
) -> int:
    """analyses 테이블에 저장하고 새 id를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analyses
                    (post_id, signal_type, summary, invalidation, scenario_json, raw_response)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id
                """,
                (
                    post_db_id,
                    signal_type,
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


# ── GPT-4o ───────────────────────────────────────────────────

async def _analyze_with_gpt(content: str) -> dict:
    """게시글 내용을 GPT-4o로 분석하고 파싱된 결과를 반환한다."""
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": content},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
    )
    raw = response.choices[0].message.content
    parsed = json.loads(raw)
    return {
        "signal_type":  parsed.get("signal_type", "HOLD").upper(),
        "summary":      parsed.get("summary", ""),
        "invalidation": parsed.get("invalidation", ""),
        "scenario":     parsed.get("scenario", []),
        "raw":          raw,
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
    """Kafka 메시지 1건: 게시글 조회 → GPT 분석 → DB 저장 → Telegram."""
    data = json.loads(msg_value)
    post_db_id = data["post_id"]

    loop = asyncio.get_event_loop()

    # 게시글 조회
    post = await loop.run_in_executor(None, _fetch_post_sync, post_db_id)
    if not post:
        print(f"[analyzer] 게시글 없음: id={post_db_id}")
        return

    print(f"[analyzer] 분석 시작: post_id={post_db_id}")

    # GPT-4o 분석
    result = await _analyze_with_gpt(post["content"])
    print(f"[analyzer] 신호: {result['signal_type']}")

    # DB 저장
    save_fn = functools.partial(
        _save_analysis_sync,
        post_db_id,
        result["signal_type"],
        result["summary"],
        result["invalidation"],
        result["scenario"],
        result["raw"],
    )
    analysis_id = await loop.run_in_executor(None, save_fn)
    print(f"[analyzer] 저장 완료: analysis_id={analysis_id}")

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
