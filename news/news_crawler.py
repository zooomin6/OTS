"""
News Crawler — 실시간 뉴스 수집 + GPT 분류 + 텔레그램 알림.

수집 소스:
  - Reuters RSS
  - CoinDesk RSS
  - Cointelegraph RSS
  - Finnhub 경제지표 캘린더 (HIGH 중요도만)

동작 흐름:
  1. RSS 폴링 (5분 주기)
  2. 키워드 1차 필터
  3. GPT-4o-mini 감성 분류 (BULLISH/BEARISH/NEUTRAL + HIGH/MEDIUM/LOW)
  4. HIGH 뉴스 → 텔레그램 즉시 알림
  5. DB 저장 (news_articles, economic_calendars)
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

DATABASE_URL       = os.environ.get("DATABASE_URL", "")
OPENAI_API_KEY     = os.environ.get("OPENAI_API_KEY", "")
FINNHUB_KEY        = os.environ.get("FINNHUB_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

POLL_INTERVAL = 300  # 초: 5분

# 1차 키워드 필터
KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth",
    "fed", "federal reserve", "interest rate", "cpi", "fomc", "inflation",
    "war", "sanction", "tariff", "regulation", "sec", "etf",
    "hack", "exploit", "liquidat",
]

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
]


# ── DB ────────────────────────────────────────────────────────

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


def _is_duplicate(external_id: str) -> bool:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM news_articles WHERE external_id = %s LIMIT 1",
                (external_id,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _save_news(
    source: str,
    external_id: str,
    title: str,
    summary: str | None,
    url: str,
    published_at: datetime,
    sentiment: str,
    impact_level: str,
    related_coins: list[str],
    gpt_analysis: str,
) -> int:
    import json
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO news_articles (
                    source, external_id, title, summary, url,
                    published_at, sentiment, impact_level,
                    related_coins, gpt_analysis, is_processed
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, TRUE)
                RETURNING id
            """, (
                source, external_id, title, summary, url,
                published_at, sentiment, impact_level,
                json.dumps(related_coins), gpt_analysis,
            ))
            news_id = cur.fetchone()[0]
        conn.commit()
        return news_id
    finally:
        conn.close()


def _save_economic_event(
    source: str,
    external_id: str,
    event_name: str,
    event_date: str,
    event_time: str | None,
    importance: str,
    description: str | None,
) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO economic_calendars (
                    source, external_id, event_name,
                    event_date, event_time, importance, description
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (external_id) DO NOTHING
            """, (
                source, external_id, event_name,
                event_date, event_time, importance, description,
            ))
        conn.commit()
    finally:
        conn.close()


def _get_open_coins() -> list[str]:
    """현재 OPEN 포지션 코인 목록을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT coin_symbol FROM positions WHERE status = 'OPEN'"
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# ── Telegram ──────────────────────────────────────────────────

async def _send_telegram(text: str) -> None:
    import httpx
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
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
            print(f"[news] Telegram 발송 실패: {e}")


# ── 키워드 필터 ───────────────────────────────────────────────

def _passes_keyword_filter(title: str, summary: str | None) -> bool:
    text = (title + " " + (summary or "")).lower()
    return any(kw in text for kw in KEYWORDS)


# ── GPT 분류 ──────────────────────────────────────────────────

async def _classify_with_gpt(title: str, summary: str | None) -> dict:
    """GPT-4o-mini로 뉴스 감성과 영향도를 분류한다."""
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    text   = f"제목: {title}\n요약: {summary or '없음'}"

    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "코인 시장에 영향을 주는 뉴스를 분석하세요. "
                    "반드시 JSON만 반환: "
                    '{"sentiment":"BULLISH"|"BEARISH"|"NEUTRAL",'
                    '"impact":"HIGH"|"MEDIUM"|"LOW",'
                    '"coins":["BTC","ETH"등 관련 코인 심볼 배열],'
                    '"reason":"한 줄 이유"}'
                ),
            },
            {"role": "user", "content": text},
        ],
        response_format={"type": "json_object"},
        temperature=0.1,
    )

    import json
    result = json.loads(response.choices[0].message.content)
    return {
        "sentiment":    result.get("sentiment", "NEUTRAL"),
        "impact":       result.get("impact", "LOW"),
        "coins":        result.get("coins", []),
        "reason":       result.get("reason", ""),
    }


# ── RSS 수집 ──────────────────────────────────────────────────

async def _fetch_rss(feed_url: str) -> list[dict]:
    import httpx
    import xml.etree.ElementTree as ET

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(feed_url, timeout=15)
            resp.raise_for_status()
    except Exception as e:
        print(f"[news] RSS 수집 실패 {feed_url}: {e}")
        return []

    items = []
    try:
        root = ET.fromstring(resp.text)
        ns   = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0
        for item in root.findall(".//item"):
            title   = (item.findtext("title") or "").strip()
            link    = (item.findtext("link") or "").strip()
            desc    = (item.findtext("description") or "").strip()
            pub     = item.findtext("pubDate") or ""
            ext_id  = hashlib.md5(link.encode()).hexdigest()

            try:
                from email.utils import parsedate_to_datetime
                published_at = parsedate_to_datetime(pub).astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                published_at = datetime.utcnow()

            items.append({
                "title":        title,
                "summary":      desc[:500] if desc else None,
                "url":          link,
                "external_id":  ext_id,
                "published_at": published_at,
                "source":       "rss",
            })
    except Exception as e:
        print(f"[news] RSS 파싱 실패 {feed_url}: {e}")

    return items



# ── Finnhub 경제지표 ──────────────────────────────────────────

async def _fetch_finnhub_calendar() -> None:
    if not FINNHUB_KEY:
        return

    import httpx
    from datetime import timedelta

    today    = datetime.utcnow().date()
    end_date = today + timedelta(days=7)
    url = (
        f"https://finnhub.io/api/v1/calendar/economic"
        f"?from={today}&to={end_date}&token={FINNHUB_KEY}"
    )

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[news] Finnhub 수집 실패: {e}")
        return

    for event in data.get("economicCalendar", []):
        importance_raw = event.get("impact", "").upper()
        importance_map = {"HIGH": "HIGH", "MEDIUM": "MEDIUM", "LOW": "LOW"}
        importance     = importance_map.get(importance_raw, "LOW")

        if importance != "HIGH":
            continue

        ext_id     = hashlib.md5(
            f"{event.get('event')}{event.get('time')}".encode()
        ).hexdigest()
        event_name = event.get("event", "")
        event_date = event.get("time", "")[:10]
        event_time = event.get("time", "")[11:19] or None

        _save_economic_event(
            source="finnhub",
            external_id=ext_id,
            event_name=event_name,
            event_date=event_date,
            event_time=event_time,
            importance="HIGH",
            description=None,
        )
        print(f"[news] 경제지표 저장: {event_name} ({event_date})")


# ── 뉴스 처리 파이프라인 ──────────────────────────────────────

async def _process_news_item(item: dict) -> None:
    """뉴스 1건 — 중복 체크 → 키워드 필터 → GPT 분류 → DB 저장 → 텔레그램."""
    if _is_duplicate(item["external_id"]):
        return

    if not _passes_keyword_filter(item["title"], item["summary"]):
        return

    try:
        gpt = await _classify_with_gpt(item["title"], item["summary"])
    except Exception as e:
        print(f"[news] GPT 분류 실패: {e}")
        return

    _save_news(
        source=item["source"],
        external_id=item["external_id"],
        title=item["title"],
        summary=item["summary"],
        url=item["url"],
        published_at=item["published_at"],
        sentiment=gpt["sentiment"],
        impact_level=gpt["impact"],
        related_coins=gpt["coins"],
        gpt_analysis=gpt["reason"],
    )

    print(f"[news] {gpt['impact']} {gpt['sentiment']} — {item['title'][:60]}")

    # HIGH 뉴스만 텔레그램 알림
    if gpt["impact"] != "HIGH":
        return

    open_coins   = _get_open_coins()
    coins_str    = ", ".join(gpt["coins"]) if gpt["coins"] else "없음"
    conflict     = any(c in open_coins for c in gpt["coins"])
    conflict_line = "\n⚠️ *현재 보유 포지션과 관련된 뉴스입니다.*" if conflict else ""

    emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "🟡"}.get(gpt["sentiment"], "🟡")
    await _send_telegram(
        f"{emoji} *[HIGH] {gpt['sentiment']} 뉴스*\n\n"
        f"*{item['title']}*\n\n"
        f"관련 코인: {coins_str}\n"
        f"분석: {gpt['reason']}"
        f"{conflict_line}"
    )


# ── 메인 루프 ─────────────────────────────────────────────────

class NewsCrawler:

    async def run(self) -> None:
        print("[news] 시작 — 뉴스 수집 중")
        while True:
            try:
                await self._collect_all()
            except Exception as e:
                print(f"[news] 수집 에러: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _collect_all(self) -> None:
        # RSS + CryptoPanic 동시 수집
        rss_tasks = [_fetch_rss(feed) for feed in RSS_FEEDS]
        results   = await asyncio.gather(*rss_tasks)

        items = []
        for r in results:
            items.extend(r)

        # 뉴스 처리 (순서 보장 불필요 — 병렬)
        await asyncio.gather(*[_process_news_item(item) for item in items])

        # 경제지표 캘린더 (별도)
        await _fetch_finnhub_calendar()

        print(f"[news] 수집 완료: {len(items)}건 처리")


if __name__ == "__main__":
    asyncio.run(NewsCrawler().run())
