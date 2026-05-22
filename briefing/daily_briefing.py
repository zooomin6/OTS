"""
Daily Briefing — 매일 KST 06:00 자동 브리핑.

브리핑 내용:
  - BTC 도미넌스 / 테더 도미넌스 / 공포탐욕지수
  - 현재 보유 포지션 현황 (미실현 손익)
  - 오늘 실현 손익
  - 이번 주 유튜버 신호 적중률
  - 다음 HIGH 경제지표 D-day

데이터 소스:
  - CoinGecko API (무료) — 도미넌스
  - Alternative.me API (무료) — 공포탐욕지수
  - Bybit API — 포지션 미실현 손익
  - DB — 실현 손익, 신호 적중률, 경제지표
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

DATABASE_URL       = os.environ.get("DATABASE_URL", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
BYBIT_API_KEY      = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET   = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_TESTNET      = os.environ.get("BYBIT_TESTNET", "true").lower() == "true"


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


def _get_today_realized_pnl() -> int:
    """오늘 실현 손익(원화)을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(realized_pnl_krw, 0)
                FROM daily_stats
                WHERE date = CURRENT_DATE
            """)
            row = cur.fetchone()
            return row[0] if row else 0
    finally:
        conn.close()


def _get_open_positions() -> list[dict]:
    """현재 OPEN 포지션 목록을 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT coin_symbol, side, avg_entry_price,
                       current_qty, leverage, current_stop_loss,
                       current_take_profit_1, opened_at
                FROM positions
                WHERE status = 'OPEN'
                ORDER BY opened_at ASC
            """)
            return [
                {
                    "coin":         row[0],
                    "side":         row[1],
                    "avg_entry":    float(row[2]),
                    "qty":          float(row[3]),
                    "leverage":     row[4],
                    "stop_loss":    float(row[5]) if row[5] else None,
                    "take_profit":  float(row[6]) if row[6] else None,
                    "opened_at":    row[7],
                }
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


def _get_weekly_signal_accuracy() -> dict:
    """
    이번 주 유튜버 신호 적중률을 계산한다.
    적중 기준: TAKE_PROFIT 알림이 TRIGGERED된 analyses 수 / 전체 활성 analyses 수
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # 이번 주 생성된 분석 수
            cur.execute("""
                SELECT COUNT(*) FROM analyses
                WHERE created_at >= DATE_TRUNC('week', NOW())
            """)
            total = cur.fetchone()[0]

            # 이번 주 TP 도달한 분석 수
            cur.execute("""
                SELECT COUNT(DISTINCT pa.analysis_id)
                FROM price_alerts pa
                WHERE pa.alert_type = 'TAKE_PROFIT'
                  AND pa.status = 'TRIGGERED'
                  AND pa.triggered_at >= DATE_TRUNC('week', NOW())
            """)
            hits = cur.fetchone()[0]

            return {"total": total, "hits": hits}
    finally:
        conn.close()


def _get_next_high_economic_event() -> dict | None:
    """다음 HIGH 중요도 경제지표를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_name, event_date, event_time
                FROM economic_calendars
                WHERE importance = 'HIGH'
                  AND event_date >= CURRENT_DATE
                ORDER BY event_date ASC, event_time ASC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return None
            return {
                "name": row[0],
                "date": row[1],
                "time": row[2],
            }
    finally:
        conn.close()


# ── 외부 데이터 수집 ──────────────────────────────────────────

async def _fetch_dominance() -> dict:
    """CoinGecko에서 BTC/USDT 도미넌스를 가져온다."""
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.coingecko.com/api/v3/global",
                timeout=10,
            )
            data = resp.json()["data"]["market_cap_percentage"]
            return {
                "btc":  round(data.get("btc", 0), 1),
                "usdt": round(data.get("usdt", 0), 1),
            }
    except Exception as e:
        print(f"[briefing] 도미넌스 조회 실패: {e}")
        return {"btc": None, "usdt": None}


async def _fetch_fear_greed() -> dict:
    """Alternative.me에서 공포탐욕지수를 가져온다."""
    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.alternative.me/fng/?limit=1",
                timeout=10,
            )
            item = resp.json()["data"][0]
            return {
                "value":       int(item["value"]),
                "label":       item["value_classification"],
            }
    except Exception as e:
        print(f"[briefing] 공포탐욕지수 조회 실패: {e}")
        return {"value": None, "label": None}


async def _fetch_current_prices(coins: list[str]) -> dict[str, float]:
    """Bybit에서 코인별 현재가를 가져온다."""
    from pybit.unified_trading import HTTP
    prices = {}
    try:
        session = HTTP(
            testnet=BYBIT_TESTNET,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )
        for coin in coins:
            resp = session.get_tickers(category="linear", symbol=f"{coin}USDT")
            if resp.get("retCode") == 0:
                price_str = resp["result"]["list"][0].get("lastPrice")
                if price_str:
                    prices[coin] = float(price_str)
    except Exception as e:
        print(f"[briefing] 현재가 조회 실패: {e}")
    return prices


# ── 브리핑 생성 ───────────────────────────────────────────────

async def generate_briefing() -> str:
    """브리핑 텍스트를 생성한다."""
    # 병렬 수집
    dominance, fear_greed = await asyncio.gather(
        _fetch_dominance(),
        _fetch_fear_greed(),
    )

    positions   = _get_open_positions()
    pnl_today   = _get_today_realized_pnl()
    accuracy    = _get_weekly_signal_accuracy()
    next_event  = _get_next_high_economic_event()

    # 보유 코인 현재가 조회
    coins        = list({p["coin"] for p in positions})
    current_prices = await _fetch_current_prices(coins) if coins else {}

    # KST 현재 시각
    kst_now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = [f"📋 *OTS 일일 브리핑 — {kst_now} KST*\n"]

    # ── 시장 지표 ──
    lines.append("*📊 시장 지표*")
    btc_dom  = f"{dominance['btc']}%" if dominance["btc"] else "-"
    usdt_dom = f"{dominance['usdt']}%" if dominance["usdt"] else "-"
    lines.append(f"BTC 도미넌스: `{btc_dom}` | USDT 도미넌스: `{usdt_dom}`")

    if fear_greed["value"] is not None:
        fg_emoji = "😱" if fear_greed["value"] < 30 else "😊" if fear_greed["value"] > 70 else "😐"
        lines.append(f"공포탐욕지수: `{fear_greed['value']}` {fg_emoji} _{fear_greed['label']}_")
    lines.append("")

    # ── 보유 포지션 ──
    lines.append("*💼 보유 포지션*")
    if not positions:
        lines.append("현재 보유 포지션 없음")
    else:
        for p in positions:
            coin        = p["coin"]
            side_emoji  = "🟢" if p["side"] == "LONG" else "🔴"
            current     = current_prices.get(coin)
            avg         = p["avg_entry"]

            if current:
                pnl_pct = ((current - avg) / avg * 100) * (1 if p["side"] == "LONG" else -1)
                pnl_str = f"{'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}%"
                price_str = f"현재가 `${current:,.2f}` | 수익률 `{pnl_str}`"
            else:
                price_str = f"진입가 `${avg:,.2f}`"

            lines.append(
                f"{side_emoji} *{coin}* {p['side']} {p['leverage']}x — {price_str}"
            )
    lines.append("")

    # ── 오늘 실현 손익 ──
    lines.append("*💰 오늘 실현 손익*")
    pnl_emoji = "🟢" if pnl_today >= 0 else "🔴"
    lines.append(f"{pnl_emoji} `{'+' if pnl_today >= 0 else ''}{pnl_today:,}원`")
    lines.append("")

    # ── 신호 적중률 ──
    lines.append("*🎯 이번 주 신호 적중률*")
    if accuracy["total"] > 0:
        rate = round(accuracy["hits"] / accuracy["total"] * 100, 1)
        lines.append(f"`{accuracy['hits']}/{accuracy['total']}` ({rate}%)")
    else:
        lines.append("이번 주 신호 없음")
    lines.append("")

    # ── 다음 경제지표 ──
    lines.append("*📅 다음 주요 경제지표*")
    if next_event:
        from datetime import date
        days_left = (next_event["date"] - date.today()).days
        time_str  = str(next_event["time"])[:5] if next_event["time"] else ""
        d_label   = "오늘" if days_left == 0 else f"D-{days_left}"
        lines.append(f"`{next_event['name']}` — {next_event['date']} {time_str} ({d_label})")
    else:
        lines.append("예정된 지표 없음")

    return "\n".join(lines)


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
            print(f"[briefing] Telegram 발송 실패: {e}")


async def send_daily_briefing() -> None:
    """브리핑을 생성하고 텔레그램으로 발송한다."""
    print("[briefing] 브리핑 생성 중...")
    text = await generate_briefing()
    await _send_telegram(text)
    print("[briefing] 발송 완료")


# ── 스케줄러 ──────────────────────────────────────────────────

def run_scheduler() -> None:
    """APScheduler로 매일 KST 06:00에 브리핑을 실행한다."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    import pytz

    scheduler = AsyncIOScheduler(timezone=pytz.timezone("Asia/Seoul"))
    scheduler.add_job(
        send_daily_briefing,
        CronTrigger(hour=6, minute=0, timezone=pytz.timezone("Asia/Seoul")),
    )
    scheduler.start()
    print("[briefing] 스케줄러 시작 — 매일 KST 06:00 브리핑")

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        scheduler.shutdown()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "now":
        # 즉시 테스트 실행: python daily_briefing.py now
        asyncio.run(send_daily_briefing())
    else:
        run_scheduler()
