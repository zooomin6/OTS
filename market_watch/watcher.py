"""
Market Watch — 시장 조건 감시 + 텔레그램 알림.

감시 조건:
  1. 테더.D (USDT.D): 7.83% 저항 도달 후 하락 전환 → BTC/ETH 진입 신호
  2. BTC: 74,000 저항 돌파 → BTC 진입 신호
"""
from __future__ import annotations

import asyncio
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
REDIS_URL          = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
DATABASE_URL       = os.environ.get("DATABASE_URL", "")

POLL_INTERVAL_USDT_D = 60   # 초: CoinGecko 폴링 주기
POLL_INTERVAL_BTC    = 30   # 초: Bybit 폴링 주기
ALERT_COOLDOWN       = 14400  # 초: 같은 신호 재알림 방지 (4시간)

# ── 감시 조건 ─────────────────────────────────────────────────
USDT_D_RESISTANCE = 7.83   # 테더.D 저항선 (%)
BTC_BREAKOUT      = 74_000  # BTC 저항 돌파 기준 ($)


# ── Redis ─────────────────────────────────────────────────────

def _redis():
    import redis as redis_lib
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def _is_alerted(key: str) -> bool:
    try:
        return _redis().exists(key) > 0
    except Exception:
        return False


def _set_alerted(key: str, ttl: int = ALERT_COOLDOWN) -> None:
    try:
        _redis().setex(key, ttl, "1")
    except Exception:
        pass


def _set_flag(key: str) -> None:
    try:
        _redis().set(key, "1")
    except Exception:
        pass


def _has_flag(key: str) -> bool:
    try:
        return _redis().exists(key) > 0
    except Exception:
        return False


def _del_flag(key: str) -> None:
    try:
        _redis().delete(key)
    except Exception:
        pass


# ── Telegram ──────────────────────────────────────────────────

async def _send(text: str) -> None:
    import httpx
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
            print(f"[market_watch] 알림 발송: {text[:60]}...")
        except Exception as e:
            print(f"[market_watch] 텔레그램 발송 실패: {e}")


# ── 데이터 수집 ────────────────────────────────────────────────

async def _fetch_usdt_dominance() -> float | None:
    """CoinGecko /global 에서 USDT 도미넌스(%) 반환."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.coingecko.com/api/v3/global")
            r.raise_for_status()
            pct = r.json()["data"]["market_cap_percentage"].get("usdt")
            return float(pct) if pct else None
    except Exception as e:
        print(f"[market_watch] USDT.D 조회 실패: {e}")
        return None


async def _fetch_btc_price() -> float | None:
    """Bybit REST API 에서 BTC 현재가 반환."""
    import httpx
    try:
        url = "https://api.bybit.com/v5/market/tickers"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params={"category": "linear", "symbol": "BTCUSDT"})
            r.raise_for_status()
            items = r.json()["result"]["list"]
            return float(items[0]["lastPrice"]) if items else None
    except Exception as e:
        print(f"[market_watch] BTC 가격 조회 실패: {e}")
        return None


# ── 조건 판단 ──────────────────────────────────────────────────

async def _check_usdt_d() -> None:
    """
    테더.D 감시:
      - USDT.D >= 7.83% → 저항 도달 플래그 세팅
      - 플래그 세팅된 상태에서 USDT.D < 7.83% → 저항 반락 알림
    """
    value = await _fetch_usdt_dominance()
    if value is None:
        return

    print(f"[market_watch] USDT.D = {value:.3f}%")

    # 거시 방향성 규칙 조건 체크
    await _check_macro_rules(value)

    flag_key   = "mw:usdt_d:above_resistance"
    alerted_key = "mw:usdt_d:rebound_alerted"

    if value >= USDT_D_RESISTANCE:
        _set_flag(flag_key)
        print(f"[market_watch] USDT.D {value:.3f}% ≥ {USDT_D_RESISTANCE}% 저항 도달 — 플래그 세팅")

    elif _has_flag(flag_key) and not _is_alerted(alerted_key):
        # 저항 도달 후 하락 전환
        _del_flag(flag_key)
        _set_alerted(alerted_key)
        await _send(
            "📉 *테더.D 저항 반락 — 진입 신호*\n\n"
            f"USDT.D `{value:.3f}%` — {USDT_D_RESISTANCE}% 저항에서 하락 전환\n\n"
            "✅ 코인 시장 자금 유입 여건 형성\n"
            "BTC / ETH 진입 구간 확인 권장"
        )


async def _check_btc() -> None:
    """BTC 74,000 저항 돌파 감시."""
    price = await _fetch_btc_price()
    if price is None:
        return

    print(f"[market_watch] BTC = ${price:,.0f}")

    alerted_key = "mw:btc:breakout_alerted"
    if price >= BTC_BREAKOUT and not _is_alerted(alerted_key):
        _set_alerted(alerted_key)
        await _send(
            "🚀 *BTC 저항 돌파 — 진입 신호*\n\n"
            f"BTC `${price:,.0f}` — {BTC_BREAKOUT:,} 저항 돌파\n\n"
            "✅ 일봉 저항 상향 돌파 확인\n"
            "BTC 롱 진입 구간 확인 권장"
        )


def _load_active_macro_rules() -> list[dict]:
    """DB에서 활성 거시 방향성 규칙 조회."""
    import psycopg2
    from urllib.parse import urlparse
    url = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://").replace("postgresql+psycopg://", "postgresql://")
    p = urlparse(url)
    try:
        conn = psycopg2.connect(host=p.hostname, port=p.port or 5432,
                                user=p.username, password=p.password,
                                dbname=p.path.lstrip("/"), options="-c client_encoding=UTF8")
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, trigger_coin, trigger_cond, trigger_level,
                       result_coin, result_direction, result_timeframe,
                       result_target, description
                FROM macro_rules WHERE is_active = TRUE
                ORDER BY created_at DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception as e:
        print(f"[market_watch] macro_rules 조회 실패 (테이블 없을 수 있음): {e}")
        return []


def _is_macro_condition_met(current: float, rule: dict) -> bool:
    """거시 규칙 조건 충족 여부 확인."""
    level = float(rule["trigger_level"])
    cond  = rule["trigger_cond"]
    if cond in ("CLOSE_ABOVE", "BREAK_ABOVE"):
        return current >= level
    if cond in ("CLOSE_BELOW", "BREAK_BELOW"):
        return current <= level
    return False


async def _check_macro_rules(usdt_d_value: float) -> None:
    """활성 거시 규칙의 조건 충족 여부 확인 후 텔레그램 경고 발송."""
    import asyncio
    rules = await asyncio.get_event_loop().run_in_executor(None, _load_active_macro_rules)

    for rule in rules:
        if rule["trigger_coin"] != "USDT.D":
            continue  # 현재는 USDT.D 기반 규칙만 처리

        if not _is_macro_condition_met(usdt_d_value, rule):
            continue

        alert_key = f"mw:macro_rule:{rule['id']}:alerted"
        if _is_alerted(alert_key):
            continue

        _set_alerted(alert_key)  # 24시간 쿨다운

        direction_kr = "하락" if rule["result_direction"] == "BEARISH" else "상승"
        tf_kr = {"WEEKLY": "주봉", "MONTHLY": "월봉", "DAILY": "일봉"}.get(rule.get("result_timeframe", ""), "")
        cond_kr = "이상" if "ABOVE" in rule["trigger_cond"] else "이하"

        text = (
            f"🚨 *유튜버 거시 방향성 발동!*\n\n"
            f"조건: {rule['trigger_coin']} {float(rule['trigger_level']):.2f}% {cond_kr} 충족\n"
            f"현재: `{usdt_d_value:.2f}%`\n\n"
            f"⛔ *{rule['result_coin']} {tf_kr} {direction_kr} 시나리오 발동*\n"
        )
        if rule.get("result_target"):
            text += f"하락 목표가: `{float(rule['result_target']):,.0f}`\n"
        text += (
            f"\n{'🔴 롱 진입 금지' if rule['result_direction'] == 'BEARISH' else '🟢 숏 진입 금지'}\n"
            f"기존 포지션 손절 기준 재확인 필요\n\n"
            f"_{rule.get('description', '')}_"
        )
        await _send(text)
        print(f"[market_watch] 거시규칙 경고 발송: rule#{rule['id']}")


# ── 메인 루프 ──────────────────────────────────────────────────

async def run() -> None:
    print(f"[market_watch] 시작 — USDT.D 저항 {USDT_D_RESISTANCE}% / BTC 돌파 {BTC_BREAKOUT:,}")

    usdt_d_task_time = 0.0
    btc_task_time    = 0.0

    import time
    while True:
        now = time.time()

        if now - usdt_d_task_time >= POLL_INTERVAL_USDT_D:
            asyncio.create_task(_check_usdt_d())
            usdt_d_task_time = now

        if now - btc_task_time >= POLL_INTERVAL_BTC:
            asyncio.create_task(_check_btc())
            btc_task_time = now

        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run())
