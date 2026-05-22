"""
Price Monitor — Bybit WebSocket 실시간 가격 모니터링 + price_alerts 트리거.

동작 흐름:
  1. DB에서 PENDING price_alerts 로드
  2. Bybit WebSocket으로 활성 코인 티커 구독
  3. 현재가가 target_price 도달 시 TRIGGERED 업데이트 + 텔레그램 알림
  4. 60초마다 DB 재조회 (새 분석 반영)
  5. PENDING_SLOT → 슬롯 여유 생기면 자동 PENDING 전환
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

from dotenv import load_dotenv

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

DATABASE_URL       = os.environ.get("DATABASE_URL", "")
REDIS_URL          = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BYBIT_TESTNET      = os.environ.get("BYBIT_TESTNET", "true").lower() == "true"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

RELOAD_INTERVAL    = 60    # 초: DB 재조회 주기
REDIS_DEDUP_TTL    = 1800  # 초: 중복 방지 30분
MAX_OPEN_POSITIONS = 2  # BTC 50% / ETH 50%

ALERT_TYPE_LABEL = {
    "ENTRY_1":       "안정형 진입",
    "ENTRY_2":       "중립형 진입",
    "ENTRY_3":       "공격형 진입",
    "ENTRY_4":       "초공격형 진입",
    "ABSOLUTE_STOP": "마지노선",
    "STOP_LOSS":     "손절",
    "TAKE_PROFIT":   "1차 목표",
    "TAKE_PROFIT_2": "2차 목표",
    "SHORT_ENTRY":   "숏 진입",
}


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


def _load_pending_alerts() -> list[dict]:
    """PENDING 상태 price_alerts를 signal_type과 함께 로드한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pa.id, pa.analysis_id, pa.coin_symbol,
                       pa.target_price, pa.alert_type, a.signal_type
                FROM price_alerts pa
                JOIN analyses a ON a.id = pa.analysis_id
                WHERE pa.status = 'PENDING'
                  AND a.is_active = TRUE
            """)
            return [
                {
                    "id":           row[0],
                    "analysis_id":  row[1],
                    "coin_symbol":  row[2],
                    "target_price": float(row[3]),
                    "alert_type":   row[4],
                    "signal_type":  row[5],
                }
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


def _trigger_alert_db(alert_id: int) -> None:
    """price_alert 상태를 TRIGGERED로 업데이트한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE price_alerts
                SET status = 'TRIGGERED', triggered_at = NOW()
                WHERE id = %s AND status = 'PENDING'
            """, (alert_id,))
        conn.commit()
    finally:
        conn.close()


def _count_open_positions() -> int:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM positions WHERE status = 'OPEN'")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _promote_pending_slots() -> list[dict]:
    """포지션 슬롯 여유만큼 PENDING_SLOT → PENDING 전환하고 전환된 목록을 반환한다."""
    open_count = _count_open_positions()
    available  = MAX_OPEN_POSITIONS - open_count
    if available <= 0:
        return []

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE price_alerts
                SET status = 'PENDING'
                WHERE id IN (
                    SELECT id FROM price_alerts
                    WHERE status = 'PENDING_SLOT'
                    ORDER BY created_at ASC
                    LIMIT %s
                )
                RETURNING id, coin_symbol, alert_type
            """, (available,))
            promoted = [
                {"id": r[0], "coin_symbol": r[1], "alert_type": r[2]}
                for r in cur.fetchall()
            ]
        conn.commit()
        return promoted
    finally:
        conn.close()


# ── Redis ─────────────────────────────────────────────────────

def _redis_client():
    import redis as redis_lib
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def _is_already_sent(alert_id: int) -> bool:
    try:
        return _redis_client().exists(f"alert_sent:{alert_id}") > 0
    except Exception:
        return False


def _mark_as_sent(alert_id: int) -> None:
    try:
        _redis_client().setex(f"alert_sent:{alert_id}", REDIS_DEDUP_TTL, "1")
    except Exception:
        pass


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
            print(f"[monitor] Telegram 발송 실패: {e}")


# ── 트리거 판정 ────────────────────────────────────────────────

def _should_trigger(current_price: float, alert: dict) -> bool:
    """현재 가격이 알림 조건을 충족하는지 판단한다."""
    target = alert["target_price"]
    signal = alert["signal_type"]
    atype  = alert["alert_type"]

    if signal == "BUY":
        if atype in ("ENTRY_1", "ENTRY_2", "ENTRY_3", "ENTRY_4",
                     "STOP_LOSS", "ABSOLUTE_STOP"):
            return current_price <= target   # 가격이 목표 이하로 내려옴
        if atype in ("TAKE_PROFIT", "TAKE_PROFIT_2"):
            return current_price >= target   # 가격이 목표 이상으로 올라감

    elif signal == "SELL":
        if atype in ("ENTRY_1", "ENTRY_2", "ENTRY_3", "ENTRY_4",
                     "SHORT_ENTRY", "STOP_LOSS"):
            return current_price >= target   # 숏: 가격이 목표 이상으로 올라감
        if atype in ("TAKE_PROFIT", "TAKE_PROFIT_2"):
            return current_price <= target   # 숏 익절: 가격이 목표 이하로 내려옴
        if atype == "ABSOLUTE_STOP":
            return current_price <= target

    return False


# ── 모니터 ────────────────────────────────────────────────────

class PriceMonitor:
    def __init__(self) -> None:
        self._alerts: list[dict]       = []
        self._prices: dict[str, float] = {}
        self._subscribed: set[str]     = set()
        self._ws                       = None
        self._last_reload: float       = 0.0
        self._trigger_queue: asyncio.Queue | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # ── WebSocket 콜백 (pybit 내부 스레드에서 호출됨) ─────────

    def _handle_ticker(self, msg: dict) -> None:
        data      = msg.get("data", {})
        symbol    = data.get("symbol", "")
        price_str = data.get("lastPrice")
        if not symbol or not price_str:
            return

        coin = symbol.replace("USDT", "")
        try:
            price = float(price_str)
        except ValueError:
            return

        self._prices[coin] = price

        for alert in self._alerts[:]:   # 스냅샷 순회 (mutation 방지)
            if alert["coin_symbol"] != coin:
                continue
            if _is_already_sent(alert["id"]):
                continue
            if _should_trigger(price, alert):
                _mark_as_sent(alert["id"])
                # call_soon_threadsafe: 비동기 큐에 스레드 안전하게 삽입
                self._loop.call_soon_threadsafe(
                    self._trigger_queue.put_nowait, (alert, price)
                )

    # ── 트리거 처리 루프 ──────────────────────────────────────

    async def _process_triggers(self) -> None:
        while True:
            alert, price = await self._trigger_queue.get()
            try:
                await self._fire_alert(alert, price)
            finally:
                self._trigger_queue.task_done()

    async def _fire_alert(self, alert: dict, price: float) -> None:
        coin   = alert["coin_symbol"]
        atype  = alert["alert_type"]
        target = alert["target_price"]
        signal = alert["signal_type"]

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _trigger_alert_db, alert["id"])

        self._alerts = [a for a in self._alerts if a["id"] != alert["id"]]

        label = ALERT_TYPE_LABEL.get(atype, atype)
        emoji = "🟢" if signal == "BUY" else "🔴"
        text = (
            f"{emoji} *가격 알림 — {label}*\n\n"
            f"코인: *{coin}*\n"
            f"현재가: `${price:,.2f}`\n"
            f"목표가: `${target:,.2f}`\n\n"
            f"분석 ID: \\#{alert['analysis_id']}"
        )
        await _send_telegram(text)
        print(f"[monitor] 알림 발송: {coin} {atype} @ {price}")

    # ── 알림 재로드 ───────────────────────────────────────────

    def _reload(self) -> None:
        """PENDING_SLOT 전환 + PENDING 알림 목록 갱신 + 새 코인 WebSocket 구독."""
        promoted = _promote_pending_slots()
        for p in promoted:
            print(f"[monitor] PENDING_SLOT → PENDING: {p['coin_symbol']} {p['alert_type']}")

        self._alerts = _load_pending_alerts()

        new_coins = {a["coin_symbol"] for a in self._alerts} - self._subscribed
        for coin in new_coins:
            symbol = f"{coin}USDT"
            try:
                self._ws.ticker_stream(symbol=symbol, callback=self._handle_ticker)
                self._subscribed.add(coin)
                print(f"[monitor] WebSocket 구독: {symbol}")
            except Exception as e:
                print(f"[monitor] 구독 실패 {symbol}: {e}")

        print(f"[monitor] 알림 로드: {len(self._alerts)}개")
        self._last_reload = time.time()

    # ── 메인 루프 ─────────────────────────────────────────────

    async def run(self) -> None:
        from pybit.unified_trading import WebSocket

        self._loop          = asyncio.get_event_loop()
        self._trigger_queue = asyncio.Queue(maxsize=200)
        self._ws            = WebSocket(testnet=BYBIT_TESTNET, channel_type="linear")

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._reload)

        asyncio.create_task(self._process_triggers())

        print("[monitor] 시작 — 가격 모니터링 중")
        while True:
            await asyncio.sleep(1)
            if time.time() - self._last_reload >= RELOAD_INTERVAL:
                await loop.run_in_executor(None, self._reload)


if __name__ == "__main__":
    asyncio.run(PriceMonitor().run())
