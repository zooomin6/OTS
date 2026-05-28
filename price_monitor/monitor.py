"""
Price Monitor — Bybit WebSocket 실시간 가격 모니터링 + price_alerts 트리거.

동작 흐름:
  1. DB에서 사용자 성향(risk_tolerance)에 맞는 PENDING price_alerts 로드
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

# risk_tolerance → 진입 alert_type 매핑
RISK_TO_ENTRY = {
    "CONSERVATIVE": "ENTRY_1",
    "MODERATE":     "ENTRY_2",
    "AGGRESSIVE":   "ENTRY_3",
}

ENTRY_LABEL = {
    "ENTRY_1": "안정형 진입",
    "ENTRY_2": "중립형 진입",
    "ENTRY_3": "공격형 진입",
    "ENTRY_4": "초공격형 진입",
}

ALERT_TYPE_LABEL = {
    "ENTRY_1":       "안정형 진입",
    "ENTRY_2":       "중립형 진입",
    "ENTRY_3":       "공격형 진입",
    "ENTRY_4":       "초공격형 진입",
    "ABSOLUTE_STOP": "마지노선 도달",
    "STOP_LOSS":     "손절가 도달",
    "TAKE_PROFIT":   "1차 목표 도달",
    "TAKE_PROFIT_2": "2차 목표 도달",
    "SHORT_ENTRY":   "숏 진입",
}

TIMEFRAME_LABEL = {
    "MONTHLY": "월봉",
    "WEEKLY":  "주봉",
    "DAILY":   "일봉",
    "HOURLY":  "시간봉",
}

# 왕복 수수료 기준 최소 수익률 (maker+taker 약 0.11%, 여유 포함)
MIN_PROFIT_PCT = 0.5


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


def _get_user_entry_type() -> str:
    """사용자 risk_tolerance에 맞는 ENTRY 타입을 반환한다. 기본값 ENTRY_2(중립형)."""
    if not TELEGRAM_CHAT_ID:
        return "ENTRY_2"
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT risk_tolerance FROM user_profiles WHERE telegram_user_id = %s",
                (int(TELEGRAM_CHAT_ID),)
            )
            row = cur.fetchone()
            if row:
                return RISK_TO_ENTRY.get(row[0], "ENTRY_2")
    except Exception as e:
        print(f"[monitor] 사용자 성향 조회 실패: {e}")
    finally:
        conn.close()
    return "ENTRY_2"


def _load_pending_alerts() -> list[dict]:
    """사용자 성향에 맞는 ENTRY 타입 + 손절/목표 PENDING 알림을 로드한다."""
    entry_type = _get_user_entry_type()
    print(f"[monitor] 진입 성향: {ENTRY_LABEL.get(entry_type, entry_type)}")
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
                  AND (
                      pa.alert_type NOT LIKE 'ENTRY_%%'
                      OR pa.alert_type = %s
                  )
            """, (entry_type,))
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


def _fetch_analysis_context(analysis_id: int) -> dict:
    """분석 ID로 진입가·손절·목표·근거 등 전체 컨텍스트를 조회한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_type, coin_symbol, timeframe,
                       entry_price_1, entry_price_2, entry_price_3,
                       stop_loss_price, absolute_stop,
                       take_profit_price, take_profit_price,
                       summary, invalidation,
                       youtuber_zone_low, youtuber_zone_high,
                       risk_reward_ratio
                FROM analyses WHERE id = %s
            """, (analysis_id,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "signal_type":       row[0],
                "coin_symbol":       row[1],
                "timeframe":         row[2],
                "entry_price_1":     float(row[3]) if row[3] else None,
                "entry_price_2":     float(row[4]) if row[4] else None,
                "entry_price_3":     float(row[5]) if row[5] else None,
                "stop_loss_price":   float(row[6]) if row[6] else None,
                "absolute_stop":     float(row[7]) if row[7] else None,
                "take_profit_price": float(row[8]) if row[8] else None,
                "summary":           row[10],
                "invalidation":      row[11],
                "zone_low":          float(row[12]) if row[12] else None,
                "zone_high":         float(row[13]) if row[13] else None,
                "rr_ratio":          float(row[14]) if row[14] else None,
            }
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
            cur.execute("SELECT COUNT(*) FROM trades WHERE status = 'OPEN'")
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


# ── 알림 메시지 구성 ───────────────────────────────────────────

def _fmt_price(price: float | None, coin: str = "") -> str:
    if price is None:
        return "—"
    if coin in ("USDT.D", "BTC.D", "ETH.D"):
        return f"{price:.2f}%"
    return f"${price:,.2f}"


def _pct_diff(a: float, b: float) -> str:
    if not a or not b:
        return ""
    diff = (b - a) / a * 100
    sign = "+" if diff >= 0 else ""
    return f" ({sign}{diff:.1f}%)"


def _build_entry_message(alert: dict, current_price: float, ctx: dict) -> str:
    coin   = alert["coin_symbol"]
    atype  = alert["alert_type"]
    signal = alert["signal_type"]
    label  = ALERT_TYPE_LABEL.get(atype, atype)
    tf     = TIMEFRAME_LABEL.get(ctx.get("timeframe", ""), ctx.get("timeframe", ""))
    emoji  = "🟢" if signal == "BUY" else "🔴"

    lines = [
        f"{emoji} *{label} — {coin}*",
        "",
        f"현재가:  `{_fmt_price(current_price, coin)}`",
        f"진입가:  `{_fmt_price(alert['target_price'], coin)}`",
    ]

    if ctx.get("zone_low") and ctx.get("zone_high"):
        lines.append(
            f"유튜버 구간: `{_fmt_price(ctx['zone_low'], coin)} ~ {_fmt_price(ctx['zone_high'], coin)}`"
        )

    lines.append("")
    lines.append("━━━ 리스크 관리 ━━━")

    entry = alert["target_price"]
    if ctx.get("stop_loss_price"):
        lines.append(
            f"손절:    `{_fmt_price(ctx['stop_loss_price'], coin)}`"
            + _pct_diff(entry, ctx["stop_loss_price"])
        )
    if ctx.get("absolute_stop"):
        lines.append(
            f"마지노선: `{_fmt_price(ctx['absolute_stop'], coin)}`"
            + _pct_diff(entry, ctx["absolute_stop"])
        )
    if ctx.get("take_profit_price"):
        lines.append(
            f"1차 목표: `{_fmt_price(ctx['take_profit_price'], coin)}`"
            + _pct_diff(entry, ctx["take_profit_price"])
        )
    if ctx.get("rr_ratio"):
        lines.append(f"손익비:  `{ctx['rr_ratio']:.1f}R`")

    # 수수료 경고: 진입가 → 목표가 수익률이 너무 낮으면 표시
    tp = ctx.get("take_profit_price")
    if tp and entry:
        profit_pct = abs(tp - entry) / entry * 100
        lines.append(f"목표까지: `+{profit_pct:.2f}%`")
        if profit_pct < MIN_PROFIT_PCT:
            lines.append(f"⚠️ 목표가까지 수익률 {profit_pct:.2f}% — 수수료({MIN_PROFIT_PCT}%) 미달, 진입 재검토")

    if tf:
        lines.append(f"기준봉:  `{tf}`")

    if ctx.get("summary"):
        lines.append("")
        lines.append("━━━ 분석 근거 ━━━")
        lines.append(ctx["summary"])

    if ctx.get("invalidation"):
        lines.append("")
        lines.append("━━━ 무효화 조건 ━━━")
        lines.append(ctx["invalidation"])

    return "\n".join(lines)


def _build_exit_message(alert: dict, current_price: float, ctx: dict) -> str:
    coin  = alert["coin_symbol"]
    atype = alert["alert_type"]
    label = ALERT_TYPE_LABEL.get(atype, atype)
    tf    = TIMEFRAME_LABEL.get(ctx.get("timeframe", ""), ctx.get("timeframe", ""))

    if atype in ("TAKE_PROFIT", "TAKE_PROFIT_2"):
        emoji = "🎯"
    elif atype in ("STOP_LOSS", "ABSOLUTE_STOP"):
        emoji = "🛑"
    else:
        emoji = "⚠️"

    lines = [
        f"{emoji} *{label} — {coin}*",
        "",
        f"현재가:  `{_fmt_price(current_price, coin)}`",
        f"목표가:  `{_fmt_price(alert['target_price'], coin)}`",
    ]

    if tf:
        lines.append(f"기준봉:  `{tf}`")

    if ctx.get("summary"):
        lines.append("")
        lines.append(ctx["summary"])

    return "\n".join(lines)


# ── 트리거 판정 ────────────────────────────────────────────────

def _should_trigger(current_price: float, alert: dict) -> bool:
    target = alert["target_price"]
    signal = alert["signal_type"]
    atype  = alert["alert_type"]

    if signal == "BUY":
        if atype in ("ENTRY_1", "ENTRY_2", "ENTRY_3", "ENTRY_4",
                     "STOP_LOSS", "ABSOLUTE_STOP"):
            return current_price <= target
        if atype in ("TAKE_PROFIT", "TAKE_PROFIT_2"):
            return current_price >= target

    elif signal == "SELL":
        if atype in ("ENTRY_1", "ENTRY_2", "ENTRY_3", "ENTRY_4",
                     "SHORT_ENTRY", "STOP_LOSS"):
            return current_price >= target
        if atype in ("TAKE_PROFIT", "TAKE_PROFIT_2"):
            return current_price <= target
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

        for alert in self._alerts[:]:
            if alert["coin_symbol"] != coin:
                continue
            if _is_already_sent(alert["id"]):
                continue
            if _should_trigger(price, alert):
                _mark_as_sent(alert["id"])
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
        loop = asyncio.get_event_loop()

        # DB 업데이트 + 분석 컨텍스트 조회
        await loop.run_in_executor(None, _trigger_alert_db, alert["id"])
        ctx = await loop.run_in_executor(None, _fetch_analysis_context, alert["analysis_id"])

        self._alerts = [a for a in self._alerts if a["id"] != alert["id"]]

        atype = alert["alert_type"]
        if atype.startswith("ENTRY_"):
            text = _build_entry_message(alert, price, ctx)
        else:
            text = _build_exit_message(alert, price, ctx)

        await _send_telegram(text)
        print(f"[monitor] 알림 발송: {alert['coin_symbol']} {atype} @ {price}")

    # ── 알림 재로드 ───────────────────────────────────────────

    def _reload(self) -> None:
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
