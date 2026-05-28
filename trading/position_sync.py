"""
Position Sync — 분석 결과를 Bybit 지정가 주문으로 연결.

모드별 동작:
  AUTO        : Bybit 지정가 주문 즉시 등록 (체결 대기)
  SEMI_AUTO   : 텔레그램 확인 버튼 → 승인 시 주문
  MANUAL      : 텔레그램 진입가 안내만 (주문 없음)
  NOTIFY_ONLY : 위와 동일

analyzer에서 BUY/SELL 저장 후 호출되며,
텔레그램 봇의 /positions 명령으로도 수동 실행 가능.
"""
from __future__ import annotations

import asyncio
import json
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL       = os.environ.get("DATABASE_URL", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")


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


def _get_trading_mode() -> str:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT mode FROM settings WHERE id = 1")
            return cur.fetchone()[0]
    finally:
        conn.close()


def _get_user_leverage(coin_symbol: str) -> int:
    """user_profiles의 leverage_config에서 코인별 레버리지 반환. 없으면 1."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT leverage_config FROM user_profiles LIMIT 1"
            )
            row = cur.fetchone()
            if row and row[0]:
                cfg = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                return int(cfg.get(coin_symbol, cfg.get("default", 1)))
            return 1
    finally:
        conn.close()


def _get_usdt_balance_for_coin(coin_symbol: str) -> float:
    """사용자 총 자산에서 해당 코인 배분 비율 계산 (trade_executor와 동일 기준)."""
    COIN_ALLOCATION = {"BTC": 0.50, "ETH": 0.50}
    ratio = COIN_ALLOCATION.get(coin_symbol, 0.0)

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT total_asset_krw FROM user_profiles LIMIT 1")
            row = cur.fetchone()
            # total_asset_krw 필드에 USDT 값 저장 중 (온보딩 USDT 입력)
            total_usdt = float(row[0]) if row and row[0] else 0.0
    finally:
        conn.close()

    return total_usdt * ratio


def _trade_exists(analysis_id: int) -> bool:
    """해당 analysis_id로 이미 trades 레코드가 있으면 중복 방지."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM trades WHERE analysis_id = %s LIMIT 1",
                (analysis_id,)
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _save_trade(
    analysis_id: int,
    coin_symbol: str,
    side: str,
    qty: float,
    price: float,
    stop_loss: float | None,
    mode: str,
    bybit_order_id: str | None = None,
) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO trades
                  (analysis_id, symbol, side, qty, price, status,
                   bybit_order_id, stop_loss_price, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    analysis_id,
                    f"{coin_symbol}USDT",
                    side,
                    qty,
                    price,
                    "FILLED" if bybit_order_id else "PENDING",
                    bybit_order_id,
                    stop_loss,
                    mode,
                ),
            )
        conn.commit()
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
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception as e:
            print(f"[sync] Telegram 발송 실패: {e}")


# ── 핵심 로직 ─────────────────────────────────────────────────

async def sync_analysis(analysis_id: int) -> None:
    """
    단일 분석 결과를 포지션으로 연결.
    analyzer에서 새 BUY/SELL 저장 직후 호출.
    """
    if _trade_exists(analysis_id):
        return

    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.signal_type, a.coin_symbol, a.timeframe,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3, a.entry_price_4,
                       a.stop_loss_price, a.take_profit_price, a.absolute_stop,
                       a.is_reference_only, a.summary
                FROM analyses a
                WHERE a.id = %s AND a.is_active = TRUE
                  AND a.signal_type IN ('BUY','SELL')
                  AND a.entry_price_1 IS NOT NULL
            """, (analysis_id,))
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return

    (signal, coin, tf, e1, e2, e3, e4,
     sl, tp, abs_stop, is_ref, summary) = row

    if is_ref:  # MONTHLY/WEEKLY 참고용은 주문 안 함
        return

    mode = _get_trading_mode()
    leverage = _get_user_leverage(coin)
    usdt_alloc = _get_usdt_balance_for_coin(coin)

    entry_prices = [float(p) for p in [e1, e2, e3, e4] if p]
    labels = ["안정형", "중립형", "공격형", "초공격형"]

    side_str = "Buy" if signal == "BUY" else "Sell"
    symbol = f"{coin}USDT"

    def _fmt(v):
        if v is None: return "-"
        return f"{v:,.0f}" if float(v) >= 1000 else f"{float(v):,.4f}"

    if mode == "AUTO":
        # Bybit 지정가 주문 등록
        from trading.bybit_client import BybitClient
        client = BybitClient()
        placed = []
        # 분할 비율: e1 40%, e2 35%, e3 25%
        alloc_ratios = [0.40, 0.35, 0.25, 0.0]
        for i, price in enumerate(entry_prices):
            ratio = alloc_ratios[i] if i < len(alloc_ratios) else 0.0
            if ratio == 0:
                continue
            usdt_for_entry = usdt_alloc * ratio
            qty = round((usdt_for_entry * leverage) / price, 4)
            if qty <= 0:
                continue
            try:
                result = client.place_order(
                    symbol=symbol,
                    side=side_str,
                    qty=qty,
                    price=price,
                    leverage=leverage,
                    position_idx=0 if signal == "BUY" else 1,
                    stop_loss=float(sl) if sl else None,
                    take_profit=float(tp) if tp else None,
                )
                order_id = result.get("orderId")
                _save_trade(analysis_id, coin, side_str, qty, price,
                            float(sl) if sl else None, "AUTO", order_id)
                placed.append(f"{labels[i]}: {_fmt(price)} ({qty} {coin})")
                print(f"[sync] AUTO 주문 등록: {symbol} {price} x{qty}")
            except Exception as e:
                print(f"[sync] Bybit 주문 실패: {e}")

        if placed:
            tf_str = {"DAILY": "일봉", "HOURLY": "시간봉", "WEEKLY": "주봉"}.get(tf or "", tf or "")
            msg = (
                f"✅ *자동 주문 등록 완료* — {signal} {coin} ({tf_str})\n\n"
                + "\n".join(f"  {p}" for p in placed)
                + f"\n\n🛡 손절: {_fmt(sl)}  ⛔ 마지노선: {_fmt(abs_stop)}"
                + f"\n\n_{summary[:120] if summary else ''}_"
            )
            await _send_telegram(msg)

    else:
        # SEMI_AUTO / MANUAL / NOTIFY_ONLY — Telegram 안내만
        tf_str = {"DAILY": "일봉", "HOURLY": "시간봉", "WEEKLY": "주봉"}.get(tf or "", tf or "")
        emoji = "🟢" if signal == "BUY" else "🔴"
        lines = [f"{emoji} *지정가 주문 안내 — {signal} {coin}* ({tf_str})\n"]
        lines.append("*진입가 (직접 Bybit에 지정가 주문 등록)*")
        for i, price in enumerate(entry_prices):
            lines.append(f"  {labels[i]}: `{_fmt(price)}`")
        lines.append(f"\n🛡 손절: `{_fmt(sl)}`")
        if tp:
            lines.append(f"🏆 목표: `{_fmt(tp)}`")
        if abs_stop:
            lines.append(f"⛔ 마지노선: `{_fmt(abs_stop)}`")
        lines.append(f"\n레버리지: {leverage}x  |  배분: ${usdt_alloc:,.0f} USDT")
        if summary:
            lines.append(f"\n_{summary[:120]}_")
        lines.append(f"\n_모드: {mode} — 자동 주문 미실행_")
        await _send_telegram("\n".join(lines))
        _save_trade(analysis_id, coin, side_str, 0, float(e1),
                    float(sl) if sl else None, mode)


async def sync_all_active() -> None:
    """
    현재 활성화된 모든 BUY/SELL 분석을 일괄 처리.
    텔레그램 /positions 명령 또는 수동 실행용.
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM analyses
                WHERE is_active = TRUE
                  AND signal_type IN ('BUY','SELL')
                  AND entry_price_1 IS NOT NULL
                  AND is_reference_only = FALSE
                ORDER BY created_at DESC
            """)
            ids = [row[0] for row in cur.fetchall()]
    finally:
        conn.close()

    print(f"[sync] 활성 시나리오 {len(ids)}개 처리 시작")
    for analysis_id in ids:
        await sync_analysis(analysis_id)
    print(f"[sync] 완료")


if __name__ == "__main__":
    asyncio.run(sync_all_active())
