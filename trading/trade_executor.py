"""
Trade Executor — 주문 흐름 총괄.

price_alerts TRIGGERED 감지 → 리스크 체크 → 주문 실행 → positions/trades 기록 → 텔레그램 알림

모드별 동작:
  AUTO       : 리스크 체크 통과 즉시 주문
  SEMI_AUTO  : 텔레그램 버튼(확인/취소) 대기 후 실행
  MANUAL     : 알림만 발송, 주문 없음
  NOTIFY_ONLY: 알림만 발송, 주문 없음
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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

POLL_INTERVAL     = 3    # 초: TRIGGERED alert 폴링 주기
SEMI_AUTO_TIMEOUT = 300  # 초: SEMI_AUTO 버튼 대기 최대 시간 (5분)

# 코인별 자산 배분 비율 (바이빗 실제 잔고 기준)
COIN_ALLOCATION = {"BTC": 0.50, "ETH": 0.50}

# 추가매수 금액 비율 (initial_capital_usdt 기준)
ADD_BUY_RATIOS = [0.25, 0.25, 0.50]  # 1차 25%, 2차 25%, 마지막 50%


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


def _load_triggered_alerts() -> list[dict]:
    """처리되지 않은 TRIGGERED price_alerts를 로드한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT pa.id, pa.analysis_id, pa.coin_symbol,
                       pa.target_price, pa.alert_type,
                       a.signal_type, a.stop_loss_price, a.take_profit_price,
                       a.entry_price_1, a.entry_price_2, a.entry_price_3, a.entry_price_4,
                       a.absolute_stop
                FROM price_alerts pa
                JOIN analyses a ON a.id = pa.analysis_id
                WHERE pa.status = 'TRIGGERED'
                  AND pa.triggered_at >= NOW() - INTERVAL '10 minutes'
                ORDER BY pa.triggered_at ASC
            """)
            return [
                {
                    "id":              row[0],
                    "analysis_id":     row[1],
                    "coin_symbol":     row[2],
                    "target_price":    float(row[3]),
                    "alert_type":      row[4],
                    "signal_type":     row[5],
                    "stop_loss_price": float(row[6]) if row[6] else None,
                    "take_profit":     float(row[7]) if row[7] else None,
                    "entry_prices":    [float(p) if p else None for p in row[8:12]],
                    "absolute_stop":   float(row[12]) if row[12] else None,
                }
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


def _get_open_position(coin_symbol: str, signal_type: str) -> dict | None:
    """해당 코인의 같은 방향(LONG/SHORT) OPEN 포지션을 반환한다."""
    side = "LONG" if signal_type == "BUY" else "SHORT"
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, avg_entry_price, initial_capital_usdt, leverage,
                       current_qty, current_stop_loss, current_take_profit_1,
                       tp1_executed, add_buy_count, bybit_position_idx
                FROM positions
                WHERE coin_symbol = %s AND side = %s AND status = 'OPEN'
                LIMIT 1
            """, (coin_symbol, side))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id":                    row[0],
                "avg_entry_price":       float(row[1]),
                "initial_capital_usdt":  float(row[2]),
                "leverage":              row[3],
                "current_qty":           float(row[4]),
                "current_stop_loss":     float(row[5]) if row[5] else None,
                "current_take_profit_1": float(row[6]) if row[6] else None,
                "tp1_executed":          row[7],
                "add_buy_count":         row[8],
                "bybit_position_idx":    row[9],
            }
    finally:
        conn.close()


def _insert_position(
    analysis_id: int,
    coin_symbol: str,
    side: str,
    avg_entry_price: float,
    initial_capital_usdt: float,
    leverage: int,
    qty: float,
    stop_loss: float | None,
    take_profit: float | None,
    position_idx: int,
) -> int:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO positions (
                    analysis_id, coin_symbol, side,
                    avg_entry_price, initial_capital_usdt, leverage,
                    current_qty, current_stop_loss, current_take_profit_1,
                    bybit_position_idx
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                analysis_id, coin_symbol, side,
                avg_entry_price, initial_capital_usdt, leverage,
                qty, stop_loss, take_profit, position_idx,
            ))
            pos_id = cur.fetchone()[0]
        conn.commit()
        return pos_id
    finally:
        conn.close()


def _update_position_after_add_buy(
    position_id: int,
    new_avg_entry: float,
    new_qty: float,
    new_stop_loss: float | None,
    new_take_profit: float | None,
) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE positions
                SET avg_entry_price       = %s,
                    current_qty           = %s,
                    current_stop_loss     = %s,
                    current_take_profit_1 = %s,
                    add_buy_count         = add_buy_count + 1
                WHERE id = %s
            """, (new_avg_entry, new_qty, new_stop_loss, new_take_profit, position_id))
        conn.commit()
    finally:
        conn.close()


def _update_position_tp_sl_only(
    position_id: int,
    stop_loss: float | None,
    take_profit: float | None,
) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE positions
                SET current_stop_loss     = COALESCE(%s, current_stop_loss),
                    current_take_profit_1 = COALESCE(%s, current_take_profit_1)
                WHERE id = %s
            """, (stop_loss, take_profit, position_id))
        conn.commit()
    finally:
        conn.close()


def _update_tp1_executed(position_id: int, remaining_qty: float) -> None:
    """1차 익절 완료 후 수량과 tp1_executed 플래그를 갱신한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE positions
                SET tp1_executed = TRUE,
                    current_qty  = %s
                WHERE id = %s
            """, (remaining_qty, position_id))
        conn.commit()
    finally:
        conn.close()


def _insert_trade(
    analysis_id: int,
    position_id: int | None,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    stop_loss_price: float | None,
    mode: str,
    bybit_order_id: str | None = None,
    status: str = "FILLED",
) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trades (
                    analysis_id, position_id, symbol, side,
                    qty, price, status, bybit_order_id,
                    stop_loss_price, mode, executed_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                analysis_id, position_id, symbol, side,
                qty, price, status, bybit_order_id,
                stop_loss_price, mode,
            ))
        conn.commit()
    finally:
        conn.close()


def _mark_alert_processed(alert_id: int) -> None:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE price_alerts SET status = 'CANCELLED' WHERE id = %s",
                (alert_id,),
            )
        conn.commit()
    finally:
        conn.close()


def _get_user_leverage() -> int:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT leverage FROM user_profiles
                WHERE onboarding_completed = TRUE LIMIT 1
            """)
            row = cur.fetchone()
            return row[0] if row else 1
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
            print(f"[executor] Telegram 발송 실패: {e}")


# ── 주문 계산 ─────────────────────────────────────────────────

def _calc_qty(usdt_amount: float, price: float, leverage: int) -> float:
    return round((usdt_amount * leverage) / price, 3)


def _calc_entry_usdt(coin: str, balance_usdt: float) -> float:
    """신규 진입 금액 — 바이빗 실제 잔고 × 코인 배분 비율."""
    allocation = COIN_ALLOCATION.get(coin.upper(), 0.50)
    return round(balance_usdt * allocation, 2)


def _calc_add_buy_usdt(initial_capital: float, add_buy_count: int) -> float:
    """추가매수 금액 — 최초 진입 자본 × 단계별 비율."""
    ratio = ADD_BUY_RATIOS[min(add_buy_count, len(ADD_BUY_RATIOS) - 1)]
    return round(initial_capital * ratio, 2)


# ── TP1 처리 ──────────────────────────────────────────────────

async def _handle_take_profit_1(alert: dict, existing: dict, mode: str) -> None:
    """TP1 도달 시 포지션 50%를 청산한다."""
    from trading.bybit_client import BybitClient

    if existing["tp1_executed"]:
        _mark_alert_processed(alert["id"])
        return

    coin      = alert["coin_symbol"]
    signal    = alert["signal_type"]
    price     = alert["target_price"]
    half_qty  = round(existing["current_qty"] / 2, 3)
    close_side = "Sell" if signal == "BUY" else "Buy"

    try:
        client = BybitClient()
        order  = client.place_order(
            symbol=f"{coin}USDT",
            side=close_side,
            qty=half_qty,
            price=price,
            leverage=existing["leverage"],
            position_idx=existing["bybit_position_idx"],
        )
        order_id = order.get("orderId")
    except Exception as e:
        await _send_telegram(f"❌ *TP1 청산 실패 — {coin}*\n{e}")
        _mark_alert_processed(alert["id"])
        return

    remaining = round(existing["current_qty"] - half_qty, 3)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _update_tp1_executed, existing["id"], remaining)
    await loop.run_in_executor(
        None, _insert_trade,
        alert["analysis_id"], existing["id"], f"{coin}USDT", close_side.upper(),
        half_qty, price, None, mode, order_id,
    )

    _mark_alert_processed(alert["id"])
    await _send_telegram(
        f"🏆 *{coin} 1차 익절 체결*\n\n"
        f"가격: `${price:,.2f}`\n"
        f"청산 수량: `{half_qty}` (50%)\n"
        f"잔여 수량: `{remaining}`"
    )


# ── 핵심 실행 로직 ────────────────────────────────────────────

async def _execute_alert(alert: dict, mode: str) -> None:
    """알림 1건을 처리한다 — 리스크 체크 → 주문 → DB 기록 → 텔레그램."""
    from trading.bybit_client import BybitClient
    from trading.risk_manager import RiskManager

    coin        = alert["coin_symbol"]
    signal      = alert["signal_type"]
    price       = alert["target_price"]
    alert_type  = alert["alert_type"]
    analysis_id = alert["analysis_id"]

    side         = "Buy" if signal == "BUY" else "Sell"
    position_idx = 0 if signal == "BUY" else 1
    loop         = asyncio.get_event_loop()

    # ABSOLUTE_STOP — 알림만 (자동 손절 없음)
    if alert_type == "ABSOLUTE_STOP":
        await _send_telegram(
            f"🔴 *마지노선 도달 — {coin}*\n\n"
            f"현재가: `${price:,.2f}`\n"
            f"유튜버 마지노선에 도달했습니다. 직접 확인하세요."
        )
        _mark_alert_processed(alert["id"])
        return

    # NOTIFY_ONLY / MANUAL — 알림만
    if mode in ("NOTIFY_ONLY", "MANUAL"):
        await _send_telegram(
            f"{'🟢' if signal == 'BUY' else '🔴'} *{coin} 신호 — {alert_type}*\n"
            f"가격: `${price:,.2f}` | 모드: {mode}\n"
            f"_(매매 없음 — 수동으로 진입하세요)_"
        )
        _mark_alert_processed(alert["id"])
        return

    # 기존 포지션 확인
    existing = await loop.run_in_executor(None, _get_open_position, coin, signal)

    # TAKE_PROFIT — 1차 익절 처리
    if alert_type == "TAKE_PROFIT":
        if existing:
            await _handle_take_profit_1(alert, existing, mode)
        else:
            _mark_alert_processed(alert["id"])
        return

    # 기존 포지션 있고 ENTRY 타입 아님 → TP/SL만 업데이트
    if existing and not alert_type.startswith("ENTRY_"):
        await loop.run_in_executor(
            None, _update_position_tp_sl_only,
            existing["id"], alert["stop_loss_price"], alert["take_profit"],
        )
        try:
            client = BybitClient()
            client.set_tp_sl(
                symbol=f"{coin}USDT",
                position_idx=existing["bybit_position_idx"],
                take_profit=alert["take_profit"],
                stop_loss=alert["stop_loss_price"],
            )
        except Exception as e:
            print(f"[executor] TP/SL 업데이트 실패: {e}")
        _mark_alert_processed(alert["id"])
        return

    is_add_buy = existing is not None and alert_type.startswith("ENTRY_")

    # 주문 금액 계산 — 바이빗 실제 잔고 기준
    leverage = await loop.run_in_executor(None, _get_user_leverage)
    try:
        client        = BybitClient()
        balance_usdt  = client.get_balance()
    except Exception as e:
        await _send_telegram(f"❌ *잔고 조회 실패 — {coin}*\n{e}")
        return

    if is_add_buy and existing:
        usdt_amount = _calc_add_buy_usdt(existing["initial_capital_usdt"], existing["add_buy_count"])
    else:
        usdt_amount = _calc_entry_usdt(coin, balance_usdt)

    qty       = _calc_qty(usdt_amount, price, leverage)
    trade_krw = int(usdt_amount * 1350)

    # 리스크 체크
    rm     = RiskManager()
    result = rm.check(trade_krw, is_new_position=not is_add_buy, signal_type=signal)
    if not result:
        await _send_telegram(f"⚠️ *리스크 체크 실패 — {coin}*\n{result.reason}")
        _mark_alert_processed(alert["id"])
        return

    # SEMI_AUTO — 텔레그램 버튼 대기
    if mode == "SEMI_AUTO":
        confirmed = await _wait_for_confirmation(coin, signal, price, qty, leverage)
        if not confirmed:
            await _send_telegram(f"❌ *{coin} 주문 취소* (시간 초과 또는 거절)")
            _mark_alert_processed(alert["id"])
            return

    # 주문 실행
    try:
        order    = client.place_order(
            symbol=f"{coin}USDT",
            side=side,
            qty=qty,
            price=price,
            leverage=leverage,
            position_idx=position_idx,
            stop_loss=alert["stop_loss_price"],
            take_profit=alert["take_profit"],
        )
        order_id = order.get("orderId")
        print(f"[executor] 주문 완료: {coin} {side} {qty} @ {price} | orderId={order_id}")
    except Exception as e:
        await _send_telegram(f"❌ *주문 실패 — {coin}*\n{e}")
        _mark_alert_processed(alert["id"])
        return

    # DB 기록
    if is_add_buy and existing:
        prev_value = existing["avg_entry_price"] * existing["current_qty"]
        total_qty  = existing["current_qty"] + qty
        new_avg    = (prev_value + price * qty) / total_qty
        await loop.run_in_executor(
            None, _update_position_after_add_buy,
            existing["id"], new_avg, total_qty,
            alert["stop_loss_price"], alert["take_profit"],
        )
        pos_id = existing["id"]
    else:
        pos_id = await loop.run_in_executor(
            None, _insert_position,
            analysis_id, coin,
            "LONG" if signal == "BUY" else "SHORT",
            price, usdt_amount, leverage, qty,
            alert["stop_loss_price"], alert["take_profit"], position_idx,
        )

    await loop.run_in_executor(
        None, _insert_trade,
        analysis_id, pos_id, f"{coin}USDT", side.upper(),
        qty, price, alert["stop_loss_price"], mode, order_id,
    )
    _mark_alert_processed(alert["id"])

    action = "추가매수" if is_add_buy else "신규 진입"
    sl_line = f"손절: `${alert['stop_loss_price']:,.2f}`\n" if alert["stop_loss_price"] else ""
    await _send_telegram(
        f"{'🟢' if signal == 'BUY' else '🔴'} *{coin} {action} 체결*\n\n"
        f"가격: `${price:,.2f}`\n"
        f"수량: `{qty}`\n"
        f"레버리지: `{leverage}x`\n"
        f"{sl_line}"
        f"\n분석 ID: \\#{analysis_id}"
    )


async def _wait_for_confirmation(
    coin: str, signal: str, price: float, qty: float, leverage: int
) -> bool:
    """SEMI_AUTO: 텔레그램 알림 발송 후 대기. TODO: 인라인 버튼 연동."""
    await _send_telegram(
        f"⏳ *{'매수' if signal == 'BUY' else '매도'} 확인 요청 — {coin}*\n\n"
        f"가격: `${price:,.2f}` | 수량: `{qty}` | 레버리지: `{leverage}x`\n\n"
        f"_(SEMI_AUTO: 5분 내 응답 없으면 자동 취소)_"
    )
    await asyncio.sleep(SEMI_AUTO_TIMEOUT)
    return False  # TODO: 버튼 연동 전까지 항상 취소


# ── 실행 루프 ─────────────────────────────────────────────────

class TradeExecutor:

    async def run(self) -> None:
        """TRIGGERED price_alerts를 주기적으로 폴링하며 주문을 실행한다."""
        print("[executor] 시작 — TRIGGERED 알림 감시 중")
        processed: set[int] = set()

        while True:
            await asyncio.sleep(POLL_INTERVAL)
            try:
                mode   = _get_trading_mode()
                alerts = _load_triggered_alerts()
                new    = [a for a in alerts if a["id"] not in processed]
                for alert in new:
                    processed.add(alert["id"])
                    asyncio.create_task(_execute_alert(alert, mode))
            except Exception as e:
                print(f"[executor] 폴링 에러: {e}")


if __name__ == "__main__":
    asyncio.run(TradeExecutor().run())
