"""
전체 기간 멀티 신호 시뮬레이션 러너.

일봉 캔들을 날짜순으로 순회하며:
  - 유튜버 신호 / 피보나치+조건 신호 체크
  - 오픈 포지션 관리 (+20% 부분익절, +40% 전량익절, SL 손절)
  - 최종 수익률 / 승률 / 최대낙폭 리포트 출력

사용법:
    python -m backtest.full_runner --coin BTC
    python -m backtest.full_runner --coin ETH --from 2024-01-01 --to 2025-01-01
    python -m backtest.full_runner --coin BTC --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

load_dotenv()

from backtest.fib_calculator import fetch_fear_greed_history
from backtest.price_fetcher import fetch_bybit_kline, fetch_usdt_dominance
from backtest.signal_engine import TradeSignal, generate_signal, get_active_youtuber_analysis

VIRTUAL_CAPITAL = 10_000.0   # 가상 시작 자본 (USDT)
TP1_PCT  = 20.0              # 1차 익절: 포지션 수익률 +20%
TP2_PCT  = 40.0              # 2차 익절: 포지션 수익률 +40%
SL_FLOOR = 0.93              # SL 없을 때 진입가 × 0.93 (-7%)


@dataclass
class OpenPosition:
    coin: str
    entry_price: float
    stop_loss: float
    qty: float              # 보유 수량 (USDT / entry_price)
    cost: float             # 투입 비용 (USDT)
    tp1_done: bool = False
    signal: TradeSignal | None = None
    entered_at: int = 0     # 진입 캔들 time_ms


@dataclass
class ClosedTrade:
    coin: str
    source: str
    fib_level: str | None
    conditions: list[str]
    entry_price: float
    exit_price: float
    pnl_pct: float
    result: str              # 'TP1' | 'TP2' | 'SL' | 'END'
    entered_at: int
    exited_at: int


def _db_connect():
    url = (
        os.environ["DATABASE_URL"]
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("postgresql+psycopg://", "postgresql://")
    )
    p = urlparse(url)
    return psycopg2.connect(
        host=p.hostname, port=p.port or 5432,
        user=p.username, password=p.password,
        dbname=p.path.lstrip("/"),
        options="-c client_encoding=UTF8",
    )


def _load_analyses(coin: str) -> list[dict]:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, signal_type, coin_symbol, entry_price_1, entry_price_2,
                       stop_loss_price, take_profit_price, created_at, expires_at
                FROM analyses
                WHERE coin_symbol = %s
                  AND signal_type = 'BUY'
                  AND is_reference_only = FALSE
                  AND expires_at IS NOT NULL
                ORDER BY created_at ASC
            """, (coin,))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _run_position(pos: OpenPosition, candle: dict) -> tuple[OpenPosition | None, list[ClosedTrade]]:
    """오픈 포지션을 현재 캔들로 업데이트. (partial exit 처리 포함)"""
    trades: list[ClosedTrade] = []
    c_high = candle["high"]
    c_low  = candle["low"]
    c_time = candle["time"]

    def _pnl(exit_px: float) -> float:
        return (exit_px - pos.entry_price) / pos.entry_price * 100

    # 손절 체크 (먼저)
    if c_low <= pos.stop_loss:
        trades.append(ClosedTrade(
            coin=pos.coin, source=pos.signal.source if pos.signal else "?",
            fib_level=pos.signal.fib_level if pos.signal else None,
            conditions=pos.signal.conditions if pos.signal else [],
            entry_price=pos.entry_price, exit_price=pos.stop_loss,
            pnl_pct=_pnl(pos.stop_loss), result="SL",
            entered_at=pos.entered_at, exited_at=c_time,
        ))
        return None, trades

    # 2차 익절 (+40%)
    tp2_price = pos.entry_price * (1 + TP2_PCT / 100)
    if pos.tp1_done and c_high >= tp2_price:
        trades.append(ClosedTrade(
            coin=pos.coin, source=pos.signal.source if pos.signal else "?",
            fib_level=pos.signal.fib_level if pos.signal else None,
            conditions=pos.signal.conditions if pos.signal else [],
            entry_price=pos.entry_price, exit_price=tp2_price,
            pnl_pct=_pnl(tp2_price), result="TP2",
            entered_at=pos.entered_at, exited_at=c_time,
        ))
        return None, trades

    # 1차 익절 (+20%)
    tp1_price = pos.entry_price * (1 + TP1_PCT / 100)
    if not pos.tp1_done and c_high >= tp1_price:
        trades.append(ClosedTrade(
            coin=pos.coin, source=pos.signal.source if pos.signal else "?",
            fib_level=pos.signal.fib_level if pos.signal else None,
            conditions=pos.signal.conditions if pos.signal else [],
            entry_price=pos.entry_price, exit_price=tp1_price,
            pnl_pct=_pnl(tp1_price), result="TP1",
            entered_at=pos.entered_at, exited_at=c_time,
        ))
        pos.tp1_done = True
        # 1차 익절 후 SL을 진입가로 이동 (본전 보호)
        pos.stop_loss = pos.entry_price
        return pos, trades

    return pos, trades


def _build_eth_btc_ratio(eth_candles: list[dict], btc_candles: list[dict]) -> dict[int, float]:
    """ETH/BTC 일별 비율 계산 {time_ms: ratio}."""
    btc_map = {c["time"]: c["close"] for c in btc_candles}
    result: dict[int, float] = {}
    for c in eth_candles:
        btc_close = btc_map.get(c["time"])
        if btc_close and btc_close > 0:
            result[c["time"]] = c["close"] / btc_close
    return result


def _is_eth_btc_strong(ratio_map: dict[int, float], current_ms: int, period: int = 30) -> bool:
    """ETH/BTC 비율이 30일 이동평균 이상이면 True (ETH 상대 강세)."""
    past = sorted((t, v) for t, v in ratio_map.items() if t <= current_ms)
    if len(past) < period:
        return True  # 데이터 부족 시 허용
    sma = sum(v for _, v in past[-period:]) / period
    current_ratio = past[-1][1]
    return current_ratio >= sma * 0.97


def run(
    coin: str,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    dry_run: bool = False,
) -> None:
    # 기간 설정
    to_date   = to_date   or datetime.now()
    from_date = from_date or (to_date - timedelta(days=365))

    start_ms = int(from_date.timestamp() * 1000)
    end_ms   = int(to_date.timestamp() * 1000)

    print(f"[full_runner] {coin} 시뮬레이션 {from_date.date()} ~ {to_date.date()}")

    # 가격 데이터 fetch
    print(f"[full_runner] 가격 데이터 로딩...")
    candles = fetch_bybit_kline(coin, "1d", start_ms - 60 * 86_400_000, end_ms)

    # ETH/BTC 상대 강도 (ETH 전용)
    eth_btc_ratio: dict[int, float] = {}
    if coin == "ETH":
        try:
            btc_candles = fetch_bybit_kline("BTC", "1d", start_ms - 60 * 86_400_000, end_ms)
            eth_btc_ratio = _build_eth_btc_ratio(candles, btc_candles)
            print(f"[full_runner] ETH/BTC 비율: {len(eth_btc_ratio)}일치 로딩")
        except Exception as e:
            print(f"[full_runner] ETH/BTC 데이터 로딩 실패 (무시): {e}")

    usdt_d = []
    if coin != "USDT.D":
        try:
            usdt_d = fetch_usdt_dominance(start_ms - 7 * 86_400_000, end_ms)
        except Exception as e:
            print(f"[full_runner] USDT.D 데이터 로딩 실패 (무시): {e}")

    # 공포탐욕지수 (최대 900일치)
    fng_days = min(900, (end_ms - start_ms) // 86_400_000 + 10)
    fng_series = fetch_fear_greed_history(days=fng_days)
    if fng_series:
        print(f"[full_runner] 공포탐욕지수: {len(fng_series)}일치 로딩")
    else:
        print(f"[full_runner] 공포탐욕지수 로딩 실패 (무시)")

    if not candles:
        print("[full_runner] 가격 데이터 없음.")
        return

    # 유튜버 분석 로딩
    analyses = _load_analyses(coin)
    print(f"[full_runner] 유튜버 분석: {len(analyses)}개")

    # 시뮬레이션
    position: OpenPosition | None = None
    all_trades: list[ClosedTrade] = []
    capital = VIRTUAL_CAPITAL
    used_analysis_ids: set[int] = set()
    cooldown_until: int = 0
    consecutive_sls: int = 0   # 연속 손절 횟수 (동적 쿨다운용)

    sim_candles = [c for c in candles if start_ms <= c["time"] <= end_ms]
    print(f"[full_runner] 시뮬레이션 캔들: {len(sim_candles)}개\n")

    for i, candle in enumerate(sim_candles):
        prev = candles[:candles.index(candle)]
        c_time = candle["time"]
        c_dt   = datetime.fromtimestamp(c_time / 1000).date()

        # USDT.D 시계열 (현재 캔들 이전)
        usdt_d_prev = [u for u in usdt_d if u["time"] <= c_time]

        # 오픈 포지션 관리
        if position:
            position, closed = _run_position(position, candle)
            for t in closed:
                pnl_sign = "✅" if t.result in ("TP1", "TP2") else "❌"
                print(f"  {pnl_sign} {c_dt} {t.result} @ {t.exit_price:,.1f}  "
                      f"P&L {t.pnl_pct:+.1f}%  [{t.source}]")
            all_trades.extend(closed)

            if not position and closed:
                last = closed[-1]
                capital *= (1 + last.pnl_pct / 100)
                if last.result == "SL":
                    consecutive_sls += 1
                    # 연속 손절 횟수에 따라 쿨다운 증가: 1번=3일, 2번=7일, 3번+=14일
                    cooldown_days = {1: 3, 2: 7}.get(consecutive_sls, 14)
                    cooldown_until = c_time + cooldown_days * 86_400_000
                else:
                    consecutive_sls = 0  # 수익 후 리셋
            continue

        # 쿨다운 중이면 신호 탐색 건너뜀
        if c_time < cooldown_until:
            continue

        # 신호 탐색 (포지션 없을 때)
        if dry_run and len(all_trades) > 20:
            break

        active_youtuber = get_active_youtuber_analysis(coin, c_time, analyses)

        # 이미 사용한 유튜버 분석이면 건너뜀
        if active_youtuber and active_youtuber.get("id") in used_analysis_ids:
            active_youtuber = None

        usdt_d_for_signal = usdt_d_prev[-10:] if usdt_d_prev else []

        # ETH/BTC 상대 강도 체크 (ETH 전용 FIB 필터)
        pair_strong = True
        if coin == "ETH" and eth_btc_ratio:
            pair_strong = _is_eth_btc_strong(eth_btc_ratio, c_time)

        signal = generate_signal(
            coin=coin,
            candle=candle,
            prev_candles=prev,
            usdt_d_series=usdt_d_for_signal,
            active_youtuber=active_youtuber,
            fng_series=fng_series,
            coin_pair_strong=pair_strong,
        )

        if signal:
            sl = max(signal.stop_loss, signal.entry_price * SL_FLOOR)
            qty  = capital / signal.entry_price
            position = OpenPosition(
                coin=coin,
                entry_price=signal.entry_price,
                stop_loss=sl,
                qty=qty,
                cost=capital,
                signal=signal,
                entered_at=c_time,
            )
            # 사용한 유튜버 분석 ID 기록 (재진입 방지)
            if signal.analysis_id:
                used_analysis_ids.add(signal.analysis_id)

            cond_str = " + ".join(signal.conditions) or "-"
            fib_str  = f" Fib{signal.fib_level}" if signal.fib_level else ""
            print(f"  🟢 {c_dt} 진입 @ {signal.entry_price:,.1f}  "
                  f"SL:{sl:,.1f}  [{signal.source}{fib_str}]  조건:{cond_str}")

    # 잔여 포지션 강제 청산 (시뮬레이션 종료)
    if position and sim_candles:
        last_candle = sim_candles[-1]
        exit_px = last_candle["close"]
        pnl = (exit_px - position.entry_price) / position.entry_price * 100
        t = ClosedTrade(
            coin=coin, source=position.signal.source if position.signal else "?",
            fib_level=position.signal.fib_level if position.signal else None,
            conditions=position.signal.conditions if position.signal else [],
            entry_price=position.entry_price, exit_price=exit_px,
            pnl_pct=pnl, result="END",
            entered_at=position.entered_at, exited_at=last_candle["time"],
        )
        all_trades.append(t)
        capital *= (1 + pnl / 100)
        print(f"  ⏹  종료 강제청산 @ {exit_px:,.1f}  P&L {pnl:+.1f}%")

    # ── 최종 리포트 ──
    if not all_trades:
        print("\n[full_runner] 진입 신호 없음.")
        return

    wins   = [t for t in all_trades if t.pnl_pct > 0]
    losses = [t for t in all_trades if t.pnl_pct <= 0]
    total_return = (capital - VIRTUAL_CAPITAL) / VIRTUAL_CAPITAL * 100

    # 최대낙폭 (MDD)
    peak = VIRTUAL_CAPITAL
    mdd  = 0.0
    running = VIRTUAL_CAPITAL
    for t in all_trades:
        running *= (1 + t.pnl_pct / 100)
        peak = max(peak, running)
        dd = (peak - running) / peak * 100
        mdd = max(mdd, dd)

    source_stats: dict[str, list[float]] = {}
    for t in all_trades:
        source_stats.setdefault(t.source, []).append(t.pnl_pct)

    print(f"""
╔══════════════════════════════════════════╗
  {coin} 시뮬레이션 결과 {'[DRY RUN]' if dry_run else ''}
  기간: {from_date.date()} ~ {to_date.date()}
  ─────────────────────────────────────────
  총 트레이드:  {len(all_trades)}개
  ✅ 수익:      {len(wins)}개 ({len(wins)/len(all_trades)*100:.1f}%)  평균 {sum(t.pnl_pct for t in wins)/max(len(wins),1):+.1f}%
  ❌ 손실:      {len(losses)}개 ({len(losses)/len(all_trades)*100:.1f}%)  평균 {sum(t.pnl_pct for t in losses)/max(len(losses),1):+.1f}%
  ─────────────────────────────────────────
  총 수익률:    {total_return:+.1f}%
  최대낙폭:     -{mdd:.1f}%
  ─────────────────────────────────────────
  신호별 성과:""")
    for src, pnls in sorted(source_stats.items()):
        w = sum(1 for p in pnls if p > 0)
        print(f"    {src}: {len(pnls)}건  승률 {w/len(pnls)*100:.0f}%  평균 {sum(pnls)/len(pnls):+.1f}%")
    print("╚══════════════════════════════════════════╝")


def main() -> None:
    parser = argparse.ArgumentParser(description="멀티 신호 시뮬레이션")
    parser.add_argument("--coin",     default="BTC", help="BTC | ETH | USDT.D")
    parser.add_argument("--from",     dest="from_date", default=None,
                        help="시작일 YYYY-MM-DD")
    parser.add_argument("--to",       dest="to_date",   default=None,
                        help="종료일 YYYY-MM-DD")
    parser.add_argument("--dry-run",  action="store_true")
    args = parser.parse_args()

    from_dt = datetime.strptime(args.from_date, "%Y-%m-%d") if args.from_date else None
    to_dt   = datetime.strptime(args.to_date,   "%Y-%m-%d") if args.to_date   else None

    run(coin=args.coin.upper(), from_date=from_dt, to_date=to_dt, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
