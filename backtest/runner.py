"""
과거 분석 일괄 백테스팅 CLI.

사용법:
    python -m backtest.runner [--coin BTC,ETH,USDT.D] [--dry-run] [--limit N]

흐름:
    1. DB에서 미평가 BUY/SELL 분석을 트레이드 스레드로 구성
    2. 각 스레드에 대해 Bybit kline 데이터 fetch
    3. virtual_trader로 시뮬레이션
    4. DB 업데이트 (feedback, virtual_pnl_pct, virtual_trade_json)
    5. 결과 리포트 출력
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

if sys.platform == "win32":
    pass  # 동기 코드만 사용

load_dotenv()

from backtest.price_fetcher import fetch_candles
from backtest.thread_builder import TARGET_COINS, build_threads
from backtest.virtual_trader import simulate_thread

# 최초 실행 시 DB 마이그레이션
MIGRATION_SQL = """
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS feedback_source VARCHAR(10);
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS virtual_pnl_pct NUMERIC(8,2);
ALTER TABLE analyses ADD COLUMN IF NOT EXISTS virtual_trade_json JSONB;
"""

# 타임프레임 → Bybit interval 매핑
_TF_INTERVAL = {
    "HOURLY":  "1h",
    "DAILY":   "1d",
    "WEEKLY":  "1w",
    "MONTHLY": "1d",
}


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


def _apply_migration(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(MIGRATION_SQL)
    conn.commit()
    print("[backtest] DB 마이그레이션 완료")


def _update_analysis(conn, analysis_id: int, feedback: str | None,
                     pnl_pct: float | None, trade_json: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE analyses
            SET feedback            = COALESCE(%s, feedback),
                feedback_source     = 'AUTO',
                virtual_pnl_pct     = %s,
                virtual_trade_json  = %s::jsonb
            WHERE id = %s
        """, (
            feedback,
            round(pnl_pct, 2) if pnl_pct is not None else None,
            json.dumps(trade_json, default=str),
            analysis_id,
        ))
    conn.commit()


def _determine_interval(opening: dict) -> str:
    tf = opening.get("timeframe") or "DAILY"
    return _TF_INTERVAL.get(tf.upper(), "1d")


def run(
    coins: tuple[str, ...] = TARGET_COINS,
    dry_run: bool = False,
    limit: int | None = None,
) -> None:
    conn = _db_connect()
    _apply_migration(conn)

    threads = build_threads(only_coins=coins, limit=limit)
    print(f"[backtest] 처리 대상: {len(threads)}개 트레이드 스레드")

    stats = {"total": 0, "correct": 0, "incorrect": 0, "skip": 0,
             "pnl_correct": [], "pnl_incorrect": []}

    for opening, updates in threads:
        analysis_id = opening["id"]
        coin        = opening["coin_symbol"]
        created_at  = opening["created_at"]
        expires_at  = opening["expires_at"]

        if not created_at or not expires_at:
            stats["skip"] += 1
            continue

        # 가격 데이터 fetch (만료 + 여유 2일)
        start_ms = int(created_at.timestamp() * 1000)
        end_ms   = int(expires_at.timestamp() * 1000) + 2 * 86_400_000
        interval = _determine_interval(opening)

        try:
            candles = fetch_candles(coin, interval, start_ms, end_ms)
        except Exception as e:
            print(f"  [SKIP] #{analysis_id} {coin} 가격 데이터 오류: {e}")
            stats["skip"] += 1
            continue

        if not candles:
            print(f"  [SKIP] #{analysis_id} {coin} 캔들 없음")
            stats["skip"] += 1
            continue

        # 시뮬레이션
        sim = simulate_thread(opening, updates, candles)
        stats["total"] += 1

        if not sim.entry_hit:
            print(f"  [⏭ SKIP] #{analysis_id} {coin} 진입 미도달")
            stats["skip"] += 1
            if not dry_run:
                _update_analysis(conn, analysis_id, None, None, sim.to_json())
            continue

        pnl = round(sim.final_pnl_pct, 2)
        label = "✅" if sim.feedback == "CORRECT" else "❌"
        print(f"  [{label}] #{analysis_id} {coin} {opening.get('timeframe','?')} "
              f"P&L {pnl:+.2f}%  ({len(sim.entries)}진입, {len(sim.exits)}청산)")

        if sim.feedback == "CORRECT":
            stats["correct"] += 1
            stats["pnl_correct"].append(pnl)
        else:
            stats["incorrect"] += 1
            stats["pnl_incorrect"].append(pnl)

        if not dry_run:
            _update_analysis(conn, analysis_id, sim.feedback, pnl, sim.to_json())

    conn.close()

    # 최종 리포트
    total = stats["total"]
    if total == 0:
        print("\n[backtest] 평가 가능한 트레이드 없음.")
        return

    avg_c = sum(stats["pnl_correct"]) / max(len(stats["pnl_correct"]), 1)
    avg_i = sum(stats["pnl_incorrect"]) / max(len(stats["pnl_incorrect"]), 1)

    print(f"""
╔══════════════════════════════════════════╗
  백테스팅 완료 {'[DRY RUN]' if dry_run else ''}
  총 {total}개 트레이드
  ✅ CORRECT  {stats['correct']}개 ({stats['correct']/total*100:.1f}%)  평균 {avg_c:+.2f}%
  ❌ INCORRECT {stats['incorrect']}개 ({stats['incorrect']/total*100:.1f}%)  평균 {avg_i:+.2f}%
  ⏭  스킵     {stats['skip']}개 [진입 미도달]
╚══════════════════════════════════════════╝""")


def main() -> None:
    parser = argparse.ArgumentParser(description="OTS 백테스팅 러너")
    parser.add_argument("--coin",    default=",".join(TARGET_COINS),
                        help="대상 코인 (쉼표 구분, 기본: BTC,ETH,USDT.D)")
    parser.add_argument("--dry-run", action="store_true",
                        help="DB 업데이트 없이 시뮬레이션만")
    parser.add_argument("--limit",   type=int, default=None,
                        help="처리할 최대 트레이드 수")
    args = parser.parse_args()

    coins = tuple(c.strip().upper() for c in args.coin.split(","))
    run(coins=coins, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
