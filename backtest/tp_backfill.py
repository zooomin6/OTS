"""
기존 분석 중 take_profit_price가 NULL인 것들을 소급 처리.

처리 순서 (분석 1개마다):
  1. scenario_json의 "익절" / "목표" 액션에서 target_price 추출
  2. 실패 시 R:R 2:1 역산 (entry_price_2 + (entry_price_2 - stop_loss) × 2)
  3. DB 업데이트

사용법:
    python -m backtest.tp_backfill [--dry-run] [--coin BTC,ETH,USDT.D]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

load_dotenv()

TARGET_COINS = ("BTC", "ETH", "USDT.D")


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


def _tp_from_scenario(scenario_json) -> float | None:
    """scenario_json 배열에서 첫 번째 익절/목표 액션의 target_price 추출."""
    if not scenario_json:
        return None
    try:
        steps = scenario_json if isinstance(scenario_json, list) else json.loads(scenario_json)
    except Exception:
        return None

    for step in sorted(steps, key=lambda s: s.get("step", 99)):
        action = (step.get("action") or "").lower()
        if any(kw in action for kw in ("익절", "목표", "tp", "take profit", "매도")):
            tp = step.get("target_price")
            if tp is not None:
                try:
                    v = float(tp)
                    if v > 0:
                        return v
                except (TypeError, ValueError):
                    pass
    return None


def _tp_from_rr(signal: str, entry: float | None, sl: float | None,
                short_entry: float | None) -> float | None:
    """R:R 2:1 역산으로 목표가 추정."""
    if signal == "BUY" and entry and sl and entry > sl:
        return entry + (entry - sl) * 2
    if signal == "SELL" and short_entry and sl and sl > short_entry:
        return short_entry - (sl - short_entry) * 2
    return None


def run(coins: tuple[str, ...] = TARGET_COINS, dry_run: bool = False) -> None:
    conn = _db_connect()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, signal_type, coin_symbol,
                   entry_price_2, stop_loss_price, short_entry_price,
                   scenario_json
            FROM analyses
            WHERE take_profit_price IS NULL
              AND coin_symbol = ANY(%s)
              AND signal_type IN ('BUY', 'SELL')
              AND is_reference_only = FALSE
        """, (list(coins),))
        rows = cur.fetchall()

    print(f"[tp_backfill] 처리 대상: {len(rows)}개 (take_profit_price NULL)")

    stats = {"scenario": 0, "rr": 0, "skip": 0}

    for row in rows:
        aid, signal, coin, e2, sl, short_e, scenario = row
        e2      = float(e2)      if e2      else None
        sl      = float(sl)      if sl      else None
        short_e = float(short_e) if short_e else None

        # Step 1: scenario_json
        tp = _tp_from_scenario(scenario)
        method = "SCENARIO"

        # Step 2: R:R 역산
        if tp is None:
            tp = _tp_from_rr(signal, e2, sl, short_e)
            method = "RR2:1"

        if tp is None:
            print(f"  [SKIP] #{aid} {coin} {signal} → TP 추출 불가")
            stats["skip"] += 1
            continue

        print(f"  [{'DRY' if dry_run else 'UPDATE'}] #{aid} {coin} {signal} "
              f"→ TP={tp:.2f} ({method})")

        if not dry_run:
            note_suffix = f" [TP소급:{method}]"
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE analyses
                    SET take_profit_price = %s,
                        feedback_note = COALESCE(feedback_note, '') || %s
                    WHERE id = %s AND take_profit_price IS NULL
                """, (round(tp, 2), note_suffix, aid))
            conn.commit()

        stats["scenario" if method == "SCENARIO" else "rr"] += 1

    conn.close()

    total = stats["scenario"] + stats["rr"]
    print(f"""
[tp_backfill] 완료 {'[DRY RUN]' if dry_run else ''}
  시나리오 추출: {stats['scenario']}개
  R:R 2:1 추정: {stats['rr']}개
  처리 불가:    {stats['skip']}개
  합계:         {total}개 업데이트{'(예정)' if dry_run else ''}
""")


def main() -> None:
    parser = argparse.ArgumentParser(description="take_profit_price NULL 소급 처리")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--coin", default=",".join(TARGET_COINS))
    args = parser.parse_args()

    coins = tuple(c.strip().upper() for c in args.coin.split(","))
    run(coins=coins, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
