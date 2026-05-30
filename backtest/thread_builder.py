"""
트레이드 스레드 구성.

BUY/SELL 오프닝 분석을 기준으로,
같은 coin_symbol + 겹치는 기간의 HOLD 업데이트 분석들을 묶어
하나의 (opening, updates) 쌍으로 반환한다.
"""
from __future__ import annotations

import os
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


def build_threads(
    only_coins: tuple[str, ...] = TARGET_COINS,
    limit: int | None = None,
    include_evaluated: bool = False,
) -> list[tuple[dict, list[dict]]]:
    """
    Returns
    -------
    [(opening_analysis, [update_analysis, ...]), ...]

    opening  : BUY/SELL 신호 분석 (feedback IS NULL 또는 포함)
    updates  : 같은 coin, 겹치는 기간의 HOLD 분석들
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            feedback_filter = "" if include_evaluated else "AND a.feedback IS NULL"
            limit_clause    = f"LIMIT {limit}" if limit else ""

            cur.execute(f"""
                SELECT
                    a.id, a.post_id, a.signal_type, a.coin_symbol, a.timeframe,
                    a.is_reference_only,
                    a.entry_price_1, a.entry_price_2, a.entry_price_3,
                    a.short_entry_price, a.short_stop_loss,
                    a.stop_loss_price, a.take_profit_price,
                    a.absolute_stop,
                    a.scenario_json,
                    a.created_at, a.expires_at,
                    a.feedback
                FROM analyses a
                WHERE a.coin_symbol = ANY(%s)
                  AND a.signal_type IN ('BUY', 'SELL')
                  AND a.is_reference_only = FALSE
                  AND a.expires_at IS NOT NULL
                  AND a.expires_at < NOW()
                  {feedback_filter}
                ORDER BY a.created_at ASC
                {limit_clause}
            """, (list(only_coins),))

            opening_rows = cur.fetchall()
            col_names = [desc[0] for desc in cur.description]

            threads: list[tuple[dict, list[dict]]] = []

            for row in opening_rows:
                opening = dict(zip(col_names, row))

                # 같은 코인의 HOLD 업데이트 분석 조회
                cur.execute("""
                    SELECT
                        a.id, a.post_id, a.signal_type, a.coin_symbol, a.timeframe,
                        a.take_profit_price, a.stop_loss_price,
                        a.created_at, a.expires_at, a.invalidation
                    FROM analyses a
                    WHERE a.coin_symbol = %s
                      AND a.signal_type = 'HOLD'
                      AND a.created_at BETWEEN %s AND %s
                    ORDER BY a.created_at ASC
                """, (
                    opening["coin_symbol"],
                    opening["created_at"],
                    opening["expires_at"],
                ))

                update_col = [desc[0] for desc in cur.description]
                updates = [dict(zip(update_col, r)) for r in cur.fetchall()]
                threads.append((opening, updates))

    finally:
        conn.close()

    return threads
