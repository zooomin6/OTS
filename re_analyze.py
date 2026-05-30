"""
기존 BUY/SELL 분석을 삭제하고 개선된 프롬프트로 재분석.
재분석 후 타임스탬프를 원래 게시글 발행일(published_at)로 수정해
백테스팅이 올바른 시점 가격 데이터를 사용하도록 한다.

실행:
    python re_analyze.py [--dry-run] [--coin ETH] [--limit 10]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

TARGET_COINS = ("BTC", "ETH", "USDT.D")

_TF_EXPIRE = {
    "HOURLY":  timedelta(hours=24),
    "DAILY":   timedelta(days=5),
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


def _get_posts_to_reanalyze(coins: tuple, limit: int | None) -> list[dict]:
    """재분석 대상 post_id + published_at 목록 조회."""
    conn = _db_connect()
    limit_clause = f"LIMIT {limit}" if limit else ""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT p.id, LEFT(p.content, 80), p.published_at,
                       array_agg(a.id) as analysis_ids
                FROM analyses a
                JOIN posts p ON a.post_id = p.id
                WHERE a.coin_symbol = ANY(%s)
                  AND a.signal_type IN ('BUY', 'SELL')
                  AND a.feedback IS NULL
                GROUP BY p.id, p.content, p.published_at
                ORDER BY p.id ASC
                {limit_clause}
            """, (list(coins),))
            rows = cur.fetchall()
    finally:
        conn.close()

    return [
        {
            "post_id":      r[0],
            "content_preview": r[1],
            "published_at": r[2],
            "analysis_ids": r[3],
        }
        for r in rows
    ]


def _delete_analyses(analysis_ids: list[int]) -> None:
    """trades → analyses 순서로 삭제."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trades  WHERE analysis_id = ANY(%s)", (analysis_ids,))
            cur.execute("DELETE FROM analyses WHERE id          = ANY(%s)", (analysis_ids,))
        conn.commit()
    finally:
        conn.close()


def _fix_timestamps(post_id: int, published_at: datetime) -> int:
    """
    재분석으로 새로 생긴 analyses의 created_at / expires_at을
    원래 게시글 발행일 기준으로 수정.
    반환: 수정된 분석 수
    """
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            # 방금 저장된 분석 (created_at이 오늘인 것들)
            cur.execute("""
                SELECT id, timeframe, is_reference_only
                FROM analyses
                WHERE post_id = %s
                  AND created_at >= NOW() - INTERVAL '5 minutes'
            """, (post_id,))
            rows = cur.fetchall()

            count = 0
            for aid, tf, is_ref in rows:
                if is_ref or tf in ("MONTHLY", "WEEKLY"):
                    expires_at = None
                else:
                    delta = _TF_EXPIRE.get(tf or "DAILY", timedelta(days=5))
                    expires_at = published_at + delta

                cur.execute("""
                    UPDATE analyses
                    SET created_at = %s,
                        expires_at = %s
                    WHERE id = %s
                """, (published_at, expires_at, aid))
                count += 1

        conn.commit()
        return count
    finally:
        conn.close()


async def _reanalyze_post(post_id: int) -> None:
    from analysis.gpt_analyzer import _process

    fake_msg = json.dumps({
        "post_id":    post_id,
        "channel_id": "reanalyze",
        "post_type":  "text",
        "image_urls": [],
    }).encode()

    await _process(fake_msg)


async def main(coins: tuple, dry_run: bool, limit: int | None) -> None:
    posts = _get_posts_to_reanalyze(coins, limit)
    print(f"[re_analyze] 재분석 대상: {len(posts)}개 게시글")

    if not posts:
        print("[re_analyze] 대상 없음.")
        return

    for i, item in enumerate(posts, 1):
        post_id      = item["post_id"]
        published_at = item["published_at"]
        preview      = (item["content_preview"] or "").replace("\n", " ")
        analysis_ids = item["analysis_ids"]

        print(f"\n[{i}/{len(posts)}] post_id={post_id}  원래날짜={published_at.date()}")
        print(f"  내용: {preview}...")
        print(f"  기존 분석 삭제: {analysis_ids}")

        if dry_run:
            print("  [DRY RUN] 생략")
            continue

        _delete_analyses(analysis_ids)

        try:
            await _reanalyze_post(post_id)
            fixed = _fix_timestamps(post_id, published_at)
            print(f"  ✅ 재분석 완료 | 타임스탬프 수정: {fixed}개 → {published_at.date()} 기준")
        except Exception as e:
            print(f"  ❌ 재분석 실패: {e}")

        time.sleep(2)

    print(f"\n[re_analyze] 완료 {'[DRY RUN]' if dry_run else ''}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--coin",  default=",".join(TARGET_COINS))
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    coins = tuple(c.strip().upper() for c in args.coin.split(","))
    asyncio.run(main(coins=coins, dry_run=args.dry_run, limit=args.limit))
