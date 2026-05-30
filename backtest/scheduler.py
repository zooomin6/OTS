"""
백테스팅 스케줄러 — Docker 서비스로 실행.

1시간마다 만료된 미평가 분석을 자동으로 평가하고 DB에 기록한다.
"""
from __future__ import annotations

import sys
import time

if sys.platform == "win32":
    pass

from backtest.runner import TARGET_COINS, run

INTERVAL_SEC = 3600  # 1시간


def main() -> None:
    print("[backtest-scheduler] 시작. 1시간마다 자동 평가 실행.")
    while True:
        print(f"\n[backtest-scheduler] 평가 시작...")
        try:
            run(coins=TARGET_COINS, dry_run=False, limit=None)
        except Exception as e:
            print(f"[backtest-scheduler] 오류: {e}")
        print(f"[backtest-scheduler] {INTERVAL_SEC // 60}분 후 재실행.")
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
