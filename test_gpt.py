"""GPT-4o 분석 단독 테스트 — DB 연결 없이 분석 결과만 확인."""
import asyncio
import os
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

os.environ.setdefault("DATABASE_URL", "postgresql://dummy")  # DB 연결 안 함

from analysis.gpt_analyzer import _analyze_with_gpt

TEST_CONTENT = """
BTC 현재 95,000달러 부근에서 매수 전략 공유드립니다.

매수구간: 91,000 ~ 95,000달러
손절 기준: 89,000달러 하향 이탈 시
목표가: 105,000달러

현재 시장은 단기 조정 후 반등 가능성이 높습니다.
RSI는 45 부근으로 과매도 직전 수준입니다.
거래량이 평균 대비 증가하고 있어 관심 필요합니다.
"""

async def main():
    print("[test] GPT-4o 분석 시작...")
    result = await _analyze_with_gpt(TEST_CONTENT)

    print("\n===== 분석 결과 =====")
    print(f"신호:          {result['signal_type']}")
    print(f"코인:          {result['coin_symbol']}")
    print(f"유튜버 구간:   {result['youtuber_zone_low']} ~ {result['youtuber_zone_high']}")
    print(f"1차 매수가:    {result['entry_price_1']} ({result['entry_ratio_1']}%)")
    print(f"2차 매수가:    {result['entry_price_2']} ({result['entry_ratio_2']}%)")
    print(f"3차 매수가:    {result['entry_price_3']} ({result['entry_ratio_3']}%)")
    print(f"손절가:        {result['stop_loss_price']}")
    print(f"목표가:        {result['take_profit_price']}")
    print(f"R:R 비율:      {result['risk_reward_ratio']}")
    print(f"RSI 신호:      {result['rsi_signal']}")
    print(f"거래량 신호:   {result['volume_signal']}")
    print(f"피보나치:      {result['fib_level']}")
    print(f"요약:          {result['summary']}")
    print(f"무효 조건:     {result['invalidation']}")
    print("====================")

asyncio.run(main())
