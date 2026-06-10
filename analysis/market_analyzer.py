"""
실시간 시장 분석 엔진.

사용자가 "BTC 어때?" 같이 물으면:
  1. Bybit에서 4개 타임프레임 OHLCV 데이터 수집
  2. 피보나치 / RSI / 이동평균 계산
  3. 유튜버 최근 분석 조회
  4. GPT-4o로 종합 분석 → 진입가/손절/목표가 산출
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from openai import AsyncOpenAI

load_dotenv()

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
DATABASE_URL   = os.environ.get("DATABASE_URL", "")

_TF_MAP = {
    "15m": "15",
    "1h":  "60",
    "4h":  "240",
    "1d":  "D",
}

_MARKET_ANALYSIS_SYSTEM = """\
당신은 암호화폐 기술적 분석 전문가입니다.
4개 타임프레임(15분/1시간/4시간/일봉) 데이터를 분석하여 진입가·손절·목표가를 제시하세요.

분석 항목:
1. 추세선 방향 (각 타임프레임별)
2. 주요 지지/저항 레벨 (가격이 여러 번 반응한 구간)
3. 피보나치 핵심 레벨 (0.382 / 0.5 / 0.618 — 되돌림 기준)
4. RSI 과매수(>70) / 과매도(<30) 여부
5. 차트 패턴:
   - 삼각수렴 (ascending/descending/symmetrical triangle)
   - 헤드앤숄더 / 역헤드앤숄더
   - 더블바텀 / 더블탑
   - 플래그 / 페넌트
   - 웨지 (rising/falling wedge)
   - 컵앤핸들
6. 유튜버 분석이 있으면 일치 여부 확인 (없으면 생략)

매매 원칙 (반드시 준수):
- 지지선 / 추세선 / 채널 하단에서만 진입 권장
- 중간 구간 진입 금지 ("애매한 구간"이면 진입 보류)
- 저점이 높아지는 구조 = 매수 신호
- 일봉 종가 기준 손절
- 진입 없으면 entry_zone_low/high 에 null 반환

반드시 JSON만 응답 (설명 텍스트 없이):
{
  "entry_zone_low": 숫자 또는 null,
  "entry_zone_high": 숫자 또는 null,
  "stop_loss": 숫자 또는 null,
  "take_profit_1": 숫자 또는 null,
  "take_profit_2": 숫자 또는 null,
  "pattern": "패턴명 또는 null",
  "trend": "UPTREND 또는 DOWNTREND 또는 SIDEWAYS",
  "summary": "2~3문장 한국어 요약",
  "key_support": 숫자 또는 null,
  "key_resistance": 숫자 또는 null,
  "entry_recommended": true 또는 false
}
"""


def _db_connect():
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


def _sma(candles: list[dict], period: int) -> float | None:
    if len(candles) < period:
        return None
    return sum(c["close"] for c in candles[-period:]) / period


def _calc_rsi_simple(candles: list[dict], period: int = 14) -> float | None:
    closes = [c["close"] for c in candles]
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_g  = sum(gains[-period:]) / period
    avg_l  = sum(losses[-period:]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100 - (100 / (1 + rs))


def _check_macro_block(coin: str) -> dict | None:
    """해당 코인에 대해 활성화된 거시 방향성 차단 규칙이 있으면 반환."""
    import httpx
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT trigger_coin, trigger_cond, trigger_level,
                       result_direction, result_timeframe, result_target, description
                FROM macro_rules
                WHERE result_coin = %s AND is_active = TRUE
                ORDER BY created_at DESC LIMIT 1
            """, (coin,))
            row = cur.fetchone()
            if not row:
                return None

            trigger_coin, trigger_cond, trigger_level, direction, tf, target, desc = row

            # 현재 트리거 조건 충족 여부 확인
            if trigger_coin == "USDT.D":
                try:
                    r = httpx.get("https://api.coingecko.com/api/v3/global", timeout=10)
                    current = r.json()["data"]["market_cap_percentage"].get("usdt", 0)
                    level = float(trigger_level)
                    met = (current >= level if "ABOVE" in trigger_cond else current <= level)
                    if met:
                        return {
                            "trigger_coin": trigger_coin,
                            "trigger_level": level,
                            "current_value": current,
                            "result_direction": direction,
                            "result_timeframe": tf,
                            "result_target": float(target) if target else None,
                            "description": desc or "",
                        }
                except Exception:
                    pass
            return None
    finally:
        conn.close()


def _fetch_youtuber_signal(coin: str) -> list[dict]:
    """해당 코인의 유효한 유튜버 분석 최근 3개 조회."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT signal_type, timeframe, entry_price_1, entry_price_2,
                       stop_loss_price, take_profit_price, absolute_stop,
                       summary, created_at
                FROM analyses
                WHERE coin_symbol = %s
                  AND is_active = TRUE
                  AND signal_type IN ('BUY','SELL','HOLD')
                ORDER BY created_at DESC
                LIMIT 3
            """, (coin,))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def _fetch_candles_sync(coin: str, interval_bybit: str, count: int) -> list[dict]:
    """동기 방식으로 Bybit kline 데이터 조회 (최근 count개)."""
    from backtest.price_fetcher import fetch_bybit_kline, fetch_usdt_dominance
    end_ms   = int(datetime.now().timestamp() * 1000)
    # interval별 ms 계산
    interval_ms = {
        "15":  15 * 60 * 1000,
        "60":  60 * 60 * 1000,
        "240": 4  * 60 * 60 * 1000,
        "D":   24 * 60 * 60 * 1000,
    }
    delta = interval_ms.get(interval_bybit, 86_400_000) * count
    start_ms = end_ms - delta

    if coin == "USDT.D":
        return fetch_usdt_dominance(start_ms, end_ms)
    return fetch_bybit_kline(coin, interval_bybit, start_ms, end_ms)


def _candles_to_summary(candles: list[dict], tf_label: str, coin: str) -> str:
    """캔들 데이터를 GPT 입력용 텍스트로 변환."""
    if not candles:
        return f"[{tf_label}] 데이터 없음\n"

    current = candles[-1]["close"]
    high    = max(c["high"] for c in candles)
    low     = min(c["low"]  for c in candles)
    rsi     = _calc_rsi_simple(candles)
    sma20   = _sma(candles, 20)
    sma50   = _sma(candles, 50)

    # 피보나치 (일봉은 60개, 나머지는 100개 기준)
    from backtest.fib_calculator import find_swing_points, calc_fib_levels
    sh, sl = find_swing_points(candles, min(len(candles), 60))
    fibs   = calc_fib_levels(sh, sl) if sh > sl else {}

    lines = [
        f"[{tf_label}]",
        f"  현재가: {current:,.2f}",
        f"  기간 고점: {high:,.2f} | 저점: {low:,.2f}",
        f"  RSI(14): {rsi:.1f}" if rsi else "  RSI: 계산불가",
        f"  SMA20: {sma20:,.2f}" if sma20 else "",
        f"  SMA50: {sma50:,.2f}" if sma50 else "",
    ]
    if fibs:
        lines.append(
            f"  피보나치 0.382={fibs.get('0.382',0):,.2f} "
            f"0.5={fibs.get('0.5',0):,.2f} "
            f"0.618={fibs.get('0.618',0):,.2f}"
        )
    # 최근 20개 OHLC 요약 (GPT에 너무 많은 데이터 주면 비쌈)
    lines.append("  최근 20개 캔들 (시가,고가,저가,종가):")
    for c in candles[-20:]:
        lines.append(f"    {c['open']:,.2f} {c['high']:,.2f} {c['low']:,.2f} {c['close']:,.2f}")

    return "\n".join(l for l in lines if l) + "\n"


async def analyze_market(coin: str) -> dict:
    """
    4개 타임프레임 + 유튜버 데이터 종합 분석.

    Parameters
    ----------
    coin : "BTC" | "ETH" | "SOL" | "XRP" | 기타

    Returns
    -------
    dict with: entry_zone_low/high, stop_loss, take_profit_1/2,
               pattern, trend, summary, key_support/resistance,
               entry_recommended, youtuber_signal
    """
    loop = asyncio.get_event_loop()

    # 거시 방향성 차단 규칙 먼저 확인
    macro_block = await loop.run_in_executor(None, _check_macro_block, coin)

    # 4개 타임프레임 병렬 fetch
    tf_configs = [
        ("15분봉",  "15",  100),
        ("1시간봉", "60",  100),
        ("4시간봉", "240", 100),
        ("일봉",    "D",   60),
    ]
    candle_tasks = [
        loop.run_in_executor(None, _fetch_candles_sync, coin, tf[1], tf[2])
        for tf in tf_configs
    ]
    candle_results = await asyncio.gather(*candle_tasks, return_exceptions=True)

    # 유튜버 분석 조회
    youtuber = await loop.run_in_executor(None, _fetch_youtuber_signal, coin)

    # GPT USER 메시지 구성
    user_parts = [f"코인: {coin}\n\n"]
    for (label, _, _), result in zip(tf_configs, candle_results):
        if isinstance(result, Exception):
            user_parts.append(f"[{label}] 데이터 조회 실패: {result}\n")
        else:
            user_parts.append(_candles_to_summary(result, label, coin))

    if youtuber:
        user_parts.append("\n[유튜버 최근 분석]\n")
        for y in youtuber:
            dt = y["created_at"].strftime("%m/%d %H:%M") if y.get("created_at") else "?"
            user_parts.append(
                f"  [{dt}] {y['signal_type']} {y['timeframe'] or ''} "
                f"진입:{y['entry_price_2'] or y['entry_price_1'] or '-'} "
                f"SL:{y['stop_loss_price'] or '-'} "
                f"| {(y['summary'] or '')[:60]}\n"
            )
    else:
        user_parts.append("\n[유튜버 분석 없음 — 기술적 분석만으로 판단]\n")

    user_msg = "".join(user_parts)

    # GPT-4o 호출
    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    response = await client.chat.completions.create(
        model="gpt-4o",
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _MARKET_ANALYSIS_SYSTEM},
            {"role": "user",   "content": user_msg},
        ],
    )

    raw = response.choices[0].message.content or "{}"
    result = json.loads(raw)
    result["youtuber_signal"] = youtuber if youtuber else None
    result["coin"]            = coin
    result["macro_block"]     = macro_block   # 거시 차단 규칙 (있으면 경고 표시)

    # 거시 차단 활성화 시 롱 진입 비추천 강제 적용
    if macro_block and macro_block.get("result_direction") == "BEARISH":
        result["entry_recommended"] = False
        result["entry_zone_low"]    = None
        result["entry_zone_high"]   = None

    return result
