"""
피보나치 레벨 계산.

사용자 방식:
  1. 일봉 기준 최근 60일 내 스윙 고점/저점 탐색
  2. 0.5 / 0.618 레벨을 핵심 매수 구간으로 사용
  3. tolerance 1.5% 이내로 터치하면 신호
"""
from __future__ import annotations

_KEY_LEVELS = ("0.382", "0.5", "0.618")   # 매수 진입 구간 (0.382 추가)
_ALL_LEVELS = ("0.236", "0.382", "0.5", "0.618", "0.786")


def find_swing_points(candles: list[dict], lookback: int = 60) -> tuple[float, float]:
    """
    최근 lookback 캔들에서 스윙 고점 / 저점을 반환.
    Returns (swing_high, swing_low)
    """
    if not candles:
        return 0.0, 0.0
    recent = candles[-lookback:]
    swing_high = max(c["high"] for c in recent)
    swing_low  = min(c["low"]  for c in recent)
    return swing_high, swing_low


def calc_fib_levels(swing_high: float, swing_low: float) -> dict[str, float]:
    """
    피보나치 리트레이스먼트 레벨 계산.
    고점→저점 하락 후 반등 시나리오 기준 (롱 매수 포지션).
    """
    diff = swing_high - swing_low
    if diff <= 0:
        return {}
    return {
        "0.0":   swing_low,
        "0.236": swing_low + diff * 0.236,
        "0.382": swing_low + diff * 0.382,
        "0.5":   swing_low + diff * 0.5,
        "0.618": swing_low + diff * 0.618,
        "0.786": swing_low + diff * 0.786,
        "1.0":   swing_high,
    }


def nearest_key_level(
    price: float,
    levels: dict[str, float],
    tolerance: float = 0.015,
) -> str | None:
    """
    가격이 핵심 레벨(0.5, 0.618) tolerance(기본 1.5%) 이내면 해당 레벨 문자열 반환.
    아니면 None.
    """
    for key in _KEY_LEVELS:
        fib_price = levels.get(key)
        if fib_price and fib_price > 0:
            if abs(price - fib_price) / fib_price <= tolerance:
                return key
    return None


def calc_rsi(candles: list[dict], period: int = 14) -> float | None:
    """단순 RSI 계산 (Wilder's smoothing)."""
    closes = [c["close"] for c in candles]
    if len(closes) < period + 1:
        return None

    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    for i in range(len(deltas) - period):
        avg_gain = (avg_gain * (period - 1) + gains[period + i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[period + i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_fear_greed_history(days: int = 30) -> list[dict]:
    """
    Alternative.me 공포탐욕지수 이력 조회 (무료 API).
    반환: [{timestamp_ms, value (0~100), classification}, ...]
    값이 낮을수록 공포 (매수 기회), 높을수록 탐욕.
    """
    try:
        import httpx
        r = httpx.get(
            "https://api.alternative.me/fng/",
            params={"limit": days, "format": "json"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        return [
            {
                "time":           int(d["timestamp"]) * 1000,
                "value":          int(d["value"]),
                "classification": d.get("value_classification", ""),
            }
            for d in data
        ]
    except Exception:
        return []


def is_fear_greed_low(
    fng_series: list[dict],
    candle_time_ms: int,
    threshold: int = 35,
) -> bool:
    """
    해당 시점의 공포탐욕지수가 threshold 이하(공포 구간)이면 True.
    0~25: Extreme Fear (극도 공포)
    25~45: Fear (공포) ← 매수 기회
    """
    if not fng_series:
        return False
    # 해당 캔들 날짜 이전의 가장 최근 FNG 값 사용
    past = [f for f in fng_series if f["time"] <= candle_time_ms]
    if not past:
        return False
    latest = max(past, key=lambda x: x["time"])
    return latest["value"] <= threshold


def is_usdt_d_falling(usdt_d_series: list[dict], lookback: int = 3, threshold: float = 0.3) -> bool:
    """
    최근 lookback 일 동안 USDT.D가 고점 대비 threshold% 이상 하락 중이면 True.
    usdt_d_series: [{time, close(=dominance_pct)}, ...] 시간순
    """
    if len(usdt_d_series) < lookback + 1:
        return False
    recent = usdt_d_series[-(lookback + 1):]
    recent_high = max(r["close"] for r in recent[:-1])
    current     = recent[-1]["close"]
    drop_pct    = (recent_high - current) / recent_high * 100
    return drop_pct >= threshold
