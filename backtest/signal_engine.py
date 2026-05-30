"""
멀티 신호 생성 엔진.

신호 우선순위:
  1. 유튜버 BUY + 피보나치 겹침  → 최강
  2. 유튜버 BUY 단독              → HIGH
  3. 피보나치 0.5/0.618 + 추가조건 1개 이상 → MEDIUM
  4. 피보나치 단독                → 신호 없음
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from backtest.fib_calculator import (
    calc_fib_levels,
    calc_rsi,
    find_swing_points,
    is_fear_greed_low,
    is_usdt_d_falling,
    nearest_key_level,
)

FIB_LOOKBACK  = 60    # 일봉 기준 고저점 탐색 기간
RSI_THRESHOLD = 40    # RSI 과매도 기준


@dataclass
class TradeSignal:
    coin: str
    entry_price: float
    stop_loss: float
    source: str                        # 'YOUTUBER' | 'FIB' | 'YOUTUBER+FIB'
    strength: str                      # 'HIGH' | 'MEDIUM'
    fib_level: str | None = None       # '0.5' | '0.618' | None
    conditions: list[str] = field(default_factory=list)
    analysis_id: int | None = None     # 유튜버 분석 ID (있을 때만)
    swing_high: float = 0.0
    swing_low: float  = 0.0


def _youtuber_entry_hit(analysis: dict, candle: dict, tolerance: float = 0.015) -> bool:
    """
    유튜버 분석의 진입가에 현재 캔들이 닿았는지 확인.
    tolerance: 진입가 대비 얼마나 위까지 허용할지 (기본 1.5%)
    실전에서는 지지라인 바로 위에서 진입하는 경우가 많음.
    """
    e2 = analysis.get("entry_price_2")
    e1 = analysis.get("entry_price_1")
    entry = float(e2 or e1 or 0)
    if entry <= 0:
        return False
    # 캔들 저가가 진입가 이하이거나, 종가가 진입가 ±tolerance 이내
    return candle["low"] <= entry or abs(candle["close"] - entry) / entry <= tolerance


def _sma(candles: list[dict], period: int) -> float | None:
    """단순 이동평균 (종가 기준)."""
    if len(candles) < period:
        return None
    return sum(c["close"] for c in candles[-period:]) / period


def generate_signal(
    coin: str,
    candle: dict,
    prev_candles: list[dict],
    usdt_d_series: list[dict],
    active_youtuber: dict | None = None,
    fng_series: list[dict] | None = None,
    coin_pair_strong: bool = True,
) -> TradeSignal | None:
    """
    단일 캔들 시점에서 진입 신호 생성.

    Parameters
    ----------
    candle           : 현재 캔들 {time, open, high, low, close}
    prev_candles     : 현재 캔들 이전의 캔들 목록
    usdt_d_series    : USDT.D 시계열 (없으면 빈 리스트)
    active_youtuber  : 현재 활성 유튜버 BUY 분석 (없으면 None)
    fng_series       : 공포탐욕지수 시계열 (없으면 빈 리스트)
    """
    all_candles = prev_candles + [candle]
    price = candle["low"]

    # ── 피보나치 계산 ──
    swing_high, swing_low = find_swing_points(prev_candles, FIB_LOOKBACK)
    levels = calc_fib_levels(swing_high, swing_low) if swing_high > swing_low else {}
    fib_level = nearest_key_level(price, levels) if levels else None

    # ── 추세 필터: 50일 SMA ──
    sma50 = _sma(prev_candles, 50)
    if sma50 and candle["close"] < sma50 * 0.97:
        fib_level = None

    # ── 추가 조건 체크 ──
    conditions: list[str] = []

    rsi = calc_rsi(all_candles)
    if rsi is not None and rsi < RSI_THRESHOLD:
        conditions.append(f"RSI:{rsi:.1f}<{RSI_THRESHOLD}")

    if is_usdt_d_falling(usdt_d_series):
        conditions.append("USDT.D↓")

    if is_fear_greed_low(fng_series or [], candle["time"]):
        conditions.append("FNG공포")

    youtuber_hit = active_youtuber and _youtuber_entry_hit(active_youtuber, candle)
    if youtuber_hit:
        conditions.append("YOUTUBER")

    # ── SL: 스윙저점 × 0.97 (피보나치 구조 기반) ──
    fib_sl = swing_low * 0.97 if swing_low > 0 else None

    # ── 신호 판정 ──

    # 1순위: 유튜버 신호 (하락장에서도 허용)
    if youtuber_hit and active_youtuber:
        e2 = active_youtuber.get("entry_price_2")
        e1 = active_youtuber.get("entry_price_1")
        entry = float(e2 or e1 or candle["close"])
        sl_raw = active_youtuber.get("stop_loss_price")
        abs_stop = active_youtuber.get("absolute_stop")
        # 유튜버 SL 우선, 없으면 마지노선, 없으면 스윙저점 기준 (-10% 여유)
        if sl_raw:
            sl = float(sl_raw)
        elif abs_stop:
            sl = float(abs_stop) * 0.97
        else:
            sl = fib_sl or entry * 0.90

        source = "YOUTUBER+FIB" if fib_level else "YOUTUBER"
        return TradeSignal(
            coin=coin,
            entry_price=entry,
            stop_loss=sl,
            source=source,
            strength="HIGH",
            fib_level=fib_level,
            conditions=conditions,
            analysis_id=active_youtuber.get("id"),
            swing_high=swing_high,
            swing_low=swing_low,
        )

    # 2순위: 피보나치 + 추가 조건 1개 이상
    # ETH/BTC 약세 시 FIB 단독 신호 억제 (유튜버 신호는 항상 허용)
    if not coin_pair_strong:
        return None

    if fib_level and len(conditions) >= 1:
        entry = levels[fib_level]
        sl = fib_sl or entry * 0.90  # 스윙저점 기준 SL

        return TradeSignal(
            coin=coin,
            entry_price=entry,
            stop_loss=sl,
            source="FIB",
            strength="MEDIUM",
            fib_level=fib_level,
            conditions=conditions,
            swing_high=swing_high,
            swing_low=swing_low,
        )

    return None


def get_active_youtuber_analysis(
    coin: str,
    candle_time_ms: int,
    analyses: list[dict],
) -> dict | None:
    """
    현재 시점에 유효한 유튜버 BUY 분석을 반환.
    created_at <= candle_time < expires_at 조건.
    """
    import datetime as _dt

    candle_dt = _dt.datetime.fromtimestamp(candle_time_ms / 1000)

    candidates = []
    for a in analyses:
        if a.get("coin_symbol") != coin:
            continue
        if a.get("signal_type") != "BUY":
            continue
        created = a.get("created_at")
        expires = a.get("expires_at")
        if not created:
            continue
        if created > candle_dt:
            continue
        if expires and expires < candle_dt:
            continue
        candidates.append(a)

    # 가장 최근 분석 우선
    if candidates:
        return sorted(candidates, key=lambda x: x["created_at"], reverse=True)[0]
    return None
