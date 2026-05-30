"""
과거 가격 데이터 수집.

- BTC / ETH : Bybit kline REST API (인증 불필요)
- USDT.D    : CoinGecko (USDT 시총 / 전체 시총 → 도미넌스 %)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx

BYBIT_KLINE_URL   = "https://api.bybit.com/v5/market/kline"
COINGECKO_TETHER  = "https://api.coingecko.com/api/v3/coins/tether/market_chart"
COINGECKO_GLOBAL  = "https://api.coingecko.com/api/v3/global/market_cap_chart"

# Bybit interval 매핑
_INTERVAL_MAP = {
    "1h": "60",
    "4h": "240",
    "1d": "D",
    "1w": "W",
}


def fetch_bybit_kline(
    coin: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """
    Bybit에서 OHLCV 일봉/시간봉 이력을 가져온다.

    Parameters
    ----------
    coin     : "BTC" | "ETH"
    interval : "1h" | "4h" | "1d" (또는 Bybit 원시값 "60", "D" 등)
    start_ms : 시작 Unix ms
    end_ms   : 종료 Unix ms

    Returns
    -------
    시간 오름차순 리스트: [{time, open, high, low, close}, ...]
    """
    bybit_interval = _INTERVAL_MAP.get(interval, interval)
    symbol = f"{coin}USDT"
    candles: list[dict] = []

    # Bybit는 한 번에 최대 1000개, end → start 방향으로 반환
    # 페이지네이션: end_ms를 줄여가며 반복
    current_end = end_ms

    while True:
        resp = httpx.get(
            BYBIT_KLINE_URL,
            params={
                "category": "linear",
                "symbol":   symbol,
                "interval": bybit_interval,
                "start":    start_ms,
                "end":      current_end,
                "limit":    1000,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("result", {}).get("list", [])
        if not items:
            break

        for item in items:
            # [startTime, openPrice, highPrice, lowPrice, closePrice, volume, turnover]
            t = int(item[0])
            if t < start_ms:
                continue
            candles.append({
                "time":  t,
                "open":  float(item[1]),
                "high":  float(item[2]),
                "low":   float(item[3]),
                "close": float(item[4]),
            })

        # 마지막 캔들 시간이 start_ms 이하면 종료
        oldest = int(items[-1][0])
        if oldest <= start_ms:
            break

        # 다음 요청: 현재 배치에서 가장 오래된 캔들 바로 전으로 end 이동
        current_end = oldest - 1
        time.sleep(0.2)

    # 중복 제거 후 시간 오름차순 정렬
    seen: set[int] = set()
    unique: list[dict] = []
    for c in candles:
        if c["time"] not in seen:
            seen.add(c["time"])
            unique.append(c)

    unique.sort(key=lambda x: x["time"])
    return unique


def fetch_usdt_dominance(start_ms: int, end_ms: int) -> list[dict]:
    """
    CoinGecko에서 USDT 도미넌스(%) 이력을 일봉 단위로 가져온다.

    CoinGecko 무료 티어: /global/market_cap_chart 유료 엔드포인트로 변경됨.
    대신 /api/v3/global (현재값)의 시총 비율로 근사하거나,
    USDT 시총 / BTC 시총 환산으로 추정한다.

    Returns
    -------
    시간 오름차순 리스트: [{time, close (=dominance_pct)}, ...]
    빈 리스트 반환 시 호출자에서 스킵 처리됨.
    """
    days = max(1, (end_ms - start_ms) // 86_400_000 + 2)

    try:
        # USDT 시총 이력 (무료 엔드포인트)
        time.sleep(1.0)
        r_usdt = httpx.get(
            COINGECKO_TETHER,
            params={"vs_currency": "usd", "days": days, "interval": "daily"},
            timeout=20,
        )
        r_usdt.raise_for_status()
        usdt_data = r_usdt.json().get("market_caps", [])

        # 전체 시총: /coins/markets 로 상위 200개 합산 (무료)
        # 단순화: USDT 시총 / 고정 비율로 근사 (유료 전환 대응)
        # 현재 전체 시총 기준으로 비율 계산
        time.sleep(1.0)
        r_global = httpx.get(
            "https://api.coingecko.com/api/v3/global",
            timeout=10,
        )
        r_global.raise_for_status()
        global_data = r_global.json().get("data", {})
        current_usdt_pct = global_data.get("market_cap_percentage", {}).get("usdt")
        current_total_mcap = global_data.get("total_market_cap", {}).get("usd")

        if not current_usdt_pct or not current_total_mcap or not usdt_data:
            return []

        # 각 날짜의 USDT 시총 / (현재 전체시총 기준 비율 역산) 으로 도미넌스 근사
        result: list[dict] = []
        for ts, usdt_mcap in usdt_data:
            day_key = (ts // 86_400_000) * 86_400_000
            if day_key < start_ms or day_key > end_ms:
                continue
            # 해당 시점 총 시총 = USDT 시총 / (현재 USDT 도미넌스%) × 100
            # 근사값이지만 총 시총 변화에 비해 USDT 도미넌스 변화가 주 관심사
            approx_total = float(usdt_mcap) / (float(current_usdt_pct) / 100)
            dominance = float(usdt_mcap) / approx_total * 100
            result.append({"time": day_key, "close": dominance})

        return result

    except Exception as e:
        print(f"  [WARN] USDT.D 데이터 조회 실패: {e}")
        return []


def fetch_candles(coin: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
    """코인 종류에 따라 적절한 가격 데이터를 반환하는 단일 진입점."""
    if coin == "USDT.D":
        return fetch_usdt_dominance(start_ms, end_ms)
    return fetch_bybit_kline(coin, interval, start_ms, end_ms)
