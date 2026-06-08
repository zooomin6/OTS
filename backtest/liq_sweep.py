"""
청산 안전선 통계 — 여러 과거 급락에 중첩 DCA 사다리를 대입해
레버리지별 '생존율'을 산출. (2단계: 안전선 굳히기)

방법:
  - 스윙 고점(±SWING_W 캔들 내 최고)을 에피소드 진입점으로 탐지
  - 고점 대비 상대 낙폭으로 분할 진입: peak*(1-d), d in DROPS
  - 청산가 = 평단*(1-1/L+MMR), 캔들 꼬리(low)로 청산 판정
  - 1개 이상 체결된 에피소드만 집계 → 레버리지별 생존율

가정:
  - 사다리 간격은 사용자 BTC 예시(약 -12/-14/-27%)를 상대화한 값
  - 격리마진 롱, 각 분할 동일 레버, 추가증거금 없음, 수수료 무시

사용법:
    python -m backtest.liq_sweep                # BTC + ETH
    python -m backtest.liq_sweep --coin ETH --days 1200
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from backtest.price_fetcher import fetch_bybit_kline

DROPS        = [0.12, 0.14, 0.27]   # 고점 대비 분할 진입 낙폭 (위→아래)
LOWER_BOX    = 0.30                  # 고점 대비 미사용 최종지지(아래 박스)
LEVERAGES    = [2, 3, 4, 5, 6, 8, 10]
MMR          = 0.005
TRANCHE      = 1000.0
SWING_W      = 15      # 스윙 고점 탐지 ±윈도우(일)
HORIZON      = 90      # 진입 후 추적 일수
MIN_PEAK_GAP = 40      # 에피소드 최소 간격(일)


def liq_long(avg: float, lev: int) -> float:
    return avg * (1 - 1.0 / lev + MMR)


def fmt(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")


def detect_peaks(candles: list[dict]) -> list[int]:
    """±SWING_W 윈도우 최고가 = 스윙 고점. MIN_PEAK_GAP일 이상 간격."""
    peaks: list[int] = []
    last_t = -10**18
    for i in range(SWING_W, len(candles) - SWING_W):
        h = candles[i]["high"]
        window = candles[i - SWING_W:i + SWING_W + 1]
        if h >= max(x["high"] for x in window):
            t = candles[i]["time"]
            if t - last_t >= MIN_PEAK_GAP * 86_400_000:
                peaks.append(i)
                last_t = t
    return peaks


def simulate(seg: list[dict], ladder: list[float], lev: int) -> tuple[bool, int, float]:
    """returns (survived, tranches_filled, max_drawdown_from_avg_pct)."""
    filled = 0
    tq = 0.0
    tu = 0.0
    max_dd = 0.0
    for c in seg:
        low = c["low"]
        while True:
            avg = (tu / tq) if tq > 0 else None
            liq = liq_long(avg, lev) if avg else -1.0
            nxt = ladder[filled] if filled < len(ladder) else None
            if nxt is not None and (avg is None or nxt > liq) and low <= nxt:
                tu += TRANCHE
                tq += TRANCHE / nxt
                filled += 1
                continue
            if tq > 0 and low <= liq:
                return False, filled, max_dd
            break
        if tq > 0:
            avg = tu / tq
            dd = (avg - low) / avg * 100
            max_dd = max(max_dd, dd)
    return True, filled, max_dd


def run_coin(coin: str, days: int) -> None:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    candles = fetch_bybit_kline(coin, "1d",
                               int(start.timestamp() * 1000),
                               int(end.timestamp() * 1000))
    if len(candles) < SWING_W * 2 + HORIZON:
        print(f"[{coin}] 캔들 부족 ({len(candles)})"); return

    peaks = detect_peaks(candles)

    # 레버리지별 생존/총 집계 + 진입 에피소드의 최대낙폭 분포
    survived = {L: 0 for L in LEVERAGES}
    total = 0
    dd_list: list[float] = []
    episodes: list[tuple[str, float, dict]] = []

    for pi in peaks:
        peak = candles[pi]["high"]
        ladder = [peak * (1 - d) for d in DROPS]
        seg = candles[pi:pi + HORIZON]
        # 최소 1개 체결되는 에피소드만 (실제 진입)
        if min(c["low"] for c in seg) > ladder[0]:
            continue
        total += 1
        res_by_lev = {}
        for L in LEVERAGES:
            ok, filled, dd = simulate(seg, ladder, L)
            res_by_lev[L] = ok
            if ok:
                survived[L] += 1
            if L == LEVERAGES[-1]:
                pass
        # 최대낙폭은 레버 무관(저레버 생존 기준) — 5x로 측정
        _, _, dd5 = simulate(seg, ladder, 3)
        dd_list.append(dd5)
        episodes.append((fmt(candles[pi]["time"]), peak, res_by_lev))

    print(f"\n══════════ {coin} ══════════")
    print(f"분석 기간: {fmt(candles[0]['time'])} ~ {fmt(candles[-1]['time'])}")
    print(f"진입 발생 에피소드: {total}개  (스윙고점 {len(peaks)}개 중)")
    if total == 0:
        return

    print(f"\n  레버 │ 생존율")
    print(f"  ─────┼───────────────────────────────")
    for L in LEVERAGES:
        rate = survived[L] / total * 100
        bar = "█" * int(rate / 5)
        print(f"  {L:>3}x │ {survived[L]:>2}/{total}  {rate:>5.0f}%  {bar}")

    dd_list.sort()
    if dd_list:
        avg_dd = sum(dd_list) / len(dd_list)
        worst = dd_list[-1]
        med = dd_list[len(dd_list) // 2]
        print(f"\n  진입 평단 대비 최대낙폭(저레버 기준): 평균 -{avg_dd:.1f}% / 중앙 -{med:.1f}% / 최악 -{worst:.1f}%")

    # 에피소드별 상세 (청산 시작 레버 표시)
    print(f"\n  에피소드별 (고점 → 청산되는 최저 레버):")
    for date, peak, res in episodes:
        first_liq = next((L for L in LEVERAGES if not res[L]), None)
        tag = f"{first_liq}x부터 청산" if first_liq else "전 레버 생존"
        print(f"    {date}  고점 {peak:>9,.0f}  →  {tag}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin", default="BOTH")
    ap.add_argument("--days", type=int, default=1200)
    args = ap.parse_args()

    coins = ["BTC", "ETH"] if args.coin == "BOTH" else [args.coin.upper()]
    print("청산 안전선 스캔  (사다리 -12/-14/-27%, 격리 롱)")
    for c in coins:
        run_coin(c, args.days)


if __name__ == "__main__":
    main()
