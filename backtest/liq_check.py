"""
청산빔 백테스트 — 중첩 DCA 사다리 + 레버리지 청산가 검증.

사용자 BTC 케이스를 실제 Bybit 일봉 꼬리(low)에 대입:
  사다리(분할 매수 라인): 72,417 / 71,150 / 60,000  (위→아래, 동일 규모)
  유튜버 아래 박스       : 56,000 ~ 58,000  (안 쓴 마지막 지지)
각 레버리지에서 실제 캔들 꼬리에 청산됐는지 / 살았는지 확인.

가정(명시):
  - 격리마진 롱, 각 분할을 동일 레버리지 L로 진입 (추가 증거금 없음)
  - 청산가 ≈ 평단 × (1 - 1/L + MMR), MMR=0.5% (유지증거금 근사, 수수료 무시)
  - 분할은 가격이 라인에 닿을 때 체결 (low <= line)
  - 캔들 내부에선 '가격 높은 이벤트 먼저' (위 분할 체결 vs 청산 경합 처리)

사용법:
    python -m backtest.liq_check
    python -m backtest.liq_check --ladder 72417,71150,60000 --box 56000,58000
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from backtest.price_fetcher import fetch_bybit_kline

LADDER     = [72417.0, 71150.0, 60000.0]   # 분할 매수 라인 (위→아래)
LOWER_BOX  = (56000.0, 58000.0)            # 유튜버 아래 박스 (안 쓴 마지막 지지)
LEVERAGES  = [3, 4, 5, 6, 8, 10]
MMR        = 0.005                          # 유지증거금률 근사
TRANCHE_USD = 1000.0                        # 분할당 투입 (동일 규모)
PEAK_BEFORE = 80000.0                       # 이 고점 이후 첫 하락을 진입 시작점으로


def liq_long(avg: float, lev: int) -> float:
    """격리마진 롱 청산가 근사."""
    return avg * (1 - 1.0 / lev + MMR)


def fmt(ms: int) -> str:
    return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d")


def locate_start(candles: list[dict]) -> int:
    """PEAK_BEFORE 이상 고점을 본 뒤 첫 사다리 최상단 하향 터치 캔들 인덱스."""
    seen_high = False
    for i, c in enumerate(candles):
        if c["high"] >= PEAK_BEFORE:
            seen_high = True
        if seen_high and c["low"] <= LADDER[0]:
            return i
    return 0


def simulate(candles: list[dict], lev: int, start_idx: int) -> dict:
    """한 레버리지에 대해 사다리 진입 + 청산 시뮬레이션."""
    filled = 0
    total_qty = 0.0
    total_usd = 0.0
    min_liq_gap = None   # 청산가까지 가장 가까웠던 % (생존 시)

    for c in candles[start_idx:]:
        low = c["low"]
        while True:
            avg = (total_usd / total_qty) if total_qty > 0 else None
            liq = liq_long(avg, lev) if avg else -1.0
            nxt = LADDER[filled] if filled < len(LADDER) else None

            # 캔들 안에서 더 높은 이벤트 먼저: 위 분할 체결이 청산보다 위면 먼저
            if nxt is not None and (avg is None or nxt > liq) and low <= nxt:
                total_usd += TRANCHE_USD
                total_qty += TRANCHE_USD / nxt
                filled += 1
                continue

            # 청산: 포지션 보유 + 캔들 저가가 청산가 이하
            if total_qty > 0 and low <= liq:
                return {
                    "liquidated": True, "date": c["time"], "liq": liq,
                    "avg": avg, "filled": filled, "low": low,
                }
            break

        if total_qty > 0:
            gap = (low - liq) / liq * 100  # 청산가 대비 저가 여유 %
            min_liq_gap = gap if min_liq_gap is None else min(min_liq_gap, gap)

    avg = (total_usd / total_qty) if total_qty > 0 else 0.0
    return {
        "liquidated": False, "liq": liq_long(avg, lev) if avg else 0.0,
        "avg": avg, "filled": filled, "min_liq_gap": min_liq_gap,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ladder", help="콤마구분 진입라인 (위→아래)")
    ap.add_argument("--box", help="콤마구분 아래박스 low,high")
    ap.add_argument("--days", type=int, default=900, help="가져올 일봉 기간")
    args = ap.parse_args()

    global LADDER, LOWER_BOX
    if args.ladder:
        LADDER = [float(x) for x in args.ladder.split(",")]
    if args.box:
        lo, hi = args.box.split(",")
        LOWER_BOX = (float(lo), float(hi))

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    print(f"[liq_check] BTC 일봉 로딩 ({fmt(start_ms)} ~ {fmt(end_ms)})...")
    candles = fetch_bybit_kline("BTC", "1d", start_ms, end_ms)
    print(f"[liq_check] 캔들 {len(candles)}개")

    s = locate_start(candles)
    seg = candles[s:]
    if not seg:
        print("진입 시작점(고점 후 사다리 터치)을 못 찾음. --days 늘리거나 --ladder 조정.")
        return

    seg_low = min(c["low"] for c in seg)
    seg_high = max(c["high"] for c in seg[:5])
    print(f"\n진입 시작: {fmt(seg[0]['time'])}  (직전 고점권 ~{seg_high:,.0f})")
    print(f"사다리: {' / '.join(f'{x:,.0f}' for x in LADDER)}")
    print(f"유튜버 아래 박스: {LOWER_BOX[0]:,.0f}~{LOWER_BOX[1]:,.0f}")
    print(f"이 구간 실제 최저 꼬리: {seg_low:,.0f}\n")

    print("╔════════════════════════════════════════════════════════════════╗")
    print(f"  레버 │ 체결 │ 평단     │ 청산가   │ 결과")
    print("  ─────┼──────┼──────────┼──────────┼─────────────────────────────")
    for lev in LEVERAGES:
        r = simulate(seg, lev, 0)
        if r["liquidated"]:
            verdict = f"💀 청산 @ {r['liq']:,.0f} ({fmt(r['date'])}, 꼬리 {r['low']:,.0f})"
        else:
            gap = r["min_liq_gap"]
            verdict = f"✅ 생존 (청산가까지 최소 {gap:+.1f}% 여유)"
        print(f"  {lev:>3}x │ {r['filled']}/{len(LADDER)}  │ {r['avg']:>8,.0f} │ {r['liq']:>8,.0f} │ {verdict}")
    print("╚════════════════════════════════════════════════════════════════╝")

    # 유튜버 아래 박스 대비 청산가 위치 비교
    print("\n[핵심] 유튜버 아래 박스 상단 = {:,.0f}".format(LOWER_BOX[1]))
    for lev in LEVERAGES:
        r = simulate(seg, lev, 0)
        rel = "위 ⚠️ (박스 닿기 전 청산)" if r["liq"] > LOWER_BOX[1] else "아래 ✅ (박스까지 생존)"
        print(f"  {lev:>3}x 청산가 {r['liq']:,.0f}  →  아래 박스보다 {rel}")


if __name__ == "__main__":
    main()
