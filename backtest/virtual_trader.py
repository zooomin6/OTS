"""
가상 트레이딩 시뮬레이션 엔진.

하나의 트레이드 스레드(오프닝 BUY/SELL + 후속 HOLD 업데이트들)를
Bybit 과거 캔들 데이터로 시뮬레이션하고 P&L을 계산한다.

전략 (CLAUDE.md 기준):
  - 1차 진입: entry_price_1 도달 시 자본 50%
  - DCA 1   : entry_price_2 도달 시 자본 25%
  - DCA 2   : entry_price_3 도달 시 자본 25%
  - 1차 익절: take_profit_price 도달 → 50% 청산 + SL → TP1 이동
  - 2차 익절: take_profit_price_2 도달 → 나머지 전량
  - 손절    : stop_loss_price(동적) 도달 → 전량 청산
  - 만료    : expires_at 이후 첫 캔들 종가로 강제 청산
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

VIRTUAL_CAPITAL = 1_000.0   # USDT

# 진입 전략:
# - 1차 진입: entry_price_2 (또는 entry_price_1) → 자본 70%
# - 2차 진입(DCA): absolute_stop이 명시된 경우에만 → 자본 30%
#   (유튜버가 "이 라인은 지켜야 한다"고 말한 마지노선 구간)
# - 손절: stop_loss_price (마지노선 아래)
LOT_MAIN = 0.70   # 1차 진입 비중
LOT_DCA  = 0.30   # 마지노선 DCA 비중 (absolute_stop 있을 때만)


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass
class SimulationResult:
    analysis_id: int
    coin: str
    side: str                        # "LONG" | "SHORT"
    entry_hit: bool
    entries: list[dict] = field(default_factory=list)
    exits: list[dict]   = field(default_factory=list)
    avg_entry: float    = 0.0
    final_pnl_pct: float = 0.0
    feedback: str | None = None      # "CORRECT" | "INCORRECT" | None
    trade_log: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            "analysis_id":   self.analysis_id,
            "coin":          self.coin,
            "side":          self.side,
            "entry_hit":     self.entry_hit,
            "entries":       self.entries,
            "exits":         self.exits,
            "avg_entry":     round(self.avg_entry, 4),
            "final_pnl_pct": round(self.final_pnl_pct, 4),
            "feedback":      self.feedback,
            "trade_log":     self.trade_log,
        }


def simulate_thread(
    opening: dict,
    updates: list[dict],
    candles: list[dict],
) -> SimulationResult:
    """
    Parameters
    ----------
    opening  : BUY/SELL 신호 분석 레코드 (dict)
    updates  : 같은 기간의 HOLD 업데이트 분석들 (시간순)
    candles  : [{time, open, high, low, close}, ...] 시간순

    Returns
    -------
    SimulationResult
    """
    analysis_id = opening["id"]
    coin        = opening["coin_symbol"] or "UNKNOWN"
    signal      = opening["signal_type"]  # "BUY" | "SELL"
    side        = "LONG" if signal == "BUY" else "SHORT"
    expires_at  = opening["expires_at"]
    expires_ms  = int(expires_at.timestamp() * 1000) if expires_at else None

    result = SimulationResult(analysis_id=analysis_id, coin=coin, side=side, entry_hit=False)

    # ── 진입가 / 손절 / 목표 설정 ──
    if signal == "BUY":
        # 주 진입가: entry_price_2 우선, 없으면 entry_price_1
        main_entry = _to_float(opening.get("entry_price_2")) or _to_float(opening.get("entry_price_1"))
        # DCA 진입가: absolute_stop (유튜버가 "이 라인 지켜야 한다"고 명시한 경우만)
        #   - 주 진입가보다 의미 있게 낮아야 DCA로 사용 (2% 이상 아래)
        abs_stop_val = _to_float(opening.get("absolute_stop"))
        dca_entry = abs_stop_val if (
            abs_stop_val and main_entry and abs_stop_val < main_entry * 0.98
        ) else None
        sl  = _to_float(opening.get("stop_loss_price"))
        tp1 = _to_float(opening.get("take_profit_price"))
        tp2 = None
    else:  # SELL (숏)
        main_entry = _to_float(opening.get("short_entry_price"))
        dca_entry  = None
        sl  = _to_float(opening.get("short_stop_loss"))
        tp1 = _to_float(opening.get("take_profit_price"))
        tp2 = None

    if not main_entry:
        result.trade_log.append("진입가 없음 → 스킵")
        return result

    if tp1 is None:
        result.trade_log.append("목표가 없음 → 스킵")
        return result

    # ── 상태 변수 ──
    main_filled  = False
    dca_filled   = False
    total_cost   = 0.0
    total_qty    = 0.0
    current_sl   = sl
    current_tp1  = tp1
    current_tp2  = tp2
    tp1_done     = False
    realized_pnl = 0.0

    # 업데이트 분석들을 시간→ (tp1, tp2, sl) 딕셔너리로 변환
    update_map: list[tuple[int, dict]] = []
    for upd in updates:
        upd_time = upd.get("created_at")
        if upd_time:
            upd_ms = int(upd_time.timestamp() * 1000)
            update_map.append((upd_ms, upd))
    update_map.sort(key=lambda x: x[0])
    update_idx = 0

    dca_label = f" | DCA:{dca_entry:.2f}" if dca_entry else ""
    result.trade_log.append(
        f"시뮬레이션 시작 | {side} | 진입가:{main_entry:.2f}{dca_label} | TP1:{tp1} | SL:{sl}"
    )

    # ── 캔들 순회 ──
    for candle in candles:
        c_time  = candle["time"]
        c_high  = candle["high"]
        c_low   = candle["low"]
        c_close = candle["close"]

        # 만료 체크
        if expires_ms and c_time > expires_ms:
            # 포지션이 열려있으면 강제 청산
            if total_qty > 0:
                pnl = _calc_pnl(side, total_qty, total_cost, c_close)
                realized_pnl += pnl
                result.exits.append({"price": c_close, "qty": total_qty, "reason": "EXPIRE", "time": c_time})
                result.trade_log.append(f"만료 강제청산 @ {c_close:.2f} → P&L {pnl:+.2f} USDT")
                total_qty = 0.0
            break

        # 업데이트 분석 TP/SL 반영
        while update_idx < len(update_map):
            upd_ms, upd = update_map[update_idx]
            if upd_ms > c_time:
                break
            new_tp = _to_float(upd.get("take_profit_price"))
            new_sl = _to_float(upd.get("stop_loss_price"))
            if new_tp and new_tp != current_tp1:
                current_tp1 = new_tp
                result.trade_log.append(f"TP1 업데이트 → {current_tp1}")
            if new_sl is not None and new_sl != current_sl:
                current_sl = new_sl
                result.trade_log.append(f"SL 업데이트 → {current_sl}")
            update_idx += 1

        # ── BUY (롱) 로직 ──
        if side == "LONG":
            # 주 진입
            if not main_filled and c_low <= main_entry:
                main_filled = True
                capital = VIRTUAL_CAPITAL * (LOT_MAIN if dca_entry else 1.0)
                qty = capital / main_entry
                total_cost += capital
                total_qty  += qty
                result.entry_hit = True
                result.entries.append({"price": main_entry, "qty": qty,
                                       "time": c_time, "type": "ENTRY_MAIN"})
                result.trade_log.append(f"주진입 @ {main_entry:.2f} qty={qty:.6f}")

            # DCA (absolute_stop 도달 시)
            if dca_entry and main_filled and not dca_filled and c_low <= dca_entry:
                dca_filled = True
                capital = VIRTUAL_CAPITAL * LOT_DCA
                qty = capital / dca_entry
                total_cost += capital
                total_qty  += qty
                result.entries.append({"price": dca_entry, "qty": qty,
                                       "time": c_time, "type": "DCA_ABSOLUTE"})
                result.trade_log.append(f"DCA(마지노선) @ {dca_entry:.2f} qty={qty:.6f}")

            if total_qty == 0:
                continue

            avg = total_cost / total_qty

            # 손절
            if current_sl and c_low <= current_sl:
                pnl = _calc_pnl(side, total_qty, total_cost, current_sl)
                realized_pnl += pnl
                result.exits.append({"price": current_sl, "qty": total_qty, "reason": "SL", "time": c_time})
                result.trade_log.append(f"손절 @ {current_sl:.2f} avg={avg:.2f} → P&L {pnl:+.2f} USDT")
                total_qty = 0.0
                break

            # 1차 익절: 포지션 수익률 +20% 도달 시 50% 청산 (유튜버 TP 또는 +20% 중 먼저)
            tp1_trigger = current_tp1 or (avg * 1.20)
            if not tp1_done and c_high >= tp1_trigger:
                close_qty = total_qty * 0.5
                pnl = _calc_pnl(side, close_qty, total_cost * 0.5, tp1_trigger)
                realized_pnl += pnl
                result.exits.append({"price": tp1_trigger, "qty": close_qty, "reason": "TP1(+20%)", "time": c_time})
                result.trade_log.append(f"1차익절(+20%) @ {tp1_trigger:.2f} qty={close_qty:.6f} → P&L {pnl:+.2f} USDT")
                total_qty   -= close_qty
                total_cost  *= 0.5
                tp1_done     = True
                current_sl   = avg  # 1차 익절 후 SL을 진입가로 이동 (본전 보호)
                result.trade_log.append(f"SL 본전 이동 → {current_sl:.2f}")

            # 2차 익절: +40% 또는 유튜버 TP2 (둘 중 먼저)
            tp2_trigger = current_tp2 or (avg * 1.40)
            if tp1_done and c_high >= tp2_trigger and total_qty > 0:
                pnl = _calc_pnl(side, total_qty, total_cost, tp2_trigger)
                realized_pnl += pnl
                result.exits.append({"price": tp2_trigger, "qty": total_qty, "reason": "TP2(+40%)", "time": c_time})
                result.trade_log.append(f"2차익절(+40%) @ {tp2_trigger:.2f} → P&L {pnl:+.2f} USDT")
                total_qty = 0.0
                break

        # ── SELL (숏) 로직 ──
        else:
            # 숏 단일 진입
            if not main_filled and c_high >= main_entry:
                main_filled = True
                qty = VIRTUAL_CAPITAL / main_entry
                total_cost += VIRTUAL_CAPITAL
                total_qty  += qty
                result.entry_hit = True
                result.entries.append({"price": main_entry, "qty": qty,
                                       "time": c_time, "type": "SHORT_ENTRY"})
                result.trade_log.append(f"숏진입 @ {main_entry:.2f} qty={qty:.6f}")

            if total_qty == 0:
                continue

            # 숏 손절 (가격 상승)
            if current_sl and c_high >= current_sl:
                pnl = _calc_pnl(side, total_qty, total_cost, current_sl)
                realized_pnl += pnl
                result.exits.append({"price": current_sl, "qty": total_qty, "reason": "SL", "time": c_time})
                result.trade_log.append(f"숏손절 @ {current_sl:.2f} → P&L {pnl:+.2f} USDT")
                total_qty = 0.0
                break

            # 숏 1차 익절 (가격 하락)
            if not tp1_done and c_low <= current_tp1:
                close_qty = total_qty * 0.5
                pnl = _calc_pnl(side, close_qty, total_cost * 0.5, current_tp1)
                realized_pnl += pnl
                result.exits.append({"price": current_tp1, "qty": close_qty, "reason": "TP1", "time": c_time})
                result.trade_log.append(f"숏1차익절 @ {current_tp1:.2f} → P&L {pnl:+.2f} USDT")
                total_qty  -= close_qty
                total_cost *= 0.5
                tp1_done    = True

            # 숏 2차 익절
            if tp1_done and current_tp2 and c_low <= current_tp2 and total_qty > 0:
                pnl = _calc_pnl(side, total_qty, total_cost, current_tp2)
                realized_pnl += pnl
                result.exits.append({"price": current_tp2, "qty": total_qty, "reason": "TP2", "time": c_time})
                result.trade_log.append(f"숏2차익절 @ {current_tp2:.2f} → P&L {pnl:+.2f} USDT")
                total_qty = 0.0
                break

    # ── 결과 계산 ──
    if not result.entry_hit:
        result.trade_log.append("진입가 미도달 → 스킵")
        return result

    result.avg_entry    = total_cost / total_qty if total_qty > 0 else (
        sum(e["price"] * e["qty"] for e in result.entries) /
        max(sum(e["qty"] for e in result.entries), 1e-10)
    )
    invested = sum(e["price"] * e["qty"] for e in result.entries)
    result.final_pnl_pct = (realized_pnl / max(invested, 1e-10)) * 100 if invested > 0 else 0.0
    result.feedback = "CORRECT" if result.final_pnl_pct > 0 else "INCORRECT"
    result.trade_log.append(
        f"최종 P&L: {result.final_pnl_pct:+.2f}%  → {result.feedback}"
    )
    return result


def _calc_pnl(side: str, qty: float, cost: float, exit_price: float) -> float:
    """USDT 기준 실현 P&L 계산 (레버리지 1배)."""
    avg_entry = cost / max(qty, 1e-10)
    if side == "LONG":
        return (exit_price - avg_entry) * qty
    else:
        return (avg_entry - exit_price) * qty
