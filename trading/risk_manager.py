"""
리스크 제어 레이어.
주문 실행 전 반드시 check()를 호출해 통과 여부를 확인합니다.

체크 항목:
  1. is_halted — 시스템 정지 여부
  2. 최대 동시 포지션 3개 초과 여부
  3. 1회 거래 한도 초과 여부
  4. 일일 손실 한도 초과 여부 (실현손실 기준)
"""
from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL       = os.environ.get("DATABASE_URL", "")
MAX_OPEN_POSITIONS = 2  # BTC 50% / ETH 50%

# 청산 가드 (backtest/liq_sweep.py 3년치 16개 급락 통계 기반)
#   BTC 5x ≈ 생존 88% / ETH 3x ≈ 75%. 그 이상은 청산빔 위험 급증.
#   ETH가 BTC보다 변동성이 커서 더 낮게 잡음.
MMR                  = 0.005             # 격리마진 유지증거금률 근사
LEVERAGE_CAP         = {"BTC": 5, "ETH": 3}
DEFAULT_LEVERAGE_CAP = 3                  # 기타 코인 보수적


def liq_price_long(avg_entry: float, leverage: int, mmr: float = MMR) -> float:
    """격리마진 롱 청산가 근사: 평단 × (1 − 1/레버 + 유지증거금률)."""
    return avg_entry * (1 - 1.0 / leverage + mmr)


def safe_max_leverage(avg_entry: float, lowest_support: float, mmr: float = MMR) -> int | None:
    """청산가가 lowest_support 아래가 되는 최대 정수 레버리지."""
    if not avg_entry or not lowest_support or lowest_support >= avg_entry:
        return None
    denom = 1 + mmr - (lowest_support / avg_entry)
    if denom <= 0:
        return 999
    return max(1, int(1.0 / denom))


# ── DB ────────────────────────────────────────────────────────

def _db_connect():
    import psycopg2
    from urllib.parse import urlparse
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


def _get_settings() -> dict:
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mode, is_halted, max_trade_amount_krw, daily_loss_limit_krw
                FROM settings WHERE id = 1
            """)
            row = cur.fetchone()
            return {
                "mode":                  row[0],
                "is_halted":             row[1],
                "max_trade_amount_krw":  row[2],
                "daily_loss_limit_krw":  row[3],
            }
    finally:
        conn.close()


def _get_open_position_count(side: str) -> int:
    """같은 방향(LONG/SHORT) 오픈 포지션 수를 반환한다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM positions WHERE status = 'OPEN' AND side = %s",
                (side,),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _get_today_realized_loss_krw() -> int:
    """오늘 실현된 손실 합계를 원화로 반환한다 (손실만, 양수값)."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(realized_pnl_krw, 0)
                FROM daily_stats
                WHERE date = CURRENT_DATE
            """)
            row = cur.fetchone()
            if not row:
                return 0
            pnl = row[0]
            return abs(pnl) if pnl < 0 else 0
    finally:
        conn.close()


def _halt_system() -> None:
    """일일 손실 한도 초과 시 시스템을 정지시킨다."""
    conn = _db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE settings SET is_halted = TRUE, updated_at = NOW()
                WHERE id = 1
            """)
            cur.execute("""
                UPDATE daily_stats SET is_halted = TRUE
                WHERE date = CURRENT_DATE
            """)
        conn.commit()
    finally:
        conn.close()


# ── 리스크 체크 결과 ──────────────────────────────────────────

class RiskCheckResult:
    def __init__(self, passed: bool, reason: str = "") -> None:
        self.passed = passed
        self.reason = reason

    def __bool__(self) -> bool:
        return self.passed

    def __repr__(self) -> str:
        return f"RiskCheckResult(passed={self.passed}, reason={self.reason!r})"


# ── 리스크 매니저 ─────────────────────────────────────────────

class RiskManager:

    def check(self, trade_amount_krw: int, is_new_position: bool = True, signal_type: str = "BUY") -> RiskCheckResult:
        """
        주문 실행 전 리스크 체크를 수행한다.

        Args:
            trade_amount_krw:  이번 주문 금액 (원화 환산)
            is_new_position:   신규 진입 여부 (추가매수는 False)
            signal_type:       'BUY' 또는 'SELL' (방향별 포지션 수 체크에 사용)

        Returns:
            RiskCheckResult — passed=True면 주문 가능
        """
        settings = _get_settings()

        # 1. 시스템 정지 여부
        if settings["is_halted"]:
            return RiskCheckResult(False, "시스템이 정지 상태입니다. /status 확인 후 재개하세요.")

        # 2. 최대 동시 포지션 수 (신규 진입만, 같은 방향 기준)
        if is_new_position:
            side = "LONG" if signal_type == "BUY" else "SHORT"
            open_count = _get_open_position_count(side)
            if open_count >= MAX_OPEN_POSITIONS:
                return RiskCheckResult(
                    False,
                    f"{side} 최대 포지션 수 초과 ({open_count}/{MAX_OPEN_POSITIONS}). 기존 포지션 정리 후 재시도하세요."
                )

        # 3. 1회 거래 한도
        max_amount = settings["max_trade_amount_krw"]
        if trade_amount_krw > max_amount:
            return RiskCheckResult(
                False,
                f"1회 거래 한도 초과: {trade_amount_krw:,}원 > {max_amount:,}원"
            )

        # 4. 일일 손실 한도
        daily_loss    = _get_today_realized_loss_krw()
        daily_limit   = settings["daily_loss_limit_krw"]
        if daily_loss >= daily_limit:
            _halt_system()
            return RiskCheckResult(
                False,
                f"일일 손실 한도 초과: {daily_loss:,}원 >= {daily_limit:,}원. 시스템 정지."
            )

        return RiskCheckResult(True)

    def leverage_cap(self, coin: str) -> int:
        """코인별 안전 레버리지 상한."""
        return LEVERAGE_CAP.get((coin or "").upper(), DEFAULT_LEVERAGE_CAP)

    def check_liquidation(
        self,
        coin: str,
        leverage: int,
        avg_entry: float | None = None,
        lowest_support: float | None = None,
        side: str = "LONG",
    ) -> RiskCheckResult:
        """
        청산 가드 — 주문 직전 호출.
          1. 레버리지 안전상한 초과 차단
          2. (롱) 청산가가 유튜버 최저 지지보다 위면 차단 (지지 닿기 전 청산 위험)
        """
        coin = (coin or "").upper()
        cap = self.leverage_cap(coin)

        if leverage > cap:
            return RiskCheckResult(
                False,
                f"{coin} 레버리지 {leverage}x > 안전상한 {cap}x — 청산빔 위험으로 차단."
            )

        if side == "LONG" and avg_entry and lowest_support:
            liq = liq_price_long(avg_entry, leverage)
            if liq >= lowest_support:
                smax = safe_max_leverage(avg_entry, lowest_support)
                smax_txt = f" 안전 최대레버 ~{smax}x." if smax else ""
                return RiskCheckResult(
                    False,
                    f"{coin} 청산가 {liq:,.0f} ≥ 유튜버 최저지지 {lowest_support:,.0f} "
                    f"— 지지 닿기 전 청산 위험.{smax_txt}"
                )

        return RiskCheckResult(True)

    def record_loss(self, loss_krw: int) -> None:
        """손절 체결 시 실현손실을 daily_stats에 기록한다."""
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_stats (date, realized_pnl_krw)
                    VALUES (CURRENT_DATE, %s)
                    ON CONFLICT (date) DO UPDATE
                    SET realized_pnl_krw = daily_stats.realized_pnl_krw + EXCLUDED.realized_pnl_krw
                """, (-abs(loss_krw),))
            conn.commit()
        finally:
            conn.close()

    def record_profit(self, profit_krw: int) -> None:
        """익절 체결 시 실현수익을 daily_stats에 기록한다."""
        conn = _db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_stats (date, realized_pnl_krw)
                    VALUES (CURRENT_DATE, %s)
                    ON CONFLICT (date) DO UPDATE
                    SET realized_pnl_krw = daily_stats.realized_pnl_krw + EXCLUDED.realized_pnl_krw
                """, (abs(profit_krw),))
            conn.commit()
        finally:
            conn.close()
