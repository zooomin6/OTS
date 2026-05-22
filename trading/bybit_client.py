"""
Bybit API 래퍼 (pybit 사용).
다른 모듈은 이 클래스만 호출하면 되고 Bybit API 세부사항은 몰라도 됩니다.

Hedge mode (양방향) + Isolated margin 기준.
positionIdx: 0 = 롱, 1 = 숏
"""
from __future__ import annotations

import os
from dotenv import load_dotenv

load_dotenv()

BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_TESTNET    = os.environ.get("BYBIT_TESTNET", "true").lower() == "true"


class BybitClient:
    def __init__(self) -> None:
        from pybit.unified_trading import HTTP
        self._session = HTTP(
            testnet=BYBIT_TESTNET,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )

    # ── 주문 ──────────────────────────────────────────────────

    def place_order(
        self,
        symbol: str,          # 예: "BTCUSDT"
        side: str,            # "Buy" | "Sell"
        qty: float,           # 수량 (코인 단위)
        price: float,         # 지정가
        leverage: int,
        position_idx: int,    # 0=롱, 1=숏
        stop_loss: float | None = None,
        take_profit: float | None = None,
    ) -> dict:
        """지정가 주문을 실행하고 Bybit 응답을 반환한다."""
        # 레버리지 먼저 설정
        self._set_leverage(symbol, leverage)

        params: dict = {
            "category":     "linear",
            "symbol":       symbol,
            "side":         side,
            "orderType":    "Limit",
            "qty":          str(qty),
            "price":        str(price),
            "positionIdx":  position_idx,
            "timeInForce":  "GTC",  # Good Till Cancel
        }
        if stop_loss:
            params["stopLoss"] = str(stop_loss)
        if take_profit:
            params["takeProfit"] = str(take_profit)

        resp = self._session.place_order(**params)
        self._raise_if_error(resp)
        return resp["result"]

    def cancel_order(self, symbol: str, order_id: str) -> None:
        """미체결 주문을 취소한다."""
        resp = self._session.cancel_order(
            category="linear",
            symbol=symbol,
            orderId=order_id,
        )
        self._raise_if_error(resp)

    # ── TP/SL 수정 ────────────────────────────────────────────

    def set_tp_sl(
        self,
        symbol: str,
        position_idx: int,
        take_profit: float | None = None,
        stop_loss: float | None = None,
    ) -> None:
        """기존 포지션의 TP/SL을 변경한다."""
        params: dict = {
            "category":    "linear",
            "symbol":      symbol,
            "positionIdx": position_idx,
        }
        if take_profit is not None:
            params["takeProfit"] = str(take_profit)
        if stop_loss is not None:
            params["stopLoss"] = str(stop_loss)

        resp = self._session.set_trading_stop(**params)
        self._raise_if_error(resp)

    # ── 조회 ──────────────────────────────────────────────────

    def get_position(self, symbol: str) -> list[dict]:
        """현재 오픈 포지션 목록을 반환한다 (롱/숏 모두)."""
        resp = self._session.get_positions(
            category="linear",
            symbol=symbol,
        )
        self._raise_if_error(resp)
        return resp["result"]["list"]

    def get_balance(self) -> float:
        """USDT 가용 잔고를 반환한다."""
        resp = self._session.get_wallet_balance(
            accountType="CONTRACT",
            coin="USDT",
        )
        self._raise_if_error(resp)
        coins = resp["result"]["list"][0]["coin"]
        for c in coins:
            if c["coin"] == "USDT":
                return float(c["availableToWithdraw"])
        return 0.0

    # ── 내부 헬퍼 ─────────────────────────────────────────────

    def _set_leverage(self, symbol: str, leverage: int) -> None:
        """레버리지를 설정한다. 이미 같은 값이면 Bybit이 에러를 반환하므로 무시한다."""
        try:
            self._session.set_leverage(
                category="linear",
                symbol=symbol,
                buyLeverage=str(leverage),
                sellLeverage=str(leverage),
            )
        except Exception:
            pass  # 동일 레버리지 재설정 시 Bybit이 에러 반환 — 무시

    @staticmethod
    def _raise_if_error(resp: dict) -> None:
        """Bybit API 응답에 에러가 있으면 예외를 발생시킨다."""
        if resp.get("retCode", 0) != 0:
            raise RuntimeError(
                f"Bybit API 에러 {resp['retCode']}: {resp.get('retMsg', '')}"
            )
