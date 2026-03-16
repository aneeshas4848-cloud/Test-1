from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class PaperPositionATR:
    side: str
    qty: int
    entry_time: datetime
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    is_open: bool = True
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None

    # kept for compatibility with existing bot/reporting code
    profit_lock_mode: Optional[str] = None
    profit_ladder_step: Optional[float] = None
    profit_ladder_max: Optional[float] = None
    profit_ladder_buffer: Optional[float] = None
    profit_ladder_confirm_candles: Optional[int] = None

    def close(self, ts: datetime, price: float, reason: str) -> None:
        self.is_open = False
        self.exit_time = ts
        self.exit_price = float(price)
        self.exit_reason = reason

    def pnl(self) -> float:
        if self.exit_price is None:
            return 0.0

        if self.side == "LONG":
            return (float(self.exit_price) - float(self.entry_price)) * int(self.qty)

        if self.side == "SHORT":
            return (float(self.entry_price) - float(self.exit_price)) * int(self.qty)

        return 0.0


class PaperBrokerATR:
    def __init__(self):
        self.position: Optional[PaperPositionATR] = None

    def has_open_position(self) -> bool:
        return self.position is not None and self.position.is_open

    def open_long(
        self,
        ts: datetime,
        price: float,
        qty: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        profit_lock_mode: Optional[str] = None,
        profit_ladder_step: Optional[float] = None,
        profit_ladder_max: Optional[float] = None,
        profit_ladder_buffer: Optional[float] = None,
        profit_ladder_confirm_candles: Optional[int] = None,
    ) -> PaperPositionATR:
        if self.has_open_position():
            raise RuntimeError("Position already open")

        self.position = PaperPositionATR(
            side="LONG",
            qty=int(qty),
            entry_time=ts,
            entry_price=float(price),
            stop_loss=None if stop_loss is None else float(stop_loss),
            take_profit=None if take_profit is None else float(take_profit),
            profit_lock_mode=profit_lock_mode,
            profit_ladder_step=profit_ladder_step,
            profit_ladder_max=profit_ladder_max,
            profit_ladder_buffer=profit_ladder_buffer,
            profit_ladder_confirm_candles=profit_ladder_confirm_candles,
        )
        return self.position

    def open_short(
        self,
        ts: datetime,
        price: float,
        qty: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        profit_lock_mode: Optional[str] = None,
        profit_ladder_step: Optional[float] = None,
        profit_ladder_max: Optional[float] = None,
        profit_ladder_buffer: Optional[float] = None,
        profit_ladder_confirm_candles: Optional[int] = None,
    ) -> PaperPositionATR:
        if self.has_open_position():
            raise RuntimeError("Position already open")

        self.position = PaperPositionATR(
            side="SHORT",
            qty=int(qty),
            entry_time=ts,
            entry_price=float(price),
            stop_loss=None if stop_loss is None else float(stop_loss),
            take_profit=None if take_profit is None else float(take_profit),
            profit_lock_mode=profit_lock_mode,
            profit_ladder_step=profit_ladder_step,
            profit_ladder_max=profit_ladder_max,
            profit_ladder_buffer=profit_ladder_buffer,
            profit_ladder_confirm_candles=profit_ladder_confirm_candles,
        )
        return self.position

    def close(self, ts: datetime, price: float, reason: str = "MANUAL_EXIT") -> Optional[PaperPositionATR]:
        if not self.has_open_position():
            return None

        self.position.close(ts, float(price), reason)
        closed = self.position
        self.position = None
        return closed

    def check_exits(
        self,
        ts: datetime,
        price: float,
        is_candle_close: bool = False,
    ) -> Optional[PaperPositionATR]:
        """
        ATR bot version:
        - handles stop loss
        - handles take profit if provided
        - does NOT run profit ladder logic
        - ATR trailing is expected to update self.position.stop_loss externally
        """
        if not self.has_open_position():
            return None

        pos = self.position
        px = float(price)

        if pos.side == "LONG":
            # stop loss
            if pos.stop_loss is not None and px <= float(pos.stop_loss):
                pos.close(ts, px, "STOP_LOSS")
                closed = pos
                self.position = None
                return closed

            # optional take profit
            if pos.take_profit is not None and px >= float(pos.take_profit):
                pos.close(ts, px, "TAKE_PROFIT")
                closed = pos
                self.position = None
                return closed

        elif pos.side == "SHORT":
            # stop loss
            if pos.stop_loss is not None and px >= float(pos.stop_loss):
                pos.close(ts, px, "STOP_LOSS")
                closed = pos
                self.position = None
                return closed

            # optional take profit
            if pos.take_profit is not None and px <= float(pos.take_profit):
                pos.close(ts, px, "TAKE_PROFIT")
                closed = pos
                self.position = None
                return closed

        return None
