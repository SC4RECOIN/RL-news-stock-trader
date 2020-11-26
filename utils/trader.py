import alpaca_trade_api as tradeapi
from typing import List
from dataclasses import dataclass
from typing import Tuple
import os
import arrow


if "APCA_API_KEY_ID" not in os.environ:
    raise Exception("Please add `APCA_API_KEY_ID` to your env vars")

if "APCA_API_SECRET_KEY" not in os.environ:
    raise Exception("Please add `APCA_API_SECRET_KEY` to your env vars")

if "BASE_URL" not in os.environ:
    raise Exception("Please add `BASE_URL` to your env vars for Alpaca")


class Trader(object):
    def __init__(self, hold_period=7, max_hold=10, starting_balance=30000):
        self.starting_balance = starting_balance
        self.balance = starting_balance
        self.positions = {}

        # arbitrary parameters
        self.hold_period = hold_period
        self.max_hold = max_hold

    def trade_on_signal(self, symbol: str, signal: str, timestamp: int):
        """
        Trades on signal from RL model.
        Buy if bullish. Sell if in position on Bearish.
        Hold position until `self.hold_period` if no bearish signals.
        Currently does not short on bearish signals.
        Can only hold `self.max_hold` positions.
        """
        if signal == "BULLISH" and len(self.positions.keys()) < self.max_hold:
            self.rebalance(symbol, timestamp)

        elif signal == "BEARISH" and symbol in self.positions:
            self.rebalance(symbol, timestamp, True)

    def rebalance(self, symbol: str, timestamp: int, remove=False):
        positions = self.positions.keys()
        quotes = get_prices(positions, timestamp)

        # sell all positions
        for symbol, qty in self.positions.items():
            self.balance += quotes[symbol] * qty

        if remove:
            positions = [p for p in positions if p != symbol]
        else:
            positions.append(symbol)

        # re-enter positions
        self.positions = []
        target_val = len(self.positions.keys()) / self.balance
        for symbol in positions:
            qty = target_val // quotes[symbol]
            self.balance -= qty * quotes[symbol]
            self.positions[symbol] = qty

    def reward(self, timestamp: int):
        """
        Reward is the current ROI.
        Calculates current value of positions over intial capital.
        """
        return 0

    @staticmethod
    def get_prices(symbols: List[str], timestamp: int):
        alpaca = tradeapi.REST()
        bars = 10

        # set window
        start = arrow.get(timestamp)
        end = start.shift(minutes=bars)

        quotes = alpaca.get_barset(
            symbols, "1Min", limit=bars, start=start.isoformat(), end=end.isoformat()
        )

        return {symbol: quotes[symbol][0].c for symbol in symbols}
