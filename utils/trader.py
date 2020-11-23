from typing import List


class Trader(object):
    def __init__(self, starting_balance=30000):
        self.starting_balance = starting_balance
        self.balance = starting_balance

    def trade_on_signal(self, symbol: str, signal: str, timestamp: int):
        raise NotImplementedError()

    def rebalance(self, symbols: List[str]):
        raise NotImplementedError()

    def reward(self):
        return 0
