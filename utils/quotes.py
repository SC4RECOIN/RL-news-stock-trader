import sqlite3
from utils.options_scraper import OptionEntry
import alpaca_trade_api as tradeapi
import arrow
from typing import List


class QuoteDB(object):
    def __init__(self):
        self.con = sqlite3.connect("quotes.db")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS quotes
            (
                id          TEXT    PRIMARY KEY     NOT NULL,
                symbol      TEXT                    NOT NULL,
                ts          DATE                    NOT NULL,
                open        REAL                            ,
                high        REAL                            ,
                low         REAL                            ,
                close       REAL                    NOT NULL,
                volume      REAL                            ,
            );
            """
        )

    def _insert_quotes(self, quotes):
        values = []
        for symbol, quotes in quotes.items():
            values.extend(
                [
                    f"(id, {symbol}, {q.t}, {q.o}, {q.h}, {q.l}, {q.c}, {q.v})"
                    for q in quotes
                ]
            )

        self.con.execute(
            f"""
            INSERT INTO quotes (id, symbol, ts, open, high, low, close, volume)
            VALUES {', '.join(values)}
            """
        )

    def get_quotes(self, symbols: List[str], timestamp: int):
        quotes = {}
        to_fetch = []

        # check for quotes in db
        for symbol in symbols:
            query = f"""
                SELECT close FROM quotes
                WHERE ts <= DATE({timestamp}) AND symbol = '{symbol}'
                ORDER BY ts
                LIMIT 1;
                """
            results = [row for row in self.con.execute(query)]
            if len(results) > 0:
                quotes[symbol] = results[0]
                continue

            to_fetch.append(symbol)

        if len(to_fetch) > 0:
            values = get_prices(to_fetch, timestamp)
            self._insert_quotes(values)
            for symbol in to_fetch:
                quotes[symbol] = values[symbol][0].c

        return quotes

    @staticmethod
    def get_prices(symbols: List[str], timestamp: int):
        alpaca = tradeapi.REST()
        bars = 10

        # set window
        start = arrow.get(timestamp)
        end = start.shift(minutes=bars)

        return alpaca.get_barset(
            symbols, "1Min", limit=bars, start=start.isoformat(), end=end.isoformat()
        )
