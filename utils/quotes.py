import sqlite3
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
                volume      REAL
            );
            """
        )

    def _insert_quotes(self, quotes):
        values = []
        for symbol, quotes in quotes.items():
            values.extend(
                [
                    f"('{symbol}{q.t}', '{symbol}', '{q.t}', {q.o}, {q.h}, {q.l}, {q.c}, {q.v})"
                    for q in quotes
                ]
            )

        self.con.execute(
            f"""
            INSERT OR IGNORE
            INTO quotes (id, symbol, ts, open, high, low, close, volume)
            VALUES {', '.join(values)}
            """
        )
        self.con.commit()

    def get_quotes(self, symbols: List[str], timestamp: int):
        quotes = {}
        to_fetch = []
        target_date = arrow.get(timestamp).isoformat()

        # check for quotes in db
        for symbol in symbols:
            query = f"""
                SELECT close FROM quotes
                WHERE ts <= '{target_date}' AND symbol = '{symbol}'
                ORDER BY ts
                LIMIT 1;
                """
            results = [row for row in self.con.execute(query)]

            if len(results) > 0:
                quotes[symbol] = results[0][0]
                continue

            to_fetch.append(symbol)

        if len(to_fetch) > 0:
            values = self._get_prices(to_fetch, timestamp)
            self._insert_quotes(values)
            for symbol in to_fetch:
                quotes[symbol] = values[symbol][0].c

        return quotes

    @staticmethod
    def _get_prices(symbols: List[str], timestamp: int):
        alpaca = tradeapi.REST()
        bars = 10
        end = arrow.get(timestamp).shift(minutes=5)

        return alpaca.get_barset(symbols, "1Min", limit=bars, end=end.isoformat())


print(QuoteDB().get_quotes(["SPY"], 1586369464))
