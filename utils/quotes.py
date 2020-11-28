import sqlite3
import alpaca_trade_api as tradeapi
import arrow
from typing import List
import pandas as pd
import time


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

        # shift 4 hours (UTC -> EST)
        target_date = arrow.get(timestamp).shift(hours=-4).format("YYYY-MM-DD HH:mm:ss")
        max_date = (
            # 3hrs 50min
            arrow.get(timestamp)
            .shift(minutes=-220)
            .format("YYYY-MM-DD HH:mm:ss")
        )

        # check for quotes in db NEED TO LIST RANGE OF ts
        for symbol in symbols:
            query = f"""
                SELECT close FROM quotes
                WHERE ts >= '{target_date}' AND ts < '{max_date}'
                AND symbol = '{symbol}'
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


def prefetch_quotes(symbols, timeframe="5Min", start="2019-01-01"):
    """
    Fetch quotes for all ticker so that the QuoteDB is fully loaded.
    If quotes are missing then they will be fetched so running this isnt
    necessary, however, it will speed up training significantly
    """
    storage = QuoteDB()
    alpaca = tradeapi.REST()
    stop = arrow.get(start)
    end = arrow.get()

    df = pd.DataFrame([])
    while end > stop:
        all_quotes = alpaca.get_barset(
            symbols, timeframe, limit=1000, end=end.isoformat()
        )
        storage._insert_quotes(all_quotes)

        for symbol in symbols:
            quotes = [
                {
                    "High": q.h,
                    "Low": q.l,
                    "Close": q.c,
                    "Open": q.o,
                    "Time": q.t,
                    "Symbol": symbol,
                }
                for q in all_quotes[symbol]
            ]
            df = pd.concat([df, pd.DataFrame(quotes)])

            if len(quotes) > 0:
                end = arrow.get(quotes[0]["Time"])

        # abide by ratelimit
        print(f"{end.format('YYYY-MM-DD')}\tlen: {len(df)}")
        time.sleep(0.4)

    df = df.sort_values(by="Time")
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    df.to_pickle("quotes.pkl")


if __name__ == "__main__":
    stocks_df = pd.read_csv("../nas100.csv")
    universe = stocks_df["Symbol"].values
    prefetch_quotes(universe)
