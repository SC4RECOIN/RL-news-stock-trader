import requests
from dataclasses import dataclass
import arrow
import os
import json
import pandas as pd
from datetime import datetime
from typing import List
from tqdm import tqdm

if "IEX_TOKEN" not in os.environ:
    raise Exception("Please add `IEX_TOKEN` to your env vars")


@dataclass
class NewsItem:
    datetime: int
    date: str
    headline: str
    source: str
    url: str
    summary: str
    related: List[str]
    symbol: str


def get_hist_news(symbol: str) -> List[NewsItem]:
    url = f"https://cloud.iexapis.com/v1/time-series/news/{symbol}"
    url = f"{url}?token={os.environ['IEX_TOKEN']}&limit=50"
    news_items = []

    date = arrow.get("2019-01-01")

    while date < arrow.now():
        date_str = date.format("YYYY-MM-DD")
        date = date.shift(days=1)

        try:
            # fetch news for specific date
            r = requests.get(f"{url}&on={date_str}")

            news_items.extend(
                [
                    NewsItem(
                        datetime=item["datetime"],
                        date=date_str,
                        headline=item["headline"],
                        source=item["source"],
                        url=item["url"],
                        summary=item["summary"],
                        related=item["related"].split(","),
                        symbol=symbol,
                    )
                    for item in r.json()
                ]
            )

        except Exception as e:
            print(f"error getting news for {symbol} on {date_str}\n{e}\n{r.json()}")

    return news_items


def fetch_hist_list(symbols: List[str]) -> pd.DataFrame:
    news_dir = "../hist_news"
    if not os.path.exists(news_dir):
        os.makedirs(news_dir)

    for symbol in tqdm(symbols, total=len(symbols)):
        news = get_hist_news(symbol)
        news = pd.DataFrame(news)

        result = news.to_json(orient="records")
        parsed = json.loads(result)

        with open(f"{news_dir}/{symbol}.json", "w") as f:
            json.dump(parsed, f, indent=4)


if __name__ == "__main__":
    fetch_hist_list(["AAPL"])
