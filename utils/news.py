import requests
from dataclasses import dataclass
import arrow
import os
import json
import pandas as pd
from datetime import datetime
from typing import List
from tqdm import tqdm
import time

if "POLYGON_KEY" not in os.environ:
    raise Exception("Please add `POLYGON_KEY` to your env vars")


@dataclass
class NewsItem:
    timestamp: str
    title: str
    source: str
    url: str
    summary: str
    keywords: List[str]
    symbol: str


def get_hist_news(symbol: str) -> List[NewsItem]:
    url = f"https://api.polygon.io/v1/meta/symbols/{symbol}/news"
    url = f"{url}?apiKey={os.environ['POLYGON_KEY']}&perpage=50"
    date, page = arrow.now(), 1
    news_items = []

    # collect all news after 2018
    while date > arrow.get("2019-01-01"):
        try:
            r = requests.get(f"{url}&page={page}")
            news_items.extend(
                [
                    NewsItem(
                        timestamp=item["timestamp"],
                        title=item["title"],
                        source=item["source"],
                        url=item["url"],
                        summary=item["summary"],
                        keywords=item["keywords"],
                        symbol=symbol,
                    )
                    for item in r.json()
                ]
            )

            date = arrow.get(news_items[-1].timestamp)
            page += 1

            if len(r.json()) == 0:
                break

        except Exception as e:
            print(f"error getting news for {symbol}")

    return news_items


def fetch_hist_list(symbols: List[str]) -> pd.DataFrame:
    news_dir = "../hist_news"
    if not os.path.exists(news_dir):
        os.makedirs(news_dir)

    for symbol in tqdm(symbols, total=len(symbols)):
        # fetch already
        if os.path.exists(f"{news_dir}/{symbol}.json"):
            continue

        news = get_hist_news(symbol)
        news = pd.DataFrame(news)

        result = news.to_json(orient="records")
        parsed = json.loads(result)

        with open(f"{news_dir}/{symbol}.json", "w") as f:
            json.dump(parsed, f, indent=4)


if __name__ == "__main__":
    stocks_df = pd.read_csv("../nas100.csv")
    universe = stocks_df["Symbol"].values
    fetch_hist_list(universe)
