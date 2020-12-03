import requests
from dataclasses import dataclass, asdict
import arrow
import os
import json
import pandas as pd
from datetime import datetime
from typing import List
from tqdm import tqdm
import time

if "NEWS_API_TOKEN" not in os.environ:
    raise Exception("Please add `NEWS_API_TOKEN` to your env vars")


@dataclass
class NewsItem:
    date: str
    title: str
    source: str
    url: str
    text: str
    sentiment: str
    tickers: List[str]


def get_hist_news(symbol: str) -> List[NewsItem]:
    url = f"https://stocknewsapi.com/api/v1?tickers={symbol}&items=50"
    url = f"{url}&token={os.environ['NEWS_API_TOKEN']}"
    news_items = []

    date = arrow.get("2019-03-01")

    while date < arrow.now():
        date_str = date.format("MMDDYYYY")
        date = date.shift(weeks=1)
        date_next_str = date.format("MMDDYYYY")

        try:
            # fetch news for specific date
            r = requests.get(f"{url}&date={date_str}-{date_next_str}")
            print(f"fetching {symbol} on {date_str}")

            news_items.extend(
                [
                    NewsItem(
                        date=item["date"],
                        title=item["title"],
                        source=item["source_name"],
                        url=item["news_url"],
                        text=item["text"],
                        sentiment=item["sentiment"],
                        tickers=item["tickers"],
                    )
                    for item in r.json()["data"]
                ]
            )

            time.sleep(0.1)

        except Exception as e:
            print(f"error getting news for {symbol} on {date_str}\n{e}\n{r.json()}")

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
    # stocks_df = pd.read_csv("../nas100.csv")
    # universe = stocks_df["Symbol"].values
    # fetch_hist_list(universe)
    fetch_hist_list(["AAPL"])
