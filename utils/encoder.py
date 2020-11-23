from sentence_transformers import SentenceTransformer
from typing import List
import string
import pandas as pd
import numpy as np
import json
import os

model = SentenceTransformer("distilbert-base-nli-stsb-mean-tokens")


def preprocessing(sentences: List[str]) -> List[str]:
    transform = lambda s: s.lower().translate(str.maketrans("", "", string.punctuation))
    return [transform(s) for s in sentences]


def encode(sentences: List[str]) -> np.ndarray:
    sentences = preprocessing(sentences)
    return model.encode(sentences)


if __name__ == "__main__":
    news_dir = "../hist_news"
    for filename in os.listdir(news_dir):
        with open(f"{news_dir}/{filename}") as f:
            news = json.load(f)

        # encode headline and summary from news item
        text = [
            f"{n['headline']}. "
            f"{n['summary'] if n['summary'] != 'No summary available.' else ''}"
            for n in news
        ]

        encodings = encode(text)
        for idx, encoding in enumerate(encodings):
            news[idx]["encoding"] = [round(x, 10) for x in encoding.tolist()]

        with open(f"{news_dir}/{filename}", "w") as f:
            json.dump(news, f)
