from sentence_transformers import SentenceTransformer
from typing import List
import string
import numpy as np

model = SentenceTransformer("distilbert-base-nli-stsb-mean-tokens")


def preprocessing(sentences: List[str]) -> List[str]:
    transform = lambda s: s.lower().translate(str.maketrans("", "", string.punctuation))
    return [transform(s) for s in sentences]


def encode(sentences: List[str]) -> np.ndarray:
    sentences = preprocessing(sentences)
    return model.encode(sentences)
