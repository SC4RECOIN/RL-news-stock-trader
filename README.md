# RL news stock trader

Train a reinforcement learning model to trade on stock news

Encoder - [Sentence Embeddings using distilBERT](https://github.com/UKPLab/sentence-transformers).  
RL model - [Phasic Policy Gradient](https://github.com/openai/phasic-policy-gradient)

## Setup

1. Fetch news
2. Encode news headlines and summaries
3. Prefetch quotes
4. Train
5. Profit?

```bash
cd utils
python3 news.py
python3 encoder.py
python3 quotes.py
cd ..
python3 train.py
```
