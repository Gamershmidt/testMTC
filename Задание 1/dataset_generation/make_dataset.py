import pandas as pd
import numpy as np
from dataclasses import make_dataclass

Text = make_dataclass("Text", [("title", str), ("text", str), ("label", int), ("url", str)])

data_gen = pd.read_csv("../data/01_prpr_data.csv")
data_gen_news = pd.read_csv("../data/02_adds_by_news.csv")
data_news = pd.read_csv("../data/lenta-ru-news.csv")

np.random.seed(100)

all_topics = np.unique(data_news["topic"][data_news["topic"].notna()].values)
topics_for_gen = ["Дом", "Из жизни", "Интернет и СМИ", "Наука и техника", "Путешествия", "Спорт"]
other_topics = [topic for topic in all_topics if topic not in topics_for_gen]

gen_numbers = [500, 500, 500, 500, 500, 500]
other_topics_size = 3000
news_indexes = []
for topic, size in zip(topics_for_gen, gen_numbers):
    indexes = data_news[data_news["topic"] == topic]["text"].index
    news_indexes += list(np.random.choice(indexes, size=size, replace=False))

indexes = data_news[data_news["topic"].isin(other_topics)]["text"].index
news_indexes += list(np.random.choice(indexes, size=other_topics_size, replace=False))


data_news = data_news.iloc[news_indexes]

texts = []
for index, row in data_news.iterrows():
    texts.append(Text(row["title"], row["text"], 0, row["url"]))
data_news = pd.DataFrame(texts)

all_data = pd.concat([data_gen, data_gen_news, data_news])

all_data.to_csv("final_dataset.csv", index=False)