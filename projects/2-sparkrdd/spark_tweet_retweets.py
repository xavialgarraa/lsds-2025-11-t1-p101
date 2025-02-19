from pyspark import SparkConf, SparkContext
import sys
from dataclasses import dataclass
from typing import Optional
import json
import logging

# Configurar el nivel de log de Spark
logging.basicConfig(level=logging.CRITICAL)

@dataclass
class Tweet:
    tweet_id: int
    text: str
    user_id: int
    user_name: str
    language: str
    timestamp_ms: int
    retweeted_id: Optional[int]
    retweeted_user_id: Optional[int]

def parse_tweet(tweet: str) -> Tweet:
    data = json.loads(tweet)
    return Tweet(
        tweet_id=data["id"] if "id" in data else None,
        text=data["text"] if "text" in data else None,
        user_id=data["user"]["id"] if "user" in data else None,
        user_name=data["user"]["name"] if "name" in data else None,
        language=data["lang"] if "lang" in data else None,
        timestamp_ms=int(data["timestamp_ms"]) if "timestamp_ms" in data else None,
        retweeted_id=data["retweeted_status"]["id"] if "retweeted_status" in data else None,
        retweeted_user_id=data["retweeted_status"]["user"]["id"] if "retweeted_status" in data else None,
    )

_, language, source = sys.argv
conf = SparkConf().setAppName("spark-tweet-retweets")
sc = SparkContext(conf=conf)

# Ajustar el nivel de log de Spark
sc.setLogLevel("OFF")

# Define lambda functions for parsing and filtering
parse_tweet = lambda line: json.loads(line) if line.strip() else None
filter_tweets = lambda tweet: tweet and tweet.get("lang") == language

# Load and process tweets
tweets_rdd = sc.textFile(source).map(parse_tweet)
tweets_rdd = tweets_rdd.filter(filter_tweets)
tweets_rdd = tweets_rdd.filter(lambda tweet: tweet is not None)

# Filter retweets
retweets_rdd = tweets_rdd.filter(lambda tweet: tweet.get("retweeted_status") is not None)

# Count retweets
retweet_counts_rdd = retweets_rdd.map(lambda tweet: (tweet["retweeted_status"]["id"], 1))
retweet_totals_rdd = retweet_counts_rdd.reduceByKey(lambda a, b: a + b)

# Get the top retweets
top_retweets = retweet_totals_rdd.takeOrdered(10, key=lambda x: -x[1])

# Mostrar el total de retweets en el top
print(f"Total de retweets procesados (top): {len(top_retweets)}")
print(f"Primeros elementos del top de retweets: {top_retweets[:10]}")


# Crear un diccionario con los textos de los tweets originales desde los retweets
original_tweets_dict = retweets_rdd.map(lambda tweet: (tweet["retweeted_status"]["id"], 
                                                       (tweet["retweeted_status"]["text"], 
                                                        tweet["retweeted_status"]["user"]["name"]))) \
                                   .collectAsMap()

# Mostrar los tweets más retweeteados
for tweet_id, retweet_count in top_retweets:
    if tweet_id in original_tweets_dict:
        text, user_name = original_tweets_dict[tweet_id]
        print(f"User: {user_name}, Retweets: {retweet_count}, Tweet: {text}")
    else:
        print(f"No se encontró el tweet con tweet_id: {tweet_id}")




sc.stop()
