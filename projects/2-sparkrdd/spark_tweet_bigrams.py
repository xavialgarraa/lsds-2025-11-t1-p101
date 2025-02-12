from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import json


def parse_tweet(line):
    tweet = json.loads(line)
    return tweet.get("lang", "unknown"), tweet.get("text", "")


def extract_bigrams(text):
    words = text.split()
    return zip(words, words[1:])


if __name__ == "__main__":
    _, lang, source, output = sys.argv

    conf = SparkConf().setAppName("spark-tweet-bigrams")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    tweets_rdd = sc.textFile(source)
    filtered_tweets_rdd = tweets_rdd.map(parse_tweet).filter(lambda x: x[0] == lang)
    bigrams_rdd = filtered_tweets_rdd.flatMap(lambda x: extract_bigrams(x[1])).map(
        lambda bigram: (" ".join(bigram), 1)
    )
    bigram_counts_rdd = bigrams_rdd.reduceByKey(lambda a, b: a + b).filter(
        lambda x: x[1] > 1
    )
    sorted_bigrams_rdd = bigram_counts_rdd.sortBy(lambda x: x[1], ascending=False)

    sorted_bigrams_rdd.saveAsTextFile(output)

    sc.stop()
