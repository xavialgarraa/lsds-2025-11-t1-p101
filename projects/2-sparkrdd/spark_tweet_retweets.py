from pyspark import SparkConf, SparkContext
import sys
from tweet_parser import parse_tweet, Tweet

_, language, source = sys.argv
conf = SparkConf().setAppName("spark-tweet-retweets")
sc = SparkContext(conf=conf)

tweets_rdd = sc.textFile(source).map(parse_tweet)

filtered_rdd = tweets_rdd.filter(lambda tweet: tweet.language == language)

retweets_rdd = filtered_rdd.filter(lambda tweet: tweet.retweeted_id is not None)

retweet_counts_rdd = retweets_rdd.map(lambda tweet: (tweet.retweeted_id, 1))

retweet_totals_rdd = retweet_counts_rdd.reduceByKey(lambda a, b: a + b)

top_retweets = retweet_totals_rdd.takeOrdered(10, key=lambda x: -x[1])

# Mostrar los resultados
for tweet_id, retweet_count in top_retweets:
    original_tweet = filtered_rdd.filter(lambda tweet: tweet.tweet_id == tweet_id).first()
    print(f"User: {original_tweet.user_name}, Retweets: {retweet_count}, Tweet: {original_tweet.text}")
