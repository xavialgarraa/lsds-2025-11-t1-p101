from pyspark import SparkConf, SparkContext
import sys
from tweet_parser import parse_tweet, Tweet
import logging


logging.basicConfig(level=logging.CRITICAL)

_, language, source = sys.argv
conf = SparkConf().setAppName("spark-tweet-retweets")
sc = SparkContext(conf=conf)


sc.setLogLevel("OFF")

parsed_tweet = lambda tweet: parse_tweet(tweet) if tweet.strip() else None
filter_tweets = lambda tweet: tweet and tweet.language == language


tweets_rdd = sc.textFile(source).map(parsed_tweet)
tweets_rdd = tweets_rdd.filter(filter_tweets)
tweets_rdd = tweets_rdd.filter(lambda tweet: tweet is not None)


retweets_rdd = tweets_rdd.filter(lambda tweet: tweet.retweeted_id is not None)


retweet_counts_rdd = retweets_rdd.map(lambda tweet: (tweet.retweeted_id, 1))
retweet_totals_rdd = retweet_counts_rdd.reduceByKey(lambda a, b: a + b)


top_retweets = retweet_totals_rdd.takeOrdered(10, key=lambda x: -x[1])


print(f"Total retweets procesed (top): {len(top_retweets)}")
print(f"Top retweets elements: {top_retweets[:10]}")


original_tweets_dict = retweets_rdd.map(
    lambda tweet: (tweet.retweeted_id, (tweet.text, tweet.retweeted_user_name))
).collectAsMap()


for tweet_id, retweet_count in top_retweets:
    if tweet_id in original_tweets_dict:
        text, user_name = original_tweets_dict[tweet_id]
        print(f"User: {user_name}, Retweets: {retweet_count}, Tweet: {text}")
    else:
        print(f"Did not find tweet with tweet_id: {tweet_id}")


sc.stop()
