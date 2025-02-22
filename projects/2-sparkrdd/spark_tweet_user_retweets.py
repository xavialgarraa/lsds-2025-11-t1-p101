from pyspark import SparkConf, SparkContext
import sys
import json
import logging

logging.basicConfig(level=logging.CRITICAL)

_, language, source = sys.argv
conf = SparkConf().setAppName("spark-tweet-user-retweets")
sc = SparkContext(conf=conf)

sc.setLogLevel("OFF")

# Function to parse tweets
parse_tweet = lambda line: json.loads(line) if line.strip() else None
filter_tweets = lambda tweet: tweet and tweet.get("lang") == language

# Load and filter tweets
tweets_rdd = sc.textFile(source).map(parse_tweet).filter(filter_tweets)

# Filter only retweets
retweets_rdd = tweets_rdd.filter(lambda tweet: tweet.get("retweeted_status") is not None)

# Get the total number of retweets per original user
user_retweets_rdd = retweets_rdd.map(lambda tweet: (tweet["retweeted_status"]["user"]["name"], 1))
user_retweet_totals_rdd = user_retweets_rdd.reduceByKey(lambda a, b: a + b)

# Get the top 10 most retweeted users
top_users = user_retweet_totals_rdd.takeOrdered(10, key=lambda x: -x[1])

# Print the results
print(f"Total users processed: {len(top_users)}")
print(f"Top 10 most retweeted users:")
for user_name, retweet_count in top_users:
    print(f"User: {user_name}, Retweets: {retweet_count}")

sc.stop()
