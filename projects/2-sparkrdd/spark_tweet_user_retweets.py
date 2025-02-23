from pyspark import SparkConf, SparkContext
import sys
import logging
import time
from tweet_parser import parse_tweet, Tweet

logging.basicConfig(level=logging.CRITICAL)

_, language, source = sys.argv
start_time = time.time()
conf = SparkConf().setAppName("spark-tweet-user-retweets")
sc = SparkContext(conf=conf)

sc.setLogLevel("OFF")

# Function to parse tweets
parsed_tweet = lambda tweet: parse_tweet(tweet) if tweet.strip() else None
filter_tweets = lambda tweet: tweet and tweet.language == language

# Load and filter tweets
tweets_rdd = sc.textFile(source).map(parsed_tweet).filter(filter_tweets)

# Filter only retweets
retweets_rdd = tweets_rdd.filter(lambda tweet: tweet.retweeted_id is not None)

# Get the total number of retweets per original user
#user_retweets_rdd = retweets_rdd.map(lambda tweet: (tweet.retweeted_user_name, 1))
user_retweets_rdd = retweets_rdd.map(lambda tweet: (tweet.retweeted_user_id, 1))
user_retweet_totals_rdd = user_retweets_rdd.reduceByKey(lambda a, b: a + b)

# Get the top 10 most retweeted users
top_users = user_retweet_totals_rdd.takeOrdered(10, key=lambda x: -x[1])

end_time = time.time()


print(f"Time taken: {end_time - start_time} seconds")
# Print the results
print(f"Total users processed: {len(top_users)}")
print(f"Top 10 most retweeted users:")
for user_id, retweet_count in top_users:
    #print(f"User: {user_name}, Retweets: {retweet_count}")
    print(f"User ID: {user_id}, Retweets: {retweet_count}")

sc.stop()
