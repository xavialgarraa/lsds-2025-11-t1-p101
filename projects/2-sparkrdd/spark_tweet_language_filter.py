import sys
import json
import shutil
import os
from tweet_parser import parse_tweet, Tweet
from pyspark import SparkConf, SparkContext

if len(sys.argv) != 4:
    sys.exit("Usage: spark-submit spark_tweet_language_filter.py <lang> <input_file> <output_file>")

language_code, input_file, output_file = sys.argv[1:4]
temp_output_dir = f"{output_file}_info"

# Spark
conf = SparkConf().setAppName("TweetLanguageFilterRDD")
sc = SparkContext(conf=conf)

# lambda function
parsed_tweet = lambda tweet: parse_tweet(tweet) if tweet.strip() else None
filter_tweets = lambda tweet: tweet and tweet.language == language_code

# RDD
tweets_rdd = sc.textFile(input_file)\
              .map(parsed_tweet)\
              .filter(filter_tweets)\
              .map(json.dumps)

# Save in the folder
tweets_rdd.coalesce(1).saveAsTextFile(temp_output_dir)

# Extract the file to put it in the output_file path
for file in os.listdir(temp_output_dir):
    if file.startswith("part-"):
        shutil.move(f"{temp_output_dir}/{file}", output_file)
        break

# Stop Spark
sc.stop()

print(f"File saved in: {output_file}")