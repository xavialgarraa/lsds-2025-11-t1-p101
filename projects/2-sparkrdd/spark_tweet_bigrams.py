import sys
import json
import shutil
import os
from tweet_parser import parse_tweet, Tweet
from pyspark import SparkConf, SparkContext

if len(sys.argv) != 4:
    sys.exit(
        "Usage: spark-submit spark_tweet_bigram_counter.py <lang> <input_file> <output_file>"
    )

language_code, input_file, output_file = sys.argv[1:4]
temp_output_dir = f"{output_file}_info"

# Configure Spark
conf = SparkConf().setAppName("TweetBigramCounter")
sc = SparkContext(conf=conf)

def extract_bigrams(text):
    words = text.split()
    return [(words[i], words[i + 1]) for i in range(len(words) - 1)]

parsed_tweet = lambda tweet: parse_tweet(tweet) if tweet.strip() else None
filter_tweets = lambda tweet: tweet and tweet.language == language_code

# Processing with RDD
tweets_rdd = (
    sc.textFile(input_file)
    .map(parsed_tweet)
    .filter(filter_tweets)
    .map(lambda tweet: tweet.text)
    .flatMap(extract_bigrams)
    .map(lambda bigram: (bigram, 1))
    .reduceByKey(lambda a, b: a + b)
    .filter(lambda x: x[1] > 1)
    .sortBy(lambda pair: -pair[1])
)

# Save the results
tweets_rdd.map(lambda pair: json.dumps({"bigram": pair[0], "count": pair[1]})).coalesce(
    1
).saveAsTextFile(temp_output_dir)

# Extract the final file
for file in os.listdir(temp_output_dir):
    if file.startswith("part-"):
        shutil.move(f"{temp_output_dir}/{file}", output_file)
        break

# Stop Spark
sc.stop()

print(f"Final file saved in: {output_file}")
