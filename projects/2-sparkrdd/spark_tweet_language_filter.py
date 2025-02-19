import sys
import json
import shutil
import os
from pyspark import SparkConf, SparkContext

if len(sys.argv) != 4:
    sys.exit("Uso: spark-submit spark_tweet_language_filter.py <lang> <input_file> <output_file>")

language_code, input_file, output_file = sys.argv[1:4]
temp_output_dir = f"{output_file}_info"

# Spark
conf = SparkConf().setAppName("TweetLanguageFilterRDD")
sc = SparkContext(conf=conf)

# lambda function
parse_tweet = lambda line: json.loads(line) if line.strip() else None
filter_tweets = lambda tweet: tweet and tweet.get("lang") == language_code

# RDD
tweets_rdd = sc.textFile(input_file)\
              .map(parse_tweet)\
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

print(f"Archivo final guardado en: {output_file}")