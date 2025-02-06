from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountBigrams").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("/opt/bitnami/spark/app/data/cat.txt")
bigrams = rdd.flatMap(lambda line: zip(line.split(), line.split()[1:])).map(lambda bigram: (" ".join(bigram), 1)).reduceByKey(lambda a, b: a + b)

for bigram, count in bigrams.collect():
    print(f"{bigram}: {count}")
