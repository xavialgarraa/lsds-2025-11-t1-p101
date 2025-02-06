from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CountPeopleCity").getOrCreate()
sc = spark.sparkContext

rdd = sc.textFile("/opt/bitnami/spark/app/data/people.txt")
city_counts = rdd.map(lambda line: line.split()[2]).map(lambda city: (city, 1)).reduceByKey(lambda a, b: a + b)

for city, count in city_counts.collect():
    print(f"{city}: {count}")
