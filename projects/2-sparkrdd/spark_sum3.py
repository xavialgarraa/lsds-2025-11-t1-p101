from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
import sys

_, source = sys.argv

conf = SparkConf().setAppName("spark-sum2")
sc = SparkContext(conf=conf)

numbers_rdd = sc.textFile(source)
numbers_int_rdd = numbers_rdd.flatMap(lambda line: map(int, line.split()))
even_numbers_rdd = numbers_int_rdd.filter(lambda x: x % 2 == 0)
result = even_numbers_rdd.sum()

print(
    f"""
------------------------------------------
------------------------------------------
------------------------------------------
SUM = {result}
------------------------------------------
------------------------------------------
------------------------------------------
"""
)

sc.stop()
