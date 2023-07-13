from pyspark import SparkConf
from pyspark.sql import SparkSession
import os

# Check current directories in docker container
print(os.listdir(r"/"))

spark = (SparkSession.builder
                    .master('local')
                    .appName('task app')
                    .config(conf=SparkConf())
                    .getOrCreate()
                 )

df = spark.read \
    .option("header", "true") \
    .csv('data/fhv_tripdata_2021-01.csv.gz')

df.show()
