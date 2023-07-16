from pyspark import SparkConf
from pyspark.sql import SparkSession
import os
from yelp.schemas import USER_SCHEMA

FILEPATH = 'data/yelp_academic_dataset_user.json'

# Check current directories in docker container
print(os.listdir(r"/"))


def read_spark_df(path, spark_session, schema=None):
    if schema:
        df = spark_session.read.option("header", "true") \
                            .schema(schema) \
                            .json(path)
    else:
        df = spark_session.read.option("header", "true") \
                            .json(path)

    return df


spark_session = (SparkSession.builder
                    .master('local')
                    .appName('task app')
                    .config(conf=SparkConf())
                    .getOrCreate()
                 )

user_df = read_spark_df(FILEPATH, spark_session, schema=None)

# vertical because horisontal does not fit fully in terminal
user_df.show(truncate=True, vertical = True)

