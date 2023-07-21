from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import os
from yelp.schemas import USER_SCHEMA, CHECKIN_SCHEMA, BUSINESS_SCHEMA, REVIEW_SCHEMA, TIP_SCHEMA
from yelp.column_variables import *

yelp_checkin_path = "data/yelp_academic_dataset_checkin.json"
yelp_business_path = "data/yelp_academic_dataset_business.json"
yelp_review_path = "data/yelp_academic_dataset_review.json"
yelp_tip_path = "data/yelp_academic_dataset_tip.json"
yelp_user_path = "data/yelp_academic_dataset_user.json"

# Check current directories in docker container
# print(os.listdir(r"/"))


def read_spark_df(path, spark_session, schema=None):
    if schema:
        df = spark_session.read.option("header", "true") \
                            .schema(schema) \
                            .json(path)
    else:
        df = spark_session.read.option("header", "true") \
                            .json(path)
    return df


def columns_with_nulls(df):
    """
    Takes as argument pyspark dataframe
    Returns dictionary of columns where nulls found and their counts
    """
    columns_counts = {}
    for c in df.columns:
        tmp = df.where(f.col(c).isNull()).limit(1)
        # if nulls found write into dictionary
        if not tmp.isEmpty():
            null_count = df.where(f.col(c).isNull()).count()
            columns_counts[c] = null_count
    return columns_counts


def get_numeric_columns(df):
    return [f.name for f in df.schema.fields if isinstance(f.dataType, t.IntegerType)]

spark_session = (SparkSession.builder
                    .master('local')
                    .appName('task app')
                    .config(conf=SparkConf())
                    .getOrCreate()
                 )


business_df = read_spark_df(yelp_business_path, spark_session, schema=BUSINESS_SCHEMA)
user_df = read_spark_df(yelp_user_path, spark_session, schema=USER_SCHEMA)
tip_df = read_spark_df(yelp_tip_path, spark_session, schema=TIP_SCHEMA)
checkin_df = read_spark_df(yelp_checkin_path, spark_session, schema=CHECKIN_SCHEMA)
review_df = read_spark_df(yelp_review_path, spark_session, schema=REVIEW_SCHEMA)

# Number of review fore each business of CA state
business_review_counts = business_df \
            .select(business_id, name, city, review_count, state) \
            .filter(f.col(state)=="CA") \
            .groupby(business_id) \
            .agg(f.first(name).alias("business_name"), f.sum(review_count).alias("Review Counts")) \
            .orderBy("Review Counts", ascending=False)

print("Number of review fore each business of CA state: ")
business_review_counts.show(10, truncate=False)
business_review_counts.write.format("csv").mode("overwrite").save("data/data_out/business_review_counts.csv")


# Average number of number of stars for cities in CA state
city_avg_stars = business_df \
            .select(city, state, stars) \
            .filter(f.col(state)=="CA") \
            .groupby(city) \
            .agg(f.avg(stars).alias("avg_city_stars"), f.count(stars).alias("business_count")) \
            .orderBy("business_count", ascending=False)

print("Average number of number of stars for cities in CA state: ")
city_avg_stars.show(10)
city_avg_stars.write.format("csv").mode("overwrite").save("data/data_out/city_avg_stars.csv")

# Top rated buisnesses in CA Santa Barbara city
city_top_rated = business_df \
            .select(business_id, name, city, state, stars, review_count) \
            .filter((f.col(state)=="CA") & (f.col(city)=="Santa Barbara")) \
            .drop(city, state) \
            .orderBy([review_count, stars], ascending=[False, False])

print("Top rated buisnesses in CA Santa Barbara city: ")
city_top_rated.show(10, truncate=False)
city_top_rated.write.format("csv").mode("overwrite").save("data/data_out/city_top_rated.csv")

# Join review data with business data
business_rating_over_time = review_df \
                    .drop(text) \
                    .join(business_df.select(business_id, city, state, name, f.col(stars).alias("business_stars")), on=business_id, how='left')
# Time series for businesses of reviews and usefullness over time
reviews_stats_business_over_time = business_rating_over_time.groupBy(date) \
                                .agg(f.avg(stars).alias("stars_avg"), f.sum(useful).alias("usefulness_sum"), f.count(review_id).alias("review_id_count")) \
                                .orderBy(f.col(date), ascending=False)

print("Time series for businesses of reviews and usefullness over time")
reviews_stats_business_over_time.show(10)
reviews_stats_business_over_time.write.format("csv").mode("overwrite").save("data/data_out/reviews_stats_business_over_time.csv")


# Join user data to business ratings over time data
business_rating_over_time_users = business_rating_over_time \
                                .join(user_df.select(f.col(user_id), f.col(name).alias("user_name"), \
                                                     f.col(review_count).alias("user_review_count"), f.col(yelping_since),\
                                                     f.col(average_stars).alias("user_average_stars")), on=user_id, how='left')
# What is average user rating for separate businesses and number of reviews
business_rating_reviews_stats = business_rating_over_time_users \
                                .groupBy(user_id, business_id) \
                                .agg(f.avg(stars).alias("avg_user_business_stars"), f.count(review_id).alias("num_user_business_reviews"),\
                                        f.first("user_average_stars").alias("user_average_stars"), f.first("business_stars").alias("business_stars")) \
                                .orderBy("num_user_business_reviews", ascending=False)
print("What is average user rating for separate businesses and number of reviews")
business_rating_reviews_stats.show(10)
business_rating_reviews_stats.write.format("csv").mode("overwrite").save("data/data_out/business_rating_reviews_stats.csv")

# Convert ratings to binary good/bad
business_df_rating_label = business_df.withColumn("bad_or_good", f.when(f.col(stars)<3, "bad").otherwise("good"))
# How many good/bad businesses
good_bad_counts = business_df_rating_label.groupBy("bad_or_good") \
                        .agg(f.count(business_id).alias("businesses_number"))

print("How many good/bad businesses: ")
good_bad_counts.show()
good_bad_counts.write.format("csv").mode("overwrite").save("data/data_out/good_bad_counts.csv")

# Number of bad or good rated businesses by state
good_bad_business_rating_by_state = business_df_rating_label.groupBy(state, "bad_or_good") \
                            .agg(f.count(business_id).alias("businesses_number")) \
                            .orderBy(state)
print("How many good/bad businesses by state: ")
good_bad_business_rating_by_state.show(10)
good_bad_business_rating_by_state.write.format("csv").mode("overwrite").save("data/data_out/good_bad_business_rating_by_state.csv")


# How may tips each user left for each of businesses
users_tips_by_business = tip_df.drop(text)\
                        .join(user_df.select(user_id, review_count, yelping_since, average_stars), on=user_id, how='left')\
                        .join(business_df.select(business_id, f.col(stars).alias("business_stars")), on=business_id, how='left')\
                        .groupBy(user_id, business_id)\
                        .agg(f.sum(compliment_count).alias("user_business_total_compliments"), f.first(review_count).alias(review_count), \
                            f.first(yelping_since).alias(yelping_since), f.first(average_stars).alias(average_stars), f.first("business_stars").alias("business_stars"))\
                        .orderBy("user_business_total_compliments", ascending=False)

users_tips_by_business.show(10)
users_tips_by_business.write.format("csv").mode("overwrite").save("data/data_out/users_tips_by_business.csv")