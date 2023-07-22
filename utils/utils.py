import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from yelp.column_variables import *


def read_spark_df(path: str, spark_session: SparkSession, schema: StructType = None) -> DataFrame:
    """
    Reads spark dataframe
    """
    if schema:
        df = spark_session.read.option("header", "true") \
                            .schema(schema) \
                            .json(path)
    else:
        df = spark_session.read.option("header", "true") \
                            .json(path)
    return df


def columns_with_nulls(df: DataFrame) -> dict:
    """
    Takes as argument pyspark dataframe
    Returns dictionary of columns where nulls found and their counts
    """
    columns_counts = {}
    for c in df.columns:
        tmp = df.where(f.col(c).isNull()).limit(1)
        # if nulls found write into dictionary
        if not tmp.rdd.isEmpty():
            null_count = df.where(f.col(c).isNull()).count()
            columns_counts[c] = null_count
    return columns_counts


def get_numeric_columns(df: DataFrame) -> list:
    """
    Takes as argument pyspark dataframe
    Returns list of column names that are numeric type
    """
    return [f.name for f in df.schema.fields if isinstance(f.dataType, t.IntegerType)]


def get_ca_review_counts(business_df: DataFrame, state_val: str = "CA") -> DataFrame:
    """
    Returns the number of reviews for each business in the given state
    """
    business_review_counts = business_df \
        .select(business_id, name, city, review_count, state) \
        .filter(f.col(state)==state_val) \
        .groupby(business_id) \
        .agg(f.first(name).alias("business_name"), f.sum(review_count).alias("Review Counts")) \
        .orderBy("Review Counts", ascending=False)
    return business_review_counts


def get_city_avg_stars(business_df: DataFrame, state_val: str = "CA") -> DataFrame:
    """
    Returns the average number of stars for cities in the given state
    """
    city_avg_stars = business_df \
        .select(city, state, stars) \
        .filter(f.col(state)==state_val) \
        .groupby(city) \
        .agg(f.avg(stars).alias("avg_city_stars"), f.count(stars).alias("business_count")) \
        .orderBy("business_count", ascending=False)
    return city_avg_stars


def get_state_city_top_rated(business_df: DataFrame, state_val: str = "CA", 
                             city_val: str = "Santa Barbara") -> DataFrame:
    """
    Returns the top rated businesses in given city given state
    """
    city_top_rated = business_df \
                .select(business_id, name, city, state, stars, review_count) \
                .filter((f.col(state)==state_val) & (f.col(city)==city_val)) \
                .drop(city, state) \
                .orderBy([review_count, stars], ascending=[False, False])
    return city_top_rated


def join_review_business(review_df: DataFrame, business_df: DataFrame) -> DataFrame:
    """
    Joins review data with business data.
    """
    business_rating_over_time = review_df \
        .drop(text) \
        .join(business_df.select(business_id, city, state, name, f.col(stars).alias("business_stars")), 
              on=business_id, how='left')
    return business_rating_over_time


def get_reviews_stats_business_over_time(business_rating_over_time: DataFrame) -> DataFrame:
    """
    Returns a time series of reviews and usefulness over time for businesses.
    """
    reviews_stats_business_over_time = business_rating_over_time.groupBy(date) \
        .agg(f.avg(stars).alias("stars_avg"), 
             f.sum(useful).alias("usefulness_sum"), 
             f.count(review_id).alias("review_id_count")) \
        .orderBy(f.col(date), ascending=False)
    return reviews_stats_business_over_time


def join_business_rating_over_time_users(business_rating_over_time: DataFrame, user_df: DataFrame) -> DataFrame:
    """
    Joins user data to business ratings over time data.
    """
    business_rating_over_time_users = business_rating_over_time \
        .join(user_df.select(f.col(user_id), 
                             f.col(name).alias("user_name"), \
                            f.col(review_count).alias("user_review_count"), 
                            f.col(yelping_since),\
                            f.col(average_stars).alias("user_average_stars")), 
                on=user_id, 
                how='left')
    return business_rating_over_time_users


def get_business_rating_reviews_stats(business_rating_over_time_users: DataFrame) -> DataFrame:
    """
    Returns the average user rating for separate businesses and number of reviews.
    """
    business_rating_reviews_stats = business_rating_over_time_users \
            .groupBy(user_id, business_id) \
            .agg(f.avg(stars).alias("avg_user_business_stars"), 
                 f.count(review_id).alias("num_user_business_reviews"),\
                 f.first("user_average_stars").alias("user_average_stars"), 
                 f.first("business_stars").alias("business_stars")) \
            .orderBy("num_user_business_reviews", ascending=False)
    
    return business_rating_reviews_stats


def convert_ratings_to_binary(business_df: DataFrame) -> DataFrame:
    """
    Converts ratings to binary good/bad.
    """
    business_df_rating_label = business_df.withColumn("bad_or_good", f.when(f.col(stars) < 3, "bad").otherwise("good"))
    return business_df_rating_label


def get_good_bad_counts(business_df_rating_label: DataFrame) -> DataFrame:
    """
    Returns the number of good/bad businesses.
    """
    good_bad_counts = business_df_rating_label.groupBy("bad_or_good") \
        .agg(f.count(business_id).alias("businesses_number"))
    return good_bad_counts


def get_good_bad_business_rating_by_state(business_df_rating_label: DataFrame) -> DataFrame:
    """
    Returns the number of bad or good rated businesses by state.
    """
    good_bad_business_rating_by_state = business_df_rating_label.groupBy(state, "bad_or_good") \
        .agg(f.count(business_id).alias("businesses_number")) \
        .orderBy(state)
    return good_bad_business_rating_by_state


def join_tip_user(tip_df: DataFrame, user_df: DataFrame) -> DataFrame:
    """
    Joins tip data with user data.
    """
    tip_user = tip_df.drop(text) \
        .join(user_df.select(user_id, review_count, yelping_since, average_stars), on=user_id, how='left')
    return tip_user


def join_tip_user_business(tip_user: DataFrame, business_df: DataFrame) -> DataFrame:
    """
    Joins tip_user data with business data.
    """
    tip_user_business = tip_user \
        .join(business_df.select(business_id, f.col(stars).alias("business_stars")), on=business_id, how='left')
    return tip_user_business


def get_users_tips_by_business(tip_user_business: DataFrame) -> DataFrame:
    """
    Returns the number of tips each user left for each of businesses.
    """
    users_tips_by_business = tip_user_business \
        .groupBy(user_id, business_id) \
        .agg(f.sum(compliment_count).alias("user_business_total_compliments"),
             f.first(review_count).alias("review_count"),
             f.first(yelping_since).alias("yelping_since"),
             f.first(average_stars).alias("average_stars"),
             f.first("business_stars").alias("business_stars")) \
        .orderBy("user_business_total_compliments", ascending=False)
    return users_tips_by_business