from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import os
from yelp.schemas import USER_SCHEMA, CHECKIN_SCHEMA, BUSINESS_SCHEMA, REVIEW_SCHEMA, TIP_SCHEMA
from yelp.column_variables import *
from utils import utils
from pyspark.sql.window import Window

yelp_checkin_path = "data/yelp_academic_dataset_checkin.json"
yelp_business_path = "data/yelp_academic_dataset_business.json"
yelp_review_path = "data/yelp_academic_dataset_review.json"
yelp_tip_path = "data/yelp_academic_dataset_tip.json"
yelp_user_path = "data/yelp_academic_dataset_user.json"

# Check current directories in docker container
# print(os.listdir(r"/"))


def main():
    spark_session = (SparkSession.builder
                        .master('local')
                        .appName('task app')
                        .config(conf=SparkConf())
                        .getOrCreate()
                    )


    business_df = utils.read_spark_df(yelp_business_path, spark_session, schema=BUSINESS_SCHEMA)
    user_df = utils.read_spark_df(yelp_user_path, spark_session, schema=USER_SCHEMA)
    tip_df = utils.read_spark_df(yelp_tip_path, spark_session, schema=TIP_SCHEMA)
    # checkin_df = utils.read_spark_df(yelp_checkin_path, spark_session, schema=CHECKIN_SCHEMA)
    review_df = utils.read_spark_df(yelp_review_path, spark_session, schema=REVIEW_SCHEMA)


    # Remove duplicates in case there are any
    business_df = business_df.dropDuplicates()
    user_df = user_df.dropDuplicates()
    tip_df = tip_df.dropDuplicates()
    # checkin_df = business_df.dropDuplicates()
    review_df = review_df.dropDuplicates()

    # Number of review fore each business of CA state
    business_review_counts = utils.get_state_review_counts(business_df=business_df, state_val="CA")
    print("Number of review fore each business of CA state: ")
    business_review_counts.show(10, truncate=False)
    business_review_counts.write.format("csv").mode("overwrite").save("data/data_out/business_review_counts.csv")


    # Average number of number of stars for cities in CA state
    city_avg_stars = utils.get_state_avg_stars(business_df=business_df, state_val="CA")
    print("Average number of number of stars for cities in CA state: ")
    city_avg_stars.show(10)
    city_avg_stars.write.format("csv").mode("overwrite").save("data/data_out/city_avg_stars.csv")


    # Top rated buisnesses in CA Santa Barbara city
    city_top_rated = utils.get_state_city_top_rated(business_df=business_df,
                                                    state_val="CA", city_val="Santa Barbara")
    print("Top rated buisnesses in CA Santa Barbara city: ")
    city_top_rated.show(10, truncate=False)
    city_top_rated.write.format("csv").mode("overwrite").save("data/data_out/city_top_rated.csv")


    # Time series for businesses of reviews and usefullness over time
    business_rating_over_time = utils.join_review_business(review_df=review_df, business_df=business_df)
    reviews_stats_business_over_time = utils.get_reviews_stats_business_over_time(business_rating_over_time)

    print("Time series for businesses of reviews and usefullness over time")
    reviews_stats_business_over_time.show(10)
    reviews_stats_business_over_time.write.format("csv").mode("overwrite").save("data/data_out/reviews_stats_business_over_time.csv")



    # What is average user rating for separate businesses and number of reviews
    business_rating_over_time_users = utils.join_business_rating_over_time_users(
        business_rating_over_time=business_rating_over_time,
        user_df=user_df
    )
    business_rating_reviews_stats = utils.get_business_rating_reviews_stats(
        business_rating_over_time_users
    )
    print("What is average user rating for separate businesses and number of reviews")
    business_rating_reviews_stats.show(10)
    business_rating_reviews_stats.write.format("csv").mode("overwrite").save("data/data_out/business_rating_reviews_stats.csv")


    # Number of good/bad businesses
    business_df_rating_label = utils.convert_ratings_to_binary(business_df=business_df)

    good_bad_counts = utils.get_good_bad_counts(business_df_rating_label)
    print("How many good/bad businesses: ")
    good_bad_counts.show()
    good_bad_counts.write.format("csv").mode("overwrite").save("data/data_out/good_bad_counts.csv")



    # Number of bad or good rated businesses by state
    good_bad_business_rating_by_state = utils.get_good_bad_business_rating_by_state(
        business_df_rating_label=business_df_rating_label
    )
    print("How many good/bad businesses by state: ")
    good_bad_business_rating_by_state.show(10)
    good_bad_business_rating_by_state.write.format("csv").mode("overwrite").save("data/data_out/good_bad_business_rating_by_state.csv")



    # Number of tips each user left for each of businesses
    tip_user = utils.join_tip_user(tip_df=tip_df, user_df=user_df)

    tip_user_business = utils.join_tip_user_business(tip_user=tip_user, business_df=business_df)

    users_tips_by_business = utils.get_users_tips_by_business(tip_user_business=tip_user_business)

    users_tips_by_business.show(10)
    users_tips_by_business.write.format("csv").mode("overwrite").save("data/data_out/users_tips_by_business.csv")


if __name__=="__main__":
    main()