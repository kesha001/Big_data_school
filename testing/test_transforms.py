import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as f
import pyspark.sql.types as t
from utils.utils import (
    read_spark_df,
    columns_with_nulls,
    get_numeric_columns,
    get_state_review_counts,
    get_state_avg_stars,
    get_state_city_top_rated,
    join_review_business,
    get_reviews_stats_business_over_time,
    join_business_rating_over_time_users,
    get_business_rating_reviews_stats,
    convert_ratings_to_binary,
    get_good_bad_counts,
    get_good_bad_business_rating_by_state,
    join_tip_user,
    join_tip_user_business,
    get_users_tips_by_business,
)


def test_read_spark_df(spark_session):
    # Create a dummy DataFrame
    data = [("abcde12345fghij67890kl", "Dummy Business 1", 100),
            ("fghij67890klabcde12345", "Dummy Business 2", 200)]
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", IntegerType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)

    # Write the DataFrame to a temporary JSON file
    path = "/tmp/test_read_spark_df.json"
    df.write.json(path)

    # Test reading the DataFrame without specifying a schema
    df_read = read_spark_df(path, spark_session)
    assert df_read.count() == 2
    assert sorted(df_read.columns) == ["business_id", "name", "review_count"]

    # Test reading the DataFrame with a specified schema
    df_read = read_spark_df(path, spark_session, schema)
    assert df_read.count() == 2
    assert sorted(df_read.columns) == ["business_id", "name", "review_count"]


def test_columns_with_nulls(spark_session):
    # Create a dummy DataFrame with null values
    data = [(None, "Dummy Business 1", 100),
            ("fghij67890klabcde12345", None, None)]
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", IntegerType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)

    # Test the columns_with_nulls function
    result = columns_with_nulls(df)
    assert result == {"business_id": 1, "name": 1, "review_count": 1}


def test_get_numeric_columns(spark_session):
    # Create a dummy DataFrame with different data types
    data = [("abcde12345fghij67890kl", "Dummy Business 1", 100),
            ("fghij67890klabcde12345", "Dummy Business 2", 200)]
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", IntegerType(), True)
    ])
    df = spark_session.createDataFrame(data, schema)

    # Test the get_numeric_columns function
    result = get_numeric_columns(df)
    assert result == ["review_count"]


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.appName("Test").getOrCreate()


def test_get_state_review_counts(spark_session):
    # Create a dummy business DataFrame
    data = [("abcde12345fghij67890kl", "Dummy Business 1", "Example City", 100, "CA"),
            ("fghij67890klabcde12345", "Dummy Business 2", "Example City", 200, "CA"),
            ("klabcde12345fghij67890", "Dummy Business 3", "Example City", 300, "NY")]
    schema = ["business_id", "name", "city", "review_count", "state"]
    business_df = spark_session.createDataFrame(data, schema)

    # Test the get_ca_review_counts function
    result = get_state_review_counts(business_df, state_val="CA")
    assert result.count() == 2
    assert result.collect()[0]["business_name"] == "Dummy Business 2"
    assert result.collect()[0]["Review Counts"] == 200


def test_get_state_avg_stars(spark_session):
    # Create a dummy business DataFrame
    data = [("abcde12345fghij67890kl", "Dummy Business 1", "Example City 1", "CA", 3.5),
            ("fghij67890klabcde12345", "Dummy Business 2", "Example City 1", "CA", 4.0),
            ("klabcde12345fghij67890", "Dummy Business 3", "Example City 2", "CA", 4.5),
            ("mnopqr67890klabcde12345", "Dummy Business 4", "Example City 2", "NY", 5.0)]
    schema = ["business_id", "name", "city", "state", "stars"]
    business_df = spark_session.createDataFrame(data, schema)

    # Test the get_ca_city_avg_stars function
    result = get_state_avg_stars(business_df)
    assert result.count() == 2
    assert result.collect()[0]["city"] == "Example City 1"
    assert result.collect()[0]["avg_city_stars"] == 3.75
    assert result.collect()[0]["business_count"] == 2


def test_get_state_city_top_rated(spark_session):
    # Create a dummy business DataFrame
    data = [("abcde12345fghij67890kl", "Dummy Business 1", "Santa Barbara", "CA", 3.5, 100),
            ("fghij67890klabcde12345", "Dummy Business 2", "Santa Barbara", "CA", 4.0, 200),
            ("klabcde12345fghij67890", "Dummy Business 3", "Santa Barbara", "CA", 4.5, 300),
            ("mnopqr67890klabcde12345", "Dummy Business 4", "Santa Barbara", "NY", 5.0, 400)]
    schema = ["business_id", "name", "city", "state","stars","review_count"]
    business_df = spark_session.createDataFrame(data, schema)

    # Test the get_ca_santa_barbara_top_rated function
    result = get_state_city_top_rated(business_df, state_val="CA", city_val="Santa Barbara")
    assert result.count() == 3
    assert result.collect()[0]["name"] == 'Dummy Business 3'


def test_join_review_business(spark_session):
    # Create dummy review and business DataFrames
    review_data = [("zdSx_SD6obEhz9VrW9uAWA", "Ha3iJu77CxlrFm-vQRs_8g", "tnhfDv5Il8EaGSXZGiuQGg", 4, "2016-03-09",
                    "Great place to hang out after work.", 0, 0, 0),
                   ("zdSx_SD6obEhz9VrW9uAWB", "Ha3iJu77CxlrFm-vQRs_8h", "tnhfDv5Il8EaGSXZGiuQGh", 3, "2016-03-10",
                    "Decent place to hang out.", 1, 1, 1)]
    review_schema = ["review_id", "user_id", "business_id", "stars", "date", "text", "useful", "funny", "cool"]
    review_df = spark_session.createDataFrame(review_data, review_schema)

    business_data = [("tnhfDv5Il8EaGSXZGiuQGg", "Dummy Business 1", "Example City 1", "CA", 3.5),
                     ("tnhfDv5Il8EaGSXZGiuQGh", "Dummy Business 2", "Example City 2", "CA", 4.0)]
    business_schema = ["business_id", "name", "city", "state","stars"]
    business_df = spark_session.createDataFrame(business_data, business_schema)

    # Test the join_review_business function
    result = join_review_business(review_df, business_df)
    assert result.count() == 2
    assert result.collect()[0]["name"] == 'Dummy Business 1'
    assert result.collect()[0]["business_stars"] == 3.5


def test_get_reviews_stats_business_over_time(spark_session):
    # Create a dummy business_rating_over_time DataFrame
    data = [("2016-03-09","zdSx_SD6obEhz9VrW9uAWA","Ha3iJu77CxlrFm-vQRs_8g","tnhfDv5Il8EaGSXZGiuQGg",
             4,"Example City 1","CA","Dummy Business 1",3.5,0,0,0),
            ("2016-03-10","zdSx_SD6obEhz9VrW9uAWB","Ha3iJu77CxlrFm-vQRs_8h","tnhfDv5Il8EaGSXZGiuQGh",
             3,"Example City 2","CA","Dummy Business 2",4.0,1,1,1)]
    schema = ["date","review_id","user_id","business_id","stars","city","state","name",
              "business_stars","useful","funny","cool"]
    business_rating_over_time = spark_session.createDataFrame(data, schema)

    # Test the get_reviews_stats_business_over_time function
    result = get_reviews_stats_business_over_time(business_rating_over_time)
    assert result.count() == 2
    assert result.collect()[0]["date"] == '2016-03-10'


def test_join_business_rating_over_time_users(spark_session):
    # Create dummy business_rating_over_time and user DataFrames
    business_rating_over_time_data = [("2016-03-09","zdSx_SD6obEhz9VrW9uAWA","Ha3iJu77CxlrFm-vQRs_8g","tnhfDv5Il8EaGSXZGiuQGg",
                                       4,"Example City 1","CA","Dummy Business 1",3.5,0,0,0),
                                      ("2016-03-10","zdSx_SD6obEhz9VrW9uAWB","Ha3iJu77CxlrFm-vQRs_8h","tnhfDv5Il8EaGSXZGiuQGh",
                                       3,"Example City 2","CA","Dummy Business 2",4.0,1,1,1)]
    business_rating_over_time_schema = ["date","review_id","user_id","business_id","stars","city","state","name",
                                        "business_stars","useful","funny","cool"]
    business_rating_over_time = spark_session.createDataFrame(business_rating_over_time_data, business_rating_over_time_schema)

    user_data = [("Ha3iJu77CxlrFm-vQRs_8g", "Dummy User 1", 100, "2016-03-09", 3.5),
                 ("Ha3iJu77CxlrFm-vQRs_8h", "Dummy User 2", 200, "2016-03-10", 4.0)]
    user_schema = ["user_id", "name", "review_count", "yelping_since", "average_stars"]
    user_df = spark_session.createDataFrame(user_data, user_schema)

    # Test the join_business_rating_over_time_users function
    result = join_business_rating_over_time_users(business_rating_over_time, user_df)
    assert result.count() == 2
    assert result.collect()[0]["user_name"] == 'Dummy User 1'
    assert result.collect()[0]["user_average_stars"] == 3.5


def test_get_business_rating_reviews_stats(spark_session):
    # Create a dummy business_rating_over_time_users DataFrame
    data = [("2016-03-09","zdSx_SD6obEhz9VrW9uAWA","Ha3iJu77CxlrFm-vQRs_8g","tnhfDv5Il8EaGSXZGiuQGg",
             4,"Example City 1","CA","Dummy Business 1",3.5,0,0,0,"Dummy User 1",100,"2016-03-09",3.5),
            ("2016-03-10","zdSx_SD6obEhz9VrW9uAWB","Ha3iJu77CxlrFm-vQRs_8h","tnhfDv5Il8EaGSXZGiuQGh",
             3,"Example City 2","CA","Dummy Business 2",4.0,1,1,1,"Dummy User 2",200,"2016-03-10",4.0)]
    schema = ["date","review_id","user_id","business_id","stars","city","state","name",
              "business_stars","useful","funny","cool","user_name","user_review_count",
              "yelping_since","user_average_stars"]
    business_rating_over_time_users = spark_session.createDataFrame(data, schema)

    # Test the get_business_rating_reviews_stats function
    result = get_business_rating_reviews_stats(business_rating_over_time_users)
    assert result.count() == 2
    assert result.collect()[0]["avg_user_business_stars"] == 4.0


def test_convert_ratings_to_binary(spark_session):
    # Create a dummy business DataFrame
    data = [("abcde12345fghij67890kl", "Dummy Business 1", "Example City 1", "CA", 3.5),
            ("fghij67890klabcde12345", "Dummy Business 2", "Example City 2", "CA", 4.0),
            ("klabcde12345fghij67890", "Dummy Business 3", "Example City 3", "CA", 2.5),
            ("mnopqr67890klabcde12345", "Dummy Business 4", "Example City 4", "NY", 1.0)]
    schema = ["business_id", "name", "city", "state","stars"]
    business_df = spark_session.createDataFrame(data, schema)

    # Test the convert_ratings_to_binary function
    result = convert_ratings_to_binary(business_df)
    assert result.count() == 4
    assert result.collect()[0]["bad_or_good"] == 'good'
    assert result.collect()[2]["bad_or_good"] == 'bad'


def test_get_good_bad_counts(spark_session):
    # Create a dummy business_df_rating_label DataFrame
    data = [("abcde12345fghij67890kl","Dummy Business 1","Example City 1","CA",3.5,"good"),
            ("fghij67890klabcde12345","Dummy Business 2","Example City 2","CA",4.0,"good"),
            ("klabcde12345fghij67890","Dummy Business 3","Example City 3","CA",2.5,"bad"),
            ("mnopqr67890klabcde12345","Dummy Business 4","Example City 4","NY",1.0,"bad")]
    schema = ["business_id","name","city","state","stars","bad_or_good"]
    business_df_rating_label = spark_session.createDataFrame(data, schema)

    # Test the get_good_bad_counts function
    result = get_good_bad_counts(business_df_rating_label)
    assert result.count() == 2
    assert result.filter(f.col("bad_or_good")=="bad").collect()[0]["businesses_number"] == 2


def test_get_good_bad_business_rating_by_state(spark_session):
    # Create a dummy business_df_rating_label DataFrame
    data = [("abcde12345fghij67890kl","Dummy Business 1","Example City 1","CA",3.5,"good"),
            ("fghij67890klabcde12345","Dummy Business 2","Example City 2","CA",4.0,"good"),
            ("klabcde12345fghij67890","Dummy Business 3","Example City 3","CA",2.5,"bad"),
            ("mnopqr67890klabcde12345","Dummy Business 4","Example City 4","NY",1.0,"bad")]
    schema = ["business_id","name","city","state","stars","bad_or_good"]
    business_df_rating_label = spark_session.createDataFrame(data, schema)

    # Test the get_good_bad_business_rating_by_state function
    result = get_good_bad_business_rating_by_state(business_df_rating_label)
    assert result.count() == 3


def test_join_tip_user(spark_session):
    # Create dummy tip and user DataFrames
    tip_data = [("zdSx_SD6obEhz9VrW9uAWA", "Ha3iJu77CxlrFm-vQRs_8g", "tnhfDv5Il8EaGSXZGiuQGg", "Great place to hang out after work.", 1),
                ("zdSx_SD6obEhz9VrW9uAWB", "Ha3iJu77CxlrFm-vQRs_8h", "tnhfDv5Il8EaGSXZGiuQGh", "Decent place to hang out.", 2)]
    tip_schema = ["tip_id", "user_id", "business_id", "text", "compliment_count"]
    tip_df = spark_session.createDataFrame(tip_data, tip_schema)

    user_data = [("Ha3iJu77CxlrFm-vQRs_8g", "Dummy User 1", 100, "2016-03-09", 3.5),
                 ("Ha3iJu77CxlrFm-vQRs_8h", "Dummy User 2", 200, "2016-03-10", 4.0)]
    user_schema = ["user_id", "name", "review_count", "yelping_since", "average_stars"]
    user_df = spark_session.createDataFrame(user_data, user_schema)

    # Test the join_tip_user function
    result = join_tip_user(tip_df, user_df)
    assert result.count() == 2
    assert result.collect()[0]["average_stars"] == 3.5


def test_join_tip_user_business(spark_session):
    # Create dummy tip_user and business DataFrames
    tip_user_data = [("zdSx_SD6obEhz9VrW9uAWA","Ha3iJu77CxlrFm-vQRs_8g","tnhfDv5Il8EaGSXZGiuQGg",
                      1,"Dummy User 1",100,"2016-03-09",3.5),
                     ("zdSx_SD6obEhz9VrW9uAWB","Ha3iJu77CxlrFm-vQRs_8h","tnhfDv5Il8EaGSXZGiuQGh",
                      2,"Dummy User 2",200,"2016-03-10",4.0)]
    tip_user_schema = ["tip_id","user_id","business_id","compliment_count","name","review_count",
                       "yelping_since","average_stars"]
    tip_user = spark_session.createDataFrame(tip_user_data, tip_user_schema)

    business_data = [("tnhfDv5Il8EaGSXZGiuQGg","Dummy Business 1","Example City 1","CA",3.5),
                     ("tnhfDv5Il8EaGSXZGiuQGh","Dummy Business 2","Example City 2","CA",4.0)]
    business_schema = ["business_id","name","city","state","stars"]
    business_df = spark_session.createDataFrame(business_data, business_schema)

    # Test the join_tip_user_business function
    result = join_tip_user_business(tip_user, business_df)
    assert result.count() == 2
    assert result.collect()[0]["business_stars"] == 3.5


def test_get_users_tips_by_business(spark_session):
    # Create a dummy tip_user_business DataFrame
    data = [("zdSx_SD6obEhz9VrW9uAWA","Ha3iJu77CxlrFm-vQRs_8g","tnhfDv5Il8EaGSXZGiuQGg",
             1,"Dummy User 1",100,"2016-03-09",3.5,3.5),
            ("zdSx_SD6obEhz9VrW9uAWB","Ha3iJu77CxlrFm-vQRs_8h","tnhfDv5Il8EaGSXZGiuQGh",
             2,"Dummy User 2",200,"2016-03-10",4.0,4.0)]
    schema = ["tip_id","user_id","business_id","compliment_count","name","review_count",
              "yelping_since","average_stars","business_stars"]
    tip_user_business = spark_session.createDataFrame(data, schema)

    # Test the get_users_tips_by_business function
    result = get_users_tips_by_business(tip_user_business)
    assert result.count() == 2
    assert result.collect()[0]["user_business_total_compliments"] == 2