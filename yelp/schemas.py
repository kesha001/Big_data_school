import pyspark.sql.types as t

CHECKIN_SCHEMA = t.StructType([
    t.StructField("business_id", t.StringType(), True),
    t.StructField("date", t.DateType(), True),
])
BUSINESS_SCHEMA = t.StructType([
    t.StructField("business_id", t.StringType(), True),
    t.StructField("name", t.StringType(), True),
    t.StructField("address", t.StringType(), True),
    t.StructField("city", t.StringType(), True),
    t.StructField("state", t.StringType(), True),
    t.StructField("postal_code", t.StringType(), True),
    t.StructField("latitude", t.FloatType(), True),
    t.StructField("longitude", t.FloatType(), True),
    t.StructField("stars", t.FloatType(), True),
    t.StructField("review_count", t.IntegerType(), True),
    t.StructField("is_open", t.IntegerType(), True),
    # t.StructField("attributes", t.StructType(), True), Modify it specifying every field in strunt tyep
    t.StructField("categories", t.ArrayType(elementType=t.StringType()), True),
    t.StructField("hours", t.MapType(keyType=t.StringType(), valueType=t.StringType()), True),
])
REVIEW_SCHEMA = t.StructType([
    t.StructField("review_id", t.StringType(), True),
    t.StructField("user_id", t.StringType(), True),
    t.StructField("business_id", t.StringType(), True),
    t.StructField("stars", t.FloatType(), True),
    t.StructField("date", t.DateType(), True),
    t.StructField("text", t.StringType(), True),
    t.StructField("useful", t.IntegerType(), True),
    t.StructField("funny", t.IntegerType(), True),
    t.StructField("cool", t.IntegerType(), True),
])
USER_SCHEMA = t.StructType([
    t.StructField("user_id", t.StringType(), True),
    t.StructField("name", t.StringType(), True),
    t.StructField("review_count", t.IntegerType(), True),
    t.StructField("yelping_since", t.DateType(), True),
    t.StructField("friends", t.StringType(), True), # t.StructField("friends", t.ArrayType(elementType=t.StringType()), True),
    t.StructField("useful", t.IntegerType(), True),
    t.StructField("funny", t.IntegerType(), True),
    t.StructField("cool", t.IntegerType(), True),
    t.StructField("fans", t.IntegerType(), True),
    t.StructField("friends", t.IntegerType(), True),# t.StructField("elite", t.ArrayType(elementType=t.IntegerType()), True),
    t.StructField("average_stars", t.FloatType(), True),
    t.StructField("compliment_hot", t.IntegerType(), True),
    t.StructField("compliment_more", t.IntegerType(), True),
    t.StructField("compliment_profile", t.IntegerType(), True),
    t.StructField("compliment_cute", t.IntegerType(), True),
    t.StructField("compliment_list", t.IntegerType(), True),
    t.StructField("compliment_note", t.IntegerType(), True),
    t.StructField("compliment_plain", t.IntegerType(), True),
    t.StructField("compliment_cool", t.IntegerType(), True),
    t.StructField("compliment_funny", t.IntegerType(), True),
    t.StructField("compliment_writer", t.IntegerType(), True),
    t.StructField("compliment_photos", t.IntegerType(), True),
])
TIP_SCHEMA = t.StructType([
    t.StructField("text", t.StringType(), True),
    t.StructField("date", t.DateType(), True),
    t.StructField("compliment_count", t.IntegerType(), True),
    t.StructField("business_id", t.StringType(), True),
    t.StructField("user_id", t.StringType(), True),
])