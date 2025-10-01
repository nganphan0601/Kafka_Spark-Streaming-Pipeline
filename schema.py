from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, MapType


fact_schema = StructType([
    StructField("id", StringType()),
    StructField("api_version", StringType()),
    StructField("collection", StringType()),
    StructField("current_url", StringType()),
    StructField("device_id", StringType()),
    StructField("email", StringType()),
    StructField("ip", StringType()),
    StructField("local_time", StringType()),
    StructField("option", ArrayType(MapType(StringType(), StringType()))),
    StructField("product_id", StringType()),
    StructField("referrer_url", StringType()),
    StructField("store_id", StringType()),
    StructField("time_stamp", LongType()),
    StructField("user_agent", StringType())
])