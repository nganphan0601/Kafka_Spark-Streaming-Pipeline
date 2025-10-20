from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

fact_schema = StructType([
    StructField("_id", StringType()),
    StructField("time_stamp", LongType()),
    StructField("ip", StringType()),
    StructField("user_agent", StringType()),
    StructField("resolution", StringType()),
    StructField("user_id_db", StringType()),
    StructField("device_id", StringType()),
    StructField("api_version", StringType()),
    StructField("store_id", StringType()),
    StructField("local_time", StringType()),
    StructField("show_recommendation", StringType()),
    StructField("current_url", StringType()),
    StructField("referrer_url", StringType()),
    StructField("email_address", StringType()),
    StructField("recommendation", StringType()),
    StructField("utm_source", StringType()),
    StructField("utm_medium", StringType()),
    StructField("collection", StringType()),
    StructField("product_id", StringType()),
    StructField("option", ArrayType(
        StructType([
            StructField("option_label", StringType()),
            StructField("option_id", StringType()),
            StructField("value_label", StringType()),
            StructField("value_id", StringType())
        ])
    )),
    StructField("id", StringType())
])
