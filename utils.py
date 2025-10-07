from user_agents import parse
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, \
    to_date, hour, date_format, regexp_extract
from pyspark.sql.types import StringType, IntegerType

# functions to parse the user agent and get the browser + os
@udf(StringType())
def get_browser(ua):
    try: 
        return parse(ua).browser.family
    except:
        return None

@udf(StringType())
def get_os(ua):
    try:
        return parse(ua).os.family
    except:
        return None
    
# transformation functions for each dim table
def transform_product(df, spark, postgres_config):
    # Read existing product IDs from Postgres 
    existing_products = spark.read \
        .format("jdbc") \
        .option("url", postgres_config["url"]) \
        .option("dbtable", "dim_product") \
        .option("user", postgres_config["user"]) \
        .option("password", postgres_config["password"]) \
        .load() \
        .select("product_id").distinct()

    # Extract unique product IDs from stream
    new_products = df.select("product_id").dropna().distinct()

    # Keep only new ones
    new_products = new_products.join(existing_products, on="product_id", how="left_anti")

    return new_products


def transform_referrer(df):
    df_ref = df.select("referrer_url").dropna().distinct()
    df_ref = df_ref.withColumn(
        "domain_name",
        regexp_extract(col("referrer_url"), r"https?://([^/]+)/?", 1)
    )
    return df_ref.select("referrer_url", "domain_name")

def transform_useragent(df):
    df_ua = df.select("user_agent").dropna().distinct()
    df_ua = (
        df_ua.withColumn("browser", get_browser(col("user_agent")))
             .withColumn("os", get_os(col("user_agent")))
    )
    return df_ua.select("user_agent", "browser", "os")


def transform_timestamp(df):
    df = df.withColumn("utc_time", to_timestamp(from_unixtime(col("time_stamp") / 1000)))
    df = df.withColumn("date", to_date("utc_time")) \
           .withColumn("hour", hour("utc_time")) \
           .withColumn("day_of_week", date_format("utc_time", "E"))
    
    df = df.withColumn("hour", col("hour").cast(IntegerType()))
    df_time = df.select("time_stamp", "date", "hour", "day_of_week", "utc_time").dropna().distinct()
    return df_time


