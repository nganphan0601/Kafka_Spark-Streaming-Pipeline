from user_agents import parse
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, \
    to_date, hour, date_format, regexp_extract
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import functions as F
import psycopg2
from psycopg2.extras import execute_values


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

def transform_product(df):
    # Extract product_id and url (current_url field)
    df_product = (
        df.select(
            F.col("product_id"),
            F.col("current_url").alias("url")
        )
        .dropna(subset=["product_id"])   # ignore null product IDs
        .dropDuplicates(["product_id"])  # keep one per product_id
    )

    return df_product


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
    df_time = df.select("date", "hour", "day_of_week", "utc_time").dropna().distinct()
    return df_time

def upsert_dim_product(batch_df, postgres_config):
    # Collect unique product_id-url pairs from this batch
    products = [
        (row["product_id"], row["url"])
        for row in batch_df.select("product_id", "url").dropna(subset=["product_id"]).distinct().collect()
    ]
    if not products:
        return

    conn = psycopg2.connect(
        dbname="dim_product",
        user=postgres_config["user"],
        password=postgres_config["password"],
        host=postgres_config["host"],
        port=postgres_config["port"]
    )
    cur = conn.cursor()

    # PostgreSQL upsert: insert or update on conflict
    execute_values(
        cur,
        """
        INSERT INTO dim_product (product_id, url)
        VALUES %s
        ON CONFLICT (product_id)
        DO UPDATE SET url = EXCLUDED.url;
        """,
        products
    )

    conn.commit()
    cur.close()
    conn.close()