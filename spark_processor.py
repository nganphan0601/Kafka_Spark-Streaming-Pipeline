from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from schema import fact_schema
from utils import transform_timestamp, transform_product, transform_referrer, transform_useragent


class SparkProcessor:
    def __init__(self, kafka_config, postgres_config, pipeline_name):
        self.spark = SparkSession.builder \
            .appName(pipeline_name) \
            .master("spark://spark:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
            .getOrCreate()
        self.kafka_config = kafka_config
        self.postgres_config = postgres_config

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .options(**self.kafka_config) \
            .load()

    def transform(self, df):
        #parse Kafka "value" column as JSON using the defined schema for fact table
        df =  df.select(from_json(col("value").cast("string"), fact_schema).alias("data")) \
        .select("data.*")

        # filter records with collection = "view_product_detail"
        filtered_df = df.filter(col("collection") == "view_product_detail")

        # Remove duplicates ids
        deduped_df = filtered_df.dropDuplicates(["id"])


        # dimensional transformation
        df_time = transform_timestamp(df)
        df_product = transform_product(df, self.spark, self.postgres_config)
        df_referrer = transform_referrer(df)
        df_useragent = transform_useragent(df)

        # fact dataframe
        df_fact = deduped_df.select(
            "id", "api_version", "collection", "current_url", "device_id", \
            "email", "ip", "local_time", "option", "product_id", "referrer_url", \
            "store_id", "time_stamp", "user_agent", "inserted_at"
        )

        return {
        "dim_time": df_time,
        "dim_product": df_product,
        "dim_referrer": df_referrer,
        "dim_user_agent": df_useragent,
        "fact_views": df_fact
        }


    def write_to_postgres(self, df, tb_name="fact_views"):
        """
        Write streaming DataFrame to Postgres (raw fact table).
        """
        return (df.writeStream
            .foreachBatch(
                lambda batch_df, _: (
                    batch_df.write
                        .format("jdbc")
                        .option("url", self.postgres_config["url"])           
                        .option("dbtable", tb_name)                      
                        .option("user",  self.postgres_config["user"])
                        .option("password",  self.postgres_config["password"])
                        .option("driver",  self.postgres_config["driver"])
                        .mode("append")
                        .save()
                )
            )
            .outputMode("append")
            .start()
        )
