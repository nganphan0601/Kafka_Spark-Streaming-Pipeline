from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from schema import fact_schema

class SparkProcessor:
    def __init__(self, kafka_config, pipeline_name):
        self.spark = SparkSession.builder \
            .appName(pipeline_name) \
            .getOrCreate()
        self.kafka_config = kafka_config

    def read_stream(self):
        return self.spark.readStream \
            .format("kafka") \
            .options(**self.kafka_config) \
            .load()

    def transform(self, df):
        #parse Kafka "value" column as JSON using the defined schema for fact table
        parsed_df = df.select(from_json(col("value").cast("string"), fact_schema).alias("data")) \
        .select("data.*")

        # Write to console for testing
        query = parsed_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", False).start()
        
        query.awaitTermination()

    def write_to_postgres(self, df, postgres_config, tb_name):
        df.writeStream \
        .foreachBatch(
            lambda batch_df, _: (
                batch_df.write.format("jdbc") \
                .option("url", postgres_config["url"])
                .option("dbtable", tb_name)
                .option("user", postgres_config["user"])
                .option("password", postgres_config["password"])
                .mode("append")
                .save()
            )
        ) \
        .start()