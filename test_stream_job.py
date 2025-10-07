import sys
import os
from config.config import KAFKA_CONFIG
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from schema import fact_schema

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

spark = SparkSession.builder.appName("read_Kafka_streaming").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .options(**KAFKA_CONFIG) \
  .load()

p_df =  df.select(from_json(col("value").cast("string"), fact_schema).alias("data")) \
        .select("data.*")

query = p_df.writeStream \
    .format("console") \
    .option("truncate","false") \
    .outputMode("append") \
    .start()

query.awaitTermination()
