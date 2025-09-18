from config.kafka_config import KAFKA_CONFIG
from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder.appName("read_Kafka_streaming").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_CONFIG["bootstrap_servers"]) \
  .option("subscribe", KAFKA_CONFIG["topic"]) \
  .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
