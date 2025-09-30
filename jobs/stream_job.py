import sys
import os
from config import KAFKA_CONFIG
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

spark = SparkSession.builder.appName("read_Kafka_streaming").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .options(**KAFKA_CONFIG) \
  .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
