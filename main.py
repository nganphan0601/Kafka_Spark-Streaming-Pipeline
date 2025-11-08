from config.config import KAFKA_CONFIG, POSTGRES_CONFIG
from spark_processor import SparkProcessor

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

print("🟢 Starting pipeline...\n")

if __name__ == "__main__":
    # pipeline_name = "Kafka_Spark_Project"
    # pipeline = SparkProcessor(KAFKA_CONFIG, POSTGRES_CONFIG, pipeline_name)
    
    # # TEMP: test raw Kafka stream
    # # pipeline.debug_print_raw_stream()

    # # Read the stream
    # raw_df = pipeline.read_stream()

    # # Transform data inside foreachBatch
    # def process_batch(batch_df, batch_id):
    #     print(f"[BATCH {batch_id}] Received {batch_df.count()} rows")

    #     dfs = pipeline.transform(batch_df)

    #     # Write each dim/fact table
    #     pipeline.write_to_postgres(dfs["dim_time"], "dim_time")
    #     pipeline.write_to_postgres(dfs["dim_product"], "dim_product")
    #     pipeline.write_to_postgres(dfs["dim_referrer"], "dim_referrer")
    #     pipeline.write_to_postgres(dfs["dim_user_agent"], "dim_user_agent")
    #     pipeline.write_to_postgres(dfs["fact_views"], "fact_views")

    # # Start the streaming query
    # query = (
    #     raw_df.writeStream
    #         .foreachBatch(process_batch)
    #         .trigger(processingTime="30 seconds")  # process every 30s
    #         .start()
    # )

    # print("STREAMING QUERY STARTED. WAITING FOR BATCHES.....\n")
    # query.awaitTermination()

    from pyspark.sql import SparkSession

    print("Starting minimal Kafka → Spark test...")

    spark = SparkSession.builder \
        .appName("MinimalKafkaTest") \
        .getOrCreate()

    # Minimal raw Kafka stream reader
    raw_df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka-0:9092,kafka-1:9092,kafka-2:9092")
            .option("subscribe", "product_view")
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_PLAINTEXT")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='kafka' password='UnigapKafka@2024';")
            .load()
    )

    # Print raw Kafka records to console
    query = (
        raw_df.writeStream
            .format("console")
            .option("truncate", False)
            .start()
    )

    print("Waiting for records from Kafka topic 'product_view'...\n")
    query.awaitTermination()
