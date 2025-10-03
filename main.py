from config.config import KAFKA_CONFIG, POSTGRES_CONFIG
from spark_processor import SparkProcessor

if __name__ == "__main__":
    pipeline_name = "test_pipeline"
    processor = SparkProcessor(KAFKA_CONFIG, pipeline_name)
    raw_df = processor.read_stream()
    transformed_df = processor.transform(raw_df)

    # Write to console for testing
    query = transformed_df.writeStream \
            .format("json") \
            .option("path", "/tmp/fact_views") \
            .option("checkpointLocation", "/tmp/fact_views_checkpoint") \
            .outputMode("append") \
            .start()
        
    query.awaitTermination()