from config.config import KAFKA_CONFIG, POSTGRES_CONFIG
from spark_processor import SparkProcessor

if __name__ == "__main__":
    pipeline_name = "Kafka_Spark_Project"
    pipeline = SparkProcessor(KAFKA_CONFIG, POSTGRES_CONFIG, pipeline_name)
    
    # Read the stream
    raw_df = pipeline.read_stream()

    # Transform data inside foreachBatch
    def process_batch(batch_df, batch_id):
        dfs = pipeline.transform(batch_df)

        # Write each dim/fact table
        pipeline.write_to_postgres(dfs["dim_time"], POSTGRES_CONFIG, "dim_time")
        pipeline.write_to_postgres(dfs["dim_product"], POSTGRES_CONFIG, "dim_product")
        pipeline.write_to_postgres(dfs["dim_referrer"], POSTGRES_CONFIG, "dim_referrer")
        pipeline.write_to_postgres(dfs["dim_user_agent"], POSTGRES_CONFIG, "dim_user_agent")
        pipeline.write_to_postgres(dfs["fact_views"], POSTGRES_CONFIG, "fact_views")

    # Start the streaming query
    query = (
        raw_df.writeStream
            .foreachBatch(process_batch)
            .trigger(processingTime="30 seconds")  # process every 30s
            .start()
    )


    query.awaitTermination()

    