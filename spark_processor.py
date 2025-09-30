from pyspark.sql import SparkSession

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
        query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .writeStream \
            .format("console") \
            .outputMode("append") \
            .start()
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