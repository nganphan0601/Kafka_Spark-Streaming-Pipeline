from config import KAFKA_CONFIG, POSTGRES_CONFIG
from spark_processor import SparkProcessor

if __name__ == "__main__":
    pipeline_name = "test_pipeline"
    processor = SparkProcessor(KAFKA_CONFIG, pipeline_name)
    raw_df = processor.read_stream()
    transformed_df = processor.transform(raw_df)
