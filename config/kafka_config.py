KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:9094,localhost:9194,localhost:9294",
    "topic": "user_data",
    "startingOffsets": "earliest",   
    "failOnDataLoss": "false"       # helps with resilience
}
