KAFKA_CONFIG = {
    "kafka.bootstrap.servers": "46.202.167.130:9094,46.202.167.130:9194,46.202.167.130:9294",
    "subscribe":"product_view",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
    	"org.apache.kafka.common.security.plain.PlainLoginModule required "
    	"username='kafka' "
    	"password='UnigapKafka@2024';"
    	),
}

# TABLES = {
#     "fact_raw"
# }
POSTGRES_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/streaming_db",
    "user": "postgres",
    "password": "06012002",
    "driver": "org.postgresql.Driver",
    "fact_table": "fact_views_raw"
}

