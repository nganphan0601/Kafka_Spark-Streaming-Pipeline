from kafka import KafkaConsumer, KafkaProducer
import json
from config.config import KAFKA_SOURCE_CONFIG, KAFKA_LOCAL_CONFIG


# 1. Setup external Kafka (source)

source_servers = KAFKA_SOURCE_CONFIG["kafka.bootstrap.servers"].split(",")
source_topic = KAFKA_SOURCE_CONFIG["subscribe"]

# Extract username and password from JAAS string
jaas_config = KAFKA_SOURCE_CONFIG["kafka.sasl.jaas.config"]
username = jaas_config.split("username='")[1].split("'")[0]
password = jaas_config.split("password='")[1].split("'")[0]

source_consumer = KafkaConsumer(
    source_topic,
    bootstrap_servers=source_servers,
    group_id="bridge-consumer-group",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    security_protocol=KAFKA_SOURCE_CONFIG["kafka.security.protocol"],
    sasl_mechanism=KAFKA_SOURCE_CONFIG["kafka.sasl.mechanism"],
    sasl_plain_username=username,
    sasl_plain_password=password,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


# 2. Setup local Kafka (target)

local_servers = KAFKA_LOCAL_CONFIG["kafka.bootstrap.servers"].split(",")
local_topic = KAFKA_LOCAL_CONFIG["subscribe"]

local_producer = KafkaProducer(
    bootstrap_servers=local_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# 3. Bridge messages

print(f"Bridging messages from external '{source_topic}' to local '{local_topic}'...\n")

for message in source_consumer:
    local_producer.send(local_topic, message.value)
    print("Message bridged:", message.value)
