from kafka.admin import KafkaAdminClient, NewTopic
import os

# Kafka broker, not Zookeeper
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")  

admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)

try:
    admin.create_topics([
        NewTopic(name="tasks", num_partitions=3, replication_factor=1),
        NewTopic(name="broadcast", num_partitions=1, replication_factor=1)
    ])
    print("Topics created.")
except Exception as e:
    print(f"Topic creation skipped or failed: {e}")
