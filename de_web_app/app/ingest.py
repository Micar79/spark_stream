"""Ingestion helpers: save metadata and produce messages to Kafka."""
import os
from kafka import KafkaProducer
import json

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "transactions"


def handle_upload(file_path: str):
    """Simple ingestion: produce a message to Kafka with filename and path."""
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    msg = {"file": os.path.basename(file_path), "path": file_path}
    producer.send(TOPIC, value=msg)
    producer.flush()
    producer.close()
