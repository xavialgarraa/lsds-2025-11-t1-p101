import sys
import json
import uuid
from time import sleep
from confluent_kafka import Producer

if len(sys.argv) != 4:
    print("Usage: python3 constant.py {metric_name} {metric_value} {period_seconds}")
    sys.exit(1)

metric_name = sys.argv[1]
metric_value = sys.argv[2]
period_seconds = int(sys.argv[3])

TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)

while True:
    value = {"value": metric_value}
    print(f"Producing {metric_name} -> {value}")
    producer.produce(TOPIC, key=metric_name, value=json.dumps(value))
    producer.flush()
    sleep(period_seconds)
