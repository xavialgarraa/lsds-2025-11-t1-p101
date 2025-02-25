import sys
import json
import uuid
from time import sleep
from confluent_kafka import Producer

if len(sys.argv) != 6:
    print(
        "Usage: python3 spikes.py {metric_name} {low_value} {spike_value} {period_seconds} {frequency}"
    )
    sys.exit(1)

metric_name = sys.argv[1]
low_value = int(sys.argv[2])
spike_value = int(sys.argv[3])
period_seconds = int(sys.argv[4])
frequency = int(sys.argv[5])


TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)
counter = 0
while True:
    if counter % frequency == 0:
        value = {"value": spike_value}
    else:
        value = {"value": low_value}
    print(f"Producing {metric_name} -> {value}")
    producer.produce(TOPIC, key=metric_name, value=json.dumps(value))
    producer.flush()
    sleep(period_seconds)
    counter += 1
