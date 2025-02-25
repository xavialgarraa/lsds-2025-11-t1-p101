import sys
import json
import uuid
from time import sleep
from confluent_kafka import Producer

if len(sys.argv) != 6:
    print(
        "Usage: python3 stairs.py {metric_name} {metric_value} {start_value} {end_value} {step} {period_seconds}"
    )
    sys.exit(1)

metric_name = sys.argv[1]
start_value = int(sys.argv[2])
end_value = int(sys.argv[3])
step = int(sys.argv[4])
period_seconds = int(sys.argv[5])


TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}


producer = Producer(PRODUCER_CONFIG)
actual_value = start_value


while True:
    if actual_value == end_value:
        actual_value = start_value
    else:
        actual_value = min(
            actual_value + step, end_value
        )  # in case of step + actual step is not exactly end_value
    value = {"value": actual_value}
    print(f"Producing {metric_name} -> {value}")
    producer.produce(TOPIC, key=metric_name, value=json.dumps(value))
    producer.flush()
    sleep(period_seconds)
