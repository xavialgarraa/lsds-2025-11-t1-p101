from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from threading import Thread
import requests


def send_discord_alert(rule_data, metric_value):
    webhook_url = rule_data['discord_webhook_url']
    message = {
        "content": f"Alert: The metric `{rule_data['metric_name']}` triggered an alarm! The value `{metric_value}` exceeded the threshold `{rule_data['threshold']}`."
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code == 204:
            print(f"Alert sent to Discord: {message['content']}")
        else:
            print(f"Failed to send alert to Discord. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending alert to Discord: {e}")

# Consumer configuration
CONSUMER_CONFIG = {
    "bootstrap.servers": "kafka-1:19092",
    "group.id": "rules-consumer-group",
    "auto.offset.reset": "earliest",
}

TOPIC = "rules"

# Consumer creator
consumer = Consumer(CONSUMER_CONFIG)

# Topic subscription
consumer.subscribe([TOPIC])

# Materialized view
rules_store = {}


def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            # Process message
            key = msg.key().decode("utf-8")
            value = msg.value()
            if value is not None:
                rules_store[key] = json.loads(value)
            else:
                if key in rules_store:
                    del rules_store[key]
            print(f"Updated rules_store: {rules_store}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    thread = Thread(target=consume_messages, args=(10,))
    thread.start()
    thread.join()
    print("thread finished...exiting")

# consumer_thread.join()
