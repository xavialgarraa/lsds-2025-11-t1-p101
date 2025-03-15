from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from threading import Thread
import requests
import uuid

RULES_TOPIC = "rules"
METRICS_TOPIC = "metrics"

# Consumer configuration
CONSUMER_CONFIG = {
    "bootstrap.servers": "kafka-1:9092",
    "group.id": f"rules-consumer-{uuid.uuid4()}",
    "auto.offset.reset": "earliest",
}

# Consumer rules creation
consumer = Consumer(CONSUMER_CONFIG)
consumer.subscribe([RULES_TOPIC])

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

            key = msg.key().decode("utf-8")
            value = msg.value()

            if value is not None:
                rules_store[key] = json.loads(value.decode("utf-8"))
                print(f"Updated rule: {rules_store[key]}")
            else:
                if key in rules_store:
                    rules_store.pop(key)
                    print(f"Rule {key} deleted")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


METRICS_CONFIG = {
    "bootstrap.servers": "kafka-1:19092",
    "group.id": "metrics-consumer-group",
    "auto.offset.reset": "earliest",
}

# Consumer metrics creation
metrics_consumer = Consumer(CONSUMER_CONFIG)
metrics_consumer.subscribe([METRICS_TOPIC])


import requests


def send_discord_alert(rule_data):
    webhook_url = rule_data["discord_webhook_url"]

    message = {
        "content": f"**Alarm triggered for rule {rule_data['rule_id']}!**",
        "embeds": [
            {
                "title": "Alarm Triggered",
                "description": f"The metric **{rule_data['metric_name']}** exceeded the threshold!",
                "color": 16711680,
                "fields": [
                    {"name": "Rule ID", "value": rule_data["rule_id"], "inline": True},
                    {
                        "name": "Metric Name",
                        "value": rule_data["metric_name"],
                        "inline": True,
                    },
                    {
                        "name": "Metric Value",
                        "value": str(rule_data["metric_value"]),
                        "inline": True,
                    },
                    {
                        "name": "Threshold",
                        "value": str(rule_data["threshold"]),
                        "inline": True,
                    },
                ],
            }
        ],
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code == 204:
            print(f"Alert sent to Discord: {message['content']}")
        else:
            print(
                f"Failed to send alert to Discord. Status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Error sending alert to Discord: {e}")


def consume_metrics():
    try:
        while True:
            msg = metrics_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            key = msg.key().decode("utf-8")

            metric = json.loads(msg.value().decode("utf-8"))
            value = metric.get("value")

            for rule in rules_store.values():
                if rule["metric_name"] == key and value > rule["threshold"]:
                    alert = {
                        "metric_name": key,
                        "metric_value": value,
                        "rule_id": rule["id"],
                        "threshold": rule["threshold"],
                        "discord_webhook_url": rule["discord_webhook_url"],
                    }
                    send_discord_alert(alert)
    except KeyboardInterrupt:
        pass
    finally:
        metrics_consumer.close()


rules_thread = Thread(target=consume_messages, daemon=True)
metrics_thread = Thread(target=consume_metrics, daemon=True)
rules_thread.start()
metrics_thread.start()
