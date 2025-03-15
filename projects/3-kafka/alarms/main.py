from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from threading import Thread
import requests

RULES_TOPIC = "rules"
METRICS_TOPIC = "metrics"
ALARMS_TOPIC = "alarms"

# Consumer configuration
CONSUMER_CONFIG = {
    "bootstrap.servers": "kafka-1:19092",
    "group.id": "rules-consumer-group",
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
            else:
                if key in rules_store:
                    rules_store.pop(key)
                    print(f"Rule {key} deleted")
            print(f"Updated rules_store: {rules_store}")
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
    webhook_url = rule_data['discord_webhook_url']

    message = {
        "content": f"**Alarm triggered for rule {rule_data['rule_id']}!**",
        "embeds": [
            {
                "title": "Alarm Triggered",
                "description": f"The metric **{rule_data['metric_name']}** exceeded the threshold!",
                "color": 16711680, 
                "fields": [
                    {
                        "name": "Rule ID",
                        "value": rule_data['rule_id'],
                        "inline": True
                    },
                    {
                        "name": "Metric Name",
                        "value": rule_data['metric_name'],
                        "inline": True
                    },
                    {
                        "name": "Metric Value",
                        "value": str(rule_data['metric_value']),
                        "inline": True
                    },
                    {
                        "name": "Threshold",
                        "value": str(rule_data['threshold']),
                        "inline": True
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message)
        if response.status_code == 204:
            print(f"Alert sent to Discord: {message['content']}")
        else:
            print(f"Failed to send alert to Discord. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error sending alert to Discord: {e}")


rules_thread = Thread(target=consume_messages, daemon=True)
rules_thread.start()
