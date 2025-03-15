from typing import Union
import json
import uuid

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, ValidationError
from confluent_kafka import Producer, KafkaException

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


# Configuración Kafka
TOPIC = "rules"
PRODUCER_CONFIG = {
    "bootstrap.servers": "kafka-1:9091",
    "client.id": f"kafka-quickstart-producer-{uuid.uuid4()}",
}

producer = Producer(PRODUCER_CONFIG)


class Rule(BaseModel):
    metric_name: str
    threshold: int
    discord_webhook_url: str


def delivery_report(err, msg):
    if err:
        print(f"Error in delivering Kafka: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [Partition: {msg.partition()}]")


rules_store = {}


@app.post("/rules")
def create_rule(rule: Rule):
    rule_id = str(uuid.uuid4())
    rule_data = rule.dict()
    rule_data["id"] = rule_id

    rules_store[rule_id] = rule_data

    producer.produce(
        TOPIC, key=rule_id, value=json.dumps(rule_data), callback=delivery_report
    )

    producer.flush()
    return rule_data


@app.delete("/rules")
def delete_rule(rule_id: str):
    if rule_id not in rules_store:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found"
        )

    del rules_store[rule_id]

    producer.produce(TOPIC, key=rule_id, value=None, callback=delivery_report)

    producer.flush()
    return {"message": "Rule deleted"}
