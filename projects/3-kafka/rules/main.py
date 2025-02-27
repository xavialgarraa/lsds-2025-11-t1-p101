from typing import Union
from uuid import uuid4
import json
import uuid

from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer

app = FastAPI()

TOPIC = "metrics"
PRODUCER_CONFIG = {
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "client.id": f"metrics-producer-{uuid.uuid4()}",
}
producer = Producer(PRODUCER_CONFIG)


class Rule(BaseModel):
    metric_name: str
    threshold: int
    discord_webhook_url: str


@app.post("/rules")
def create_rule(rule: Rule):
    rule_id = str(uuid.uuid4())
    rule_dict = rule.dict()
    rule_dict["id"] = rule_id
    producer.send("rules", key=rule_id.encode("utf-8"), value=rule_dict)
    producer.flush()
    return rule_dict
