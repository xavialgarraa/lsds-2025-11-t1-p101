from typing import Union
from uuid import uuid4
import json
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


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


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


@app.post("/rules")
def create_rule(rule: Rule):
    try:
        print(rule.dict())
        rule_id = str(uuid.uuid4())
        rule_dict = rule.dict()
        rule_dict["id"] = rule_id
        producer.produce(
            "rules",
            key=rule_id.encode("utf-8"),
            value=json.dumps(rule_dict).encode("utf-8"),
            callback=delivery_report,
        )
        producer.flush()
        return rule_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# information about the Discord webhook and the delete/post requests in /rules/README.md
