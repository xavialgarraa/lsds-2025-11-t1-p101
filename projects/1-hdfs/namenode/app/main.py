from typing import Union
from fastapi import FastAPI
import json

app = FastAPI()


def load_config():
    with open("namenode/config.json") as file:
        data = json.load(file)
    return data


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/info")
def read_info():
    return {"studentId": 123, "universityName": "upf"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


config = load_config()


@app.get("/datanodes")
def get_datanodes():
    return config
