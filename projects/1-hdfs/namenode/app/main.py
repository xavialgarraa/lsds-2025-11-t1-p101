from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
import json

app = FastAPI()


class File(BaseModel):
    name: str
    size: int


def load_config():
    with open("app/config.json") as file:
        data = json.load(file)
    return data


def save_files(data):
    with open("app/files.json", "r") as file:
        current_data = json.load(file)

    current_data.append(data)

    with open("app/files.json", "w") as file:
        json.dump(data, file, indent=4)


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


datanodes = config["datanodes"]
block_size = config["block_size"]
num_replicas = config["replication_factor"]


@app.post("/files")
def upload_files(file: File):
    if file.size % block_size != 0:
        num_blocks = (file.size // block_size) + 1
    else:
        num_blocks = file.size // block_size

    blocks = []
    rest_size = file.size

    for i in range(num_blocks):
        size = min(block_size, rest_size)
        rest_size -= size
        datanode_idx = i % len(datanodes)

        replicas = []
        for j in range(num_replicas):
            replica_idx = (datanode_idx + j + 1) % len(datanodes)
            replicas.append(datanodes[replica_idx])

        blocks.append(
            {
                "number": i,
                "size": size,
                "replicas": replicas,
            }
        )

    data = {"file_name": file.name, "size": file.size, "blocks": blocks}

    save_files(data)

    return {"number_blocks": num_blocks, "datanode": datanodes[datanode_idx]}
