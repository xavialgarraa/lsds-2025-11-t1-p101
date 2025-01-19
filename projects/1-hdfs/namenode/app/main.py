from typing import Union
from fastapi import FastAPI, HTTPException
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
    try:
        current_data = []
        file = open("app/files.json", "r")
        current_data = json.load(file)
        # Append the new data to the current data list
        current_data.append(data)
        # Save the updated data back to the file
        try:
            with open("app/files.json", "w") as file:
                json.dump(current_data, file, indent=4)
            print("Data saved successfully.")
        except Exception as e:
            print(f"Error writing to JSON file: {e}")
            raise RuntimeError("Failed to save data to the JSON file.")
    except Exception as e:
        print(f"Unexpected error in save_files: {e}")
        raise RuntimeError("Failed to save the file in the system.")


def load_files():
    with open("app/files.json") as file:
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


# Can be outside the functions, because the config is not modified while the server is running
config = load_config()
datanodes = config["datanodes"]
block_size = config["block_size"]
num_replicas = config["replication_factor"]


@app.get("/datanodes")
def get_datanodes():
    return config


@app.post("/files")
def upload_files(file: File):
    try:
        # Calculate the number of blocks needed
        if file.size % block_size != 0:
            num_blocks = (file.size // block_size) + 1
        else:
            num_blocks = file.size // block_size

        blocks = []  # List to store block information
        rest_size = file.size

        # Create blocks with size and replicas
        for i in range(num_blocks):
            size = min(block_size, rest_size)
            rest_size -= size
            datanode_idx = i % len(datanodes)

            replicas = []
            for j in range(num_replicas):
                replica_idx = (datanode_idx + j) % len(datanodes)
                replicas.append(datanodes[replica_idx])
            blocks.append(
                {
                    "number": i,
                    "size": size,
                    "replicas": replicas,
                }
            )

        data = {"file_name": file.name, "size": file.size, "blocks": blocks}
        save_files(data)  # Save the file data

        return {
            "file_name": file.name,
            "size": file.size,
            "number_blocks": num_blocks,
            "blocks": blocks,
        }

    except Exception as e:
        print(f"Unexpected error in upload_files: {e}")
        return {"error": "An unexpected error occurred while processing the file."}


@app.get("/files/{filename}")
def read_file(filename: str):
    files_metadata = load_files()
    for file_info in files_metadata:
        if file_info["file_name"] == filename:
            return file_info

    raise HTTPException(status_code=404, detail=f"File {filename} not found.")
