from typing import Union
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import requests

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
        current_data = {}
        with open("app/files.json", "r") as file:
            current_data = json.load(file)
    except Exception as e:
        print(f"Unexpected error in read the file: {e}")
        raise RuntimeError("Failed to read the file in the system.")

    # Append the new data to the current data dict
    if data["file_name"] not in current_data:
        current_data[data["file_name"]] = data
    else:
        raise HTTPException(status_code=409, detail="File already in dictionary.")

    # Save the updated data back to the file
    try:
        with open("app/files.json", "w") as file:
            json.dump(current_data, file, indent=4)
        print("Data saved successfully.")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
        raise RuntimeError("Failed to save data to the JSON file.")


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


@app.get("/files/{filename}")
def read_file(filename: str):
    files_metadata = load_files()
    if filename in files_metadata:
        return files_metadata[filename]
    else:
        raise HTTPException(status_code=400, detail=f"File {filename} not found.")


datanode_map = {}
for datanode in datanodes:
    datanode_id = datanode["id"]
    datanode_map[datanode_id] = datanode


@app.post("/filesad0q8")
def upload_files(file: File):
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
            replicas.append(datanodes[replica_idx]["id"])
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


@app.get("/filesad0q8/{filename}")
def read_file(filename: str):
    files_metadata = load_files()
    if filename in files_metadata:
        file_data = files_metadata[filename]
        for block in file_data["blocks"]:
            for i, replica_id in enumerate(block["replicas"]):
                datanode = datanode_map.get(replica_id)
                if datanode:
                    block["replicas"][i] = {
                        "id": replica_id,
                        "host": datanode["host"],
                        "port": datanode["port"],
                    }
        return file_data
    else:
        raise HTTPException(status_code=400, detail=f"File {filename} not found.")


@app.post("/filesad0q9")
def upload_files(file: File):
    # Calculate the number of blocks needed
    if file.size % block_size != 0:
        num_blocks = (file.size // block_size) + 1
    else:
        num_blocks = file.size // block_size

    # Initialize the datanode block count
    datanode_block_count = {datanode["id"]: 0 for datanode in datanodes}

    blocks = []  # List to store block information
    rest_size = file.size

    # Create blocks with size and replicas
    for i in range(num_blocks):
        size = min(block_size, rest_size)
        rest_size -= size

        # Find the datanode with the least number of blocks
        sorted_datanodes = sorted(datanode_block_count.items(), key=lambda x: x[1])
        datanode_idx = sorted_datanodes[0][0]  # Select the least loaded datanode

        datanode_block_count[datanode_idx] += 1

        replicas = [datanode_idx]
        for j in range(1, num_replicas):
            available_datanodes = [
                (datanode, count)
                for datanode, count in datanode_block_count.items()
                if datanode != datanode_idx
                and count
                < 2  # Limit the number of blocks per datanode to 2 as seen in theory
            ]

            # If there are no available datanodes, select from all datanodes
            if not available_datanodes:
                available_datanodes = [
                    (datanode, count)
                    for datanode, count in datanode_block_count.items()
                ]

            # Select the datanode with the least number of blocks
            sorted_available_datanodes = sorted(available_datanodes, key=lambda x: x[1])
            replica_idx = sorted_available_datanodes[0][0]
            replicas.append(replica_idx)
            datanode_block_count[replica_idx] += 1

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


@app.delete("/files/{filename}")
def delete_file(filename: str):
    print(f"Attempting to delete file: {filename}")
    try:
        files_metadata = load_files()
        print(f"Loaded files metadata: {files_metadata}")
    except Exception as e:
        print(f"Error loading files metadata: {e}")
        raise HTTPException(status_code=500, detail="Error loading files metadata")

    if filename not in files_metadata:
        print(f"File {filename} not found in metadata.")
        raise HTTPException(status_code=404, detail=f"File {filename} not found.")

    # Notify datanodes to delete file blocks
    for block in files_metadata[filename]["blocks"]:
        for replica in block["replicas"]:
            datanode_url = f"http://{replica['host']}:{replica['port']}/files/{filename}/blocks/{block['number']}"
            print(f"Sending DELETE request to {datanode_url}")
            try:
                response = requests.delete(datanode_url)
                if response.status_code != 200:
                    print(
                        f"Error deleting block {block['number']} from {datanode_url}: {response.text}"
                    )
                    raise HTTPException(
                        status_code=500,
                        detail=f"Error deleting block {block['number']} from {datanode_url}: {response.text}",
                    )
            except Exception as e:
                print(
                    f"Exception occurred while sending DELETE request to {datanode_url}: {e}"
                )
                raise HTTPException(
                    status_code=500,
                    detail=f"Exception occurred while sending DELETE request to {datanode_url}: {e}",
                )

    # Remove file metadata
    try:
        del files_metadata[filename]
        print(f"File metadata for {filename} removed.")
    except Exception as e:
        print(f"Error removing file metadata for {filename}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error removing file metadata for {filename}: {e}"
        )

    # Save updated metadata
    try:
        with open("app/files.json", "w") as file:
            json.dump(files_metadata, file, indent=4)
        print(f"Metadata for file {filename} deleted successfully.")
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to save updated metadata to the JSON file."
        )

    return {"message": f"File {filename} deleted successfully."}
