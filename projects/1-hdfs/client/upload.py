import requests
import os

NN_URL_CONFIG = "http://localhost:8000/datanodes"
NN_URL_FILES = "http://localhost:8000/files"


def upload():
    path = input("File path: ")
    filename = input("Filename: ")

    if not os.path.isfile(file_path):
        print(f"Error: The path '{path}' does not exist.")
        exit(1)

    size = os.path.getsize(path)
    files_r = requests.post(NN_URL, json={"file_name": filename, "size": size})
    files_info = files_r.json()
    config_r = requests.get(NN_URL_CONFIG)
    config_info = config_r.json()

    blocks = files_info.get("blocks")
    block_size = config_info.get(NN_URL_CONFIG)
