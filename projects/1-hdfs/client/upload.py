import requests
import os

NN_URL_CONFIG = "http://localhost:8000/datanodes"
NN_URL_FILES = "http://localhost:8000/files"


def upload():
    path = input("File path: ")
    filename = input("Filename: ")

    if not os.path.isfile(path):
        print(f"Error: The path '{path}' does not exist.")
        exit(1)

    size = os.path.getsize(path)

    files_r = requests.post(NN_URL_FILES, json={"file_name": filename, "size": size})
    files_info = files_r.json()

    config_r = requests.get(NN_URL_CONFIG)
    config_info = config_r.json()

    blocks = files_info.get("blocks")
    block_size = config_info.get("block_size")

    with open(path, "rb") as file:
        for block_info in blocks:
            block_number = block_info['number']
            block_replicas = block_info['replicas']
            block_size = block_info['size']

            block_data = file.read(block_size)
            if not block_data:
                break  
            
            for replica_index, datanode in enumerate(block_replicas):
                datanode_url = f"http://{datanode['host']}:{datanode['port']}/blocks/{filename}/{block_number}/{replica_index}"
                response = requests.put(datanode_url, data=block_data)
                
                if response.status_code == 200:
                    print(f"Block {block_number} replica {replica_index} uploaded successfully to {datanode['host']}:{datanode['port']}")
                else:
                    print(f"Failed to upload Block {block_number} replica {replica_index} to {datanode['host']}:{datanode['port']}")


if __name__ == "__main__":
    upload()

