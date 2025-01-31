import requests
import os

# Configuration
NAMENODE_URL = "http://localhost:8000"


def get_file_info():
    """Prompts the user for the file path and retrieves its size."""
    file_path = input("Enter the file path: ").strip()
    if not os.path.exists(file_path):
        print("Error: The file does not exist.")
        return None, None, None

    file_name = input("Enter the file name in the HDFS: ").strip()
    file_size = os.path.getsize(file_path)
    return file_path, file_name, file_size


def create_file_in_namenode(file_name, file_size):
    """Creates a file in the namenode and gets the block allocation."""
    response = requests.post(
        f"{NAMENODE_URL}/files", json={"name": file_name, "size": file_size}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error creating file in the Namenode: {response.text}")
        return None


def upload_blocks(file_path, file_name, file_metadata):
    """Reads the file in blocks and uploads them to the assigned datanodes."""
    with open(file_path, "rb") as f:
        for block in file_metadata["blocks"]:
            block_data = f.read(block["size"])  # Read the block size

            for replica in block["replicas"]:
                datanode_url = f"http://{replica['host']}:{replica['port']}/files/{file_name}/blocks/{block['number']}/content"

                # Debugging
                print(f"Uploading block {block['number']} to {datanode_url}")

                response = requests.put(datanode_url, files={"file": block_data})

                if response.status_code == 200:
                    print(
                        f"Block {block['number']} successfully uploaded to {datanode_url}"
                    )
                else:
                    print(
                        f"Error uploading block {block['number']} to {datanode_url}: {response.text}"
                    )


if __name__ == "__main__":
    local_path, hdfs_name, size = get_file_info()

    if local_path and hdfs_name and size:
        metadata = create_file_in_namenode(hdfs_name, size)
        if metadata:
            upload_blocks(local_path, hdfs_name, metadata)
