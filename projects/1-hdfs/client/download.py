import requests
import os

# Configuration
NAMENODE_URL = "http://localhost:8000"


def get_file_metadata(hdfs_file_name):
    """Fetches the metadata for a given file from the Namenode."""
    response = requests.get(f"{NAMENODE_URL}/files/{hdfs_file_name}/metadata")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching metadata: {response.text}")
        return None


def download_file_blocks(hdfs_file_name, file_metadata, local_destination_path):
    """Downloads each block of the file from the DataNodes and assembles them into the final destination."""
    with open(local_destination_path, "wb") as output_file:
        for block_info in file_metadata["blocks"]:
            block_num = block_info["number"]
            first_replica = block_info["replicas"][
                0
            ]  # Download from the first available replica
            datanode_url = f"http://{first_replica['host']}:{first_replica['port']}/files/{hdfs_file_name}/blocks/{block_num}/content"

            print(f"Downloading block {block_num} from {datanode_url}...")

            response = requests.get(datanode_url)
            if response.status_code == 200:
                output_file.write(response.content)
                print(f"Block {block_num} downloaded successfully.")
            else:
                print(f"Error downloading block {block_num}: {response.text}")
                return False
    return True


if __name__ == "__main__":
    hdfs_file_name = input("Enter the name of the file to download: ").strip()
    local_destination_path = input("Enter the local destination path: ").strip()

    file_metadata = get_file_metadata(hdfs_file_name)
    if file_metadata:
        success = download_file_blocks(
            hdfs_file_name, file_metadata, local_destination_path
        )
        if success:
            print(
                f"File '{hdfs_file_name}' downloaded successfully to '{local_destination_path}'"
            )
        else:
            print(f"Error downloading the file.")
