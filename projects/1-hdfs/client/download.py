import requests
import os

# Configuration
NAMENODE_ENDPOINT = "http://localhost:8000"


def fetch_file_metadata(filename):
    """Retrieves the file metadata from the Namenode."""
    response = requests.get(f"{NAMENODE_ENDPOINT}/files/{filename}")
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error retrieving metadata: {response.text}")
        return None


def download_file_blocks(filename, metadata, output_path):
    """Downloads each file block from the DataNodes and assembles it at the final destination."""
    with open(output_path, "wb") as destination_file:
        for block_info in metadata["blocks"]:
            block_id = block_info["number"]
            primary_replica = block_info["replicas"][
                0
            ]  # Download from the first available replica
            block_url = f"http://{primary_replica['host']}:{primary_replica['port']}/files/{filename}/blocks/{block_id}/content"

            print(f"Downloading block {block_id} from {block_url}...")

            response = requests.get(block_url)
            if response.status_code == 200:
                destination_file.write(response.content)
                print(f"Block {block_id} downloaded successfully.")
            else:
                print(f"Error downloading block {block_id}: {response.text}")
                return False
    return True


if __name__ == "__main__":
    filename = input("Enter the name of the file to download: ").strip()
    output_path = input("Enter the destination path: ").strip()

    metadata = fetch_file_metadata(filename)
    if metadata:
        download_successful = download_file_blocks(filename, metadata, output_path)
        if download_successful:
            print(f"File '{filename}' successfully downloaded to '{output_path}'")
        else:
            print(f"An error occurred while downloading the file.")
