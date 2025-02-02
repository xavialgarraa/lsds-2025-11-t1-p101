import requests
import os

# Configuration
NAMENODE_URL = "http://localhost:8000"


def get_upload_file_info():
    """Prompts the user for the file path and retrieves its size."""
    upload_fpath = input("Enter the file path: ").strip()
    if not os.path.exists(upload_fpath):
        print("Error: The file does not exist.")
        return None, None, None

    upload_fname = input("Enter the file name in the HDFS: ").strip()
    upload_fsize = os.path.getsize(upload_fpath)
    return upload_fpath, upload_fname, upload_fsize


def create_upload_file_in_namenode(upload_fname, upload_fsize):
    """Creates a file in the namenode and gets the block allocation."""
    response = requests.post(
        f"{NAMENODE_URL}/files",
        json={"name": upload_fname, "size": upload_fsize},
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error creating file in the Namenode: {response.text}")
        return None


def upload_file_blocks(upload_fpath, upload_fname, upload_fmetadata):
    """Reads the file in blocks and uploads them to the assigned datanodes."""
    with open(upload_fpath, "rb") as f:
        for block in upload_fmetadata["blocks"]:
            block_data = f.read(block["size"])
            for replica in block["replicas"]:
                datanode_url = f"http://{replica['host']}:{replica['port']}/files/{upload_fname}/blocks/{block['number']}/content"
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
    upload_path, hdfs_upload_name, upload_size = get_upload_file_info()

    if upload_path and hdfs_upload_name and upload_size:
        upload_metadata = create_upload_file_in_namenode(hdfs_upload_name, upload_size)
        if upload_metadata:
            upload_file_blocks(upload_path, hdfs_upload_name, upload_metadata)
