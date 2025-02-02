import requests

# Configuration
NAMENODE_URL = "http://localhost:8000"


def list_files_in_namenode():
    """Lists all files in the Namenode."""
    response = requests.get(f"{NAMENODE_URL}/files")
    if response.status_code == 200:
        files = response.json()
        print("Files in Namenode:")
        for file in files:
            print(file)
    else:
        print(f"Error listing files in Namenode: {response.text}")


if __name__ == "__main__":
    list_files_in_namenode()
