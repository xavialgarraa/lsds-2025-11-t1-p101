import requests

# Configuration
NAMENODE_URL = "http://localhost:8000"


def delete_file_from_hdfs(filename):
    """Deletes a file from the HDFS."""
    response = requests.delete(f"{NAMENODE_URL}/files/{filename}")
    if response.status_code == 200:
        print(f"File {filename} deleted successfully.")
    else:
        print(f"Error deleting file {filename}: {response.text}")


if __name__ == "__main__":
    filename = input("Enter the name of the file to delete: ").strip()
    delete_file_from_hdfs(filename)
