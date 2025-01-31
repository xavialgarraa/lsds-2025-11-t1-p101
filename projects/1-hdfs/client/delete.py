import requests

# Configuration
NAMENODE_URL = "http://localhost:8000"


def get_file_name():
    """Prompts the user for the file name in the HDFS."""
    file_name = input("Enter the file name in the HDFS: ").strip()
    return file_name


def delete_file_in_namenode(file_name):
    """Deletes a file in the namenode."""
    delete_url = f"{NAMENODE_URL}/files/{file_name}?op=DELETE"
    response = requests.delete(delete_url)
    if response.status_code == 200:
        print(f"File {file_name} successfully deleted from the Namenode.")
    else:
        print(f"Error deleting file {file_name} from the Namenode: {response.text}")


if __name__ == "__main__":
    hdfs_file_name = get_file_name()
    if hdfs_file_name:
        delete_file_in_namenode(hdfs_file_name)
