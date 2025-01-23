import requests

NN_URL = "http://localhost:8000/datanodes"


def list_datanodes():
    r = requests.get(NN_URL)
    datanodes = r.json()
    print("List of Datanodes:")
    for idx, datanode in enumerate(datanodes, start=1):
        print(f"{idx}. Host: {datanode['host']}, Port: {datanode['port']}")


if __name__ == "__main__":
    list_datanodes()
