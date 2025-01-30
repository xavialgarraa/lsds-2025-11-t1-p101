import requests

NN_URL = "http://localhost:8000/datanodes"

r = requests.get(NN_URL)
datanodes = r.json()
print("List of Datanodes:")

for idx, datanode in enumerate(datanodes["datanodes"]):
    print(f"Datanode: {idx + 1}, Port: {datanode['port']}")
