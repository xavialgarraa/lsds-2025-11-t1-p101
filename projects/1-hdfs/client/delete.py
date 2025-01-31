import requests


def delete_file_in_namenode(file_name):
    namenode_url = "http://namenode:50070/webhdfs/v1/"
    delete_url = f"{namenode_url}{file_name}?op=DELETE"
    response = requests.delete(delete_url)
    if response.status_code == 200:
        print(f"Archivo {file_name} eliminado correctamente del Namenode.")
    else:
        print(f"Error al eliminar archivo {file_name} del Namenode: {response.text}")


if __name__ == "__main__":
    file_name = input("Ingrese el nombre del archivo en el sistema HDFS: ")
    delete_file_in_namenode(file_name)
