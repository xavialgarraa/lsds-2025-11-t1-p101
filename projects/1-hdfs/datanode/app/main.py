from fastapi import FastAPI, UploadFile
import os

app = FastAPI()


def create_directory(path: str):
    os.makedirs(path, exist_ok=True)


@app.put("/files/{filename}/blocks/{block_number}/content")
async def upload_file_block(filename: str, block_number: int, file: UploadFile):
    storage_path = os.path.join("datanode/storage", filename, str(block_number))

    create_directory(os.path.dirname(storage_path))

    with open(storage_path, "wb") as f:
        content = await file.read()
        f.write(content)

    return {
        "message": f"Block {block_number} of file {filename} uploaded successfully."
    }
