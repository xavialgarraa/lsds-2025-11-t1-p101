from fastapi import FastAPI, UploadFile
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
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


@app.get("/files/{filename}/blocks/{block_number}/content")
async def get_file_block(filename: str, block_number: int):
    # Path to the block
    storage_path = os.path.join("datanode/storage", filename, str(block_number))
    # Check if the file exists
    if not os.path.exists(storage_path):
        raise HTTPException(status_code=404, detail="Block not found")
    
    print(f"Checking file at: {storage_path}")

    # Return the file
    try:
        return FileResponse(storage_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file block: {e}")