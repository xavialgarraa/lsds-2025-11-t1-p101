from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


# Define the model for the request body
class SumRequest(BaseModel):
    x: float
    y: float


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.post("/sum")
def calculate_sum(payload: SumRequest):
    result = payload.x + payload.y
    return {"result": result}
