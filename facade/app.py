import uvicorn
from fastapi import FastAPI
import httpx
import uuid
from pydantic import BaseModel

class Message(BaseModel):
    msg: str

app = FastAPI()
MSGS_URL = "http://localhost:8081"
LOG_URL = "http://localhost:8082"

# FACADE

@app.get("/facade_service")
async def get_facade():
    return

@app.post("/facade_service")
async def post_facade(msg: Message):
    msg_id = str(uuid.uuid4())
    async with httpx.AsyncClient() as client:
        await client.post(LOG_URL + "/logging_service", json={"msg_text": msg.msg, "msg_id": msg_id})
    # log_response = httpx.post(LOG_URL, json={"msg": msg.msg, "id": msg_id})
    return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)