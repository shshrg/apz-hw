import uvicorn
from fastapi import FastAPI
import httpx
import uuid
from pydantic import BaseModel

class Message(BaseModel):
    msg: str

app = FastAPI()
MSGS_URL = "http://localhost:8081/messages_service"
LOG_URL = "http://localhost:8082/logging_service"

# FACADE

@app.get("/facade_service")
async def get_facade():
    facade_response = {}
    async with httpx.AsyncClient() as client:
        log_response = await client.get(LOG_URL)
        msgs_response = await client.get(MSGS_URL)
    facade_response["logging_response"] = log_response.json()
    facade_response["messages_response"] = msgs_response.json()
    return facade_response

@app.post("/facade_service")
async def post_facade(msg: Message):
    msg_id = str(uuid.uuid4())
    async with httpx.AsyncClient() as client:
        await client.post(LOG_URL, json={"msg_text": msg.msg, "msg_id": msg_id})
    return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)