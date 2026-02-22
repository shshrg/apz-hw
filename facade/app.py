import uvicorn
from fastapi import FastAPI
import httpx
import asyncio
import time
from pydantic import BaseModel

class ClientMessage(BaseModel):
    user_Id: int
    amount: int

app = FastAPI()
COUNT_URL = "http://localhost:8081/counter_service"
LOG_URL = "http://localhost:8082/logging_service"

@app.post("/facade_service")
async def post_facade(msg: ClientMessage):
    timestamp = int(time.time())
    async with httpx.AsyncClient() as client:
        log_task = client.post(LOG_URL, json={"transaction_ID": timestamp,
                                        "user_Id": msg.user_Id,
                                        "amount": msg.amount})
        count_task = client.post(COUNT_URL, json={"transaction_ID": timestamp,
                                        "user_Id": msg.user_Id,
                                        "amount": msg.amount})
        _, count_response = await asyncio.gather(log_task, count_task)
    
    return {"transaction_ID": timestamp, "balance": count_response.json()}


@app.get("/facade_service/user/{userId}")
async def get_user_balance_transactions(userId: str):
    async with httpx.AsyncClient() as client:
        log_task = client.get(LOG_URL+"/user/"+userId)
        count_task = client.get(COUNT_URL+"/user/"+userId)
        log_response, count_response = await asyncio.gather(log_task, count_task)
    return {"balance": count_response.json(),
            "transactions": log_response.json()}


@app.get("/facade_service/accounts")
async def get_accounts():
    async with httpx.AsyncClient() as client:
        count_response = await client.get(COUNT_URL+"/accounts")
    return count_response.json()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)