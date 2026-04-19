import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from pydantic import BaseModel
import hazelcast
import httpx
import socket, os

class Transaction(BaseModel):
    transaction_ID: int
    user_Id: int
    amount: int

logging_map = None
CONFIG_URL = os.getenv("CONFIG_URL", "http://config-service:8083")

app = FastAPI()

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = hazelcast.HazelcastClient(
        cluster_members=["hazelcast-node:5701"],
        cluster_name="dev"
    )
    global logging_map
    logging_map = client.get_map("logging-map")

    hostname = socket.gethostname()
    ip_addr = socket.gethostbyname(hostname)
    registration_data = {
        "service_name": "logging",
        "service_ip": f"{ip_addr}:8082"
    }
    async with httpx.AsyncClient() as client:
        await client.post(CONFIG_URL, json=registration_data)
    yield

    client.shutdown()

@app.post("/logging_service")
async def post_logging(transaction: Transaction):
    logging_map.put(
        transaction.transaction_ID,
        {"user_Id": transaction.user_Id,
         "amount": transaction.amount})
    print(f"user_Id: {transaction.user_Id} amount: {transaction.amount}")


@app.get("/logging_service/user/{user_Id}")
async def get_user_logs(user_Id: str):
    user_logs = []
    
    entries = logging_map.entry_set()
    entries = entries.result()

    for transaction_id , data in entries:
        if data.get("user_Id") == int(user_Id):
            user_logs.append({
                "transaction_id": transaction_id,
                "amount": data["amount"]
            })
    return user_logs


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)