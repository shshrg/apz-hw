import uvicorn
from fastapi import FastAPI
import httpx
import asyncio
import time
from pydantic import BaseModel
from contextlib import asynccontextmanager

class ClientMessage(BaseModel):
    user_Id: int
    amount: int

class ServiceState:
    client: httpx.AsyncClient = None
    log_time: float = 0
    count_time: float = 0

state = ServiceState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=500)
    state.client = httpx.AsyncClient(timeout=None, limits=limits)
    yield
    await state.client.aclose()

app = FastAPI(lifespan=lifespan)

COUNT_URL = "http://localhost:8081/counter_service"
LOG_URL = "http://localhost:8082/logging_service"


async def measure_request(func, *args, **kwargs):
    start_time = time.perf_counter()
    response = await func(*args, **kwargs)
    end_time = time.perf_counter()
    return response, end_time - start_time



@app.post("/facade_service")
async def post_facade(msg: ClientMessage):
    timestamp = int(time.time())
    log_task = measure_request(state.client.post, LOG_URL, json={"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})
    count_task = measure_request(state.client.post, COUNT_URL, json={"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})
    (_, log_t), (count_response, count_t) = await asyncio.gather(log_task, count_task)
    state.log_time += log_t
    state.count_time += count_t
    return {"transaction_ID": timestamp, "balance": count_response.json()}


@app.get("/facade_service/user/{userId}")
async def get_user_balance_transactions(userId: str):
    log_task = measure_request(state.client.get, LOG_URL+"/user/"+userId)
    count_task = measure_request(state.client.get, COUNT_URL+"/user/"+userId)

    (log_response, log_t), (count_response, count_t) = await asyncio.gather(log_task, count_task)
    state.log_time += log_t
    state.count_time += count_t
    return {"balance": count_response.json(),
            "transactions": log_response.json()}


@app.get("/facade_service/accounts")
async def get_accounts():
    count_task = measure_request(state.client.get, COUNT_URL+"/accounts")
    (count_response, count_t) = await count_task
    state.count_time += count_t
    return count_response.json()



@app.get("/facade_service/debug/service_times")
async def get_times():
    return {"logging_service": state.log_time, "counter_service": state.count_time}

@app.post("/facade_service/debug/service_times")
async def reset_times():
    state.log_time = 0
    state.count_time = 0
    print("[debug] service time counters reset")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)