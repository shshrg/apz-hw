import uvicorn
from fastapi import FastAPI
import httpx
import asyncio
import time, os, json, socket, random
from pydantic import BaseModel
from contextlib import asynccontextmanager
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable



producer = None
for i in range(50):
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka successfully.")
        break
    except NoBrokersAvailable:
        time.sleep(2)
if not producer:
    raise Exception("Could not connect to Kafka. Restarting...")



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



async def measure_request(func, *args, **kwargs):
    start_time = time.perf_counter()
    try:
        response = await func(*args, **kwargs)
    except Exception as e:
        response = e
    finally:
        end_time = time.perf_counter()
        return response, end_time - start_time



@app.post("/facade_service")
async def post_facade(msg: ClientMessage):

    timestamp = int(time.time())
    producer.send('transaction-events', {"transaction_ID": timestamp,
                                         "user_Id": msg.user_Id,
                                         "amount": msg.amount})


    # TODO remove static address, add k8s logic
    log_addr = f"http://logging-service/logging_service"
    log_task = measure_request(state.client.post, log_addr, json={"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})
    _, log_t = await(log_task)
    state.log_time += log_t
    return {"transaction_ID": timestamp}
    # return {"transaction_ID": "logging-service unavailible"}



@app.get("/facade_service/user/{userId}")
async def get_user_balance_transactions(userId: str):

    tasks = []
    # TODO
    log_addr = f"http://logging-service/logging_service"
    log_task = measure_request(state.client.get, log_addr+"/user/"+userId)
    tasks.append(log_task)
    

    count_addr = f"http://counter-service/counter_service"
    count_task = measure_request(state.client.get, count_addr+"/user/"+userId)
    tasks.append(count_task)

    result = await asyncio.gather(*tasks)
    log_response, count_response = None, None
    i = 0

    log_response = result[0][0].json() if not isinstance(result[0][0], Exception) else None
    state.log_time += result[0][1]
    i = 1
    count_response = result[i][0].json()["balance"] if not isinstance(result[i][0], Exception) else None
    state.count_time += result[i][1]
        
    return {"balance": count_response,
            "transactions": log_response}


@app.get("/facade_service/accounts")
async def get_accounts():
    # TODO
    count_addr = f"http://counter-service/counter_service"

    count_task = measure_request(state.client.get, count_addr+"/accounts")
    (count_response, count_t) = await count_task
    state.count_time += count_t
    if not isinstance(count_response, Exception):
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