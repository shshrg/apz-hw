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


CONFIG_URL = os.getenv("CONFIG_URL", "http://config-service:8083")


class ClientMessage(BaseModel):
    user_Id: int
    amount: int

class ServiceRegistration(BaseModel):
    service_name: str
    service_ip: str

class ServiceState:
    client: httpx.AsyncClient = None
    log_time: float = 0
    count_time: float = 0


state = ServiceState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=500)
    state.client = httpx.AsyncClient(timeout=None, limits=limits)

    hostname = socket.gethostname()
    ip_addr = socket.gethostbyname(hostname)
    registration_data = ServiceRegistration(service_name="facade",
                                            service_ip=f"{ip_addr}:8080")
    await state.client.post(CONFIG_URL, json=registration_data.model_dump())

    yield
    await state.client.aclose()

app = FastAPI(lifespan=lifespan)



async def measure_request(func, *args, **kwargs):
    start_time = time.perf_counter()
    response = await func(*args, **kwargs)
    end_time = time.perf_counter()
    return response, end_time - start_time



@app.post("/facade_service")
async def post_facade(msg: ClientMessage):
    log_ips = []
    while not log_ips:
        log_ips = await state.client.get(f"{CONFIG_URL}/logging")

    log_addr = f"http://{random.choice(log_ips.json())}/logging_service"




    timestamp = int(time.time())
    log_task = measure_request(state.client.post, log_addr, json={"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})

    producer.send('transaction-events', {"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})
    _, log_t = await(log_task)
    state.log_time += log_t
    return {"transaction_ID": timestamp}


@app.get("/facade_service/user/{userId}")
async def get_user_balance_transactions(userId: str):
    log_ips, count_ips = [], []
    while not log_ips or not count_ips:
        log_ips = await state.client.get(f"{CONFIG_URL}/logging")
        count_ips = await state.client.get(f"{CONFIG_URL}/counter")

    log_addr = f"http://{random.choice(log_ips.json())}/logging_service"
    count_addr = f"http://{random.choice(count_ips.json())}/counter_service"

    log_task = measure_request(state.client.get, log_addr+"/user/"+userId)
    count_task = measure_request(state.client.get, count_addr+"/user/"+userId)

    (log_response, log_t), (count_response, count_t) = await asyncio.gather(log_task, count_task)
    state.log_time += log_t
    state.count_time += count_t
    return {"balance": count_response.json(),
            "transactions": log_response.json()}


@app.get("/facade_service/accounts")
async def get_accounts():
    count_ips = []
    while not count_ips:
        count_ips = await state.client.get(f"{CONFIG_URL}/counter")
    count_addr = f"http://{random.choice(count_ips.json())}/counter_service"

    count_task = measure_request(state.client.get, count_addr+"/accounts")
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