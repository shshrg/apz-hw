import uvicorn
from fastapi import FastAPI
import httpx
import asyncio
import time
import random
from pydantic import BaseModel
from contextlib import asynccontextmanager
import hazelcast
from kubernetes import client, config

class ClientMessage(BaseModel):
    user_Id: int
    amount: int

class ServiceState:
    client: httpx.AsyncClient = None
    log_time: float = 0
    count_time: float = 0

hz_mq = None
v1 = None
state = ServiceState()

@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=1000, max_keepalive_connections=500)
    state.client = httpx.AsyncClient(timeout=None, limits=limits)

    hz_client = hazelcast.HazelcastClient(
        cluster_members=["hazelcast-node:5701"],
        cluster_name="dev"
    )

    global v1
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    global hz_mq
    hz_mq = client.get_queue("message-queue")

    yield
    await state.client.aclose()
    hz_client.shutdown()

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

def get_service_ips(service_name, namespace="default"):
    endpoints = v1.read_namespaced_endpoints(service_name, namespace)
    instances = []
    for subset in endpoints.subsets:
        for address in subset.addresses:
            for port in subset.ports:
                instances.append({
                    "ip": address.ip,
                    "port": port.port,
                })
    return instances

@app.post("/facade_service")
async def post_facade(msg: ClientMessage):

    timestamp = int(time.time())

    hz_mq.offer({"transaction_ID": timestamp,
                 "user_Id": msg.user_Id,
                 "amount": msg.amount})


    logging_addresses = get_service_ips("logging-service")
    if not logging_addresses:
        return {"error": "logging-service unavailible"}

    log_addr = random.choice(logging_addresses)
    log_task = measure_request(state.client.post, log_addr, json={"transaction_ID": timestamp,
                                                            "user_Id": msg.user_Id,
                                                            "amount": msg.amount})
    _, log_t = await(log_task)
    state.log_time += log_t
    return {"transaction_ID": timestamp}

@app.get("/facade_service/user/{userId}")
async def get_user_balance_transactions(userId: str):

    tasks = []
    logging_addresses = get_service_ips("logging-service")

    if logging_addresses:
        log_addr = random.choice(logging_addresses)
        log_task = measure_request(state.client.get, f"http://{log_addr["ip"]}:{log_addr["port"]}/user/{userId}")
        tasks.append(log_task)
    

    counter_addresses = get_service_ips("counter-service")
    if counter_addresses:
        count_addr = random.choice(counter_addresses)
        count_task = measure_request(state.client.get, f"http://{count_addr["ip"]}:{count_addr["port"]}/user/{userId}")
        tasks.append(count_task)

    result = await asyncio.gather(*tasks)
    log_response, count_response = None, None
    i = 0

    if logging_addresses:
        log_response = result[0][0].json() if not isinstance(result[0][0], Exception) else None
        state.log_time += result[0][1]
        i = 1
    if counter_addresses:
        count_response = result[i][0].json()["balance"] if not isinstance(result[i][0], Exception) else None
        state.count_time += result[i][1]
        
    return {"balance": count_response,
            "transactions": log_response}


@app.get("/facade_service/accounts")
async def get_accounts():
    counter_addresses = get_service_ips("counter-service")
    if counter_addresses:
        count_addr = random.choice(counter_addresses)

        count_task = measure_request(state.client.get, count_addr+"/accounts")
        (count_response, count_t) = await count_task
        state.count_time += count_t
        if not isinstance(count_response, Exception):
            return count_response.json()
    return {"error": "counter-service unavailable"}



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