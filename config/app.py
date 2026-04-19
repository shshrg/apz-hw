from fastapi import FastAPI
import uvicorn

app = FastAPI()

service_ips = {
    "facade": [],
    "logging": [],
    "counter": []
}

@app.post("/")
async def register_service(service_name: str, service_ip: str):
    service_ips[service_name] = service_ips.get(service_name, []) + [service_ip]


@app.get("/")
async def send_ips(service_name: str):
    return service_ips.get(service_name, [])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)