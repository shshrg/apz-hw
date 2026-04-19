from fastapi import FastAPI
import uvicorn
from pydantic import BaseModel

app = FastAPI()

service_ips = {
    "facade": [],
    "logging": [],
    "counter": []
}

class ServiceRegistration(BaseModel):
    service_name: str
    service_ip: str

@app.post("/")
async def register_service(data: ServiceRegistration):
    service_ips[data.service_name] = service_ips.get(data.service_name, []) + [data.service_ip]
    print(f"Registered {data.service_name}: {data.service_ip}")


@app.get("/{service_name}")
async def send_ips(service_name: str):
    return service_ips.get(service_name, [])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)