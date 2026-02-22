import uvicorn
from fastapi import FastAPI

app = FastAPI()

@app.get("/messages_service")
async def get_messages():
    return "messages service is not implemented yet..."

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)