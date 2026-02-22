import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

class Message(BaseModel):
    msg_text: str
    msg_id: str


app = FastAPI()
msgs_table = {}
# LOGGING


# here maybe change structure of params idk
@app.post("/logging_service")
async def post_logging(msg: Message):
    msg_id = msg.msg_id
    msg_text = msg.msg_text
    msgs_table[msg_id] = msg_text
    print(msg_text)
    return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)