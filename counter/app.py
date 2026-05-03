import uvicorn
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
import hazelcast
import os
import asyncio

balance_table = {}
hz_mq = None
conn_string = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = hazelcast.HazelcastClient(
        cluster_members=["hazelcast-node:5701"],
        cluster_name="dev"
    )
    global hz_mq
    hz_mq = client.get_queue("message-queue")

    task = asyncio.create_task(consume())
    yield

    task.cancel()
    client.shutdown()

app = FastAPI(lifespan=lifespan)

async def consume():
    while True:
        try:
            head = await asyncio.wrap_future(hz_mq.take())
            await save_to_db(head)
        except Exception as e:
            print(f"Error in consume loop: {e}")
            await asyncio.sleep(1)

async def save_to_db(data):
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO balances (user_id, balance) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET balance = balances.balance + EXCLUDED.balance;",
                (data["user_Id"], data["amount"])
            )


async def get_conn():
    async with await psycopg.AsyncConnection.connect(conn_string, row_factory=dict_row) as conn:
        yield conn



@app.get("/counter_service/user/{user_Id}")
async def get_user_balance(user_Id: str, conn=Depends(get_conn)):
    sql = "SELECT * FROM balances WHERE user_id = %s;"
    async with conn.cursor() as cursor:
        await cursor.execute(sql, (user_Id,))
        return await cursor.fetchone()


@app.get("/counter_service/accounts")
async def get_accounts(conn=Depends(get_conn)):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * from balances;")
        return await cursor.fetchall()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)