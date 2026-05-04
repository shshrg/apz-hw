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
CONN_STRING = os.getenv("DATABASE_URL")
HZ_URL = os.getenv("HAZELCAST_URL", "localhost:5701")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Connecting to Hazelcast at {HZ_URL}...")
    client = hazelcast.HazelcastClient(
        cluster_members=[HZ_URL],
        cluster_name="dev",
    )
    print("Connected to Hazelcast successfully.")
    global hz_mq
    hz_mq = client.get_queue("message-queue")

    print("Initializing database...")
    await init_db()
    print("Database initialized.")

    task = asyncio.create_task(consume())
    yield

    task.cancel()
    client.shutdown()

app = FastAPI(lifespan=lifespan)

async def consume():
    loop = asyncio.get_running_loop()
    while True:
        try:
            data = await loop.run_in_executor(None, lambda: hz_mq.take().result())
            if data:
                # print(f"saving data to db: {data}")
                await save_to_db(data)
        except Exception as e:
            # continue
            # print(f"Error in consume loop: {e}")
            await asyncio.sleep(1)

async def init_db():
    async with await psycopg.AsyncConnection.connect(CONN_STRING) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS balances (
                    user_id VARCHAR PRIMARY KEY,
                    balance DECIMAL(12,2) DEFAULT 0.00
                );
            """)

async def save_to_db(data):
    async with await psycopg.AsyncConnection.connect(CONN_STRING) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO balances (user_id, balance) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET balance = balances.balance + EXCLUDED.balance;",
                (data["user_Id"], data["amount"])
            )


async def get_conn():
    async with await psycopg.AsyncConnection.connect(CONN_STRING, row_factory=dict_row) as conn:
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