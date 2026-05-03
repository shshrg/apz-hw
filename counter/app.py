import uvicorn
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
from aiokafka import AIOKafkaConsumer
import os, socket, time, json
import httpx
import asyncio


async def consume_kafka():
    consumer = AIOKafkaConsumer(
        'transaction-events',
        bootstrap_servers=['kafka:9092'],
        group_id='transactions-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    await consumer.start()
    try:
        async for message in consumer:
            data = message.value
            await save_to_db(data)
    except Exception as e:
        print(f"Kafka consume error: {e}")
    finally:
        await consumer.stop()


async def save_to_db(data):
    async with await psycopg.AsyncConnection.connect(conn_string) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO balances (user_id, balance) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET balance = balances.balance + EXCLUDED.balance;",
                (data["user_Id"], data["amount"])
            )

class Transaction(BaseModel):
    transaction_ID: int
    user_Id: int
    amount: int

class ServiceRegistration(BaseModel):
    service_name: str
    service_ip: str


balance_table = {}

CONFIG_URL = os.getenv("CONFIG_URL", "http://config-service:8083")

@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_task = asyncio.create_task(consume_kafka())
    yield

app = FastAPI(lifespan=lifespan)

conn_string = os.getenv("DATABASE_URL")

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