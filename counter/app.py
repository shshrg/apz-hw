import uvicorn
from fastapi import FastAPI, Depends
from pydantic import BaseModel
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import os

class Transaction(BaseModel):
    transaction_ID: int
    user_Id: int
    amount: int

app = FastAPI()
balance_table = {}


conn_string = os.getenv("DATABASE_URL")
pool = AsyncConnectionPool(conn_string, open=False)

@app.on_event("startup")
async def open_pool():
    await pool.open()

async def get_conn():
    async with await pool.connection() as conn:
        yield conn


@app.post("/counter_service")
async def post_counter(transaction: Transaction, conn=Depends(get_conn)):
    sql = """
        INSERT INTO balances (user_id, balance)
        VALUES (%s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET balance = balances.balance + EXCLUDED.balance;
    """
    sql_2 = "SELECT * FROM balances where user_id = %s ;"

    async with conn.cursor() as cursor:
        await cursor.execute(sql, (transaction.user_Id, transaction.amount))
        await cursor.execute(sql_2, (transaction.user_Id,))
        return await cursor.fetchone()



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