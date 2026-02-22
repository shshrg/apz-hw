import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

class Transaction(BaseModel):
    transaction_ID: int
    user_Id: int
    amount: int

app = FastAPI()
balance_table = {}

@app.post("/counter_service")
async def post_counter(transaction: Transaction):
    user_balance = balance_table.get(transaction.user_Id, 0)
    user_balance += transaction.amount
    balance_table[transaction.user_Id] = user_balance
    return user_balance



@app.get("/counter_service/user/{user_Id}")
async def get_user_balance(user_Id: str):
    user_balance = balance_table.get(int(user_Id), 0)
    return user_balance


@app.get("/counter_service/accounts")
async def get_accounts():
    return balance_table


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)