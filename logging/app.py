import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

class Transaction(BaseModel):
    transaction_ID: int
    user_Id: int
    amount: int


app = FastAPI()
transactions_table = {}



@app.post("/logging_service")
async def post_logging(transaction: Transaction):
    transactions_table[transaction.transaction_ID] = {
        "user_Id": transaction.user_Id,
        "amount": transaction.amount
    }
    print(f"user_Id: {transaction.user_Id} amount: {transaction.amount}")


@app.get("/logging_service/user/{user_Id}")
async def get_user_logs(user_Id: str):
    user_logs = []
    for t in transactions_table:
        if transactions_table[t]["user_Id"] == int(user_Id):
            user_logs.append({"transaction_id": t,
                              "amount": transactions_table[t]["amount"]})
    return user_logs


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)