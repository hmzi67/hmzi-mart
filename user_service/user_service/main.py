from sqlmodel import Field, Session, SQLModel, create_engine, select, Sequence
from fastapi import FastAPI, Depends



app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "User Service"}