from fastapi import FastAPI
from app.routers import logs

app = FastAPI(title="Log Platform")

app.include_router(logs.router, prefix="/logs", tags=["logs"])


@app.get("/")
async def root():
    return {"message": "Log Platform API"}