from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers import logs
from app.database import engine, Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Log Platform", lifespan=lifespan)

app.include_router(logs.router, prefix="/logs", tags=["logs"])


@app.get("/")
async def root():
    return {"message": "Log Platform API"}