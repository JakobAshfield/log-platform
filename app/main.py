from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers import logs
from app.database import engine, Base
from app.routers import auth
from prometheus_fastapi_instrumentator import Instrumentator
import os
from app.database import engine, Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("ENV") != "test":
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(title="Log Platform", lifespan=lifespan)

app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])
Instrumentator().instrument(app).expose(app)


@app.get("/")
async def root():
    return {"message": "Log Platform API"}