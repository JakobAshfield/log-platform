import asyncio
import os
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from prometheus_fastapi_instrumentator import Instrumentator
from app.database import engine, Base
from app.routers import logs, auth
from app.kafka_producer import get_producer, stop_producer
from app.logging_config import setup_logging

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    
    if os.getenv("ENV") != "test":
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        for attempt in range(10):
            try:
                await get_producer()
                break
            except Exception:
                await asyncio.sleep(5)
    yield
    try:
        await stop_producer()
    except Exception:
        pass


app = FastAPI(title="Log Platform", lifespan=lifespan)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    duration = round((time.time() - start) * 1000, 2)
    
    logger.info(
        "HTTP request processed",
        extra={
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": duration,
        }
    )
    return response


app.include_router(logs.router, prefix="/logs", tags=["logs"])
app.include_router(auth.router, prefix="/auth", tags=["auth"])
Instrumentator().instrument(app).expose(app)


@app.get("/")
async def root():
    return {"message": "Log Platform API"}
