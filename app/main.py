from fastapi import FastAPI
from app.routers import logs
from app.database import engine, Base

app = FastAPI(title="Log Platform")

app.include_router(logs.router, prefix="/logs", tags=["logs"])


@app.get("/")
async def root():
    return {"message": "Log Platform API"}

@app.on_event("startup")
async def startup_event():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)