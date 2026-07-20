from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import AsyncSessionLocal
from fastapi import HTTPException, status, Request
from app.redis_client import redis_client

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

async def rate_limit(request: Request):
    client_ip = request.client.host if request.client else "127.0.0.1"
    cache_key = f"rate_limit:{client_ip}"

    limit = 5
    window_seconds = 10

    pipe = redis_client.pipeline()
    pipe.incr(cache_key)
    pipe.ttl(cache_key)
    current_count, ttl = await pipe.execute()

    if current_count == 1 or ttl == -1:
        await redis_client.expire(cache_key, window_seconds)
    if current_count > limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded, try again later"
        )