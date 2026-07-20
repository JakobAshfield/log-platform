import asyncio
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.main import app
from app.database import Base
from app.dependencies import get_db, rate_limit 

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
)

TestSessionLocal = async_sessionmaker(
    bind=test_engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

async def init_test_db() -> None:
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

asyncio.run(init_test_db())

async def override_get_db() -> AsyncSession:
    async with TestSessionLocal() as session:
        yield session

# 2. Add an empty dependency override to completely bypass the rate limiter during tests
async def override_rate_limit():
    pass

app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[rate_limit] = override_rate_limit  # Apply rate limit bypass
app.router.on_startup.clear()

# 3. Use pytest fixtures to mock Redis operations before any test executes
@pytest.fixture(autouse=True)
def mock_redis(monkeypatch):
    """
    Automatically intercept and patch all redis calls to prevent tests 
    from trying to connect to a real Redis server container.
    """
    class MockRedis:
        async def get(self, key): return None
        async def set(self, key, val, ex=None): return True
        async def setex(self, key, ttl, val): return True
        async def delete(self, key): return True
        
    # Overwrite the actual app connection instance with our mock object
    monkeypatch.setattr("app.routers.logs.redis_client", MockRedis())

client = TestClient(app)


def test_get_nonexistent_log_returns_404():
    response = client.get("/logs/99999")
    assert response.status_code == 404

def test_create_and_get_log_entry():
    # Create a log entry
    response = client.post("/logs/", json={"level": "info", "message": "Test log entry"})
    assert response.status_code == 201
    log_entry = response.json()
    assert log_entry["level"] == "info"
    assert log_entry["message"] == "Test log entry"
    assert "id" in log_entry
    assert "timestamp" in log_entry

    # Get the created log entry by ID
    log_id = log_entry["id"]
    response = client.get(f"/logs/{log_id}")
    assert response.status_code == 200
    fetched_entry = response.json()
    assert fetched_entry == log_entry
    # Expect a MISS in tests because the mock redis always returns None on get()
    assert response.headers.get("X-Cache") == "MISS"


def test_get_logs_with_level_filter(): 
    client.post("/logs/", json={"level": "info", "message": "Info log"})
    client.post("/logs/", json={"level": "warn", "message": "Warn log"})
    client.post("/logs/", json={"level": "error", "message": "Error log"})

    response = client.get("/logs/?level=warn")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) >= 1                              
    assert all(log["level"] == "warn" for log in logs) 


def test_bulk_fetch_logs():
    # Create multiple log entries
    response1 = client.post("/logs/", json={"level": "info", "message": "Bulk log 1"})
    response2 = client.post("/logs/", json={"level": "warn", "message": "Bulk log 2"})
    id1 = response1.json()["id"]
    id2 = response2.json()["id"]

    # Bulk fetch logs by IDs
    response = client.get(f"/logs/bulk-fetch?ids={id1},{id2}")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) == 2
    assert {log["id"] for log in logs} == {id1, id2}


def test_slow_fetch_logs():
    # Create multiple log entries
    response1 = client.post("/logs/", json={"level": "info", "message": "Slow log 1"})
    response2 = client.post("/logs/", json={"level": "warn", "message": "Slow log 2"})
    id1 = response1.json()["id"]
    id2 = response2.json()["id"]

    # Slow fetch logs by IDs
    response = client.get(f"/logs/slow-fetch?ids={id1},{id2}")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) == 2
    assert {log["id"] for log in logs} == {id1, id2}
