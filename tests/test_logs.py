import asyncio
import pytest
import os

# 1. Ensure environment variables are assigned before anything else to prevent key errors
os.environ["SECRET_KEY"] = "test-secret-key-not-for-production-needs-to-be-long"
os.environ["ALGORITHM"] = "HS256"
os.environ["ENV"] = "test"

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.main import app
from app.database import Base
from app.dependencies import get_db, rate_limit 
from app.auth import create_access_token

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

async def override_rate_limit():
    pass

app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[rate_limit] = override_rate_limit  
app.router.on_startup.clear()

@pytest.fixture(autouse=True)
def mock_redis(monkeypatch):
    class MockRedis:
        async def get(self, key): return None
        async def set(self, key, val, ex=None): return True
        async def setex(self, key, ttl, val): return True
        async def delete(self, key): return True
    class MockKafkaProducer:
        async def send(self, topic, value):
            future = asyncio.Future()
            future.set_result(True)
            return future
        async def start(self): return True
        async def stop(self): return True

    # Intercept the global app factory helper directly
    async def mock_get_producer():
        return MockKafkaProducer()  

    monkeypatch.setattr(
    "app.routers.logs.get_producer", mock_get_producer,)
    monkeypatch.setattr("app.routers.logs.redis_client", MockRedis())

client = TestClient(app)

# --- GENERATE REUSABLE TEST HEADERS ---
# Generating a standard authenticated header mock dictionary
TEST_USER_TOKEN = create_access_token({"sub": "test_user", "role": "user"})
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_USER_TOKEN}"}


def test_get_nonexistent_log_returns_404():
    response = client.get("/logs/99999")
    assert response.status_code == 404


def test_create_and_get_log_entry():
    # Pass AUTH_HEADERS to successfully authenticate the creation path
    response = client.post("/logs/", json={"level": "info", "message": "Test log entry"}, headers=AUTH_HEADERS)
    assert response.status_code == 201
    log_entry = response.json()
    assert log_entry["level"] == "info"
    assert log_entry["message"] == "Test log entry"
    assert "id" in log_entry
    assert "timestamp" in log_entry

    log_id = log_entry["id"]
    response = client.get(f"/logs/{log_id}")
    assert response.status_code == 200
    fetched_entry = response.json()
    assert fetched_entry == log_entry
    assert response.headers.get("X-Cache") == "MISS"


def test_get_logs_with_level_filter(): 
    client.post("/logs/", json={"level": "info", "message": "Info log"}, headers=AUTH_HEADERS)
    client.post("/logs/", json={"level": "warn", "message": "Warn log"}, headers=AUTH_HEADERS)
    client.post("/logs/", json={"level": "error", "message": "Error log"}, headers=AUTH_HEADERS)

    response = client.get("/logs/?level=warn")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) >= 1                              
    assert all(log["level"] == "warn" for log in logs) 


def test_bulk_fetch_logs():
    response1 = client.post("/logs/", json={"level": "info", "message": "Bulk log 1"}, headers=AUTH_HEADERS)
    response2 = client.post("/logs/", json={"level": "warn", "message": "Bulk log 2"}, headers=AUTH_HEADERS)
    id1 = response1.json()["id"]
    id2 = response2.json()["id"]

    response = client.get(f"/logs/bulk-fetch?ids={id1},{id2}")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) == 2
    assert {log["id"] for log in logs} == {id1, id2}


def test_slow_fetch_logs():
    response1 = client.post("/logs/", json={"level": "info", "message": "Slow log 1"}, headers=AUTH_HEADERS)
    response2 = client.post("/logs/", json={"level": "warn", "message": "Slow log 2"}, headers=AUTH_HEADERS)
    id1 = response1.json()["id"]
    id2 = response2.json()["id"]

    response = client.get(f"/logs/slow-fetch?ids={id1},{id2}")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) == 2
    assert {log["id"] for log in logs} == {id1, id2}


def test_delete_log_rbac_protections():
    # Setup - authenticate the post payload using our user credentials
    create_resp = client.post("/logs/", json={"level": "error", "message": "Eviction target"}, headers=AUTH_HEADERS)
    log_id = create_resp.json()["id"]

    # 1. No Token Passed -> Expect 401 Unauthorized
    no_auth_resp = client.delete(f"/logs/{log_id}")
    assert no_auth_resp.status_code == 401

    # 2. Token Passed with standard user privileges -> Expect 403 Forbidden
    user_resp = client.delete(f"/logs/{log_id}", headers=AUTH_HEADERS)
    assert user_resp.status_code == 403

    # 3. Token Passed with admin privileges -> Expect 200 Success
    admin_token = create_access_token({"sub": "admin_user", "role": "admin"})
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    admin_resp = client.delete(f"/logs/{log_id}", headers=admin_headers)
    assert admin_resp.status_code == 200
    assert admin_resp.json() == {"message": "Log entry deleted"}

def test_create_log_with_invalid_level_type():
    """1. Expect 422 Unprocessable Entity when passing an invalid literal level type."""
    response = client.post(
        "/logs/", 
        json={"level": "critical_panic", "message": "Malformatted payload"}, 
        headers=AUTH_HEADERS
    )
    assert response.status_code == 422


def test_create_log_missing_required_fields():
    """2. Expect 422 error when the required body text message field is missing."""
    response = client.post(
        "/logs/", 
        json={"level": "info"}, 
        headers=AUTH_HEADERS
    )
    assert response.status_code == 422


def test_delete_nonexistent_log_as_admin():
    """3. Expect 404 Not Found when an authorized admin attempts to delete a ghost ID."""
    admin_token = create_access_token({"sub": "admin_user", "role": "admin"})
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    
    response = client.delete("/logs/999999", headers=admin_headers)
    assert response.status_code == 404


def test_get_logs_filter_returns_empty_array():
    """4. Expect a clean HTTP 200 with an empty list when looking for a level with zero records."""
    # First, fetch all current logs to see if any 'warn' logs leaked from other tests
    base_response = client.get("/logs/?level=warn")
    assert base_response.status_code == 200
    initial_warn_count = len(base_response.json())
    
    # Create an 'info' log (this shouldn't affect our 'warn' filter count)
    client.post("/logs/", json={"level": "info", "message": "Insulation log node"}, headers=AUTH_HEADERS)
    
    # Query for 'warn' logs again
    response = client.get("/logs/?level=warn")
    assert response.status_code == 200
    current_warn_logs = response.json()
    
    # Ensure our 'info' log creation didn't leak into our filtered 'warn' log count
    assert len(current_warn_logs) == initial_warn_count
    assert all(log["level"] == "warn" for log in current_warn_logs)



def test_bulk_fetch_handles_missing_ids_gracefully():
    """5. Expect the bulk engine to skip missing entries without crashing out."""
    # Create one good entry
    resp = client.post("/logs/", json={"level": "info", "message": "Valid target"}, headers=AUTH_HEADERS)
    valid_id = resp.json()["id"]
    
    # Query one valid ID mixed with non-existent database indexes
    response = client.get(f"/logs/bulk-fetch?ids={valid_id},88888,99999")
    assert response.status_code == 200
    logs = response.json()
    assert len(logs) == 1
    assert logs[0]["id"] == valid_id


def test_bulk_fetch_with_empty_string_ids():
    """6. Expect 422 or ValueError processing failure when malformatted comma string parameters are passed."""
    with pytest.raises((ValueError, KeyError)):
        # If your router logic doesn't explicitly throw HTTP errors for bad string splits,
        # parsing an empty item string to int inside a list comprehension triggers a native Python exception.
        client.get("/logs/bulk-fetch?ids=,")


def test_access_token_tampered_signature():
    """7. Expect 401 Unauthorized when a token's cryptographic signature has been modified."""
    tampered_token = TEST_USER_TOKEN + "corrupted_bits"
    bad_headers = {"Authorization": f"Bearer {tampered_token}"}
    
    response = client.post("/logs/", json={"level": "warn", "message": "Attack vector"}, headers=bad_headers)
    assert response.status_code == 401


def test_access_token_malformatted_bearer_prefix():
    """8. Expect 401 Unauthorized when the Authorization header drops the Bearer protocol prefix."""
    malformed_headers = {"Authorization": f"Tokens_R_Us {TEST_USER_TOKEN}"}
    response = client.post("/logs/", json={"level": "info", "message": "Bad protocol wrapper"}, headers=malformed_headers)
    assert response.status_code == 401


def test_get_log_by_id_type_safety_constraint():
    """9. Expect 422 Unprocessable Entity when passing a string instead of an integer path parameter ID."""
    response = client.get("/logs/abc_string_id")
    assert response.status_code == 422


def test_batch_ingest_endpoint_smoke_validation():
    """10. Expect HTTP 202 Accepted when posting arrays to your streaming high-volume Kafka producer path."""
    batch_payload = {
        "entries": [
            {"level": "info", "message": "Batch element alpha"},
            {"level": "warn", "message": "Batch element beta"}
        ]
    }
    response = client.post("/logs/batch", json=batch_payload, headers=AUTH_HEADERS)
    # Verifies the route successfully registers without dropping execution parameters
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
