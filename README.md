# Log Platform

A production-shaped distributed log processing platform built with FastAPI, PostgreSQL, Redis, Kafka, and a full Prometheus + Grafana observability stack. Designed to ingest, store, query, and stream log events at high volume with real-time delivery, live dashboards, and structured logging built in.

---

## Features

**Ingest**
- `POST /logs` — single log entry, writes directly to Postgres, broadcasts to all WebSocket clients
- `POST /logs/batch` — bulk ingest via Kafka, returns `202 Accepted` immediately, consumer writes to Postgres asynchronously in micro-batches of up to 100 messages per 1s window
- Rate limiting on ingest: sliding window via Redis, configurable limit and window per IP
- Raw batch payloads archived to S3 before Kafka enqueue

**Storage & Retrieval**
- PostgreSQL persistence via SQLAlchemy 2.0 async ORM
- Redis cache-aside on `GET /logs/{id}`: 300s TTL, cache invalidated on delete
- `X-Cache: HIT/MISS` response header on every single-log fetch
- Level filtering: `GET /logs?level=error`
- Concurrent bulk fetch via `asyncio.gather`: `GET /logs/bulk-fetch?ids=1,2,3`
- Aggregate stats by level: `GET /logs/stats`

**Real-time**
- WebSocket endpoint `ws://host/logs/ws/tail` — every `POST /logs` broadcasts the new entry to all connected clients instantly
- `ConnectionManager` handles arbitrary numbers of concurrent WebSocket clients

**Auth**
- JWT access tokens (15 min) + refresh tokens (7 days)
- `POST /auth/register`, `POST /auth/login`, `POST /auth/refresh`
- RBAC via token payload — `user` role can read and create, `admin` role can delete
- `require_admin` FastAPI dependency wires onto any route with one line

**Kafka Pipeline**
- `AIOKafkaProducer` singleton warmed at startup, reused across all requests
- Consumer worker (`group_id: log-writer`) — bulk INSERTs to Postgres, up to 100 messages per 1s window via `getmany()`
- Alerter worker (`group_id: alert-system`) — independent consumer on the same topic, fires on error-level logs
- Consumer retry logic — 3 attempts per message before routing to S3 dead letter queue
- Malformed JSON isolated immediately before batch insert to avoid blocking a whole batch
- Two consumer groups with independent offset tracking — alerter lag never delays writer

**Observability**
- Prometheus scrapes `/metrics` every 5 seconds
- Grafana dashboards at `:3000`:
  - Ingest rate by level (`rate(log_ingest_total[1m])`)
  - Cache hit ratio
  - HTTP request rate by endpoint and status
  - p95 latency histogram
- Custom Prometheus counters: `log_ingest_total` (labeled by level), `cache_hits_total`, `cache_misses_total`
- Structured JSON logging via `python-json-logger` — every service emits machine-readable JSON
- HTTP request middleware — logs method, path, status, duration_ms on every request

**CI/CD**
- GitHub Actions pipeline on every push to `main`
- Spins up Postgres + Redis service containers
- Runs Alembic migrations against test DB
- Runs full pytest suite (mock Redis, SQLite in-memory, RBAC coverage)
- Builds Docker image to confirm Dockerfile integrity

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| API framework | FastAPI + Uvicorn (ASGI) |
| Data validation | Pydantic v2 |
| ORM | SQLAlchemy 2.0 async |
| Database | PostgreSQL 16 |
| Migrations | Alembic |
| Cache + rate limiting | Redis 7 |
| Message streaming | Apache Kafka + Zookeeper |
| Kafka Python client | aiokafka |
| Auth | python-jose (JWT) + passlib (bcrypt) |
| Metrics | Prometheus + prometheus-fastapi-instrumentator |
| Dashboards | Grafana |
| Structured logging | python-json-logger |
| Object storage | AWS S3 + boto3 |
| Containerisation | Docker + Docker Compose |
| CI/CD | GitHub Actions |
| Load testing | Locust |
| Testing | pytest + httpx TestClient |

---

## Project Structure

```
log-platform/
├── app/
│   ├── main.py                 # App entry point, lifespan, middleware, router mounting
│   ├── database.py             # Async engine, session factory, Base
│   ├── models.py               # Pydantic models (request/response contracts)
│   ├── orm_models.py           # SQLAlchemy ORM models (LogEntryORM, UserORM)
│   ├── auth.py                 # JWT encode/decode, bcrypt hashing
│   ├── dependencies.py         # get_db, rate_limit, get_current_user, require_admin
│   ├── metrics.py              # Prometheus counters
│   ├── redis_client.py         # Async Redis connection
│   ├── kafka_producer.py       # AIOKafkaProducer singleton
│   ├── kafka_consumer.py       # Consumer worker — Kafka → Postgres (micro-batch writes + DLQ)
│   ├── kafka_alerter.py        # Alerter worker — structured JSON alerts on error logs
│   ├── websocket_manager.py    # ConnectionManager for WebSocket broadcast
│   └── logging_config.py      # Structured JSON logging setup
│   └── routers/
│       ├── logs.py             # All /logs routes + WebSocket
│       └── auth.py             # /auth/register, /login, /refresh
├── alembic/                    # Database migrations
│   └── versions/               # logs table, users table
├── tests/
│   └── test_logs.py            # Full test suite — mock Redis, SQLite, RBAC
├── .github/
│   └── workflows/
│       └── ci.yml              # Test + build pipeline
├── prometheus.yml              # Prometheus scrape config
├── locustfile.py               # Load test scenarios (50 concurrent users)
├── Dockerfile
├── docker-compose.yml          # API, Postgres, Redis, Kafka, Zookeeper, Consumer, Alerter, Prometheus, Grafana
├── alembic.ini
├── requirements.txt
└── .env                        # Local secrets (gitignored)
```

---

## Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.11+

### Run locally

```bash
git clone https://github.com/YOUR_USERNAME/log-platform.git
cd log-platform

cp .env.example .env
# fill in SECRET_KEY, S3_BUCKET, AWS credentials

docker compose up --build
```

| Service | URL |
|---------|-----|
| API docs | http://localhost:8000/docs |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 (admin / admin) |
| Raw metrics | http://localhost:8000/metrics |

### Run tests

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

### Load test

```bash
locust --host=http://localhost:8000 --users=50 --spawn-rate=5 --run-time=60s --headless
```

Observed results on MacBook Pro (Docker Desktop): batch ingest p50 ~410ms, p95 ~2400ms under 50 concurrent users. Single ingest p50 ~500ms. `GET /logs/` slows under large datasets, pagination is the next improvement.

### WebSocket tail

```bash
brew install websocat
websocat ws://localhost:8000/logs/ws/tail
# open a second terminal and POST a log — it appears here instantly
```

---

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgresql+asyncpg://user:pass@db:5432/logs` |
| `REDIS_URL` | Redis connection string | `redis://redis:6379` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `kafka:29092` |
| `SECRET_KEY` | JWT signing key, generate with `secrets.token_hex(32)` | — |
| `ALGORITHM` | JWT algorithm | `HS256` |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | Access token lifetime | `15` |
| `REFRESH_TOKEN_EXPIRE_DAYS` | Refresh token lifetime | `7` |
| `S3_BUCKET` | S3 bucket for log archives + dead letters | `my-log-platform-bucket` |
| `AWS_DEFAULT_REGION` | AWS region | `us-east-1` |
| `ENV` | Set to `test` to skip DB init at startup | `test` |

---

## API Reference

### Auth

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/auth/register` | None | Create a new user account |
| `POST` | `/auth/login` | None | Login, receive access + refresh tokens |
| `POST` | `/auth/refresh` | None | Exchange refresh token for new access token |

### Logs

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/logs/` | User | Single log entry — direct Postgres write, WebSocket broadcast |
| `POST` | `/logs/batch` | User | Bulk ingest via Kafka — 202 Accepted, async Postgres write |
| `GET` | `/logs/` | User | List all logs, optional `?level=info\|warn\|error` filter |
| `GET` | `/logs/stats` | User | Count of logs grouped by level |
| `GET` | `/logs/{id}` | User | Single log fetch — Redis cache-aside, X-Cache header |
| `GET` | `/logs/bulk-fetch?ids=1,2,3` | User | Concurrent fetch via asyncio.gather |
| `GET` | `/logs/slow-fetch?ids=1,2,3` | User | Sequential fetch — demonstrates gather vs sequential |
| `DELETE` | `/logs/{id}` | Admin | Delete log + invalidate Redis cache entry |
| `WS` | `/logs/ws/tail` | None | Real-time log stream |

---

## Grafana Dashboards

Add Prometheus as a data source (`http://prometheus:9090`) then create panels with these queries:

| Panel | Query |
|-------|-------|
| Ingest rate by level | `rate(log_ingest_total[1m])` |
| Cache hit ratio | `rate(cache_hits_total[1m]) / (rate(cache_hits_total[1m]) + rate(cache_misses_total[1m]))` | this was not working at time of publication
| HTTP request rate | `rate(http_requests_total[1m])` |
| p95 latency | `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[1m]))` |

---

## Deployment (AWS)

Target architecture: EC2 (t3.micro) running Docker Compose, RDS Postgres, ElastiCache Redis, S3 for storage.
not implemented ($) but planned for as a means of learning a little bit.
```bash
# Build and push to ECR
aws ecr create-repository --repository-name log-platform
docker build -t log-platform .
docker tag log-platform:latest <account>.dkr.ecr.<region>.amazonaws.com/log-platform:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/log-platform:latest

# On EC2
ssh ec2-user@<your-ec2-ip>
docker pull <account>.dkr.ecr.<region>.amazonaws.com/log-platform:latest
docker compose up -d
alembic upgrade head
```

Point `DATABASE_URL` at your RDS endpoint — no other code changes needed.

---

## Design Decisions

**Why Kafka for batch ingest and not just Postgres directly?**
Postgres can absorb moderate write rates but becomes a bottleneck under sustained high-volume ingest. Kafka acts as a buffer the API responds in microseconds (Kafka write) and the consumer writes to Postgres at a sustainable rate. Messages survive consumer downtime. a crashed consumer picks up from its last committed offset.

**Why micro-batch writes in the consumer?**
One INSERT per message means one round-trip to Postgres per message. Batching up to 100 messages into a single bulk INSERT reduces that to one round-trip per batch regardless of batch size. Under load the consumer was sustaining multiple 100-row batches per second with no Postgres connection pressure.

**Why two consumer groups?**
The log-writer and alert-system consumer groups both read `log-events` independently. Kafka gives each group its own offset — the alerter being slow or restarting never delays the writer. This is the advantage of a log-based broker over a traditional queue where each message is consumed once.

**Why Redis cache-aside and not write-through?**
Write-through caches every write: the cache always has the latest value but you pay the write cost on every ingest. Cache-aside only populates on reads, so hot entries are cached and cold entries never consume cache memory. 

**Why short-lived access tokens + refresh tokens?**
A stolen access token is usable until expiry. 15 minutes reduces harm compared to a longer token. The refresh token is only sent to one endpoint which is easier to monitor and rate-limit. Proper revocation would require a Redis token blacklist keyed by JTI.

**Why structured JSON logging?**
Plaintext logs are unsearchable at scale. JSON logs are indexable by any log aggregator. Every field in the `extra` dict becomes a top-level key in the JSON output.