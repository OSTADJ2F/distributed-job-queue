# Reliable Distributed Job Queue

[![CI/CD](https://github.com/OSTADJ2F/distributed-job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/OSTADJ2F/distributed-job-queue/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Redis](https://img.shields.io/badge/Redis-7.4-DC382D?logo=redis&logoColor=white)](https://redis.io/)

A production-style background job system that demonstrates durable state, atomic queue operations, horizontal workers, failure recovery, and observable operations. FastAPI provides the control plane, PostgreSQL is the source of truth, and Redis handles low-latency scheduling and claims.

The project is intentionally small enough to understand end to end while still addressing the failure modes that distinguish a toy queue from a reliable distributed system.

## Why this project is interesting

- **Atomic priority scheduling:** a Redis Lua script promotes due jobs and claims the highest-priority item as one indivisible operation.
- **Crash recovery:** visibility deadlines, heartbeats, and reconciliation allow another worker to recover abandoned work.
- **Safe retries:** configurable backoff, per-job timeouts, attempt limits, and a dead-letter queue prevent silent job loss.
- **Concurrency correctness:** database row locks and per-attempt claim tokens stop stale workers from overwriting newer results.
- **Idempotent submission:** callers can safely retry requests using an `Idempotency-Key`, including concurrent submissions.
- **Operational visibility:** health checks, Prometheus metrics, a live dashboard, and a provisioned Grafana dashboard are included.
- **Production delivery:** one Docker image runs either the API or worker, Compose provides a complete local stack, and CI/CD validates and deploys it.

## Architecture

```text
                                      PostgreSQL
                                  authoritative state
                                         ▲  │
                                         │  │
Client ── HTTP ──▶ FastAPI API ──────────┘  │
                       │                     │
                       ▼                     ▼
                     Redis ◀──── worker pool (N processes × M slots)
              ┌────────┼────────┐
          ready lists  scheduled ZSET  processing ZSET
          priority 0–9  delayed jobs    visibility deadlines
              └────────┼────────┘
                    dead-letter list
```

PostgreSQL owns durable job and worker state. Redis can be reconstructed from queued rows, so a Redis interruption does not erase the system's record of work. Workers claim jobs atomically, transition their rows under a lock, and only finalize results when their unique claim token still matches.

## Job lifecycle

```text
                    ┌──── retry with backoff ────┐
                    │                             │
queued ── claim ──▶ running ── success ──▶ completed
   │                │
   │                ├── attempt limit ──▶ failed ──▶ dead-letter queue
   │                │
   └──────────── cancellation ──────────▶ cancelled
```

Delivery is **at least once after a crash**. The queue protects its own state from duplicates and stale completions, but handlers that call external services should use the job ID as an idempotency key. A queue alone cannot guarantee exactly-once side effects across arbitrary external systems.

## Technology

| Area | Choice | Purpose |
| --- | --- | --- |
| API | FastAPI, Pydantic | Async HTTP API and boundary validation |
| Persistence | PostgreSQL, SQLAlchemy async, asyncpg | Durable lifecycle state and row-level locking |
| Queue | Redis, Lua | Atomic priority claims, delayed work, visibility tracking |
| Workers | asyncio | Configurable concurrency per process |
| Observability | Prometheus, Grafana | Queue, throughput, latency, failure, and retry metrics |
| Delivery | Docker, Compose, Render, GitHub Actions | Reproducible runtime and gated deployments |
| Quality | pytest, pytest-asyncio, pytest-cov, Ruff | Unit, API, contract, and live integration checks |

## Quick start

You need Docker with Compose v2. The repository includes safe local defaults, so no secrets are needed for local development.
Published development ports bind to `127.0.0.1` and are not exposed to the local network.

```bash
git clone https://github.com/OSTADJ2F/distributed-job-queue.git
cd distributed-job-queue
docker compose up --build --wait
```

Once the health checks pass:

- OpenAPI documentation: <http://localhost:8000/docs>
- Operations dashboard: <http://localhost:8000/dashboard>
- Prometheus metrics: <http://localhost:8000/metrics>

Start the optional monitoring stack:

```bash
docker compose --profile monitoring up -d
```

Prometheus is available at <http://localhost:9090> and Grafana at <http://localhost:3000>. The local Grafana login is `admin` / `admin`; do not reuse those credentials in a deployed environment.

## API example

Submit a job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: monthly-sales-2026-09" \
  -d '{
    "type": "generate_report",
    "payload": {
      "report_name": "monthly-sales",
      "rows": 5000
    },
    "priority": 8,
    "max_attempts": 3,
    "timeout_seconds": 30
  }'
```

Use the returned job ID to inspect, cancel, or retry work:

```bash
curl http://localhost:8000/jobs/JOB_ID
curl -X DELETE http://localhost:8000/jobs/JOB_ID
curl -X POST http://localhost:8000/jobs/JOB_ID/retry
curl http://localhost:8000/queues
curl http://localhost:8000/workers
```

Larger priority values run first; FIFO ordering is preserved within a priority. Set `delay_seconds` or a timezone-aware RFC 3339 `scheduled_at` value to schedule future work.

## Reliability model

| Failure | Response |
| --- | --- |
| Handler raises or times out | Reschedule with configured backoff; dead-letter after the attempt limit |
| Worker exits mid-job | Recover the row after its visibility deadline and return it to Redis |
| API commits but enqueue fails | Reconciliation finds queued rows missing from Redis and re-enqueues them |
| Client repeats a request | Return the original job through the unique idempotency key |
| Old worker finishes after recovery | Reject its update because the claim token no longer matches |
| Worker stops heartbeating | Mark it offline when its heartbeat becomes stale |

The recovery scans are intentionally bounded. At much larger scale, the next step would be queue sharding plus a transactional outbox or change-data-capture dispatcher.

## Configuration

Copy `.env.example` to `.env` to override the Compose defaults.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | local PostgreSQL | SQLAlchemy async database URL |
| `REDIS_URL` | local Redis | Redis connection URL |
| `QUEUE_NAME` | `default` | Redis namespace for this queue |
| `QUEUE_DEPTH_LIMIT` | `10000` | Backpressure limit across ready, scheduled, and running jobs |
| `VISIBILITY_TIMEOUT_SECONDS` | `60` | Base deadline before an abandoned claim is recoverable |
| `WORKER_HEARTBEAT_SECONDS` | `5` | Worker heartbeat interval |
| `RECOVERY_INTERVAL_SECONDS` | `10` | Recovery and reconciliation interval |
| `DEFAULT_JOB_TIMEOUT_SECONDS` | `30` | Default handler timeout |
| `BACKOFF_SCHEDULE_SECONDS` | `5,30,300` | Retry delays; the final value is reused for later attempts |
| `WORKER_CONCURRENCY` | `4` | Async consumer slots per worker process |

## Testing

Install the development dependencies and run the fast suite:

```bash
python -m pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest -m "not integration" --cov=app --cov-report=term-missing
```

The fast suite covers validation, idempotency races, API state transitions, Redis command contracts, priority key construction, retry and dead-letter behavior, and stale-claim protection. Coverage is branch-aware and enforced at **75% minimum**.

Run the live suite against PostgreSQL, Redis, the API, and multiple workers:

```bash
docker compose up --detach --build --wait

# Bash
QUEUE_INTEGRATION_URL=http://localhost:8000 pytest -m integration

# PowerShell
$env:QUEUE_INTEGRATION_URL="http://localhost:8000"
pytest -m integration
```

The live tests submit 100 concurrent jobs, confirm single-attempt processing across multiple workers, verify idempotent submission and retry, and drive a timed-out job into the dead-letter queue.

## CI/CD

Every push and pull request runs independent GitHub Actions jobs for:

1. Ruff lint and formatting checks.
2. Unit/API tests on Python 3.12 and 3.13 with a coverage gate and downloadable XML report.
3. Docker Compose validation and a clean production-image build.
4. Full multi-service integration tests after all fast checks pass.

Pushes to `main` reach the production deployment job only after the complete quality gate succeeds. Render auto-deploy is disabled in `render.yaml`; configure the following GitHub production-environment values to enable gated deployment:

- Secret `RENDER_API_DEPLOY_HOOK_URL`
- Secret `RENDER_WORKER_DEPLOY_HOOK_URL`
- Optional variable `RENDER_SERVICE_URL` for the GitHub deployment link

If the deploy-hook secrets are absent, CI remains usable for forks and reports that deployment was skipped. The Render Blueprint provisions one API service, two worker instances, PostgreSQL, and persistent Redis-compatible Key Value storage.

## Project structure

```text
app/
├── main.py           # FastAPI endpoints, health, metrics, dashboard
├── worker.py         # Consumers, heartbeats, retries, recovery
├── redis_queue.py    # Lua-backed scheduling and queue primitives
├── models.py         # Persistent jobs and worker registry
├── schemas.py        # Request and response contracts
└── handlers.py       # Validated job implementations
tests/
├── test_api.py               # HTTP behavior and state transitions
├── test_redis_queue.py       # Redis command contracts
├── test_worker.py            # Claim, retry, DLQ, stale-result behavior
└── test_integration_live.py  # Real multi-service concurrency tests
monitoring/                    # Prometheus and Grafana provisioning
.github/workflows/ci.yml       # CI quality gate and production CD
docker-compose.yml             # Local multi-service environment
render.yaml                    # Production infrastructure blueprint
```

## Extending the queue

Add a validated payload model and async handler in `app/handlers.py`, register it in `HANDLERS`, and extend the `JobCreate.type` contract. Handlers receive their validated payload and current attempt number. Keep external effects idempotent and return JSON-serializable results.

The current example creates tables idempotently at startup for a one-command demo. A long-lived production installation should use versioned Alembic migrations and a retention policy for terminal jobs before evolving the schema.
