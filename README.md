# Reliable distributed job queue

A compact production-style background job system built with FastAPI, PostgreSQL, Redis, and asyncio workers. PostgreSQL is the source of truth; Redis supplies fast, atomic scheduling and claiming.

## What is included

- Durable job metadata and worker registry in PostgreSQL
- Atomic priority claiming across ten priority levels in Redis Lua
- Multiple worker processes, with configurable in-process concurrency
- Delayed jobs, retries with `5s → 30s → 5m` backoff, timeouts, cancellation, and a dead-letter queue
- Visibility deadlines, worker heartbeats, stale-worker detection, and abandoned-job recovery
- Idempotency keys and a queue depth limit
- Prometheus metrics, a built-in dashboard, and an optional provisioned Grafana dashboard
- Unit, API/integration, concurrency, retry, and timeout coverage
- Docker Compose, a load generator, seed data, and GitHub Actions CI

## Architecture

```text
Client ──HTTP──▶ FastAPI ───────▶ PostgreSQL (authoritative job/worker state)
                    │
                    └───────────▶ Redis
                                  ├─ ready lists, priority 0..9
                                  ├─ scheduled sorted set
                                  ├─ processing/visibility sorted set
                                  ├─ active-job deduplication set
                                  └─ dead-letter list
                                           │
                                    atomic Lua claim
                                           ▼
                                worker-1 / worker-2 / worker-3
                                           │
                                  validated job handlers
```

## Run locally

Requirements: Docker with Compose v2.

```bash
docker compose up --build --wait
```

Compose has safe local defaults. Copy `.env.example` to `.env` only when you want to override them.

Open:

- API documentation: <http://localhost:8000/docs>
- Live dashboard: <http://localhost:8000/dashboard>
- Prometheus metrics: <http://localhost:8000/metrics>

Start the optional monitoring stack with:

```bash
docker compose --profile monitoring up -d
```

Prometheus is on <http://localhost:9090>; Grafana is on <http://localhost:3000> (`admin` / `admin` for local use).

### Submit and inspect a job

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: monthly-sales-2026-09" \
  -d '{
    "type": "generate_report",
    "payload": {"report_name": "monthly-sales", "rows": 5000},
    "priority": 8,
    "max_attempts": 3,
    "timeout_seconds": 30
  }'

curl http://localhost:8000/jobs/JOB_ID
curl -X DELETE http://localhost:8000/jobs/JOB_ID
curl -X POST http://localhost:8000/jobs/JOB_ID/retry
curl http://localhost:8000/queues
curl http://localhost:8000/workers
```

For a delayed job, supply `"delay_seconds": 60` or an RFC 3339 `scheduled_at`. Larger priority numbers run first. FIFO order is preserved within a priority.

Generate demo traffic:

```bash
python scripts/seed.py
python scripts/load_test.py --jobs 1000 --concurrency 50
```

## Job lifecycle

```text
                         cancellation
queued ──claim──▶ running ─────────────▶ cancelled
  ▲                 │
  │                 ├──success────────▶ completed
  │                 │
  └──backoff/retry──┤
                    └──attempt limit──▶ failed ──▶ dead-letter queue
```

1. The API validates the type-specific payload and commits a `queued` job to PostgreSQL.
2. It enqueues the job in a Redis ready list or scheduled sorted set. An active-ID set makes this operation idempotent.
3. One Lua script promotes due jobs, checks priorities from 9 down to 0, removes one ID, and records its visibility deadline. This is atomic, so normal operation cannot hand the same queue entry to two workers.
4. The winner changes the PostgreSQL row from `queued` to `running`, increments `attempts`, and records both a worker ID and a unique claim token.
5. Completion/failure updates are accepted only when that claim token still owns the running row, preventing a stale worker from overwriting a recovered job.

## Failure handling

- **Handler error:** the job returns to the scheduled set using the configured backoff. After `max_attempts`, it becomes `failed` and enters the dead-letter list.
- **Handler timeout:** `asyncio.wait_for` cancels the handler and follows the same retry path.
- **Worker crash:** the database visibility deadline expires. Any surviving worker locks the abandoned rows with `FOR UPDATE SKIP LOCKED`, resets them, and atomically releases their Redis entries.
- **Database/Redis handoff crash:** the recovery loop finds `queued` database rows absent from Redis's active set and safely re-enqueues them.
- **Duplicate submission:** `Idempotency-Key` has a unique database constraint; concurrent repeats return the original job.
- **Stale completion:** a per-attempt claim token makes a late result a no-op.

Delivery is **at least once after crashes**. Handlers should still make their external side effects idempotent: for example, use the job ID as an email-provider or object-storage idempotency key. Exactly-once side effects cannot be guaranteed across arbitrary external systems by a queue alone.

## Data model

`jobs` stores payload, lifecycle state, priority, attempts, schedule/retry times, timeout, result/error, idempotency key, owner, claim token, and visibility deadline. `workers` stores identity, host, lifecycle state, heartbeat, and completed count. Indexes support lifecycle listings and visibility-timeout scans.

Tables are created idempotently during startup to keep this example one-command. For a long-lived production installation, replace startup `create_all` with versioned Alembic migrations before making schema changes.

## Tests

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest -m "not integration"

# With the Compose stack running:
$env:QUEUE_INTEGRATION_URL="http://localhost:8000"  # PowerShell
pytest -m integration
```

The live integration suite sends 100 concurrent jobs, verifies single-attempt processing across at least two workers, checks idempotent submission and retry, and forces a timeout into the dead-letter queue.

### Manual crash-recovery drill

Submit a job whose `processing_seconds` exceeds the visibility setting, stop the worker container that owns it, and observe another worker recover it after the deadline:

```bash
docker compose stop worker-1
curl http://localhost:8000/jobs/JOB_ID
docker compose logs -f worker-2
```

The exact owning container is visible in `GET /workers` and the job's `worker_id`. The original crash consumes an attempt, as expected for at-least-once delivery.

## Metrics

`/metrics` exposes:

- persistent jobs by status
- ready, scheduled, running, and dead-letter depth
- completed jobs in the last minute
- average successful processing time
- terminal failure ratio
- cumulative retry attempts represented in stored jobs

The built-in dashboard polls `/queues` and `/workers`. Prometheus/Grafana provide history and alerting when the monitoring profile is enabled.

## Deployment

The image runs either process without modification:

```bash
# API
uvicorn app.main:app --host 0.0.0.0 --port 8000

# worker
python -m app.worker
```

On Render, Railway, Fly.io, AWS, or Azure, deploy the same image as one web service and one or more worker services. Supply managed PostgreSQL and Redis URLs through `DATABASE_URL` and `REDIS_URL`, keep only the web service publicly reachable, and scale workers independently. Run schema migration/creation once before rolling out the processes. Do not use the local Compose passwords in a cloud environment.

## Design tradeoffs

- PostgreSQL remains authoritative; Redis can be rebuilt from queued/running rows. This costs a database transition per attempt but gives strong auditability.
- Ten lists make priority claiming cheap and deterministic. This is simpler than embedding priority and FIFO values in one floating-point sorted-set score.
- Recovery scans are deliberately bounded. At very high scale, shard queues and use an outbox/change-data-capture dispatcher rather than increasing scan size indefinitely.
- PostgreSQL records terminal jobs indefinitely. A production retention task should archive or delete them according to policy.
- The example handler simulates report work; real handlers belong in `app/handlers.py` and must validate payloads and make external effects idempotent.
