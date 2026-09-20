import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest
from redis.asyncio import Redis
from sqlalchemy import case, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import close_database, create_schema, get_session
from app.models import Job, JobStatus, Worker, WorkerStatus
from app.redis_queue import RedisJobQueue
from app.schemas import HealthRead, JobCreate, JobRead, QueueRead, WorkerRead

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await create_schema()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    await redis.ping()
    app.state.redis = redis
    app.state.queue = RedisJobQueue(redis, settings.queue_name)
    yield
    await redis.aclose()
    await close_database()


app = FastAPI(
    title="Reliable Job Queue",
    version="0.1.0",
    description="Redis-coordinated jobs with PostgreSQL persistence and crash recovery.",
    lifespan=lifespan,
)


def get_queue(request: Request) -> RedisJobQueue:
    return request.app.state.queue


SessionDep = Annotated[AsyncSession, Depends(get_session)]
QueueDep = Annotated[RedisJobQueue, Depends(get_queue)]
IdempotencyKeyHeader = Annotated[str | None, Header(alias="Idempotency-Key")]


@app.post("/jobs", response_model=JobRead, status_code=status.HTTP_201_CREATED)
async def create_job(
    request: JobCreate,
    response: Response,
    session: SessionDep,
    queue: QueueDep,
    idempotency_key_header: IdempotencyKeyHeader = None,
) -> Job:
    if await queue.depth() >= settings.queue_depth_limit:
        raise HTTPException(status_code=429, detail="queue depth limit reached")

    idempotency_key = idempotency_key_header or request.idempotency_key
    if idempotency_key:
        existing = await session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        if existing:
            response.status_code = status.HTTP_200_OK
            return existing

    scheduled_at = request.effective_schedule()
    job = Job(
        type=request.type,
        payload=request.payload.model_dump(),
        priority=request.priority,
        max_attempts=request.max_attempts,
        timeout_seconds=request.timeout_seconds or settings.default_job_timeout_seconds,
        scheduled_at=scheduled_at,
        idempotency_key=idempotency_key,
    )
    session.add(job)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        if not idempotency_key:
            raise
        existing = await session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        if existing is None:
            raise
        response.status_code = status.HTTP_200_OK
        return existing
    await session.refresh(job)
    await queue.enqueue(job.id, job.priority, scheduled_at)
    return job


@app.get("/jobs/{job_id}", response_model=JobRead)
async def get_job(job_id: uuid.UUID, session: SessionDep) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.delete("/jobs/{job_id}", response_model=JobRead)
async def cancel_job(
    job_id: uuid.UUID,
    session: SessionDep,
    queue: QueueDep,
) -> Job:
    job = await session.get(Job, job_id, with_for_update=True)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status in (JobStatus.completed, JobStatus.failed, JobStatus.cancelled):
        raise HTTPException(status_code=409, detail=f"cannot cancel {job.status.value} job")
    job.status = JobStatus.cancelled
    job.completed_at = datetime.now(UTC)
    job.visibility_deadline = None
    job.claim_token = None
    await queue.remove(job.id, job.priority)
    await session.commit()
    await session.refresh(job)
    return job


@app.post("/jobs/{job_id}/retry", response_model=JobRead)
async def retry_job(
    job_id: uuid.UUID,
    session: SessionDep,
    queue: QueueDep,
) -> Job:
    job = await session.get(Job, job_id, with_for_update=True)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status not in (JobStatus.failed, JobStatus.cancelled):
        raise HTTPException(status_code=409, detail="only failed or cancelled jobs can be retried")
    job.status = JobStatus.queued
    job.attempts = 0
    job.started_at = None
    job.completed_at = None
    job.next_retry_at = None
    job.error_message = None
    job.result = None
    job.worker_id = None
    await session.commit()
    await session.refresh(job)
    await queue.remove_from_dead_letter(job.id)
    await queue.enqueue(job.id, job.priority)
    return job


@app.get("/queues", response_model=list[QueueRead])
async def get_queues(
    session: SessionDep,
    queue: QueueDep,
) -> list[QueueRead]:
    queue_stats = await queue.stats()
    oldest = await session.scalar(
        select(func.min(Job.created_at)).where(Job.status == JobStatus.queued)
    )
    return [
        QueueRead(
            name=settings.queue_name,
            ready=queue_stats.ready,
            scheduled=queue_stats.scheduled,
            running=queue_stats.running,
            dead_letter=queue_stats.dead_letter,
            oldest_queued_at=oldest,
            depth_limit=settings.queue_depth_limit,
        )
    ]


@app.get("/workers", response_model=list[WorkerRead])
async def get_workers(session: SessionDep) -> list[Worker]:
    stale_before = datetime.now(UTC) - timedelta(seconds=settings.worker_heartbeat_seconds * 3)
    await session.execute(
        update(Worker)
        .where(
            Worker.status == WorkerStatus.online,
            Worker.last_heartbeat < stale_before,
        )
        .values(status=WorkerStatus.offline)
    )
    await session.commit()
    return list(
        (await session.execute(select(Worker).order_by(Worker.started_at.desc()))).scalars()
    )


@app.get("/health", response_model=HealthRead)
async def health(
    session: SessionDep,
    queue: QueueDep,
) -> HealthRead:
    await session.execute(select(1))
    await queue.ping()
    return HealthRead(status="ok")


@app.get("/metrics", include_in_schema=False)
async def metrics(
    session: SessionDep,
    queue: QueueDep,
) -> Response:
    registry = CollectorRegistry()
    jobs = Gauge(
        "job_queue_jobs",
        "Jobs by persistent state",
        ["status"],
        registry=registry,
    )
    counts = dict(
        (await session.execute(select(Job.status, func.count()).group_by(Job.status))).all()
    )
    for job_status in JobStatus:
        jobs.labels(status=job_status.value).set(counts.get(job_status, 0))

    queue_stats = await queue.stats()
    for metric_name, help_text, value in (
        ("job_queue_ready", "Jobs ready to be claimed", queue_stats.ready),
        ("job_queue_scheduled", "Jobs waiting for their scheduled time", queue_stats.scheduled),
        ("job_queue_running", "Jobs in Redis visibility tracking", queue_stats.running),
        ("job_queue_dead_letter", "Jobs in the dead-letter queue", queue_stats.dead_letter),
    ):
        Gauge(metric_name, help_text, registry=registry).set(value)

    cutoff = datetime.now(UTC) - timedelta(minutes=1)
    completed_last_minute = await session.scalar(
        select(func.count()).where(Job.status == JobStatus.completed, Job.completed_at >= cutoff)
    )
    failed = counts.get(JobStatus.failed, 0)
    completed = counts.get(JobStatus.completed, 0)
    retry_count = await session.scalar(
        select(func.coalesce(func.sum(case((Job.attempts > 1, Job.attempts - 1), else_=0)), 0))
    )
    average_duration = await session.scalar(
        select(func.avg(func.extract("epoch", Job.completed_at - Job.started_at))).where(
            Job.status == JobStatus.completed
        )
    )
    Gauge(
        "job_queue_completed_last_minute",
        "Jobs completed in the last minute",
        registry=registry,
    ).set(completed_last_minute or 0)
    Gauge("job_queue_retry_count", "Total retry attempts", registry=registry).set(retry_count or 0)
    Gauge(
        "job_queue_average_processing_seconds",
        "Average completed job duration",
        registry=registry,
    ).set(float(average_duration or 0))
    Gauge(
        "job_queue_failure_ratio",
        "Terminal failures divided by terminal jobs",
        registry=registry,
    ).set(failed / max(failed + completed, 1))
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard() -> HTMLResponse:
    dashboard_path = Path(__file__).with_name("static") / "dashboard.html"
    return HTMLResponse(dashboard_path.read_text(encoding="utf-8"))
