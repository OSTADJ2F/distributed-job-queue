import asyncio
import contextlib
import logging
import signal
import socket
import uuid
from datetime import UTC, datetime, timedelta

from redis.asyncio import Redis
from sqlalchemy import select, update

from app.config import Settings, get_settings
from app.database import SessionLocal, close_database, create_schema
from app.handlers import get_handler
from app.models import Job, JobStatus, Worker, WorkerStatus
from app.redis_queue import RedisJobQueue

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("queue.worker")


class JobWorker:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.worker_id = uuid.uuid4()
        self.hostname = socket.gethostname()
        self.redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        self.queue = RedisJobQueue(self.redis, self.settings.queue_name)
        self.stop_event = asyncio.Event()

    async def register(self) -> None:
        async with SessionLocal() as session:
            session.add(
                Worker(
                    id=self.worker_id,
                    hostname=self.hostname,
                    status=WorkerStatus.online,
                    last_heartbeat=datetime.now(UTC),
                )
            )
            await session.commit()
        logger.info("registered worker=%s hostname=%s", self.worker_id, self.hostname)

    async def heartbeat_loop(self) -> None:
        while not self.stop_event.is_set():
            async with SessionLocal() as session:
                await session.execute(
                    update(Worker)
                    .where(Worker.id == self.worker_id)
                    .values(status=WorkerStatus.online, last_heartbeat=datetime.now(UTC))
                )
                await session.commit()
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), self.settings.worker_heartbeat_seconds
                )
            except TimeoutError:
                pass

    async def recovery_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await self.recover_abandoned_jobs()
                await self.reconcile_queued_jobs()
            except Exception:
                logger.exception("recovery pass failed")
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), self.settings.recovery_interval_seconds
                )
            except TimeoutError:
                pass

    async def recover_abandoned_jobs(self) -> None:
        now = datetime.now(UTC)
        recovered: list[uuid.UUID] = []
        async with SessionLocal() as session:
            result = await session.execute(
                select(Job)
                .where(
                    Job.status == JobStatus.running,
                    Job.visibility_deadline < now,
                )
                .with_for_update(skip_locked=True)
                .limit(100)
            )
            for job in result.scalars():
                await self.queue.release(job.id, job.priority)
                job.status = JobStatus.queued
                job.worker_id = None
                job.claim_token = None
                job.visibility_deadline = None
                job.error_message = "worker disappeared before visibility timeout"
                recovered.append(job.id)
            await session.commit()
        for job_id in recovered:
            logger.warning("recovered abandoned job=%s", job_id)

    async def reconcile_queued_jobs(self) -> None:
        async with SessionLocal() as session:
            jobs = (
                await session.execute(
                    select(Job)
                    .where(Job.status == JobStatus.queued)
                    .order_by(Job.created_at)
                    .limit(200)
                )
            ).scalars()
            pending = [
                (job.id, job.priority, job.next_retry_at or job.scheduled_at) for job in jobs
            ]
        for job_id, priority, run_at in pending:
            if not await self.queue.is_active(job_id):
                await self.queue.enqueue(job_id, priority, run_at)

    async def process_one(self) -> bool:
        job_id_string = await self.queue.claim(self.settings.visibility_timeout_seconds)
        if job_id_string is None:
            return False
        try:
            job_id = uuid.UUID(job_id_string)
        except ValueError:
            await self.queue.ack(job_id_string)
            logger.error("discarded malformed job id=%r", job_id_string)
            return True

        token = uuid.uuid4()
        now = datetime.now(UTC)
        async with SessionLocal() as session:
            job = await session.get(Job, job_id, with_for_update=True)
            if job is None or job.status != JobStatus.queued:
                await session.rollback()
                await self.queue.ack(job_id)
                return True
            job.status = JobStatus.running
            job.attempts += 1
            job.started_at = now
            job.worker_id = self.worker_id
            job.claim_token = token
            job.visibility_deadline = now + timedelta(
                seconds=max(self.settings.visibility_timeout_seconds, job.timeout_seconds + 5)
            )
            job.next_retry_at = None
            await session.commit()
            job_type = job.type
            payload = job.payload
            attempt = job.attempts
            timeout_seconds = job.timeout_seconds

        try:
            handler = get_handler(job_type)
            result = await asyncio.wait_for(handler(payload, attempt), timeout=timeout_seconds)
        except Exception as error:
            await self.fail_job(job_id, token, error)
        else:
            await self.complete_job(job_id, token, result)
        return True

    async def complete_job(self, job_id: uuid.UUID, token: uuid.UUID, result: dict) -> None:
        completed = False
        async with SessionLocal() as session:
            job = await session.get(Job, job_id, with_for_update=True)
            if job and job.status == JobStatus.running and job.claim_token == token:
                job.status = JobStatus.completed
                job.result = result
                job.completed_at = datetime.now(UTC)
                job.visibility_deadline = None
                job.claim_token = None
                job.error_message = None
                await session.execute(
                    update(Worker)
                    .where(Worker.id == self.worker_id)
                    .values(jobs_processed=Worker.jobs_processed + 1)
                )
                completed = True
                await self.queue.ack(job_id)
            await session.commit()
        if completed:
            logger.info("completed job=%s", job_id)

    async def fail_job(self, job_id: uuid.UUID, token: uuid.UUID, error: Exception) -> None:
        retry_at: datetime | None = None
        priority = 0
        terminal = False
        async with SessionLocal() as session:
            job = await session.get(Job, job_id, with_for_update=True)
            if job is None or job.status != JobStatus.running or job.claim_token != token:
                await session.rollback()
                return
            job.error_message = f"{type(error).__name__}: {error}"[:4000]
            job.worker_id = None
            job.claim_token = None
            job.visibility_deadline = None
            priority = job.priority
            if job.attempts < job.max_attempts:
                index = min(job.attempts - 1, len(self.settings.backoff_schedule_seconds) - 1)
                retry_at = datetime.now(UTC) + timedelta(
                    seconds=self.settings.backoff_schedule_seconds[index]
                )
                job.status = JobStatus.queued
                job.next_retry_at = retry_at
            else:
                terminal = True
                job.status = JobStatus.failed
                job.completed_at = datetime.now(UTC)
            if terminal:
                await self.queue.dead_letter(job_id)
            elif retry_at is not None:
                await self.queue.reschedule(job_id, priority, retry_at)
            await session.commit()
        if terminal:
            logger.error("dead-lettered job=%s error=%s", job_id, error)
        elif retry_at is not None:
            logger.warning("retry scheduled job=%s run_at=%s", job_id, retry_at)

    async def consumer_loop(self, slot: int) -> None:
        while not self.stop_event.is_set():
            try:
                processed = await self.process_one()
                if not processed:
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=0.25)
                    except TimeoutError:
                        pass
            except Exception:
                logger.exception("consumer slot=%s failed", slot)
                await asyncio.sleep(1)

    async def run(self) -> None:
        await create_schema()
        await self.register()
        tasks = [asyncio.create_task(self.heartbeat_loop(), name="heartbeat")]
        tasks.append(asyncio.create_task(self.recovery_loop(), name="recovery"))
        tasks.extend(
            asyncio.create_task(self.consumer_loop(slot), name=f"consumer-{slot}")
            for slot in range(self.settings.worker_concurrency)
        )
        await self.stop_event.wait()
        async with SessionLocal() as session:
            await session.execute(
                update(Worker)
                .where(Worker.id == self.worker_id)
                .values(status=WorkerStatus.stopping, last_heartbeat=datetime.now(UTC))
            )
            await session.commit()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        async with SessionLocal() as session:
            await session.execute(
                update(Worker)
                .where(Worker.id == self.worker_id)
                .values(status=WorkerStatus.offline, last_heartbeat=datetime.now(UTC))
            )
            await session.commit()
        await self.redis.aclose()
        await close_database()
        logger.info("worker stopped worker=%s", self.worker_id)


async def async_main() -> None:
    worker = JobWorker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.stop_event.set)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(async_main())
