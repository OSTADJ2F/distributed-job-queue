from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import FakeSession

from app.config import Settings
from app.models import JobStatus
from app.worker import JobWorker


def build_worker(queue: SimpleNamespace, **settings_overrides) -> JobWorker:
    worker = JobWorker.__new__(JobWorker)
    worker.settings = Settings(_env_file=None, **settings_overrides)
    worker.worker_id = uuid.uuid4()
    worker.queue = queue
    return worker


def session_factory(session: FakeSession):
    return lambda: session


@pytest.mark.asyncio
async def test_process_one_discards_malformed_queue_entry() -> None:
    queue = SimpleNamespace(claim=AsyncMock(return_value="not-a-uuid"), ack=AsyncMock())
    worker = build_worker(queue)

    assert await worker.process_one() is True
    queue.ack.assert_awaited_once_with("not-a-uuid")


@pytest.mark.asyncio
async def test_process_one_returns_false_when_no_job_is_available() -> None:
    queue = SimpleNamespace(claim=AsyncMock(return_value=None))
    worker = build_worker(queue)

    assert await worker.process_one() is False


@pytest.mark.asyncio
async def test_process_one_completes_claimed_job(monkeypatch, job_factory) -> None:
    job = job_factory()
    queue = SimpleNamespace(
        claim=AsyncMock(return_value=str(job.id)),
        ack=AsyncMock(),
        dead_letter=AsyncMock(),
        reschedule=AsyncMock(),
    )
    worker = build_worker(queue, visibility_timeout_seconds=20)
    session = FakeSession(get_result=job)
    handler = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(session))
    monkeypatch.setattr("app.worker.get_handler", lambda _job_type: handler)

    assert await worker.process_one() is True

    assert job.status == JobStatus.completed
    assert job.attempts == 1
    assert job.result == {"ok": True}
    assert job.claim_token is None
    handler.assert_awaited_once_with(job.payload, 1)
    queue.ack.assert_awaited_once_with(job.id)


@pytest.mark.asyncio
async def test_process_one_acks_job_that_is_no_longer_queued(monkeypatch, job_factory) -> None:
    job = job_factory(status=JobStatus.cancelled)
    queue = SimpleNamespace(claim=AsyncMock(return_value=str(job.id)), ack=AsyncMock())
    worker = build_worker(queue)
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(FakeSession(get_result=job)))

    assert await worker.process_one() is True
    queue.ack.assert_awaited_once_with(job.id)


@pytest.mark.asyncio
async def test_failed_attempt_is_rescheduled_with_configured_backoff(
    monkeypatch, job_factory
) -> None:
    token = uuid.uuid4()
    job = job_factory(status=JobStatus.running, attempts=1, claim_token=token, priority=7)
    queue = SimpleNamespace(reschedule=AsyncMock(), dead_letter=AsyncMock())
    worker = build_worker(queue, backoff_schedule_seconds=(11, 22))
    session = FakeSession(get_result=job)
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(session))

    await worker.fail_job(job.id, token, RuntimeError("temporary"))

    assert job.status == JobStatus.queued
    assert job.error_message == "RuntimeError: temporary"
    assert job.next_retry_at > datetime.now(UTC)
    queue.reschedule.assert_awaited_once_with(job.id, 7, job.next_retry_at)
    queue.dead_letter.assert_not_awaited()


@pytest.mark.asyncio
async def test_final_failed_attempt_goes_to_dead_letter(monkeypatch, job_factory) -> None:
    token = uuid.uuid4()
    job = job_factory(
        status=JobStatus.running,
        attempts=3,
        max_attempts=3,
        claim_token=token,
    )
    queue = SimpleNamespace(reschedule=AsyncMock(), dead_letter=AsyncMock())
    worker = build_worker(queue)
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(FakeSession(get_result=job)))

    await worker.fail_job(job.id, token, TimeoutError())

    assert job.status == JobStatus.failed
    assert job.completed_at is not None
    queue.dead_letter.assert_awaited_once_with(job.id)
    queue.reschedule.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_completion_cannot_overwrite_new_claim(monkeypatch, job_factory) -> None:
    current_token = uuid.uuid4()
    stale_token = uuid.uuid4()
    job = job_factory(status=JobStatus.running, claim_token=current_token)
    queue = SimpleNamespace(ack=AsyncMock())
    worker = build_worker(queue)
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(FakeSession(get_result=job)))

    await worker.complete_job(job.id, stale_token, {"stale": True})

    assert job.status == JobStatus.running
    assert job.result is None
    queue.ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_stale_failure_cannot_reschedule_new_claim(monkeypatch, job_factory) -> None:
    job = job_factory(status=JobStatus.running, claim_token=uuid.uuid4())
    queue = SimpleNamespace(reschedule=AsyncMock(), dead_letter=AsyncMock())
    worker = build_worker(queue)
    session = FakeSession(get_result=job)
    monkeypatch.setattr("app.worker.SessionLocal", session_factory(session))

    await worker.fail_job(job.id, uuid.uuid4(), RuntimeError("late"))

    assert session.rollback_count == 1
    assert job.status == JobStatus.running
    queue.reschedule.assert_not_awaited()
