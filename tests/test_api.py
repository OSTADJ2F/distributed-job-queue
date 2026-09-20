from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import FakeSession
from sqlalchemy.exc import IntegrityError

from app.models import JobStatus
from app.redis_queue import QueueStats


def fake_queue(*, depth: int = 0) -> SimpleNamespace:
    return SimpleNamespace(
        depth=AsyncMock(return_value=depth),
        enqueue=AsyncMock(),
        remove=AsyncMock(),
        remove_from_dead_letter=AsyncMock(),
        stats=AsyncMock(return_value=QueueStats(ready=2, scheduled=1, running=1, dead_letter=0)),
        ping=AsyncMock(return_value=True),
    )


@pytest.mark.asyncio
async def test_create_job_persists_and_enqueues(api_client) -> None:
    session = FakeSession(scalar_results=[None])
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.post(
        "/jobs",
        json={
            "payload": {"report_name": "portfolio", "rows": 25},
            "priority": 8,
            "delay_seconds": 0,
        },
        headers={"Idempotency-Key": "report-2026-09"},
    )

    assert response.status_code == 201
    assert response.json()["status"] == "queued"
    assert response.json()["idempotency_key"] == "report-2026-09"
    assert session.commit_count == 1
    queue.enqueue.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_job_returns_existing_idempotent_job(api_client, job_factory) -> None:
    existing = job_factory(idempotency_key="same-request")
    session = FakeSession(scalar_results=[existing])
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.post(
        "/jobs",
        json={"payload": {"report_name": "duplicate"}},
        headers={"Idempotency-Key": "same-request"},
    )

    assert response.status_code == 200
    assert response.json()["id"] == str(existing.id)
    assert session.added == []
    queue.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_idempotent_create_returns_winning_job(api_client, job_factory) -> None:
    existing = job_factory(idempotency_key="raced-request")
    session = FakeSession(
        scalar_results=[None, existing],
        commit_error=IntegrityError("insert", {}, RuntimeError("unique violation")),
    )
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.post(
        "/jobs",
        json={"payload": {"report_name": "raced"}},
        headers={"Idempotency-Key": "raced-request"},
    )

    assert response.status_code == 200
    assert response.json()["id"] == str(existing.id)
    assert session.rollback_count == 1
    queue.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_job_rejects_requests_when_queue_is_full(api_client) -> None:
    client = await api_client(FakeSession(), fake_queue(depth=10_000))

    response = await client.post("/jobs", json={"payload": {"report_name": "overflow"}})

    assert response.status_code == 429
    assert response.json() == {"detail": "queue depth limit reached"}


@pytest.mark.asyncio
async def test_create_job_reports_validation_errors(api_client) -> None:
    client = await api_client(FakeSession(), fake_queue())

    response = await client.post("/jobs", json={"payload": {"report_name": ""}, "priority": 99})

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_job_returns_404_for_unknown_id(api_client) -> None:
    client = await api_client(FakeSession(), fake_queue())

    response = await client.get("/jobs/00000000-0000-0000-0000-000000000001")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_cancel_queued_job_updates_state_and_queue(api_client, job_factory) -> None:
    job = job_factory()
    session = FakeSession(get_result=job)
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.delete(f"/jobs/{job.id}")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert job.completed_at is not None
    queue.remove.assert_awaited_once_with(job.id, job.priority)


@pytest.mark.asyncio
async def test_cancel_terminal_job_returns_conflict(api_client, job_factory) -> None:
    job = job_factory(status=JobStatus.completed, completed_at=datetime.now(UTC))
    client = await api_client(FakeSession(get_result=job), fake_queue())

    response = await client.delete(f"/jobs/{job.id}")

    assert response.status_code == 409
    assert response.json()["detail"] == "cannot cancel completed job"


@pytest.mark.asyncio
async def test_retry_failed_job_resets_attempt_state(api_client, job_factory) -> None:
    job = job_factory(
        status=JobStatus.failed,
        attempts=3,
        completed_at=datetime.now(UTC),
        error_message="boom",
        result={"partial": True},
    )
    session = FakeSession(get_result=job)
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["attempts"] == 0
    assert response.json()["error_message"] is None
    queue.remove_from_dead_letter.assert_awaited_once_with(job.id)
    queue.enqueue.assert_awaited_once_with(job.id, job.priority)


@pytest.mark.asyncio
async def test_retry_running_job_returns_conflict(api_client, job_factory) -> None:
    job = job_factory(status=JobStatus.running)
    client = await api_client(FakeSession(get_result=job), fake_queue())

    response = await client.post(f"/jobs/{job.id}/retry")

    assert response.status_code == 409


@pytest.mark.asyncio
async def test_health_checks_database_and_redis(api_client) -> None:
    session = FakeSession()
    queue = fake_queue()
    client = await api_client(session, queue)

    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    queue.ping.assert_awaited_once()


@pytest.mark.asyncio
async def test_queue_summary_combines_redis_and_database_state(api_client) -> None:
    oldest = datetime.now(UTC)
    session = FakeSession(scalar_results=[oldest])
    client = await api_client(session, fake_queue())

    response = await client.get("/queues")

    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "default",
            "ready": 2,
            "scheduled": 1,
            "running": 1,
            "dead_letter": 0,
            "oldest_queued_at": oldest.isoformat().replace("+00:00", "Z"),
            "depth_limit": 10_000,
        }
    ]


@pytest.mark.asyncio
async def test_dashboard_is_served(api_client) -> None:
    client = await api_client(FakeSession(), fake_queue())

    response = await client.get("/dashboard")

    assert response.status_code == 200
    assert "Queue control room" in response.text
