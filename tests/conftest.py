from __future__ import annotations

import uuid
from collections import deque
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from app.database import get_session
from app.main import app
from app.models import Job, JobStatus


def make_job(**overrides: Any) -> Job:
    """Build a fully populated Job without requiring a database round trip."""
    now = datetime.now(UTC)
    values: dict[str, Any] = {
        "id": uuid.uuid4(),
        "type": "generate_report",
        "payload": {"report_name": "test", "rows": 10, "processing_seconds": 0},
        "status": JobStatus.queued,
        "priority": 5,
        "attempts": 0,
        "max_attempts": 3,
        "timeout_seconds": 30,
        "created_at": now,
        "scheduled_at": None,
        "started_at": None,
        "completed_at": None,
        "next_retry_at": None,
        "visibility_deadline": None,
        "error_message": None,
        "result": None,
        "idempotency_key": None,
        "worker_id": None,
        "claim_token": None,
    }
    values.update(overrides)
    return Job(**values)


class ScalarCollection:
    def __init__(self, values: list[Any]) -> None:
        self.values = values

    def scalars(self) -> ScalarCollection:
        return self

    def all(self) -> list[Any]:
        return self.values

    def __iter__(self):
        return iter(self.values)


class FakeSession:
    """Small async SQLAlchemy session double used by API and worker unit tests."""

    def __init__(
        self,
        *,
        get_result: Any = None,
        scalar_results: list[Any] | None = None,
        execute_results: list[Any] | None = None,
        commit_error: Exception | None = None,
    ) -> None:
        self.get_result = get_result
        self.scalar_results = deque(scalar_results or [])
        self.execute_results = deque(execute_results or [])
        self.commit_error = commit_error
        self.added: list[Any] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.refresh_count = 0

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    def add(self, value: Any) -> None:
        if isinstance(value, Job):
            value.id = value.id or uuid.uuid4()
            value.status = value.status or JobStatus.queued
            value.attempts = value.attempts or 0
            value.created_at = value.created_at or datetime.now(UTC)
            for field in (
                "started_at",
                "completed_at",
                "next_retry_at",
                "visibility_deadline",
                "error_message",
                "result",
                "worker_id",
                "claim_token",
            ):
                if field not in value.__dict__:
                    setattr(value, field, None)
        self.added.append(value)

    async def get(self, *_args: Any, **_kwargs: Any) -> Any:
        return self.get_result

    async def scalar(self, _statement: Any) -> Any:
        return self.scalar_results.popleft() if self.scalar_results else None

    async def execute(self, _statement: Any) -> Any:
        if self.execute_results:
            return self.execute_results.popleft()
        return ScalarCollection([])

    async def commit(self) -> None:
        self.commit_count += 1
        if self.commit_error is not None:
            error, self.commit_error = self.commit_error, None
            raise error

    async def rollback(self) -> None:
        self.rollback_count += 1

    async def refresh(self, _value: Any) -> None:
        self.refresh_count += 1


@pytest.fixture
def job_factory():
    return make_job


@pytest.fixture
async def api_client():
    created_overrides: list[Any] = []

    async def build(session: FakeSession, queue: Any) -> httpx.AsyncClient:
        async def override_session():
            yield session

        app.dependency_overrides[get_session] = override_session
        app.state.queue = queue
        transport = httpx.ASGITransport(app=app)
        client = httpx.AsyncClient(transport=transport, base_url="http://test")
        created_overrides.append(client)
        return client

    yield build

    for client in created_overrides:
        await client.aclose()
    app.dependency_overrides.clear()
