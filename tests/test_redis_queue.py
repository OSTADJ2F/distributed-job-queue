from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.redis_queue import ACK_SCRIPT, CLAIM_SCRIPT, ENQUEUE_SCRIPT, RedisJobQueue


class FakePipeline:
    def __init__(self, results: list[int] | None = None) -> None:
        self.commands: list[tuple[str, tuple[Any, ...]]] = []
        self.results = results or []

    async def __aenter__(self) -> FakePipeline:
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    def __getattr__(self, name: str):
        def record(*args: Any, **_kwargs: Any) -> FakePipeline:
            self.commands.append((name, args))
            return self

        return record

    async def execute(self) -> list[int]:
        return self.results


class FakeRedis:
    def __init__(self) -> None:
        self.eval_result: Any = None
        self.eval_calls: list[tuple[Any, ...]] = []
        self.pipeline_results: list[int] = []
        self.pipelines: list[FakePipeline] = []
        self.scheduled_members: list[str | bytes] = []
        self.active = False
        self.lrem_calls: list[tuple[Any, ...]] = []

    async def eval(self, *args: Any) -> Any:
        self.eval_calls.append(args)
        return self.eval_result

    def pipeline(self, **_kwargs: Any) -> FakePipeline:
        pipeline = FakePipeline(self.pipeline_results)
        self.pipelines.append(pipeline)
        return pipeline

    async def zrange(self, *_args: Any) -> list[str | bytes]:
        return self.scheduled_members

    async def sismember(self, *_args: Any) -> bool:
        return self.active

    async def lrem(self, *args: Any) -> int:
        self.lrem_calls.append(args)
        return 1

    async def ping(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_enqueue_uses_namespaced_keys_and_schedule_timestamp(monkeypatch) -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis, "emails")
    job_id = uuid.uuid4()
    run_at = datetime.now(UTC) + timedelta(minutes=5)
    monkeypatch.setattr("app.redis_queue.time.time", lambda: 123.0)

    await queue.enqueue(job_id, priority=7, run_at=run_at)

    call = redis.eval_calls[0]
    assert call[:2] == (ENQUEUE_SCRIPT, 4)
    assert call[2:6] == (
        "queue:emails:active",
        "queue:emails:scheduled",
        "queue:emails:ready:7",
        "queue:emails:dead",
    )
    assert call[6:] == (str(job_id), 7, run_at.timestamp(), 123.0)


@pytest.mark.asyncio
async def test_claim_checks_all_priorities_and_decodes_job_id(monkeypatch) -> None:
    redis = FakeRedis()
    redis.eval_result = b"job-123"
    queue = RedisJobQueue(redis, "critical")
    monkeypatch.setattr("app.redis_queue.time.time", lambda: 1000.0)

    claimed = await queue.claim(visibility_timeout=45)

    assert claimed == "job-123"
    call = redis.eval_calls[0]
    assert call[:4] == (
        CLAIM_SCRIPT,
        12,
        "queue:critical:scheduled",
        "queue:critical:processing",
    )
    assert call[4:14] == tuple(f"queue:critical:ready:{priority}" for priority in range(10))
    assert call[14:] == (1000.0, 1045.0, 100)


@pytest.mark.asyncio
async def test_claim_returns_none_when_queue_is_empty() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis)

    assert await queue.claim(30) is None


@pytest.mark.asyncio
async def test_ack_removes_processing_and_active_membership() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis)

    await queue.ack("job-1")

    assert redis.eval_calls == [
        (
            ACK_SCRIPT,
            2,
            "queue:default:processing",
            "queue:default:active",
            "job-1",
        )
    ]


@pytest.mark.asyncio
async def test_remove_cleans_ready_processing_active_and_scheduled_entries() -> None:
    redis = FakeRedis()
    redis.scheduled_members = [b"2:other", b"8:job-1"]
    queue = RedisJobQueue(redis)

    await queue.remove("job-1", priority=8)

    assert redis.pipelines[0].commands == [
        ("lrem", ("queue:default:ready:8", 0, "job-1")),
        ("zrem", ("queue:default:processing", "job-1")),
        ("srem", ("queue:default:active", "job-1")),
        ("zrem", ("queue:default:scheduled", "8:job-1")),
    ]


@pytest.mark.asyncio
async def test_dead_letter_and_release_issue_atomic_pipeline_commands() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis)

    await queue.dead_letter("job-1")
    await queue.release("job-2", priority=4)

    assert redis.pipelines[0].commands[-1] == ("rpush", ("queue:default:dead", "job-1"))
    assert redis.pipelines[1].commands == [
        ("zrem", ("queue:default:processing", "job-2")),
        ("sadd", ("queue:default:active", "job-2")),
        ("lrem", ("queue:default:dead", 0, "job-2")),
        ("rpush", ("queue:default:ready:4", "job-2")),
    ]


@pytest.mark.asyncio
async def test_reschedule_preserves_active_membership() -> None:
    redis = FakeRedis()
    queue = RedisJobQueue(redis)
    run_at = datetime.now(UTC) + timedelta(seconds=10)

    await queue.reschedule("job-9", 6, run_at)

    assert redis.pipelines[0].commands == [
        ("zrem", ("queue:default:processing", "job-9")),
        ("zadd", ("queue:default:scheduled", {"6:job-9": run_at.timestamp()})),
    ]


@pytest.mark.asyncio
async def test_stats_depth_and_health() -> None:
    redis = FakeRedis()
    redis.pipeline_results = [1, 2, 0, 0, 0, 0, 0, 0, 0, 3, 4, 5, 6]
    redis.active = True
    queue = RedisJobQueue(redis)

    stats = await queue.stats()

    assert stats.ready == 6
    assert stats.scheduled == 4
    assert stats.running == 5
    assert stats.dead_letter == 6
    assert await queue.depth() == 15
    assert await queue.is_active("job") is True
    assert await queue.ping() is True
    await queue.remove_from_dead_letter("job")
    assert redis.lrem_calls == [("queue:default:dead", 0, "job")]
