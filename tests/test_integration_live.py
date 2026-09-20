import asyncio
import os
import uuid

import httpx
import pytest

BASE_URL = os.getenv("QUEUE_INTEGRATION_URL")
pytestmark = pytest.mark.skipif(not BASE_URL, reason="set QUEUE_INTEGRATION_URL to run")


async def wait_for_terminal(
    client: httpx.AsyncClient, job_id: str, timeout_seconds: float = 30
) -> dict:
    async with asyncio.timeout(timeout_seconds):
        while True:
            response = await client.get(f"/jobs/{job_id}")
            response.raise_for_status()
            job = response.json()
            if job["status"] in {"completed", "failed", "cancelled"}:
                return job
            await asyncio.sleep(0.1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_one_hundred_jobs_are_processed_once_across_workers() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        nonce = uuid.uuid4().hex
        responses = await asyncio.gather(
            *(
                client.post(
                    "/jobs",
                    json={
                        "payload": {
                            "report_name": f"concurrency-{index}",
                            "rows": 1,
                            "processing_seconds": 0.02,
                        },
                        "priority": index % 10,
                    },
                    headers={"Idempotency-Key": f"integration-{nonce}-{index}"},
                )
                for index in range(100)
            )
        )
        for response in responses:
            response.raise_for_status()
        ids = [response.json()["id"] for response in responses]
        jobs = await asyncio.gather(*(wait_for_terminal(client, job_id) for job_id in ids))
        assert all(job["status"] == "completed" for job in jobs)
        assert all(job["attempts"] == 1 for job in jobs)
        assert len({job["worker_id"] for job in jobs}) >= 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_and_idempotency() -> None:
    key = f"retry-{uuid.uuid4().hex}"
    body = {
        "payload": {
            "report_name": "eventually works",
            "fail_until_attempt": 1,
            "processing_seconds": 0,
        },
        "max_attempts": 3,
    }
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        first = await client.post("/jobs", json=body, headers={"Idempotency-Key": key})
        duplicate = await client.post("/jobs", json=body, headers={"Idempotency-Key": key})
        first.raise_for_status()
        duplicate.raise_for_status()
        assert first.json()["id"] == duplicate.json()["id"]
        job = await wait_for_terminal(client, first.json()["id"], timeout_seconds=45)
        assert job["status"] == "completed"
        assert job["attempts"] == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_timeout_reaches_dead_letter_queue() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        response = await client.post(
            "/jobs",
            json={
                "payload": {"report_name": "timeout", "processing_seconds": 2},
                "timeout_seconds": 1,
                "max_attempts": 1,
            },
        )
        response.raise_for_status()
        job = await wait_for_terminal(client, response.json()["id"])
        assert job["status"] == "failed"
        assert "TimeoutError" in job["error_message"]
        queues = (await client.get("/queues")).json()
        assert queues[0]["dead_letter"] >= 1
