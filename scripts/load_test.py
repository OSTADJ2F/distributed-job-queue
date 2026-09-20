import argparse
import asyncio
import time

import httpx


async def submit(client: httpx.AsyncClient, index: int) -> str:
    response = await client.post(
        "/jobs",
        json={
            "type": "generate_report",
            "payload": {"report_name": f"load-{index}", "rows": 10},
            "priority": index % 10,
        },
        headers={"Idempotency-Key": f"load-{index}"},
    )
    response.raise_for_status()
    return response.json()["id"]


async def main(total: int, concurrency: int) -> None:
    limits = httpx.Limits(max_connections=concurrency)
    started = time.perf_counter()
    async with httpx.AsyncClient(base_url="http://localhost:8000", limits=limits) as client:
        semaphore = asyncio.Semaphore(concurrency)

        async def limited(index: int) -> str:
            async with semaphore:
                return await submit(client, index)

        ids = await asyncio.gather(*(limited(index) for index in range(total)))
    elapsed = time.perf_counter() - started
    print(f"submitted={len(ids)} seconds={elapsed:.2f} rate={len(ids) / elapsed:.1f}/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(main(args.jobs, args.concurrency))
