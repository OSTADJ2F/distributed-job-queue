import asyncio

import httpx


async def main() -> None:
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=30) as client:
        for index in range(25):
            payload = {
                "type": "generate_report",
                "payload": {
                    "report_name": f"demo-{index:02d}",
                    "rows": 100 + index,
                    "processing_seconds": 0.2,
                    "fail_until_attempt": 1 if index % 8 == 0 else 0,
                },
                "priority": 9 if index % 10 == 0 else index % 5,
                "max_attempts": 3,
                "delay_seconds": 10 if index % 7 == 0 else 0,
            }
            response = await client.post(
                "/jobs", json=payload, headers={"Idempotency-Key": f"seed-{index}"}
            )
            response.raise_for_status()
            print(response.json()["id"], response.json()["status"])


if __name__ == "__main__":
    asyncio.run(main())
