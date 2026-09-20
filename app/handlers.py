import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from app.schemas import GenerateReportPayload

JobHandler = Callable[[dict[str, Any], int], Awaitable[dict[str, Any]]]


async def generate_report(payload: dict[str, Any], attempt: int) -> dict[str, Any]:
    request = GenerateReportPayload.model_validate(payload)
    await asyncio.sleep(request.processing_seconds)
    if attempt <= request.fail_until_attempt:
        raise RuntimeError(f"intentional demonstration failure on attempt {attempt}")
    return {
        "report_name": request.report_name,
        "rows_processed": request.rows,
        "message": f"Generated {request.report_name} with {request.rows} rows",
    }


HANDLERS: dict[str, JobHandler] = {"generate_report": generate_report}


def get_handler(job_type: str) -> JobHandler:
    try:
        return HANDLERS[job_type]
    except KeyError as error:
        raise ValueError(f"unknown job type: {job_type}") from error
