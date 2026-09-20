import pytest
from pydantic import ValidationError

from app.handlers import generate_report, get_handler


@pytest.mark.asyncio
async def test_generate_report_returns_summary() -> None:
    result = await generate_report(
        {"report_name": "sales", "rows": 42, "processing_seconds": 0}, attempt=1
    )
    assert result == {
        "report_name": "sales",
        "rows_processed": 42,
        "message": "Generated sales with 42 rows",
    }


@pytest.mark.asyncio
async def test_generate_report_can_fail_for_retry_demonstrations() -> None:
    with pytest.raises(RuntimeError, match="attempt 2"):
        await generate_report(
            {
                "report_name": "flaky",
                "rows": 1,
                "processing_seconds": 0,
                "fail_until_attempt": 2,
            },
            attempt=2,
        )


@pytest.mark.asyncio
async def test_handler_validates_payload_at_execution_boundary() -> None:
    with pytest.raises(ValidationError):
        await generate_report({"report_name": "", "rows": 0}, attempt=1)


def test_unknown_handler_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown job type"):
        get_handler("does_not_exist")
