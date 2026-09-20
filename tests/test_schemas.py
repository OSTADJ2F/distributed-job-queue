from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.schemas import JobCreate


def test_job_payload_and_controls_are_validated() -> None:
    request = JobCreate.model_validate(
        {
            "payload": {"report_name": "quarterly", "rows": 1000},
            "priority": 9,
            "max_attempts": 5,
            "timeout_seconds": 120,
        }
    )
    assert request.type == "generate_report"
    assert request.payload.rows == 1000


def test_naive_schedule_is_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        JobCreate(
            payload={"report_name": "bad schedule"},
            scheduled_at=datetime(2030, 1, 1),
        )


def test_absolute_and_relative_schedule_are_mutually_exclusive() -> None:
    with pytest.raises(ValidationError, match="not both"):
        JobCreate(
            payload={"report_name": "ambiguous"},
            scheduled_at=datetime.now(UTC),
            delay_seconds=5,
        )


def test_absolute_schedule_is_returned_unchanged() -> None:
    scheduled_at = datetime.now(UTC)
    request = JobCreate(
        payload={"report_name": "scheduled"},
        scheduled_at=scheduled_at,
    )
    assert request.effective_schedule() == scheduled_at
