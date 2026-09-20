import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

from app.models import JobStatus, WorkerStatus


class GenerateReportPayload(BaseModel):
    report_name: str = Field(min_length=1, max_length=120)
    rows: int = Field(default=100, ge=1, le=100_000)
    fail_until_attempt: int = Field(default=0, ge=0, le=20)
    processing_seconds: float = Field(default=0.1, ge=0, le=600)


class JobCreate(BaseModel):
    type: Literal["generate_report"] = "generate_report"
    payload: GenerateReportPayload
    priority: int = Field(default=5, ge=0, le=9)
    max_attempts: int = Field(default=3, ge=1, le=20)
    timeout_seconds: int | None = Field(default=None, ge=1, le=3600)
    scheduled_at: datetime | None = None
    delay_seconds: int | None = Field(default=None, ge=0, le=2_592_000)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)

    @model_validator(mode="after")
    def one_schedule_option(self) -> "JobCreate":
        if self.scheduled_at is not None and self.delay_seconds is not None:
            raise ValueError("provide scheduled_at or delay_seconds, not both")
        if self.scheduled_at is not None and self.scheduled_at.tzinfo is None:
            raise ValueError("scheduled_at must include a timezone")
        return self

    def effective_schedule(self) -> datetime | None:
        if self.delay_seconds is not None:
            from datetime import timedelta

            return datetime.now(UTC) + timedelta(seconds=self.delay_seconds)
        return self.scheduled_at


class JobRead(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    type: str
    payload: dict
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    timeout_seconds: int
    created_at: datetime
    scheduled_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    next_retry_at: datetime | None
    error_message: str | None
    result: dict | None
    idempotency_key: str | None
    worker_id: uuid.UUID | None


class WorkerRead(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    hostname: str
    status: WorkerStatus
    started_at: datetime
    last_heartbeat: datetime
    jobs_processed: int


class QueueRead(BaseModel):
    name: str
    ready: int
    scheduled: int
    running: int
    dead_letter: int
    oldest_queued_at: datetime | None
    depth_limit: int


class HealthRead(BaseModel):
    status: Literal["ok"]


IdempotencyHeader = Annotated[str | None, Field(max_length=255)]
