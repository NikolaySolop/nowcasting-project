from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class IngestionJobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PARTIAL = "partial"


class IngestionJobRequest(BaseModel):
    source_codes: list[str] | None = None
    force: bool = False


class IngestionJobResult(BaseModel):
    job_id: UUID = Field(default_factory=uuid4)
    status: IngestionJobStatus = IngestionJobStatus.QUEUED
    source_codes: list[str] = Field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    loaded_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0
    messages: list[str] = Field(default_factory=list)

    def start(self) -> None:
        self.status = IngestionJobStatus.RUNNING
        self.started_at = datetime.now(timezone.utc)

    def finish(self) -> None:
        self.finished_at = datetime.now(timezone.utc)
        if self.error_count and self.loaded_count:
            self.status = IngestionJobStatus.PARTIAL
        elif self.error_count:
            self.status = IngestionJobStatus.FAILED
        else:
            self.status = IngestionJobStatus.SUCCEEDED
