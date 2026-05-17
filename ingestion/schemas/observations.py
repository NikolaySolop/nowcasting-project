from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class ObservationKind(str, Enum):
    QUOTE = "quote"
    MACRO = "macro"
    CALENDAR = "calendar"
    NEWS = "news"
    EVENT = "event"


class ParsedObservation(BaseModel):
    series_code: str = Field(min_length=1, max_length=50)
    source_code: str = Field(min_length=1, max_length=50)
    observed_at: datetime
    period_start: datetime | None = None
    period_end: datetime | None = None
    value_numeric: Decimal | None = None
    value_text: str | None = None
    publication_at: datetime | None = None
    vintage_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_revised: bool = False
    is_final: bool = True
    kind: ObservationKind = ObservationKind.MACRO
    raw_payload: dict[str, Any] | None = None

    @field_validator("observed_at", "period_start", "period_end", "publication_at", "vintage_at")
    @classmethod
    def ensure_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @model_validator(mode="after")
    def validate_value(self) -> "ParsedObservation":
        if self.value_numeric is None and self.value_text is None:
            raise ValueError("either value_numeric or value_text must be set")
        return self

    @property
    def duplicate_key(self) -> tuple[object, ...]:
        return (
            self.series_code,
            self.source_code,
            self.observed_at,
            self.publication_at,
            self.value_numeric,
            self.value_text,
        )


class ObservationIn(BaseModel):
    series_code: str = Field(min_length=1, max_length=50)
    source_code: str = Field(min_length=1, max_length=50)
    reference_date: date | None = None
    reference_start: datetime
    reference_end: datetime
    value: Decimal
    published_at: datetime
    compress_equal_runs: bool = False
    skip_equal_to_previous: bool = False

    @field_validator("reference_start", "reference_end", "published_at")
    @classmethod
    def ensure_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @model_validator(mode="after")
    def ensure_reference_date(self) -> "ObservationIn":
        if self.reference_date is None:
            self.reference_date = self.reference_start.date()
        return self

    @property
    def duplicate_key(self) -> tuple[object, ...]:
        return (
            self.series_code,
            self.source_code,
            self.reference_date,
            self.reference_start,
            self.reference_end,
            self.published_at,
            self.value,
        )


class IngestionBatch(BaseModel):
    source_code: str
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    observations: list[ParsedObservation] = Field(default_factory=list)
    raw_payload: dict[str, Any] | None = None
