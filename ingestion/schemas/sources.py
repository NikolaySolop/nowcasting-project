from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator


class SourceKind(str, Enum):
    API = "api"
    CSV = "csv"
    MANUAL = "manual"
    WEB = "web"
    VENDOR = "vendor"
    EXCHANGE = "exchange"


class WebScrapeSpec(BaseModel):
    url: HttpUrl | None = None
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, str] = Field(default_factory=dict)
    parser: Literal["html_table", "json", "text"] = "html_table"

    table_selector: str = "table"
    row_selector: str = "tr"
    cell_selector: str = "td,th"
    date_column: str | int = 0
    value_column: str | int = 1
    text_column: str | int | None = None
    series_code: str | None = None
    date_format: str | None = None
    start_date: datetime | None = None

    extra: dict[str, Any] = Field(default_factory=dict)

    @field_validator("start_date", mode="before")
    @classmethod
    def parse_start_date(cls, value: Any) -> Any:
        if value is None or isinstance(value, datetime):
            return value
        if isinstance(value, str):
            raw = value.strip()
            for fmt in ("%d.%m.%Y", "%m.%d.%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(raw, fmt)
                except ValueError:
                    continue
        return value


class CsvSpec(BaseModel):
    path: Path | None = None
    url: HttpUrl | None = None
    delimiter: str = ","
    date_column: str = "observed_at"
    value_column: str = "value"
    text_column: str | None = None
    series_code_column: str | None = None
    series_code: str | None = None
    date_format: str | None = None
    release_date_column: str | None = None
    vintage_date_column: str | None = None
    store_in_observations: bool = False


class SeriesDefinition(BaseModel):
    series_code: str = Field(min_length=1, max_length=50)
    series_name: str | None = None
    frequency: Literal["15min", "daily", "weekly", "monthly", "annual"] | None = None
    group_code: str | None = None
    subgroup_code: str | None = None
    description: str | None = None
    units: str | None = None
    default_transform: Literal["level", "log_return", "diff", "spread", "yoy", "mom"] | None = None
    is_model_input: bool | None = None


class SourceDefinition(BaseModel):
    source_code: str = Field(min_length=1, max_length=50)
    source_name: str
    source_type: SourceKind = SourceKind.WEB
    adapter_name: str = "web"
    enabled: bool = True
    schedule_cron: str | None = None
    scrape: WebScrapeSpec | None = None
    csv: CsvSpec | None = None
    series: list[SeriesDefinition] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
