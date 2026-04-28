from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


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

    extra: dict[str, Any] = Field(default_factory=dict)


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


class SeriesDefinition(BaseModel):
    series_code: str = Field(min_length=1, max_length=50)
    series_name: str | None = None


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
