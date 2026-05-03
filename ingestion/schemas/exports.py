from datetime import datetime
from pathlib import Path

from pydantic import BaseModel


class CsvExportRequest(BaseModel):
    series_codes: list[str] | None = None
    source_codes: list[str] | None = None


class CsvExportFile(BaseModel):
    series_code: str
    series_name: str
    path: Path
    row_count: int


class CsvExportResult(BaseModel):
    export_started_at: datetime
    export_dir: Path
    file_count: int
    row_count: int
    files: list[CsvExportFile]
