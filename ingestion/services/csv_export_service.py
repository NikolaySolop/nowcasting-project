from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.schemas.exports import CsvExportFile, CsvExportResult
from storage.models.raw_obsevations import RawObservation
from storage.models.series import Series
from storage.models.source import DataSource


class CsvExportService:
    def __init__(self, session: AsyncSession, export_dir: Path) -> None:
        self.session = session
        self.export_dir = export_dir

    async def export(
        self,
        *,
        series_codes: list[str] | None = None,
        source_codes: list[str] | None = None,
    ) -> CsvExportResult:
        export_started_at = datetime.now(timezone.utc)
        rows = await self._load_rows(series_codes=series_codes, source_codes=source_codes)
        grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped_rows[(row["series_code"], row["series_name"])].append(row)

        self.export_dir.mkdir(parents=True, exist_ok=True)
        files: list[CsvExportFile] = []
        for (series_code, series_name), series_rows in sorted(grouped_rows.items()):
            path = self._next_export_path(series_code, export_started_at)
            self._write_csv(path, series_rows)
            files.append(
                CsvExportFile(
                    series_code=series_code,
                    series_name=series_name,
                    path=path,
                    row_count=len(series_rows),
                )
            )

        return CsvExportResult(
            export_started_at=export_started_at,
            export_dir=self.export_dir,
            file_count=len(files),
            row_count=sum(file.row_count for file in files),
            files=files,
        )

    async def _load_rows(
        self,
        *,
        series_codes: list[str] | None,
        source_codes: list[str] | None,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(
                Series.series_code,
                Series.series_name,
                DataSource.source_code,
                RawObservation.observed_at,
                RawObservation.period_start,
                RawObservation.period_end,
                RawObservation.value_numeric,
                RawObservation.value_text,
                RawObservation.publication_at,
                RawObservation.vintage_at,
                RawObservation.is_revised,
                RawObservation.is_final,
            )
            .join(RawObservation, RawObservation.series_id == Series.id)
            .join(DataSource, RawObservation.source_id == DataSource.id)
            .order_by(Series.series_code, RawObservation.observed_at, RawObservation.publication_at)
        )
        if series_codes:
            stmt = stmt.where(Series.series_code.in_(series_codes))
        if source_codes:
            stmt = stmt.where(DataSource.source_code.in_(source_codes))

        result = await self.session.execute(stmt)
        return [
            {
                "series_code": series_code,
                "series_name": series_name,
                "source_code": source_code,
                "observed_at": self._format_datetime(observed_at),
                "period_start": self._format_datetime(period_start),
                "period_end": self._format_datetime(period_end),
                "value_numeric": str(value_numeric) if value_numeric is not None else "",
                "value_text": value_text or "",
                "publication_at": self._format_datetime(publication_at),
                "vintage_at": self._format_datetime(vintage_at),
                "is_revised": is_revised,
                "is_final": is_final,
            }
            for (
                series_code,
                series_name,
                source_code,
                observed_at,
                period_start,
                period_end,
                value_numeric,
                value_text,
                publication_at,
                vintage_at,
                is_revised,
                is_final,
            ) in result.all()
        ]

    def _next_export_path(self, series_code: str, export_started_at: datetime) -> Path:
        timestamp = export_started_at.strftime("%Y%m%dT%H%M%S%fZ")
        stem = f"{self._safe_filename(series_code)}_{timestamp}"
        path = self.export_dir / f"{stem}.csv"
        suffix = 1
        while path.exists():
            path = self.export_dir / f"{stem}_{suffix}.csv"
            suffix += 1
        return path

    @staticmethod
    def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
        fieldnames = [
            "series_code",
            "series_name",
            "source_code",
            "observed_at",
            "period_start",
            "period_end",
            "value_numeric",
            "value_text",
            "publication_at",
            "vintage_at",
            "is_revised",
            "is_final",
        ]
        with path.open("x", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _format_datetime(value: datetime | None) -> str:
        if value is None:
            return ""
        return value.isoformat()

    @staticmethod
    def _safe_filename(value: str) -> str:
        normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
        return normalized.strip("._-") or "series"
