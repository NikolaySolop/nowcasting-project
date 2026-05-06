import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import RawObservationIn


class ManualCsvAdapter(BaseAdapter):
    name = "manual_csv"

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.csv
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no csv spec")

        if spec.url is not None:
            async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds) as client:
                response = await client.get(str(spec.url))
                response.raise_for_status()
            content = response.text
        elif spec.path is not None:
            content = spec.path.read_text(encoding="utf-8")
        else:
            raise AdapterError(f"source {context.source.source_code} has no csv path or url")

        reader = csv.DictReader(content.splitlines(), delimiter=spec.delimiter)
        observations: list[RawObservationIn] = []
        for row in reader:
            series_code = row.get(spec.series_code_column) if spec.series_code_column else spec.series_code
            if not series_code:
                raise AdapterError("csv row has no series code")
            raw_value = row.get(spec.value_column, "")
            value_numeric = self._parse_decimal(raw_value)
            value_text = row.get(spec.text_column) if spec.text_column else None
            observed_at = self._parse_date(row[spec.date_column], spec.date_format)
            pub_raw = row.get(spec.release_date_column) if spec.release_date_column else None
            publication_at = self._parse_date(pub_raw, None) if pub_raw and pub_raw.strip() else None
            vintage_raw = row.get(spec.vintage_date_column) if spec.vintage_date_column else None
            vintage_at = self._parse_date(vintage_raw, None) if vintage_raw and vintage_raw.strip() else None
            observations.append(
                RawObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    publication_at=publication_at,
                    vintage_at=vintage_at or datetime.now(timezone.utc),
                    value_numeric=value_numeric,
                    value_text=value_text if value_numeric is None else None,
                    raw_payload=dict(row),
                )
            )

        return FetchResult(observations=observations)

    @staticmethod
    def _parse_date(value: str, date_format: str | None) -> datetime:
        if date_format:
            parsed = datetime.strptime(value, date_format)
        else:
            parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _parse_decimal(value: str) -> Decimal | None:
        try:
            return Decimal(value.replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return None
