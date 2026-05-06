from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class WebPageAdapter(BaseAdapter):
    name = "web"

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None or spec.url is None:
            raise AdapterError(f"source {context.source.source_code} has no web scrape url")

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds) as client:
            response = await client.request(
                spec.method,
                str(spec.url),
                headers=headers,
                params=spec.params,
            )
            response.raise_for_status()

        if spec.parser == "html_table":
            observations = self._parse_html_table(context, response.text)
        elif spec.parser == "json":
            observations = self._parse_json(context, response.json())
        else:
            observations = [
                RawObservationIn(
                    series_code=spec.series_code or context.source.source_code,
                    source_code=context.source.source_code,
                    observed_at=datetime.now().astimezone(),
                    value_text=response.text,
                    kind=ObservationKind.EVENT,
                    raw_payload={"url": str(spec.url)},
                )
            ]

        return FetchResult(
            observations=observations,
            raw_payload={
                "url": str(spec.url),
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
            },
        )

    def _parse_html_table(self, context: FetchContext, html: str) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one(spec.table_selector)
        if table is None:
            raise AdapterError(f"table selector did not match: {spec.table_selector}")

        observations: list[RawObservationIn] = []
        headers: list[str] = []
        for row in table.select(spec.row_selector):
            cells = [cell.get_text(" ", strip=True) for cell in row.select(spec.cell_selector)]
            if not cells:
                continue
            if row.select("th"):
                headers = cells
                continue

            row_payload = {"cells": cells, "headers": headers}
            observed_at = self._parse_date(self._cell(cells, headers, spec.date_column), spec.date_format)
            value_text = self._cell(cells, headers, spec.text_column) if spec.text_column is not None else None
            value_numeric = self._parse_decimal(self._cell(cells, headers, spec.value_column))

            observations.append(
                RawObservationIn(
                    series_code=spec.series_code or context.source.source_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    value_numeric=value_numeric,
                    value_text=value_text if value_numeric is None else None,
                    kind=ObservationKind.MACRO,
                    raw_payload=row_payload,
                )
            )

        return observations

    def _parse_json(self, context: FetchContext, payload: Any) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        rows = payload if isinstance(payload, list) else payload.get("data", [])
        if not isinstance(rows, list):
            raise AdapterError("json parser expects a list payload or a top-level data list")

        series_code = spec.series_code or context.source.source_code
        start_dt = spec.start_date.replace(tzinfo=timezone.utc) if spec.start_date else None
        latest = context.latest_observed_at_by_series.get(series_code)
        extra = spec.extra or {}
        pub_nth_bday = extra.get("publication_at_nth_bday_next_month")
        pub_col = spec.extra.get("publication_at_column") if spec.extra else None

        observations: list[RawObservationIn] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            observed_at = self._parse_date(str(row[spec.date_column]), spec.date_format)
            if start_dt and observed_at < start_dt:
                continue
            if latest and observed_at <= latest:
                continue
            raw_value = row.get(spec.value_column)
            value_numeric = self._parse_decimal(str(raw_value))
            value_text = str(row.get(spec.text_column)) if spec.text_column else None
            if pub_col and row.get(pub_col):
                publication_at = self._parse_date(str(row[pub_col]), None)
            elif pub_nth_bday is not None:
                publication_at = self._nth_business_day_of_next_month(observed_at, int(pub_nth_bday))
            else:
                publication_at = None
            observations.append(
                RawObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    publication_at=publication_at,
                    value_numeric=value_numeric,
                    value_text=value_text if value_numeric is None else None,
                    raw_payload=row,
                )
            )
        return observations

    @staticmethod
    def _nth_business_day_of_next_month(observed_at: datetime, n: int) -> datetime:
        """Return the nth Mon–Fri of the month following observed_at (UTC midnight)."""
        if observed_at.month == 12:
            year, month = observed_at.year + 1, 1
        else:
            year, month = observed_at.year, observed_at.month + 1
        d = observed_at.replace(year=year, month=month, day=1,
                                hour=0, minute=0, second=0, microsecond=0,
                                tzinfo=timezone.utc)
        count = 0
        while True:
            if d.weekday() < 5:
                count += 1
                if count == n:
                    return d
            d += timedelta(days=1)

    @staticmethod
    def _cell(cells: list[str], headers: list[str], column: str | int | None) -> str:
        if column is None:
            return ""
        if isinstance(column, int) or str(column).isdigit():
            return cells[int(column)]
        try:
            return cells[headers.index(str(column))]
        except ValueError as exc:
            raise AdapterError(f"column not found in table headers: {column}") from exc

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
        normalized = value.replace(" ", "").replace(",", ".")
        try:
            return Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None
