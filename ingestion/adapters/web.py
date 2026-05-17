from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, RawObservationIn


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

        store_in_observations = bool((spec.extra or {}).get("store_in_observations", False))
        if spec.parser == "html_table":
            observations = self._parse_html_table(context, response.text)
            table_observations: list[ObservationIn] = []
        elif spec.parser == "json":
            if store_in_observations:
                observations = []
                table_observations = self._parse_json_table_observations(context, response.json())
            else:
                observations = self._parse_json(context, response.json())
                table_observations = []
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
            table_observations = []

        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=table_observations,
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

    def _parse_json_table_observations(self, context: FetchContext, payload: Any) -> list[ObservationIn]:
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
        pub_col = extra.get("publication_at_column")

        observations: list[ObservationIn] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            observed_at = self._parse_date(str(row[spec.date_column]), spec.date_format)
            if start_dt and observed_at < start_dt:
                continue
            if latest and observed_at <= latest:
                continue
            value_numeric = self._parse_decimal(str(row.get(spec.value_column)))
            if value_numeric is None:
                continue
            if pub_col and row.get(pub_col):
                published_at = self._parse_date(str(row[pub_col]), None)
            else:
                published_at = datetime.now(timezone.utc)

            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    reference_date=observed_at.date(),
                    reference_start=observed_at,
                    reference_end=self._month_end(observed_at),
                    value=value_numeric,
                    published_at=published_at,
                )
            )
        return observations

    @staticmethod
    def _month_end(value: datetime) -> datetime:
        if value.month == 12:
            next_month = value.replace(year=value.year + 1, month=1, day=1)
        else:
            next_month = value.replace(month=value.month + 1, day=1)
        return next_month - datetime.resolution

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
