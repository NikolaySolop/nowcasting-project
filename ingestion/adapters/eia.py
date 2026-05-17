import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, ParsedObservation


class EiaAdapter(BaseAdapter):
    name = "eia"
    api_base_url = "https://api.eia.gov/v2"
    new_york_tz = ZoneInfo("America/New_York")
    central_tz = ZoneInfo("America/Chicago")

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no EIA scrape spec")

        extra = spec.extra or {}
        series_id = str(extra.get("eia_series_id") or "").strip()
        if not series_id:
            raise AdapterError("eia adapter requires scrape.extra.eia_series_id")

        api_key = self._api_key(context, extra)
        if not api_key:
            raise AdapterError("eia adapter requires API_EIA_KEY, EIA_API_KEY, INGESTION_EIA_API_KEY, or scrape.extra.api_key")

        url = str(extra.get("api_url") or f"{self.api_base_url}/seriesid/{series_id}")
        params = self._request_params(api_key, extra)
        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds) as client:
            try:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"EIA API HTTP {exc.response.status_code} for {series_id}: {exc.response.text[:300]}"
                ) from exc

        payload = response.json()
        self._raise_for_api_error(payload, series_id)

        rows = self._parse_api_rows(payload, extra)
        rows.sort(key=lambda item: item["observed_at"])
        if not rows:
            raise AdapterError(f"EIA API returned no rows for {series_id}")

        loaded_at = datetime.now(timezone.utc)
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = self._to_observations(context, rows, extra)
        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=(
                self._to_table_observations(context, observations, extra, loaded_at=loaded_at)
                if store_in_observations
                else []
            ),
            loaded_at=loaded_at,
            raw_payload={
                "url": url,
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
                "eia_series_id": series_id,
                "row_count": len(rows),
                "api_version": payload.get("apiVersion"),
            },
        )

    def _to_table_observations(
        self,
        context: FetchContext,
        observations: list[ParsedObservation],
        extra: dict[str, Any],
        *,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        frequency = str(extra.get("frequency") or extra.get("frequency_code") or "").lower()
        backfill_run = self._is_backfill_run(context, observations)
        table_observations: list[ObservationIn] = []
        for observation in observations:
            if observation.value_numeric is None:
                continue
            reference_date, reference_start, reference_end = self._reference_period(
                observation.period_start or observation.observed_at,
                frequency,
                extra,
            )
            published_at = self._table_published_at(
                reference_end,
                extra,
                loaded_at=loaded_at,
                backfill_run=backfill_run,
            )
            table_observations.append(
                ObservationIn(
                    series_code=observation.series_code,
                    source_code=observation.source_code,
                    reference_date=reference_date,
                    reference_start=reference_start,
                    reference_end=reference_end,
                    value=observation.value_numeric,
                    published_at=observation.publication_at or published_at,
                )
            )
        return table_observations

    def _table_published_at(
        self,
        release_after: datetime,
        extra: dict[str, Any],
        *,
        loaded_at: datetime,
        backfill_run: bool,
    ) -> datetime:
        mode = str(extra.get("backfill_published_at") or "").lower()
        if not backfill_run:
            return loaded_at
        if mode == "next_wednesday_1030_et":
            return self._next_wednesday_1030_et(release_after)
        if mode == "friday_1200_ct":
            return self._friday_1200_ct(release_after)
        return loaded_at

    @staticmethod
    def _is_backfill_run(context: FetchContext, observations: list[ParsedObservation]) -> bool:
        if not observations:
            return False

        latest_by_series = [
            context.latest_observed_at_by_series.get(observation.series_code)
            for observation in observations
        ]
        latest_by_series = [latest for latest in latest_by_series if latest is not None]
        if not latest_by_series:
            return True

        start_date = context.source.scrape.start_date if context.source.scrape else None
        if start_date is None:
            return False
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        bootstrap_latest = start_date - timedelta(days=1)
        return all(latest <= bootstrap_latest for latest in latest_by_series)

    def _next_wednesday_1030_et(self, reference_start: datetime) -> datetime:
        reference_date = reference_start.astimezone(self.new_york_tz).date()
        days_until_wednesday = (2 - reference_date.weekday()) % 7
        if days_until_wednesday == 0:
            days_until_wednesday = 7
        release_date = reference_date + timedelta(days=days_until_wednesday)
        return datetime(
            release_date.year,
            release_date.month,
            release_date.day,
            10,
            30,
            tzinfo=self.new_york_tz,
        )

    def _friday_1200_ct(self, reference_start: datetime) -> datetime:
        reference_date = reference_start.astimezone(self.central_tz).date()
        days_until_friday = (4 - reference_date.weekday()) % 7
        release_date = reference_date + timedelta(days=days_until_friday)
        return datetime(
            release_date.year,
            release_date.month,
            release_date.day,
            12,
            0,
            tzinfo=self.central_tz,
        )

    @staticmethod
    def _reference_period(reference_at: datetime, frequency: str, extra: dict[str, Any]):
        report_week = str(extra.get("report_week") or "").lower()
        period_date_role = str(extra.get("period_date_role") or "").lower()
        if (
            frequency in {"weekly", "week", "w"}
            and report_week == "saturday_friday"
            and period_date_role == "week_end"
        ):
            reference_end = reference_at.replace(hour=23, minute=59, second=59, microsecond=999999)
            reference_start = (reference_end - timedelta(days=6)).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            return reference_end.date(), reference_start, reference_end

        if (
            frequency in {"weekly", "week", "w"}
            and report_week == "monday_friday"
            and period_date_role == "week_end"
        ):
            reference_end = reference_at.replace(hour=23, minute=59, second=59, microsecond=999999)
            reference_start = (reference_end - timedelta(days=4)).replace(
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
            return reference_end.date(), reference_start, reference_end

        reference_start = reference_at
        reference_end = EiaAdapter._reference_end(reference_start, frequency)
        return reference_start.date(), reference_start, reference_end

    @staticmethod
    def _reference_end(reference_start: datetime, frequency: str) -> datetime:
        if frequency in {"weekly", "week", "w"}:
            return reference_start + timedelta(days=7) - timedelta(microseconds=1)
        if frequency in {"monthly", "month", "m"}:
            month = reference_start.month + 1
            year = reference_start.year
            if month == 13:
                month = 1
                year += 1
            return datetime(
                year,
                month,
                1,
                tzinfo=reference_start.tzinfo,
            ) - timedelta(microseconds=1)
        if frequency in {"annual", "yearly", "year", "a", "y"}:
            return datetime(
                reference_start.year + 1,
                1,
                1,
                tzinfo=reference_start.tzinfo,
            ) - timedelta(microseconds=1)
        return reference_start + timedelta(days=1) - timedelta(microseconds=1)

    def _request_params(self, api_key: str, extra: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {"api_key": api_key}
        for key in ("start", "end", "offset", "length"):
            value = extra.get(key)
            if value not in (None, ""):
                params[key] = value
        return params

    def _to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        extra: dict[str, Any],
    ) -> list[ParsedObservation]:
        spec = context.source.scrape
        if spec is None:
            return []

        target_series_code = spec.series_code or context.source.source_code
        latest_observed_at = context.latest_observed_at_by_series.get(target_series_code)
        transform = str(extra.get("transform", "level")).lower()
        scale = self._parse_decimal(str(extra.get("scale", "1"))) or Decimal("1")

        observations: list[ParsedObservation] = []
        previous_value: Decimal | None = None
        for row in rows:
            observed_at = row["observed_at"]
            value = row["value"]
            value_numeric = value
            if transform in {"diff", "change", "period_diff"}:
                if previous_value is None:
                    previous_value = value
                    continue
                value_numeric = value - previous_value
            previous_value = value

            if latest_observed_at is not None and observed_at <= latest_observed_at:
                continue

            observations.append(
                ParsedObservation(
                    series_code=target_series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=observed_at,
                    value_numeric=value_numeric * scale,
                    kind=ObservationKind.MACRO,
                    raw_payload={
                        "eia_series_id": extra.get("eia_series_id"),
                        "source_value": str(value),
                        "transform": transform,
                    },
                )
            )
        return observations

    def _parse_api_rows(self, payload: Any, extra: dict[str, Any]) -> list[dict[str, Any]]:
        rows = self._extract_rows(payload)
        parsed: list[dict[str, Any]] = []
        for row in rows:
            period, raw_value, raw_payload = self._row_period_and_value(row, extra)
            if period is None or raw_value is None:
                continue
            observed_at = self._parse_period(period)
            value = self._parse_decimal(str(raw_value))
            if observed_at is None or value is None:
                continue
            parsed.append(
                {
                    "observed_at": observed_at,
                    "value": value,
                    "raw_payload": raw_payload,
                }
            )
        return parsed

    def _extract_rows(self, payload: Any) -> list[Any]:
        if isinstance(payload, dict):
            response = payload.get("response")
            if isinstance(response, dict) and isinstance(response.get("data"), list):
                return response["data"]
            if isinstance(payload.get("data"), list):
                return payload["data"]
            series = payload.get("series")
            if isinstance(series, list) and series:
                first_series = series[0]
                if isinstance(first_series, dict) and isinstance(first_series.get("data"), list):
                    return first_series["data"]
        if isinstance(payload, list):
            return payload
        return []

    def _row_period_and_value(self, row: Any, extra: dict[str, Any]) -> tuple[str | None, Any | None, dict[str, Any] | None]:
        period_column = str(extra.get("period_column") or "period")
        value_column = str(extra.get("value_column") or "value")

        if isinstance(row, dict):
            period = row.get(period_column) or row.get("date") or row.get("period")
            raw_value = row.get(value_column)
            if raw_value is None:
                raw_value = self._first_numeric_value(row, excluded={period_column, "date", "period"})
            return str(period) if period is not None else None, raw_value, row

        if isinstance(row, (list, tuple)) and len(row) >= 2:
            return str(row[0]), row[1], {"row": list(row)}

        return None, None, None

    @staticmethod
    def _first_numeric_value(row: dict[str, Any], *, excluded: set[str]) -> Any | None:
        for key, value in row.items():
            if key in excluded:
                continue
            if EiaAdapter._parse_decimal(str(value)) is not None:
                return value
        return None

    @staticmethod
    def _parse_period(value: str) -> datetime | None:
        raw = value.strip()
        formats = (
            "%Y-%m-%d",
            "%Y%m%d",
            "%Y-%m",
            "%Y%m",
            "%Y",
        )
        for date_format in formats:
            try:
                parsed = datetime.strptime(raw, date_format)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _parse_decimal(value: str) -> Decimal | None:
        normalized = value.replace(",", "").replace(" ", "").strip()
        if not normalized or normalized in {"-", "--", "NA", "W", "None"}:
            return None
        try:
            return Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _api_key(context: FetchContext, extra: dict[str, Any]) -> str | None:
        return (
            extra.get("api_key")
            or getattr(context.settings, "eia_api_key", None)
            or os.getenv("API_EIA_KEY")
            or os.getenv("EIA_API_KEY")
            or os.getenv("INGESTION_EIA_API_KEY")
        )

    @staticmethod
    def _raise_for_api_error(payload: Any, series_id: str) -> None:
        if not isinstance(payload, dict) or "error" not in payload:
            return
        error = payload["error"]
        if isinstance(error, dict):
            code = error.get("code") or "EIA_API_ERROR"
            message = error.get("message") or error
        else:
            code = "EIA_API_ERROR"
            message = error
        raise AdapterError(f"EIA API error for {series_id}: {code}: {message}")
