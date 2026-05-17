import os
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, RawObservationIn


class FredAdapter(BaseAdapter):
    name = "fred"
    api_base_url = "https://api.stlouisfed.org/fred/series/observations"
    dubai_tz = ZoneInfo("Asia/Dubai")
    new_york_tz = ZoneInfo("America/New_York")

    async def fetch(self, context: FetchContext) -> FetchResult:
        url, response, payload, rows, fred_series_id = await self._fetch_rows(context)
        observations = self._to_observations(context, rows, fred_series_id)
        return FetchResult(
            observations=observations,
            raw_payload=self._raw_payload(url, response, payload, fred_series_id, rows),
        )

    async def _fetch_rows(
        self,
        context: FetchContext,
    ) -> tuple[str, httpx.Response, Any, list[dict[str, Any]], str]:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no FRED scrape spec")

        extra = spec.extra or {}
        fred_series_id = str(extra.get("fred_series_id") or "").strip()
        if not fred_series_id:
            raise AdapterError("fred adapter requires scrape.extra.fred_series_id")

        api_key = self._api_key(context, extra)
        if not api_key:
            raise AdapterError("fred adapter requires API_FRED_KEY, FRED_API_KEY, INGESTION_FRED_API_KEY, or scrape.extra.api_key")

        url = str(spec.url or extra.get("api_url") or self.api_base_url)
        params = self._request_params(context, fred_series_id, api_key, extra)
        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds) as client:
            try:
                response = await client.get(url, headers=headers, params=params)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"FRED API HTTP {exc.response.status_code} for {fred_series_id}: {exc.response.text[:300]}"
                ) from exc

        payload = response.json()
        self._raise_for_api_error(payload, fred_series_id)

        rows = self._parse_api_rows(payload)
        if not rows:
            raise AdapterError(f"FRED API returned no rows for {fred_series_id}")

        return url, response, payload, rows, fred_series_id

    @staticmethod
    def _raw_payload(
        url: str,
        response: httpx.Response,
        payload: Any,
        fred_series_id: str,
        rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "url": url,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "fred_series_id": fred_series_id,
            "row_count": len(rows),
            "count": payload.get("count") if isinstance(payload, dict) else None,
        }

    def _request_params(
        self,
        context: FetchContext,
        fred_series_id: str,
        api_key: str,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "series_id": fred_series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "asc",
        }
        for key in ("realtime_start", "realtime_end", "observation_end", "limit", "offset", "units", "frequency", "aggregation_method"):
            value = extra.get(key)
            if value not in (None, ""):
                params[key] = value

        observation_start = extra.get("observation_start") or self._observation_start(context)
        if observation_start:
            params["observation_start"] = observation_start
        return params

    def _observation_start(self, context: FetchContext) -> str | None:
        spec = context.source.scrape
        if spec is None:
            return None

        target_series_code = spec.series_code or context.source.source_code
        latest_observed_at = context.latest_observed_at_by_series.get(target_series_code)
        if latest_observed_at is not None:
            return latest_observed_at.date().isoformat()
        if spec.start_date is not None:
            return spec.start_date.date().isoformat()
        return None

    def _to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        fred_series_id: str,
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        target_series_code = spec.series_code or context.source.source_code
        latest_observed_at = context.latest_observed_at_by_series.get(target_series_code)

        observations: list[RawObservationIn] = []
        for row in rows:
            observed_at = row["observed_at"]
            if latest_observed_at is not None and observed_at <= latest_observed_at:
                continue

            observations.append(
                RawObservationIn(
                    series_code=target_series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=observed_at,
                    value_numeric=row["value"],
                    kind=ObservationKind.MACRO,
                    raw_payload={
                        "fred_series_id": fred_series_id,
                        "source_value": str(row["value"]),
                        "realtime_start": row["raw_payload"].get("realtime_start"),
                        "realtime_end": row["raw_payload"].get("realtime_end"),
                    },
                )
            )
        return observations

    def _parse_api_rows(self, payload: Any) -> list[dict[str, Any]]:
        observations = payload.get("observations") if isinstance(payload, dict) else None
        if not isinstance(observations, list):
            return []

        parsed: list[dict[str, Any]] = []
        for row in observations:
            if not isinstance(row, dict):
                continue
            observed_at = self._parse_date(str(row.get("date") or ""))
            value = self._parse_decimal(str(row.get("value") or ""))
            if observed_at is None or value is None:
                continue
            parsed.append(
                {
                    "observed_at": observed_at,
                    "value": value,
                    "raw_payload": row,
                }
            )
        return parsed

    @staticmethod
    def _parse_date(value: str) -> datetime | None:
        try:
            parsed = datetime.strptime(value.strip(), "%Y-%m-%d")
        except ValueError:
            return None
        return parsed.replace(tzinfo=timezone.utc)

    @staticmethod
    def _parse_decimal(value: str) -> Decimal | None:
        normalized = value.replace(",", "").replace(" ", "").strip()
        if not normalized or normalized in {".", "-", "--", "NA", "NaN", "None"}:
            return None
        try:
            return Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _api_key(context: FetchContext, extra: dict[str, Any]) -> str | None:
        return (
            extra.get("api_key")
            or getattr(context.settings, "fred_api_key", None)
            or os.getenv("API_FRED_KEY")
            or os.getenv("FRED_API_KEY")
            or os.getenv("INGESTION_FRED_API_KEY")
        )

    @staticmethod
    def _raise_for_api_error(payload: Any, fred_series_id: str) -> None:
        if not isinstance(payload, dict) or "error_code" not in payload:
            return
        code = payload.get("error_code") or "FRED_API_ERROR"
        message = payload.get("error_message") or payload
        raise AdapterError(f"FRED API error for {fred_series_id}: {code}: {message}")


class FredObservationsAdapter(FredAdapter):
    name = "fred_observations"

    async def fetch(self, context: FetchContext) -> FetchResult:
        url, response, payload, rows, fred_series_id = await self._fetch_rows(context)
        observations = self._to_table_observations(context, rows)
        return FetchResult(
            table_observations=observations,
            raw_payload=self._raw_payload(url, response, payload, fred_series_id, rows),
        )

    def _to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        target_series_code = spec.series_code or context.source.source_code
        latest_reference_start = context.latest_observed_at_by_series.get(target_series_code)

        observations: list[ObservationIn] = []
        for row in rows:
            reference_start = row["observed_at"]
            if latest_reference_start is not None and reference_start <= latest_reference_start:
                continue

            observations.append(
                ObservationIn(
                    series_code=target_series_code,
                    source_code=context.source.source_code,
                    reference_start=reference_start,
                    reference_end=self._reference_end(reference_start),
                    value=row["value"],
                    published_at=self._published_at(reference_start),
                )
            )
        return observations

    @staticmethod
    def _reference_end(reference_start: datetime) -> datetime:
        return reference_start + timedelta(days=1) - timedelta(microseconds=1)

    def _published_at(self, reference_start: datetime) -> datetime:
        next_date = reference_start.astimezone(timezone.utc).date() + timedelta(days=1)
        published_in_new_york = datetime.combine(
            next_date,
            time(hour=8),
            tzinfo=self.new_york_tz,
        )
        return published_in_new_york.astimezone(self.dubai_tz)


class FredSofrAdapter(FredObservationsAdapter):
    name = "fred_sofr"
