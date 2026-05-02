import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class EiaAdapter(BaseAdapter):
    name = "eia"
    api_base_url = "https://api.eia.gov/v2"

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

        observations = self._to_observations(context, rows, extra)
        return FetchResult(
            observations=observations,
            raw_payload={
                "url": url,
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type"),
                "eia_series_id": series_id,
                "row_count": len(rows),
                "api_version": payload.get("apiVersion"),
            },
        )

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
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        target_series_code = spec.series_code or context.source.source_code
        latest_observed_at = context.latest_observed_at_by_series.get(target_series_code)
        transform = str(extra.get("transform", "level")).lower()
        scale = self._parse_decimal(str(extra.get("scale", "1"))) or Decimal("1")

        observations: list[RawObservationIn] = []
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
                RawObservationIn(
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
