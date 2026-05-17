from __future__ import annotations

import html
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, RawObservationIn


class CbrSecInfoAdapter(BaseAdapter):
    name = "cbr_secinfo"

    endpoint_url = "https://www.cbr.ru/secinfo/secinfo.asmx"
    namespace = "http://web.cbr.ru/"
    soap_namespace = "http://schemas.xmlsoap.org/soap/envelope/"

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR SecInfo scrape spec")

        extra = spec.extra or {}
        operation = str(extra.get("operation") or "").strip()
        if not operation:
            raise AdapterError("CBR SecInfo adapter requires scrape.extra.operation")

        endpoint = str(spec.url or extra.get("endpoint") or self.endpoint_url)
        headers = {
            "User-Agent": context.settings.request_user_agent,
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"\"{extra.get('soap_action') or self.namespace + operation}\"",
        }
        headers.update(spec.headers)

        window_days = int(extra.get("max_range_days") or extra.get("window_days") or 0)
        start_datetime = self._start_datetime(context)
        end_datetime = self._end_datetime(context)
        if window_days > 0 and start_datetime.date() > end_datetime.date():
            return FetchResult(
                observations=[],
                raw_payload={
                    "url": endpoint,
                    "operation": operation,
                    "row_count": 0,
                    "observation_count": 0,
                    "requests": [],
                },
            )

        store_in_observations = bool(extra.get("store_in_observations", False))
        observations: list[RawObservationIn] = []
        table_observations: list[ObservationIn] = []
        loaded_at = datetime.now(timezone.utc)
        request_summaries: list[dict[str, Any]] = []
        row_count = 0
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            windows = (
                self._date_windows(start_datetime, end_datetime, window_days)
                if window_days > 0
                else [(start_datetime, end_datetime)]
            )
            for window_start, window_end in windows:
                params = self._request_params(
                    context,
                    start_datetime=window_start,
                    end_datetime=window_end,
                )
                window_observations, window_table_observations, summary = await self._fetch_window(
                    client=client,
                    context=context,
                    endpoint=endpoint,
                    operation=operation,
                    headers=headers,
                    params=params,
                    loaded_at=loaded_at,
                    store_in_observations=store_in_observations,
                )
                observations.extend(window_observations)
                table_observations.extend(window_table_observations)
                row_count += int(summary["row_count"])
                request_summaries.append(summary)

        total_observation_count = len(table_observations) if store_in_observations else len(observations)
        if total_observation_count == 0 and bool(extra.get("raise_on_empty", False)):
            raise AdapterError(f"CBR SecInfo {operation} returned no observations")

        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "url": endpoint,
                "operation": operation,
                "row_count": row_count,
                "observation_count": total_observation_count,
                "requests": request_summaries,
            },
        )

    async def _fetch_window(
        self,
        *,
        client: httpx.AsyncClient,
        context: FetchContext,
        endpoint: str,
        operation: str,
        headers: dict[str, str],
        params: dict[str, str],
        loaded_at: datetime,
        store_in_observations: bool,
    ) -> tuple[list[RawObservationIn], list[ObservationIn], dict[str, Any]]:
        spec = context.source.scrape
        if spec is None:
            return [], [], {}

        envelope = self._soap_envelope(operation, params)
        try:
            response = await client.post(endpoint, headers=headers, content=envelope.encode("utf-8"))
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise AdapterError(
                f"CBR SecInfo HTTP {exc.response.status_code} for {operation}: {exc.response.text[:300]}"
            ) from exc
        except httpx.RequestError as exc:
            raise AdapterError(f"CBR SecInfo request failed for {operation}: {type(exc).__name__}: {exc!r}") from exc

        result = self._extract_result(response.text, operation, str((spec.extra or {}).get("result_tag") or ""))
        rows = self._extract_rows(result, str((spec.extra or {}).get("row_tag") or ""))
        observations = [] if store_in_observations else self._rows_to_observations(context, rows)
        table_observations = (
            self._rows_to_table_observations(context, rows, loaded_at=loaded_at)
            if store_in_observations
            else []
        )
        return observations, table_observations, {
            "params": params,
            "status_code": response.status_code,
            "content_type": response.headers.get("content-type"),
            "row_count": len(rows),
            "observation_count": len(table_observations) if store_in_observations else len(observations),
        }

    def _request_params(
        self,
        context: FetchContext,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
    ) -> dict[str, str]:
        spec = context.source.scrape
        if spec is None:
            return {}

        extra = spec.extra or {}
        raw_params = extra.get("params")
        if isinstance(raw_params, dict):
            return {
                str(key): self._resolve_param_value(
                    value,
                    context,
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
                for key, value in raw_params.items()
            }

        if bool(extra.get("no_params", False)):
            return {}

        from_param = str(extra.get("date_from_param") or "DateFrom")
        to_param = str(extra.get("date_to_param") or "DateTo")
        return {
            from_param: self._format_soap_datetime(start_datetime or self._start_datetime(context)),
            to_param: self._format_soap_datetime(end_datetime or self._end_datetime(context)),
        }

    def _resolve_param_value(
        self,
        value: Any,
        context: FetchContext,
        *,
        start_datetime: datetime | None = None,
        end_datetime: datetime | None = None,
    ) -> str:
        if isinstance(value, str):
            marker = value.strip().lower()
            if marker in {"$start_date", "{start_date}"}:
                return self._format_soap_datetime(start_datetime or self._start_datetime(context))
            if marker in {"$end_date", "{end_date}", "$today", "{today}"}:
                return self._format_soap_datetime(end_datetime or self._end_datetime(context))
        if isinstance(value, datetime):
            return self._format_soap_datetime(value)
        return str(value)

    def _start_datetime(self, context: FetchContext) -> datetime:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        date_mode = str(extra.get("date_mode") or "calendar").lower()
        interval_days = int(extra.get("interval_days") or 1)
        latest_values = [
            observed_at for observed_at in context.latest_observed_at_by_series.values() if observed_at is not None
        ]
        if latest_values:
            return self._calendar_midnight(min(latest_values) + timedelta(days=interval_days), date_mode)
        if spec is not None and spec.start_date is not None:
            return self._calendar_midnight(self._ensure_utc(spec.start_date), date_mode)
        return self._calendar_midnight(
            datetime.now(timezone.utc) - timedelta(days=int(extra.get("lookback_days") or 7)),
            date_mode,
        )

    def _end_datetime(self, context: FetchContext) -> datetime:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        explicit = self._parse_datetime(extra.get("end_date") or extra.get("to"))
        return explicit or datetime.now(timezone.utc)

    @staticmethod
    def _date_windows(
        start_datetime: datetime,
        end_datetime: datetime,
        window_days: int,
    ) -> list[tuple[datetime, datetime]]:
        if window_days <= 0:
            return [(start_datetime, end_datetime)]

        windows: list[tuple[datetime, datetime]] = []
        current = start_datetime
        while current.date() <= end_datetime.date():
            window_end = min(current + timedelta(days=window_days - 1), end_datetime)
            windows.append((current, window_end))
            current = CbrSecInfoAdapter._calendar_midnight(window_end + timedelta(days=1), "calendar")
        return windows

    def _soap_envelope(self, operation: str, params: dict[str, str]) -> str:
        params_xml = "".join(f"<{name}>{html.escape(value)}</{name}>" for name, value in params.items())
        return (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
            'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
            f'xmlns:soap="{self.soap_namespace}">'
            "<soap:Body>"
            f'<{operation} xmlns="{self.namespace}">{params_xml}</{operation}>'
            "</soap:Body>"
            "</soap:Envelope>"
        )

    def _extract_result(self, response_text: str, operation: str, result_tag: str) -> ET.Element:
        try:
            root = ET.fromstring(response_text)
        except ET.ParseError as exc:
            raise AdapterError(f"CBR SecInfo returned invalid XML: {exc}") from exc

        fault = self._first_descendant(root, "Fault")
        if fault is not None:
            fault_text = "".join(fault.itertext()).strip()
            raise AdapterError(f"CBR SecInfo SOAP fault: {fault_text}")

        result = self._first_descendant(root, result_tag or f"{operation}Result")
        if result is None:
            raise AdapterError(f"CBR SecInfo response has no {operation}Result")
        return result

    def _extract_rows(self, result: ET.Element, row_tag: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        candidates = result.iter() if row_tag else self._default_row_candidates(result)
        for element in candidates:
            if element is result:
                continue
            if row_tag and self._local_name(element.tag) != row_tag:
                continue
            row = self._element_to_row(element)
            if row:
                rows.append(row)
        return rows

    def _default_row_candidates(self, result: ET.Element) -> list[ET.Element]:
        return [
            element
            for element in result.iter()
            if element is not result and list(element) and all(not list(child) for child in element)
        ]

    def _element_to_row(self, element: ET.Element) -> dict[str, Any]:
        row: dict[str, Any] = {f"@{self._local_name(key)}": value for key, value in element.attrib.items()}
        for child in list(element):
            key = self._local_name(child.tag)
            value = child.text.strip() if child.text else ""
            if key in row:
                suffix = 2
                while f"{key}_{suffix}" in row:
                    suffix += 1
                key = f"{key}_{suffix}"
            row[key] = value
        if not row and element.text and element.text.strip():
            row[self._local_name(element.tag)] = element.text.strip()
        return row

    def _rows_to_observations(self, context: FetchContext, rows: list[dict[str, Any]]) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        metrics = self._metrics(context)
        observations: list[RawObservationIn] = []
        date_mode = str((spec.extra or {}).get("date_mode") or "calendar").lower()
        for row in rows:
            for metric in metrics:
                observed_at = self._parse_observed_at(
                    self._row_value(row, metric.get("date_column") or spec.date_column),
                    date_mode,
                )
                if observed_at is None:
                    continue

                series_code = str(metric["series_code"])
                latest = context.latest_observed_at_by_series.get(series_code)
                if latest is not None and observed_at <= latest:
                    continue

                raw_value = self._row_value(row, metric.get("value_column") or spec.value_column)
                value_numeric = self._parse_decimal(raw_value)
                scale = self._parse_decimal(metric.get("scale"))
                if value_numeric is not None and scale is not None:
                    value_numeric *= scale

                value_text = None if value_numeric is not None else raw_value
                if value_numeric is None and not value_text:
                    continue

                observations.append(
                    RawObservationIn(
                        series_code=series_code,
                        source_code=context.source.source_code,
                        observed_at=observed_at,
                        period_start=self._parse_datetime(self._row_value(row, metric.get("period_start_column"))),
                        period_end=self._parse_datetime(self._row_value(row, metric.get("period_end_column"))),
                        publication_at=self._parse_datetime(self._row_value(row, metric.get("publication_column"))),
                        value_numeric=value_numeric,
                        value_text=value_text,
                        kind=self._observation_kind(metric.get("kind") or (spec.extra or {}).get("kind")),
                        raw_payload={
                            "operation": (spec.extra or {}).get("operation"),
                            "row": row,
                            "value_column": metric.get("value_column") or spec.value_column,
                        },
                    )
                )

        observations.sort(key=lambda item: (item.observed_at, item.series_code))
        return observations

    def _rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        *,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        metrics = self._metrics(context)
        observations: list[ObservationIn] = []
        date_mode = str((spec.extra or {}).get("date_mode") or "calendar").lower()
        for row in rows:
            for metric in metrics:
                reference_at = self._parse_observed_at(
                    self._row_value(row, metric.get("date_column") or spec.date_column),
                    date_mode,
                )
                if reference_at is None:
                    continue

                series_code = str(metric["series_code"])
                latest = context.latest_observed_at_by_series.get(series_code)
                if latest is not None and reference_at <= latest:
                    continue

                raw_value = self._row_value(row, metric.get("value_column") or spec.value_column)
                numeric_value = self._parse_decimal(raw_value)
                scale = self._parse_decimal(metric.get("scale"))
                if numeric_value is not None and scale is not None:
                    numeric_value *= scale
                if numeric_value is None:
                    continue

                reference_start = self._parse_datetime(self._row_value(row, metric.get("period_start_column")))
                reference_end = self._parse_datetime(self._row_value(row, metric.get("period_end_column")))
                published_at = self._table_published_at(
                    metric,
                    row,
                    reference_at,
                    loaded_at,
                    spec.extra or {},
                    latest is not None,
                )

                observations.append(
                    ObservationIn(
                        series_code=series_code,
                        source_code=context.source.source_code,
                        reference_date=reference_at.date(),
                        reference_start=reference_start or reference_at,
                        reference_end=reference_end or reference_at,
                        value=numeric_value,
                        published_at=published_at,
                        skip_equal_to_previous=bool(metric.get("skip_equal_to_previous", False)),
                    )
                )

        observations.sort(key=lambda item: (item.reference_start, item.series_code))
        return observations

    def _table_published_at(
        self,
        metric: dict[str, Any],
        row: dict[str, Any],
        reference_at: datetime,
        loaded_at: datetime,
        extra: dict[str, Any],
        has_existing_data: bool,
    ) -> datetime:
        explicit = self._parse_datetime(self._row_value(row, metric.get("publication_column")))
        if explicit is not None:
            return explicit
        publish_tz_name = str(extra.get("backfill_published_timezone") or "UTC")
        publish_time = self._parse_time_config(str(extra.get("backfill_published_time") or "00:00"))
        try:
            publish_tz = timezone.utc if publish_tz_name.upper() == "UTC" else ZoneInfo(publish_tz_name)
        except Exception as exc:
            raise AdapterError(f"invalid SecInfo published timezone: {publish_tz_name}") from exc
        scheduled = datetime.combine(reference_at.astimezone(publish_tz).date(), publish_time, tzinfo=publish_tz)
        if (
            bool(extra.get("live_published_at_loaded_at", False))
            and has_existing_data
            and scheduled.astimezone(publish_tz).date() >= loaded_at.astimezone(publish_tz).date()
        ):
            return loaded_at
        return scheduled

    def _metrics(self, context: FetchContext) -> list[dict[str, Any]]:
        spec = context.source.scrape
        if spec is None:
            return []

        extra = spec.extra or {}
        raw_metrics = extra.get("series")
        if isinstance(raw_metrics, list) and raw_metrics:
            metrics = [dict(item) for item in raw_metrics if isinstance(item, dict)]
        else:
            value_columns = extra.get("value_columns")
            if isinstance(value_columns, dict):
                metrics = [
                    {"value_column": column, "series_code": series_code}
                    for column, series_code in value_columns.items()
                ]
            else:
                metrics = [
                    {
                        "value_column": extra.get("value_column") or spec.value_column,
                        "series_code": spec.series_code
                        or extra.get("series_code")
                        or (context.source.series[0].series_code if context.source.series else context.source.source_code),
                    }
                ]

        default_date_column = extra.get("date_column") or spec.date_column
        for metric in metrics:
            metric.setdefault("date_column", default_date_column)
            if not metric.get("series_code"):
                raise AdapterError(f"CBR SecInfo metric requires series_code: {metric}")
            if not metric.get("value_column"):
                raise AdapterError(f"CBR SecInfo metric requires value_column: {metric}")
        return metrics

    @staticmethod
    def _row_value(row: dict[str, Any], key: Any) -> str | None:
        if key is None:
            return None
        if isinstance(key, int) or str(key).isdigit():
            values = list(row.values())
            index = int(key)
            return str(values[index]) if 0 <= index < len(values) else None
        value = row.get(str(key))
        return str(value) if value is not None else None

    @staticmethod
    def _parse_observed_at(value: Any, date_mode: str) -> datetime | None:
        if value is None or value == "":
            return None
        if date_mode in {"timestamp", "instant", "preserve"}:
            return CbrSecInfoAdapter._parse_datetime(value)

        raw = str(value).strip()
        date_part = raw.split("T", 1)[0].split(" ", 1)[0]
        for date_format in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(date_part, date_format).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return CbrSecInfoAdapter._parse_datetime(value)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return CbrSecInfoAdapter._ensure_utc(value)

        raw = str(value).strip()
        for date_format in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d.%m.%Y",
        ):
            try:
                parsed = datetime.strptime(raw.replace("Z", "+0000"), date_format)
                return CbrSecInfoAdapter._ensure_utc(parsed)
            except ValueError:
                continue
        try:
            return CbrSecInfoAdapter._ensure_utc(datetime.fromisoformat(raw))
        except ValueError:
            return None

    @staticmethod
    def _parse_time_config(raw_value: str) -> time:
        try:
            hour, minute = raw_value.split(":", maxsplit=1)
            return time(hour=int(hour), minute=int(minute))
        except ValueError as exc:
            raise AdapterError(f"invalid SecInfo time value: {raw_value}") from exc

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _format_soap_datetime(value: datetime) -> str:
        return CbrSecInfoAdapter._ensure_utc(value).strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _calendar_midnight(value: datetime, date_mode: str) -> datetime:
        value = CbrSecInfoAdapter._ensure_utc(value)
        if date_mode in {"timestamp", "instant", "preserve"}:
            return value
        return datetime.combine(value.date(), datetime.min.time(), tzinfo=timezone.utc)

    @staticmethod
    def _parse_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        normalized = str(value).strip().replace(" ", "").replace(",", ".")
        if not normalized or normalized in {"-", "--", "None", "null"}:
            return None
        try:
            return Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _observation_kind(value: Any) -> ObservationKind:
        if value is None:
            return ObservationKind.MACRO
        try:
            return ObservationKind(str(value).lower())
        except ValueError:
            return ObservationKind.MACRO

    @staticmethod
    def _first_descendant(root: ET.Element, local_name: str) -> ET.Element | None:
        if not local_name:
            return None
        for element in root.iter():
            if CbrSecInfoAdapter._local_name(element.tag) == local_name:
                return element
        return None

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag
