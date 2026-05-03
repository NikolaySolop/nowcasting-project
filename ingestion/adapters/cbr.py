from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class CbrAdapter(BaseAdapter):
    name = "cbr"

    daily_url = "https://www.cbr.ru/scripts/XML_daily.asp"
    dynamic_url = "https://www.cbr.ru/scripts/XML_dynamic.asp"
    key_rate_url = "https://www.cbr.ru/hd_base/KeyRate/"
    ruonia_dynamics_url = "https://www.cbr.ru/hd_base/ruonia/dynamics/"
    currency_ids = {
        "USD": "R01235",
        "EUR": "R01239",
        "CNY": "R01375",
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        mode = str((spec.extra or {}).get("mode") or "latest_daily").lower()
        if mode in {"ruonia", "ruonia_dynamics", "ruonia_history"}:
            return await self._fetch_ruonia_dynamics(context)
        if mode in {"key_rate", "key_rate_history", "history_key_rate"}:
            return await self._fetch_key_rate_history(context)
        if mode in {"history", "history_daily", "dynamic"}:
            return await self._fetch_history_daily(context)
        if mode in {"latest", "latest_daily", "current", "daily"}:
            return await self._fetch_latest_daily(context)
        raise AdapterError(f"unsupported CBR mode: {mode}")

    async def _fetch_ruonia_dynamics(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        metrics = self._ruonia_metrics(context)
        date_from = self._ruonia_start_date(context, [metric["series_code"] for metric in metrics])
        date_to = self._end_date(extra)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                raw_payload={
                    "mode": "ruonia_dynamics",
                    "date_from": date_from.date().isoformat(),
                    "date_to": date_to.date().isoformat(),
                    "row_count": 0,
                    "observation_count": 0,
                },
            )

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        params = {
            "UniDbQuery.From": self._format_cbr_query_date(date_from),
            "UniDbQuery.To": self._format_cbr_query_date(date_to),
            "UniDbQuery.Posted": "True",
        }

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            response = await client.get(
                str(spec.url or extra.get("ruonia_dynamics_url") or self.ruonia_dynamics_url),
                headers=headers,
                params=params,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR RUONIA HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc

        rows = self._parse_ruonia_rows(response.text)
        observations = self._ruonia_rows_to_observations(context, rows, metrics)
        return FetchResult(
            observations=observations,
            raw_payload={
                "mode": "ruonia_dynamics",
                "url": str(response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(observations),
            },
        )

    async def _fetch_key_rate_history(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        series_code = self._series_code(context)
        date_from = self._start_date(context, series_code)
        date_to = self._end_date(extra)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                raw_payload={
                    "mode": "key_rate_history",
                    "date_from": date_from.date().isoformat(),
                    "date_to": date_to.date().isoformat(),
                    "row_count": 0,
                    "observation_count": 0,
                },
            )

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        params = {
            "UniDbQuery.From": self._format_cbr_query_date(date_from),
            "UniDbQuery.To": self._format_cbr_query_date(date_to),
            "UniDbQuery.Posted": "True",
        }

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            response = await client.get(
                str(spec.url or extra.get("key_rate_url") or self.key_rate_url),
                headers=headers,
                params=params,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR key rate HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc

        rows = self._parse_key_rate_rows(response.text)
        observations = self._key_rate_rows_to_observations(context, rows, series_code)
        return FetchResult(
            observations=observations,
            raw_payload={
                "mode": "key_rate_history",
                "url": str(response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(observations),
            },
        )

    async def _fetch_history_daily(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        currency_id = self._currency_id(extra)
        series_code = self._series_code(context)
        date_from = self._start_date(context, series_code)
        date_to = self._end_date(extra)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                raw_payload={
                    "mode": "history_daily",
                    "currency_id": currency_id,
                    "date_from": date_from.date().isoformat(),
                    "date_to": date_to.date().isoformat(),
                    "row_count": 0,
                    "observation_count": 0,
                },
            )

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        params = {
            "date_req1": self._format_cbr_date(date_from),
            "date_req2": self._format_cbr_date(date_to),
            "VAL_NM_RQ": currency_id,
        }

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            response = await client.get(
                str(spec.url or extra.get("dynamic_url") or self.dynamic_url),
                headers=headers,
                params=params,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR history HTTP {exc.response.status_code} for {currency_id}: {exc.response.text[:300]}"
                ) from exc

        rows = self._parse_dynamic_rows(response.content, currency_id)
        observations = self._rows_to_observations(context, rows, series_code)
        return FetchResult(
            observations=observations,
            raw_payload={
                "mode": "history_daily",
                "url": str(response.url),
                "currency_id": currency_id,
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(observations),
            },
        )

    async def _fetch_latest_daily(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        currency_id = self._currency_id(extra)
        series_code = self._series_code(context)
        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        params: dict[str, str] = {}
        date_req = self._parse_datetime(extra.get("date_req") or extra.get("date"))
        if date_req is not None:
            params["date_req"] = self._format_cbr_date(date_req)

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            response = await client.get(
                str(spec.url or extra.get("daily_url") or self.daily_url),
                headers=headers,
                params=params,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR daily HTTP {exc.response.status_code} for {currency_id}: {exc.response.text[:300]}"
                ) from exc

        row = self._parse_daily_row(response.content, currency_id, str(extra.get("char_code") or ""))
        observations = self._rows_to_observations(context, [row], series_code)
        return FetchResult(
            observations=observations,
            raw_payload={
                "mode": "latest_daily",
                "url": str(response.url),
                "currency_id": currency_id,
                "published_date": row["date"].date().isoformat(),
                "observation_count": len(observations),
            },
        )

    def _rows_to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        latest = context.latest_observed_at_by_series.get(series_code)
        observations: list[RawObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            observed_at = row["date"]
            if latest is not None and observed_at <= latest:
                continue
            rate = row["rate"]
            if bool((spec.extra or {}).get("invert", False)):
                if rate == 0:
                    raise AdapterError("cannot invert zero CBR rate")
                rate = Decimal("1") / rate

            observations.append(
                RawObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=observed_at,
                    value_numeric=rate,
                    kind=ObservationKind.QUOTE,
                    raw_payload={
                        "source": "cbr_xml",
                        "currency_id": row.get("currency_id"),
                        "char_code": row.get("char_code"),
                        "nominal": str(row.get("nominal")) if row.get("nominal") is not None else None,
                        "value": str(row.get("value")) if row.get("value") is not None else None,
                        "vunit_rate": str(row.get("vunit_rate")) if row.get("vunit_rate") is not None else None,
                        "inverted": bool((spec.extra or {}).get("invert", False)),
                    },
                )
            )
        return observations

    def _key_rate_rows_to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
    ) -> list[RawObservationIn]:
        latest = context.latest_observed_at_by_series.get(series_code)
        observations: list[RawObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            observed_at = row["date"]
            if latest is not None and observed_at <= latest:
                continue

            observations.append(
                RawObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=observed_at,
                    value_numeric=row["rate"],
                    kind=ObservationKind.MACRO,
                    raw_payload={
                        "source": "cbr_key_rate_html",
                        "unit": "percent_per_annum",
                        "value": str(row["rate"]),
                    },
                )
            )
        return observations

    def _ruonia_rows_to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        metrics: list[dict[str, Any]],
    ) -> list[RawObservationIn]:
        observations: list[RawObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            observed_at = row["date"]
            for metric in metrics:
                series_code = str(metric["series_code"])
                latest = context.latest_observed_at_by_series.get(series_code)
                if latest is not None and observed_at <= latest:
                    continue

                raw_value = self._ruonia_row_value(row, metric["value_column"])
                value_numeric = self._parse_decimal(raw_value)
                if value_numeric is None:
                    continue

                observations.append(
                    RawObservationIn(
                        series_code=series_code,
                        source_code=context.source.source_code,
                        observed_at=observed_at,
                        period_start=observed_at,
                        publication_at=row.get("publication_at"),
                        value_numeric=value_numeric,
                        kind=ObservationKind.MACRO,
                        raw_payload={
                            "source": "cbr_ruonia_dynamics_html",
                            "status": row.get("status"),
                            "value_column": metric["value_column"],
                            "cells": row.get("cells"),
                            "headers": row.get("headers"),
                        },
                    )
                )

        observations.sort(key=lambda item: (item.observed_at, item.series_code))
        return observations

    def _parse_dynamic_rows(self, content: bytes, currency_id: str) -> list[dict[str, Any]]:
        root = self._parse_xml(content)
        rows: list[dict[str, Any]] = []
        for record in root.findall("Record"):
            observed_at = self._parse_cbr_date(record.get("Date"))
            if observed_at is None:
                continue
            nominal = self._parse_decimal(record.findtext("Nominal")) or Decimal("1")
            value = self._parse_decimal(record.findtext("Value"))
            vunit_rate = self._parse_decimal(record.findtext("VunitRate"))
            rate = vunit_rate or (value / nominal if value is not None and nominal != 0 else None)
            if rate is None:
                continue
            rows.append(
                {
                    "date": observed_at,
                    "currency_id": record.get("Id") or currency_id,
                    "nominal": nominal,
                    "value": value,
                    "vunit_rate": vunit_rate,
                    "rate": rate,
                }
            )
        return rows

    def _parse_daily_row(self, content: bytes, currency_id: str, char_code: str) -> dict[str, Any]:
        root = self._parse_xml(content)
        observed_at = self._parse_cbr_date(root.get("Date"))
        if observed_at is None:
            raise AdapterError("CBR daily response has no ValCurs Date")

        valute = None
        for item in root.findall("Valute"):
            if item.get("ID") == currency_id or (char_code and item.findtext("CharCode") == char_code):
                valute = item
                break
        if valute is None:
            raise AdapterError(f"CBR daily response has no currency {currency_id or char_code}")

        nominal = self._parse_decimal(valute.findtext("Nominal")) or Decimal("1")
        value = self._parse_decimal(valute.findtext("Value"))
        vunit_rate = self._parse_decimal(valute.findtext("VunitRate"))
        rate = vunit_rate or (value / nominal if value is not None and nominal != 0 else None)
        if rate is None:
            raise AdapterError(f"CBR daily response has no rate for {currency_id}")

        return {
            "date": observed_at,
            "currency_id": valute.get("ID") or currency_id,
            "char_code": valute.findtext("CharCode"),
            "nominal": nominal,
            "value": value,
            "vunit_rate": vunit_rate,
            "rate": rate,
        }

    def _parse_key_rate_rows(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.data") or soup.select_one("table")
        if table is None:
            raise AdapterError("CBR key rate page has no data table")

        rows: list[dict[str, Any]] = []
        for table_row in table.select("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in table_row.select("td,th")]
            if len(cells) < 2 or cells[0].lower() == "дата":
                continue

            observed_at = self._parse_cbr_date(cells[0])
            rate = self._parse_decimal(cells[1])
            if observed_at is None or rate is None:
                continue
            rows.append({"date": observed_at, "rate": rate})

        if not rows:
            raise AdapterError("CBR key rate page has no parseable rows")
        return rows

    def _parse_ruonia_rows(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.data") or soup.select_one("table")
        if table is None:
            raise AdapterError("CBR RUONIA page has no data table")

        headers: list[str] = []
        rows: list[dict[str, Any]] = []
        for table_row in table.select("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in table_row.select("td,th")]
            if not cells:
                continue
            if table_row.select("th"):
                headers = cells
                continue
            if len(cells) < 9:
                continue

            observed_at = self._parse_cbr_date(cells[0])
            if observed_at is None:
                continue
            rows.append(
                {
                    "date": observed_at,
                    "publication_at": self._parse_cbr_date(cells[10]) if len(cells) > 10 else None,
                    "status": cells[9] if len(cells) > 9 else None,
                    "cells": cells,
                    "headers": headers,
                }
            )

        if not rows:
            raise AdapterError("CBR RUONIA page has no parseable rows")
        return rows

    @staticmethod
    def _parse_xml(content: bytes) -> ET.Element:
        try:
            return ET.fromstring(content)
        except ET.ParseError as exc:
            raise AdapterError(f"CBR returned invalid XML: {exc}") from exc

    def _currency_id(self, extra: dict[str, Any]) -> str:
        configured = str(extra.get("currency_id") or extra.get("val_nm_rq") or "").strip()
        if configured:
            return configured
        char_code = str(extra.get("char_code") or extra.get("currency") or "USD").upper()
        try:
            return self.currency_ids[char_code]
        except KeyError as exc:
            raise AdapterError(f"unknown CBR currency code: {char_code}") from exc

    @staticmethod
    def _series_code(context: FetchContext) -> str:
        spec = context.source.scrape
        if spec is None:
            return context.source.source_code
        return (
            spec.series_code
            or (spec.extra or {}).get("series_code")
            or (context.source.series[0].series_code if context.source.series else context.source.source_code)
        )

    def _ruonia_metrics(self, context: FetchContext) -> list[dict[str, Any]]:
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
                        "series_code": self._series_code(context),
                    }
                ]

        for metric in metrics:
            if not metric.get("series_code"):
                raise AdapterError(f"CBR RUONIA metric requires series_code: {metric}")
            if metric.get("value_column") is None:
                raise AdapterError(f"CBR RUONIA metric requires value_column: {metric}")
        return metrics

    def _ruonia_start_date(self, context: FetchContext, series_codes: list[str]) -> datetime:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        latest_values = [
            context.latest_observed_at_by_series[series_code]
            for series_code in series_codes
            if context.latest_observed_at_by_series.get(series_code) is not None
        ]
        if len(latest_values) == len(series_codes) and latest_values:
            return self._ensure_utc(min(latest_values)) + timedelta(days=1)
        if spec is not None and spec.start_date is not None:
            return self._ensure_utc(spec.start_date)
        return datetime.now(timezone.utc) - timedelta(days=int(extra.get("lookback_days") or 30))

    def _start_date(self, context: FetchContext, series_code: str) -> datetime:
        spec = context.source.scrape
        latest = context.latest_observed_at_by_series.get(series_code)
        if latest is not None:
            return self._ensure_utc(latest) + timedelta(days=1)
        if spec is not None and spec.start_date is not None:
            return self._ensure_utc(spec.start_date)
        return datetime.now(timezone.utc) - timedelta(days=int((spec.extra or {}).get("lookback_days") or 30))

    @staticmethod
    def _end_date(extra: dict[str, Any]) -> datetime:
        explicit = CbrAdapter._parse_datetime(extra.get("end_date") or extra.get("to"))
        offset_days = int(extra.get("end_date_offset_days") or 0)
        return (explicit or datetime.now(timezone.utc)) + timedelta(days=offset_days)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return CbrAdapter._ensure_utc(value)
        raw = str(value).strip()
        for date_format in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return CbrAdapter._ensure_utc(datetime.strptime(raw, date_format))
            except ValueError:
                continue
        try:
            return CbrAdapter._ensure_utc(datetime.fromisoformat(raw))
        except ValueError:
            return None

    @staticmethod
    def _parse_cbr_date(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%d.%m.%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _ruonia_row_value(row: dict[str, Any], column: Any) -> str | None:
        cells = row.get("cells")
        headers = row.get("headers")
        if not isinstance(cells, list):
            return None
        if isinstance(column, int) or str(column).isdigit():
            index = int(column)
            return str(cells[index]) if 0 <= index < len(cells) else None
        if isinstance(headers, list):
            try:
                index = headers.index(str(column))
            except ValueError:
                return None
            return str(cells[index]) if 0 <= index < len(cells) else None
        return None

    @staticmethod
    def _format_cbr_date(value: datetime) -> str:
        return CbrAdapter._ensure_utc(value).strftime("%d/%m/%Y")

    @staticmethod
    def _format_cbr_query_date(value: datetime) -> str:
        return CbrAdapter._ensure_utc(value).strftime("%d.%m.%Y")

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

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
