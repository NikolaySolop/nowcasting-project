from __future__ import annotations

import re
import zipfile
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from io import BytesIO
from typing import Any
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, RawObservationIn


class CbrAdapter(BaseAdapter):
    name = "cbr"

    daily_url = "https://www.cbr.ru/scripts/XML_daily.asp"
    dynamic_url = "https://www.cbr.ru/scripts/XML_dynamic.asp"
    key_rate_url = "https://www.cbr.ru/hd_base/KeyRate/"
    key_rate_calendar_url = "https://www.cbr.ru/dkp/cal_mp/"
    ruonia_dynamics_url = "https://www.cbr.ru/hd_base/ruonia/dynamics/"
    dkfs_url = "https://www.cbr.ru/statistics/macro_itm/dkfs/"
    credit_m2x_url = "https://www.cbr.ru/Content/Document/File/177307/credit_m2x.xlsx"
    xlsx_namespace = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
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
        if mode in {
            "key_rate_meetings",
            "key_rate_meeting_dummy",
            "meetings_calendar",
        }:
            return await self._fetch_key_rate_meetings(context)
        if mode in {"key_rate", "key_rate_history", "history_key_rate"}:
            return await self._fetch_key_rate_history(context)
        if mode in {"history", "history_daily", "dynamic"}:
            return await self._fetch_history_daily(context)
        if mode in {"latest", "latest_daily", "current", "daily"}:
            return await self._fetch_latest_daily(context)
        if mode in {"credit_m2x", "money_credit", "monetary_aggregates_credit"}:
            return await self._fetch_credit_m2x(context)
        raise AdapterError(f"unsupported CBR mode: {mode}")

    async def _fetch_credit_m2x(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        metrics = self._credit_m2x_metrics(context)
        date_from = self._multi_series_start_date(context, [metric["series_code"] for metric in metrics])
        date_to = self._end_date(extra)
        loaded_at = datetime.now(timezone.utc)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                loaded_at=loaded_at,
                raw_payload={
                    "mode": "credit_m2x",
                    "date_from": date_from.date().isoformat(),
                    "date_to": date_to.date().isoformat(),
                    "row_count": 0,
                    "observation_count": 0,
                },
            )

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            page_response = await client.get(str(spec.url or extra.get("page_url") or self.dkfs_url), headers=headers)
            try:
                page_response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR DKFS page HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc

            xlsx_url = self._credit_m2x_xlsx_url(page_response.text, str(page_response.url), extra)
            xlsx_response = await client.get(xlsx_url, headers=headers)
            try:
                xlsx_response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR credit_m2x XLSX HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc

        publication_dates = self._parse_credit_m2x_publication_dates(page_response.text)
        page_last_update = self._parse_page_last_update(page_response.text)
        xlsx_last_modified = self._parse_http_datetime(xlsx_response.headers.get("last-modified"))
        rows = self._parse_credit_m2x_rows(xlsx_response.content, metrics)
        rows = [row for row in rows if date_from.date() <= row["date"].date() <= date_to.date()]
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = [] if store_in_observations else self._credit_m2x_rows_to_observations(
            context,
            rows,
            publication_dates,
            page_last_update=page_last_update,
            xlsx_last_modified=xlsx_last_modified,
        )
        table_observations = (
            self._credit_m2x_rows_to_table_observations(
                context,
                rows,
                publication_dates,
                page_last_update=page_last_update,
                loaded_at=loaded_at,
            )
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "credit_m2x",
                "page_url": str(page_response.url),
                "xlsx_url": str(xlsx_response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(table_observations) if store_in_observations else len(observations),
                "page_last_update": page_last_update.isoformat() if page_last_update else None,
                "xlsx_last_modified": xlsx_last_modified.isoformat() if xlsx_last_modified else None,
            },
        )

    async def _fetch_key_rate_meetings(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        mode = str(extra.get("mode") or "key_rate_meetings")
        series_code = self._series_code(context)
        date_from = self._start_date(context, series_code)
        if bool(extra.get("refresh_all_observations", False)) and spec.start_date is not None:
            date_from = self._ensure_utc(spec.start_date)
        date_to = self._end_date(extra)
        loaded_at = datetime.now(timezone.utc)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                loaded_at=loaded_at,
                raw_payload={
                    "mode": mode,
                    "date_from": date_from.date().isoformat(),
                    "date_to": date_to.date().isoformat(),
                    "row_count": 0,
                    "observation_count": 0,
                },
            )

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            response = await client.get(
                str(spec.url or extra.get("key_rate_calendar_url") or self.key_rate_calendar_url),
                headers=headers,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR key rate calendar HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc

        rows = self._parse_key_rate_meeting_rows(response.text)
        rows = [row for row in rows if date_from.date() <= row["date"].date() <= date_to.date()]
        page_last_update = self._parse_page_last_update(response.text)
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = (
            []
            if store_in_observations
            else self._key_rate_meeting_rows_to_observations(context, rows, series_code)
        )
        table_observations = (
            self._key_rate_meeting_rows_to_table_observations(
                context,
                rows,
                series_code,
                page_last_update=page_last_update,
                loaded_at=loaded_at,
            )
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": mode,
                "url": str(response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "page_last_update": page_last_update.isoformat() if page_last_update else None,
                "observation_count": len(table_observations) if store_in_observations else len(observations),
            },
        )

    async def _fetch_ruonia_dynamics(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        metrics = self._ruonia_metrics(context)
        date_from = self._ruonia_start_date(context, [metric["series_code"] for metric in metrics])
        date_to = self._end_date(extra)
        loaded_at = datetime.now(timezone.utc)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                loaded_at=loaded_at,
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
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = [] if store_in_observations else self._ruonia_rows_to_observations(context, rows, metrics)
        table_observations = (
            self._ruonia_rows_to_table_observations(context, rows, metrics, loaded_at=loaded_at)
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "ruonia_dynamics",
                "url": str(response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(table_observations) if store_in_observations else len(observations),
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
        loaded_at = datetime.now(timezone.utc)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                loaded_at=loaded_at,
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
        store_in_observations = bool(extra.get("store_in_observations", False))
        decision_rows: list[dict[str, Any]] = []
        calendar_url: str | None = None

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
            if store_in_observations:
                calendar_url = str(extra.get("key_rate_calendar_url") or self.key_rate_calendar_url)
                calendar_response = await client.get(calendar_url, headers=headers)
                try:
                    calendar_response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise AdapterError(
                        f"CBR key rate calendar HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                    ) from exc
                decision_rows = self._parse_key_rate_meeting_rows(calendar_response.text)

        rows = self._parse_key_rate_rows(response.text)
        observations = self._key_rate_rows_to_observations(context, rows, series_code)
        table_observations = (
            self._key_rate_rows_to_table_observations(
                context,
                rows,
                series_code,
                loaded_at=loaded_at,
                decision_rows=decision_rows,
            )
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "key_rate_history",
                "url": str(response.url),
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "decision_calendar_url": calendar_url,
                "decision_count": len(decision_rows),
                "observation_count": len(table_observations) if store_in_observations else len(observations),
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
        loaded_at = datetime.now(timezone.utc)
        if date_from.date() > date_to.date():
            return FetchResult(
                observations=[],
                loaded_at=loaded_at,
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
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = self._rows_to_observations(context, rows, series_code)
        table_observations = (
            self._rows_to_table_observations(context, rows, series_code, loaded_at=loaded_at)
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "history_daily",
                "url": str(response.url),
                "currency_id": currency_id,
                "date_from": date_from.date().isoformat(),
                "date_to": date_to.date().isoformat(),
                "row_count": len(rows),
                "observation_count": len(table_observations) if store_in_observations else len(observations),
            },
        )

    async def _fetch_latest_daily(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR scrape spec")

        extra = spec.extra or {}
        currency_id = self._currency_id(extra)
        series_code = self._series_code(context)
        loaded_at = datetime.now(timezone.utc)
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
        store_in_observations = bool(extra.get("store_in_observations", False))
        observations = self._rows_to_observations(context, [row], series_code)
        table_observations = (
            self._rows_to_table_observations(context, [row], series_code, loaded_at=loaded_at)
            if store_in_observations
            else []
        )
        return FetchResult(
            observations=[] if store_in_observations else observations,
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "latest_daily",
                "url": str(response.url),
                "currency_id": currency_id,
                "published_date": row["date"].date().isoformat(),
                "observation_count": len(table_observations) if store_in_observations else len(observations),
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

    def _rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
        *,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        latest = context.latest_observed_at_by_series.get(series_code)
        observations: list[ObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            rate_date = row["date"]
            reference_start = self._cbr_reference_start(rate_date, spec.extra or {})
            reference_end = self._cbr_reference_end(rate_date, spec.extra or {})
            if latest is not None and reference_start <= latest:
                continue
            rate = row["rate"]
            if bool((spec.extra or {}).get("invert", False)):
                if rate == 0:
                    raise AdapterError("cannot invert zero CBR rate")
                rate = Decimal("1") / rate
            scheduled_published_at = self._backfill_published_at(reference_end, spec.extra or {})

            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    reference_date=rate_date.date(),
                    reference_start=reference_start,
                    reference_end=reference_end,
                    value=rate,
                    published_at=(
                        scheduled_published_at
                        if latest is None
                        else self._cbr_published_at(scheduled_published_at, loaded_at, spec.extra or {})
                    ),
                )
            )
        return observations

    def _cbr_published_at(
        self,
        scheduled_published_at: datetime,
        loaded_at: datetime,
        extra: dict[str, Any],
    ) -> datetime:
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        loaded_local_date = loaded_at.astimezone(publish_tz).date()
        scheduled_local_date = scheduled_published_at.astimezone(publish_tz).date()
        if scheduled_local_date >= loaded_local_date:
            return loaded_at
        return scheduled_published_at

    def _cbr_reference_start(self, rate_date: datetime, extra: dict[str, Any]) -> datetime:
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        reference_time = self._parse_time_config(str(extra.get("rate_reference_start_time") or "10:00"))
        reference_date = rate_date.astimezone(publish_tz).date() - timedelta(days=1)
        return datetime.combine(reference_date, reference_time, tzinfo=publish_tz)

    def _cbr_reference_end(self, rate_date: datetime, extra: dict[str, Any]) -> datetime:
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        reference_time = self._parse_time_config(str(extra.get("rate_reference_end_time") or "15:30"))
        reference_date = rate_date.astimezone(publish_tz).date() - timedelta(days=1)
        return datetime.combine(reference_date, reference_time, tzinfo=publish_tz)

    def _backfill_published_at(self, reference_end: datetime, extra: dict[str, Any]) -> datetime:
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        publish_time = self._parse_time_config(str(extra.get("backfill_published_time") or "17:00"))
        publish_date = reference_end.astimezone(publish_tz).date()
        return datetime.combine(publish_date, publish_time, tzinfo=publish_tz)

    @staticmethod
    def _parse_time_config(raw_value: str) -> time:
        try:
            hour, minute = raw_value.split(":", maxsplit=1)
            return time(hour=int(hour), minute=int(minute))
        except ValueError as exc:
            raise AdapterError(f"invalid time value: {raw_value}") from exc

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

    def _key_rate_rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
        *,
        loaded_at: datetime,
        decision_rows: list[dict[str, Any]],
    ) -> list[ObservationIn]:
        decisions = self._key_rate_decisions_by_meeting_date(decision_rows)
        rate_rows = sorted(rows, key=lambda item: item["date"])
        observations: list[ObservationIn] = []
        for index, (meeting_date, published_at) in enumerate(decisions):
            if published_at > loaded_at:
                continue

            next_meeting_date, next_published_at = self._next_key_rate_decision(index, decisions, loaded_at)
            rate = self._key_rate_rate_for_decision(meeting_date, next_meeting_date, rate_rows)
            if rate is None:
                continue

            reference_end = (
                next_published_at - timedelta(milliseconds=1)
                if next_published_at is not None
                else loaded_at
            )
            if reference_end < published_at:
                continue

            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    reference_date=published_at.date(),
                    reference_start=published_at,
                    reference_end=reference_end,
                    value=rate,
                    published_at=published_at,
                )
            )
        return observations

    @staticmethod
    def _next_key_rate_decision(
        index: int,
        decisions: list[tuple[date, datetime]],
        loaded_at: datetime,
    ) -> tuple[date | None, datetime | None]:
        for meeting_date, published_at in decisions[index + 1 :]:
            if published_at <= loaded_at:
                return meeting_date, published_at
            return meeting_date, None
        return None, None

    @staticmethod
    def _key_rate_rate_for_decision(
        meeting_date: date,
        next_meeting_date: date | None,
        rate_rows: list[dict[str, Any]],
    ) -> Decimal | None:
        for row in rate_rows:
            rate_date = row["date"].date()
            if rate_date <= meeting_date:
                continue
            if next_meeting_date is not None and rate_date >= next_meeting_date:
                return None
            rate = row.get("rate")
            return rate if isinstance(rate, Decimal) else None
        return None

    @staticmethod
    def _key_rate_decisions_by_meeting_date(rows: list[dict[str, Any]]) -> list[tuple[date, datetime]]:
        decisions: list[tuple[date, datetime]] = []
        for row in rows:
            published_at = row.get("date")
            meeting_date = row.get("meeting_date")
            if not isinstance(published_at, datetime) or not isinstance(meeting_date, str):
                continue
            try:
                decisions.append((datetime.fromisoformat(meeting_date).date(), published_at))
            except ValueError:
                continue
        return sorted(decisions, key=lambda item: item[0])

    def _key_rate_meeting_rows_to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        event_type = str(extra.get("event_type") or "key_rate_meeting")
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
                    publication_at=observed_at,
                    value_numeric=Decimal("1"),
                    kind=ObservationKind.EVENT,
                    raw_payload={
                        "source": "cbr_key_rate_calendar_html",
                        "event_type": event_type,
                        "event_title": row["title"],
                        "date_text": row["date_text"],
                        "meeting_date": row["meeting_date"],
                        "release_time_msk": row.get("release_time_msk"),
                    },
                )
            )
        return observations

    def _key_rate_meeting_rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        series_code: str,
        *,
        page_last_update: datetime | None,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        latest = context.latest_observed_at_by_series.get(series_code)
        refresh_all = bool(extra.get("refresh_all_observations", False))
        calendar_publication_dates = self._calendar_publication_dates(extra)
        observations: list[ObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            reference_at = row["date"]
            if not refresh_all and latest is not None and reference_at <= latest:
                continue
            published_at = self._key_rate_meeting_published_at(
                row,
                calendar_publication_dates,
                page_last_update=page_last_update,
                loaded_at=loaded_at,
            )

            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    reference_date=reference_at.date(),
                    reference_start=reference_at,
                    reference_end=reference_at,
                    value=Decimal("1"),
                    published_at=published_at,
                )
            )
        return observations

    @classmethod
    def _key_rate_meeting_published_at(
        cls,
        row: dict[str, Any],
        calendar_publication_dates: dict[int, datetime],
        *,
        page_last_update: datetime | None,
        loaded_at: datetime,
    ) -> datetime:
        reference_at = row["date"]
        if cls._is_unscheduled_key_rate_meeting(row):
            return reference_at
        return calendar_publication_dates.get(reference_at.year) or page_last_update or loaded_at

    @staticmethod
    def _is_unscheduled_key_rate_meeting(row: dict[str, Any]) -> bool:
        title = CbrAdapter._normalize_text(str(row.get("title") or "")).lower()
        return "внеочеред" in title or "unscheduled" in title

    @staticmethod
    def _calendar_publication_dates(extra: dict[str, Any]) -> dict[int, datetime]:
        raw_dates = extra.get("calendar_publication_dates")
        if not isinstance(raw_dates, dict):
            return {}
        dates: dict[int, datetime] = {}
        for raw_year, raw_value in raw_dates.items():
            try:
                year = int(raw_year)
            except (TypeError, ValueError):
                continue
            parsed = CbrAdapter._parse_calendar_publication_date(raw_value)
            if parsed is not None:
                dates[year] = parsed
        return dates

    @staticmethod
    def _parse_calendar_publication_date(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return CbrAdapter._parse_cbr_date(value)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)

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

    def _ruonia_rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        metrics: list[dict[str, Any]],
        *,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        observations: list[ObservationIn] = []
        for row in sorted(rows, key=lambda item: item["date"]):
            reference_at = row["date"]
            publication_at = self._ruonia_published_at(row.get("publication_at"), reference_at, loaded_at, extra)
            for metric in metrics:
                series_code = str(metric["series_code"])
                latest = context.latest_observed_at_by_series.get(series_code)
                if latest is not None and reference_at <= latest:
                    continue

                raw_value = self._ruonia_row_value(row, metric["value_column"])
                numeric_value = self._parse_decimal(raw_value)
                if numeric_value is None:
                    continue

                observations.append(
                    ObservationIn(
                        series_code=series_code,
                        source_code=context.source.source_code,
                        reference_date=reference_at.date(),
                        reference_start=reference_at,
                        reference_end=reference_at,
                        value=numeric_value,
                        published_at=publication_at,
                    )
                )

        observations.sort(key=lambda item: (item.reference_start, item.series_code))
        return observations

    def _ruonia_published_at(
        self,
        raw_publication_at: Any,
        reference_at: datetime,
        loaded_at: datetime,
        extra: dict[str, Any],
    ) -> datetime:
        publication_at = raw_publication_at if isinstance(raw_publication_at, datetime) else reference_at
        scheduled = self._ruonia_scheduled_published_at(publication_at, extra)
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        if scheduled.astimezone(publish_tz).date() >= loaded_at.astimezone(publish_tz).date():
            return loaded_at
        return scheduled

    def _ruonia_scheduled_published_at(self, publication_at: datetime, extra: dict[str, Any]) -> datetime:
        publish_tz = ZoneInfo(str(extra.get("backfill_published_timezone") or "Europe/Moscow"))
        publish_time = self._parse_time_config(str(extra.get("backfill_published_time") or "15:00"))
        publish_date = publication_at.astimezone(publish_tz).date()
        return datetime.combine(publish_date, publish_time, tzinfo=publish_tz)

    def _credit_m2x_rows_to_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        publication_dates: dict[datetime, datetime],
        *,
        page_last_update: datetime | None,
        xlsx_last_modified: datetime | None,
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        observations: list[RawObservationIn] = []
        for row in sorted(rows, key=lambda item: (item["date"], item["series_code"])):
            series_code = str(row["series_code"])
            observed_at = row["date"]
            latest = context.latest_observed_at_by_series.get(series_code)
            if latest is not None and observed_at <= latest:
                continue

            publication_at, publication_source = self._credit_m2x_publication_at(
                observed_at,
                publication_dates,
                page_last_update=page_last_update,
                fallback=str(extra.get("publication_at_fallback") or "estimated_same_month_day"),
                fallback_day=int(extra.get("publication_at_fallback_day") or 22),
            )
            vintage_at = xlsx_last_modified or page_last_update or datetime.now(timezone.utc)
            observations.append(
                RawObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=observed_at,
                    publication_at=publication_at,
                    vintage_at=vintage_at,
                    value_numeric=row["value"],
                    kind=ObservationKind.MACRO,
                    raw_payload={
                        "source": "cbr_credit_m2x_xlsx",
                        "row_label": row["row_label"],
                        "unit": "billion_rub",
                        "value": str(row["value"]),
                        "publication_at_source": publication_source,
                        "page_last_update": page_last_update.isoformat() if page_last_update else None,
                        "xlsx_last_modified": xlsx_last_modified.isoformat() if xlsx_last_modified else None,
                    },
                )
            )

        return observations

    def _credit_m2x_rows_to_table_observations(
        self,
        context: FetchContext,
        rows: list[dict[str, Any]],
        publication_dates: dict[datetime, datetime],
        *,
        page_last_update: datetime | None,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        observations: list[ObservationIn] = []
        for row in sorted(rows, key=lambda item: (item["date"], item["series_code"])):
            series_code = str(row["series_code"])
            reference_at = row["date"]
            latest = context.latest_observed_at_by_series.get(series_code)
            if latest is not None and reference_at <= latest:
                continue

            published_at, _publication_source = self._credit_m2x_publication_at(
                reference_at,
                publication_dates,
                page_last_update=page_last_update,
                fallback=str(extra.get("publication_at_fallback") or "estimated_same_month_day"),
                fallback_day=int(extra.get("publication_at_fallback_day") or 22),
            )
            if published_at is None:
                published_at = loaded_at

            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=context.source.source_code,
                    reference_date=reference_at.date(),
                    reference_start=reference_at,
                    reference_end=self._month_end(reference_at),
                    value=row["value"],
                    published_at=published_at,
                )
            )

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

    def _parse_key_rate_meeting_rows(self, html: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        tab_years = self._calendar_tab_years(soup)
        default_release_time = self._parse_cbr_release_time(soup.get_text(" ", strip=True))

        rows: list[dict[str, Any]] = []
        seen_dates: set[datetime] = set()

        def add_row(date_text: str, title: str, tab_id: str | None, node: Any | None = None) -> None:
            date_text = self._normalize_text(date_text)
            fallback_year = tab_years.get(tab_id) if tab_id else None
            meeting_date = self._parse_cbr_calendar_date(date_text, fallback_year)
            if meeting_date is None:
                return

            title = self._normalize_text(title)
            if not self._is_key_rate_meeting_title(title):
                return
            if meeting_date in seen_dates:
                return

            release_time = (
                self._parse_cbr_release_time_from_links(node)
                or self._parse_cbr_release_time(title)
                or default_release_time
            )
            observed_at = self._apply_moscow_time(meeting_date, release_time) if release_time else meeting_date
            seen_dates.add(meeting_date)
            rows.append(
                {
                    "date": observed_at,
                    "date_text": date_text,
                    "meeting_date": meeting_date.date().isoformat(),
                    "release_time_msk": self._format_release_time(release_time),
                    "title": title,
                }
            )

        for day in soup.select(".calendar-main-events .main-events_day"):
            date_tag = day.select_one(".date")
            if date_tag is None:
                continue
            add_row(
                date_tag.get_text(" ", strip=True),
                day.get_text(" ", strip=True),
                self._calendar_tab_id(day),
                day,
            )

        for tab in soup.select("[data-tabs-content]"):
            tab_id = str(tab.get("data-tabs-content") or "")
            for table_row in tab.select("table tr"):
                cells = [cell.get_text(" ", strip=True) for cell in table_row.select("td,th")]
                if len(cells) < 2:
                    continue
                if self._normalize_text(cells[0]).lower() == "дата":
                    continue
                add_row(cells[0], cells[1], tab_id, table_row)

        if not rows:
            raise AdapterError("CBR key rate calendar has no parseable meeting rows")
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

    def _parse_credit_m2x_rows(self, content: bytes, metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = self._xlsx_sheet_rows(content, "млрд рублей")
        if not rows:
            raise AdapterError("CBR credit_m2x workbook has no 'млрд рублей' rows")

        date_row = rows[0]
        dates_by_column = {
            column: self._parse_excel_date(value)
            for column, value in date_row.items()
            if column > 1
        }
        dates_by_column = {column: value for column, value in dates_by_column.items() if value is not None}
        if not dates_by_column:
            raise AdapterError("CBR credit_m2x workbook has no date columns")

        row_by_label = {
            self._normalize_text(str(row.get(1) or "")).lower(): row
            for row in rows[1:]
            if row.get(1) is not None
        }

        parsed_rows: list[dict[str, Any]] = []
        for metric in metrics:
            row_label = str(metric["row_label"])
            row = row_by_label.get(self._normalize_text(row_label).lower())
            if row is None:
                raise AdapterError(f"CBR credit_m2x workbook has no row label: {row_label}")
            for column, observed_at in dates_by_column.items():
                value = self._parse_decimal(row.get(column))
                if value is None:
                    continue
                parsed_rows.append(
                    {
                        "series_code": metric["series_code"],
                        "row_label": row_label,
                        "date": observed_at,
                        "value": value,
                    }
                )

        return parsed_rows

    def _xlsx_sheet_rows(self, content: bytes, sheet_name: str) -> list[dict[int, str]]:
        ns = self.xlsx_namespace
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            workbook_root = ET.fromstring(workbook.read("xl/workbook.xml"))
            rels_root = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
            rels = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels_root}
            sheet_path = None
            for sheet in workbook_root.findall("a:sheets/a:sheet", ns):
                name = str(sheet.attrib.get("name") or "")
                if self._normalize_text(name).lower() != self._normalize_text(sheet_name).lower():
                    continue
                rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
                if rel_id is not None and rel_id in rels:
                    target = rels[rel_id]
                    sheet_path = target if target.startswith("xl/") else f"xl/{target}"
                    break
            if sheet_path is None:
                raise AdapterError(f"CBR XLSX workbook has no sheet: {sheet_name}")

            shared_strings = self._xlsx_shared_strings(workbook)
            sheet_root = ET.fromstring(workbook.read(sheet_path))

        rows: list[dict[int, str]] = []
        for row in sheet_root.findall(".//a:sheetData/a:row", ns):
            values: dict[int, str] = {}
            for cell in row.findall("a:c", ns):
                cell_ref = str(cell.attrib.get("r") or "")
                column = self._xlsx_column_index(cell_ref)
                if column is None:
                    continue
                value = self._xlsx_cell_value(cell, shared_strings)
                if value is not None:
                    values[column] = value
            if values:
                rows.append(values)
        return rows

    def _xlsx_shared_strings(self, workbook: zipfile.ZipFile) -> list[str]:
        ns = self.xlsx_namespace
        if "xl/sharedStrings.xml" not in workbook.namelist():
            return []
        root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
        return [
            "".join(text.text or "" for text in item.findall(".//a:t", ns))
            for item in root.findall("a:si", ns)
        ]

    def _xlsx_cell_value(self, cell: ET.Element, shared_strings: list[str]) -> str | None:
        ns = self.xlsx_namespace
        value_node = cell.find("a:v", ns)
        if value_node is None or value_node.text is None:
            inline = cell.find("a:is", ns)
            if inline is None:
                return None
            return "".join(text.text or "" for text in inline.findall(".//a:t", ns))
        value = value_node.text
        if cell.attrib.get("t") == "s":
            try:
                return shared_strings[int(value)]
            except (ValueError, IndexError):
                return None
        return value

    @staticmethod
    def _xlsx_column_index(cell_ref: str) -> int | None:
        match = re.match(r"^([A-Z]+)", cell_ref.upper())
        if match is None:
            return None
        column = 0
        for char in match.group(1):
            column = column * 26 + ord(char) - ord("A") + 1
        return column

    @staticmethod
    def _parse_excel_date(value: Any) -> datetime | None:
        parsed = CbrAdapter._parse_datetime(value)
        if parsed is not None:
            return parsed.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            serial = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
        return (datetime(1899, 12, 30, tzinfo=timezone.utc) + timedelta(days=float(serial))).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    def _credit_m2x_metrics(self, context: FetchContext) -> list[dict[str, str]]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        raw_metrics = extra.get("series")
        if isinstance(raw_metrics, list) and raw_metrics:
            metrics = [dict(item) for item in raw_metrics if isinstance(item, dict)]
        else:
            metrics = [
                {"series_code": "RU_M2", "row_label": "Денежная масса М2"},
                {"series_code": "RU_CREDIT_HOUSEHOLDS", "row_label": "кредиты физическим лицам"},
                {"series_code": "RU_CREDIT_CORPORATES", "row_label": "корпоративные кредиты"},
            ]

        normalized: list[dict[str, str]] = []
        for metric in metrics:
            series_code = str(metric.get("series_code") or "").strip()
            row_label = str(metric.get("row_label") or "").strip()
            if not series_code:
                raise AdapterError(f"CBR credit_m2x metric requires series_code: {metric}")
            if not row_label:
                raise AdapterError(f"CBR credit_m2x metric requires row_label: {metric}")
            normalized.append({"series_code": series_code, "row_label": row_label})
        return normalized

    def _credit_m2x_xlsx_url(self, html: str, page_url: str, extra: dict[str, Any]) -> str:
        explicit = str(extra.get("xlsx_url") or "").strip()
        if explicit:
            return explicit
        soup = BeautifulSoup(html, "html.parser")
        for link in soup.select("a[href]"):
            href = str(link.get("href") or "")
            text = self._normalize_text(link.get_text(" ", strip=True)).lower()
            if "credit_m2x.xlsx" in href.lower() or "приложение к материалу" in text:
                return str(httpx.URL(page_url).join(href))
        return self.credit_m2x_url

    def _parse_credit_m2x_publication_dates(self, html: str) -> dict[datetime, datetime]:
        soup = BeautifulSoup(html, "html.parser")
        publication_dates: dict[datetime, datetime] = {}
        for link in soup.select("a.versions_item"):
            observed_at = self._parse_cbr_date(link.get_text(" ", strip=True))
            tooltip = str(link.get("data-tooltip-content") or "")
            publication_at = self._parse_russian_publication_date(tooltip)
            if observed_at is not None and publication_at is not None:
                publication_dates[observed_at] = publication_at
        return publication_dates

    @staticmethod
    def _parse_page_last_update(html: str) -> datetime | None:
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        match = re.search(r"(?:Дата последнего обновления|Последнее обновление страницы):\s*(\d{2}\.\d{2}\.\d{4})", text)
        return CbrAdapter._parse_cbr_date(match.group(1)) if match else None

    @staticmethod
    def _parse_http_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        return CbrAdapter._ensure_utc(parsed)

    @staticmethod
    def _parse_russian_publication_date(value: str) -> datetime | None:
        month_numbers = {
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12,
        }
        normalized = CbrAdapter._normalize_text(value).lower()
        match = re.search(r"(\d{1,2})\s+([а-яё]+)\s+(\d{4})", normalized)
        if match is None:
            return None
        month = month_numbers.get(match.group(2))
        if month is None:
            return None
        try:
            return datetime(int(match.group(3)), month, int(match.group(1)), tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _credit_m2x_publication_at(
        observed_at: datetime,
        publication_dates: dict[datetime, datetime],
        *,
        page_last_update: datetime | None,
        fallback: str,
        fallback_day: int,
    ) -> tuple[datetime | None, str | None]:
        observed_key = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
        if observed_key in publication_dates:
            return publication_dates[observed_key], "cbr_versions_tooltip"
        if fallback == "page_last_update":
            return page_last_update, "page_last_update" if page_last_update else None
        if fallback == "none":
            return None, None
        if fallback == "estimated_same_month_day":
            day = max(1, min(28, fallback_day))
            estimated = observed_at.replace(day=day, hour=0, minute=0, second=0, microsecond=0)
            while estimated.weekday() >= 5:
                estimated += timedelta(days=1)
            return estimated, "estimated_same_month_day"
        return None, None

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

    def _multi_series_start_date(self, context: FetchContext, series_codes: list[str]) -> datetime:
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
        return datetime.now(timezone.utc) - timedelta(days=int(extra.get("lookback_days") or 45))

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
    def _calendar_tab_years(soup: BeautifulSoup) -> dict[str, int]:
        tab_years: dict[str, int] = {}
        for tab in soup.select("[data-tabs-tab]"):
            tab_id = str(tab.get("data-tabs-tab") or "")
            match = re.search(r"\b(20\d{2})\b", tab.get_text(" ", strip=True))
            if tab_id and match:
                tab_years[tab_id] = int(match.group(1))
        return tab_years

    @staticmethod
    def _calendar_tab_id(day: Any) -> str | None:
        parent = day.parent
        while parent is not None:
            tab_id = parent.get("data-tabs-content") if hasattr(parent, "get") else None
            if tab_id:
                return str(tab_id)
            parent = parent.parent
        return None

    @staticmethod
    def _parse_cbr_calendar_date(value: str, fallback_year: int | None = None) -> datetime | None:
        month_numbers = {
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12,
            "january": 1,
            "february": 2,
            "march": 3,
            "april": 4,
            "may": 5,
            "june": 6,
            "july": 7,
            "august": 8,
            "september": 9,
            "october": 10,
            "november": 11,
            "december": 12,
        }
        normalized = CbrAdapter._normalize_text(value).lower()
        day: int | None = None
        month: int | None = None
        year: int | None = None

        match = re.search(r"^(\d{1,2})\s+([a-zа-яё]+)(?:\s+(\d{4}))?", normalized)
        if match is not None:
            day = int(match.group(1))
            month = month_numbers.get(match.group(2).strip("."))
            year = int(match.group(3)) if match.group(3) else fallback_year
        else:
            match = re.search(r"^([a-zа-яё]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?", normalized)
            if match is None:
                return None
            month = month_numbers.get(match.group(1).strip("."))
            day = int(match.group(2))
            year = int(match.group(3)) if match.group(3) else fallback_year

        if day is None or month is None or year is None:
            return None

        try:
            return datetime(year, month, day, tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _parse_cbr_release_time(value: str) -> tuple[int, int] | None:
        normalized = CbrAdapter._normalize_text(value).lower()
        patterns = (
            r"публикаци[ия][^0-9]{0,120}(\d{1,2})[:.](\d{2})",
            r"press release[^0-9]{0,160}(\d{1,2})[:.](\d{2})",
        )
        match = None
        for pattern in patterns:
            match = re.search(pattern, normalized)
            if match is not None:
                break
        if match is None:
            return None

        hour = int(match.group(1))
        minute = int(match.group(2))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        return hour, minute

    @staticmethod
    def _parse_cbr_release_time_from_links(node: Any | None) -> tuple[int, int] | None:
        if node is None or not hasattr(node, "select"):
            return None
        for link in node.select("a[href]"):
            link_text = CbrAdapter._normalize_text(link.get_text(" ", strip=True)).lower()
            href = str(link.get("href") or "")
            if "press release" not in link_text and "пресс-релиз" not in link_text:
                continue
            if "key" not in (link_text + " " + href).lower() and "ставк" not in link_text:
                continue
            match = re.search(r"_(\d{2})(\d{2})(\d{2})(?:key|ключ)", href, flags=re.IGNORECASE)
            if match is None:
                match = re.search(r"_(\d{2})(\d{2})(\d{2})", href)
            if match is None:
                continue
            hour = int(match.group(1))
            minute = int(match.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return hour, minute
        return None

    @staticmethod
    def _apply_moscow_time(value: datetime, release_time: tuple[int, int]) -> datetime:
        moscow_tz = timezone(timedelta(hours=3))
        hour, minute = release_time
        return value.replace(hour=hour, minute=minute, tzinfo=moscow_tz).astimezone(timezone.utc)

    @staticmethod
    def _format_release_time(value: tuple[int, int] | None) -> str | None:
        if value is None:
            return None
        hour, minute = value
        return f"{hour:02d}:{minute:02d}"

    @staticmethod
    def _parse_cbr_date(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%d.%m.%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _month_end(value: datetime) -> datetime:
        if value.month == 12:
            next_month = value.replace(year=value.year + 1, month=1, day=1)
        else:
            next_month = value.replace(month=value.month + 1, day=1)
        return next_month - timedelta(microseconds=1)

    @staticmethod
    def _is_key_rate_meeting_title(value: str) -> bool:
        normalized = CbrAdapter._normalize_text(value).lower()
        if "заседание совета директоров" in normalized:
            return (
                "ключевой ставке" in normalized
                or "денежно-кредитной политике" in normalized
                or "денежно-кредитной политики" in normalized
            )
        if "board of" not in normalized or "meeting" not in normalized:
            return False
        return "key rate" in normalized or "monetary policy" in normalized

    @staticmethod
    def _normalize_text(value: str) -> str:
        return " ".join(value.replace("\xa0", " ").split())

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
