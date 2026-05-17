from __future__ import annotations

import asyncio
import random
import re
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn, ObservationKind, ParsedObservation
from ingestion.services.isdayoff import IsDayOffClient, IsDayOffError


class ExchangeRatesAdapter(BaseAdapter):
    name = "exchangerates"
    ajax_url = "https://www.exchangerates.org.uk/ajax-commodities-charts-24-48.php"
    history_url = "https://www.exchangerates.org.uk/commodities/URALS-USD-history.html"
    referer = "https://www.exchangerates.org.uk/commodities/live-urals-crude-oil-prices/URALS-USD.html"
    default_exchange_timezone = "Europe/Moscow"
    default_trading_session_open = time(hour=10, minute=0)
    default_daily_session_close = time(hour=18, minute=45)

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        mode = str(extra.get("mode") or self._infer_mode(str(spec.url or "")))
        if mode == "history_daily":
            return await self._fetch_history_daily(context)
        if mode == "live_html":
            return await self._fetch_live_html(context)
        if mode != "live_ajax":
            raise AdapterError(f"unsupported ExchangeRates mode: {mode}")

        return await self._fetch_live_ajax(context)

    async def _fetch_live_ajax(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        series_code = str(spec.series_code or extra.get("series_code") or "EXCHANGERATES:URALSUSD")
        code = str(extra.get("code") or extra.get("ticker") or "URALSUSD")
        iso = str(extra.get("iso") or "USD")
        chart_range = int(extra.get("range", 48))
        interval_minutes = int(extra.get("interval_minutes", 15))
        nonce = str(extra.get("nonce") or context.settings.exchangerates_ajax_nonce or "")
        cookie = str(extra.get("cookie") or context.settings.exchangerates_cookie or "")

        if not nonce:
            raise AdapterError("ExchangeRates AJAX requires scrape.extra.nonce or EXCHANGERATES_AJAX_NONCE")
        if not cookie:
            raise AdapterError("ExchangeRates AJAX requires scrape.extra.cookie or EXCHANGERATES_COOKIE")

        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-GB,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": str(extra.get("referer") or self.referer),
            "User-Agent": context.settings.request_user_agent,
            "X-Ajax-Nonce": nonce,
            "X-Requested-With": "XMLHttpRequest",
            "Cookie": cookie,
        }
        headers.update(spec.headers)

        params = {
            "code": code,
            "iso": iso,
            "range": str(chart_range),
            "meta": str(int(bool(extra.get("meta", True)))),
            "nonce": nonce,
        }
        params.update(spec.params)

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            response = await client.get(str(spec.url or extra.get("ajax_url") or self.ajax_url), headers=headers, params=params)
            response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "json" not in content_type.lower():
            raise AdapterError(
                "ExchangeRates AJAX returned non-json response: "
                f"status={response.status_code}, content_type={content_type}"
            )

        payload = response.json()
        points = self._extract_points(payload)
        observations = self._points_to_observations(
            points=points,
            source_code=context.source.source_code,
            series_code=series_code,
            interval_minutes=interval_minutes,
        )

        return FetchResult(
            observations=observations,
            raw_payload={
                "url": str(response.url),
                "status_code": response.status_code,
                "code": code,
                "iso": iso,
                "range": chart_range,
                "point_count": len(points),
                "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else None,
            },
        )

    async def _fetch_history_daily(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        series_code = str(spec.series_code or extra.get("series_code") or "EXCHANGERATES:URALSUSD:DAILY")
        start_date = self._normalize_datetime(spec.start_date) or datetime(2015, 1, 1, tzinfo=timezone.utc)
        end_date = self._parse_date_only(extra.get("end_date")) or datetime.now(timezone.utc).date()
        url = str(spec.url or extra.get("history_url") or self.history_url)
        pair = str(extra.get("pair") or "URALS/USD")
        max_range_years = int(extra.get("max_range_years", 5))
        per_page = int(extra.get("per") or extra.get("per_page") or 90)
        delay_min_seconds = max(4.5, float(extra.get("history_delay_min_seconds", extra.get("history_request_delay_seconds", 4.5))))
        delay_max_seconds = max(delay_min_seconds, float(extra.get("history_delay_max_seconds", delay_min_seconds + 4.0)))
        store_in_observations = bool(extra.get("store_in_observations", False))
        loaded_at = datetime.now(timezone.utc)
        request_summaries: list[dict[str, Any]] = []
        all_rows: list[dict[str, Any]] = []
        session_state: dict[str, str] = {}

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            for window_start, window_end in self._date_windows(start_date.date(), end_date, max_range_years):
                page = 1
                while True:
                    payload, summary = await self._fetch_history_json_page(
                        client=client,
                        context=context,
                        url=url,
                        window_start=window_start,
                        window_end=window_end,
                        page=page,
                        per_page=per_page,
                        session_state=session_state,
                    )
                    request_summaries.append(summary)
                    rows = self._parse_history_json_rows(payload, pair=pair)
                    all_rows.extend(rows)
                    pages = self._parse_int(payload.get("pages")) if isinstance(payload, dict) else None
                    if pages is None or page >= pages:
                        break
                    await self._sleep_between_history_requests(delay_min_seconds, delay_max_seconds)
                    page += 1
                await self._sleep_between_history_requests(delay_min_seconds, delay_max_seconds)

        rows_by_date = {row["date"]: row for row in all_rows}
        rows = [rows_by_date[observed_at] for observed_at in sorted(rows_by_date)]
        filtered_rows = [row for row in rows if start_date is None or row["date"] >= start_date]
        table_observations = self._history_rows_to_table_observations(
            filtered_rows,
            source_code=context.source.source_code,
            series_code=series_code,
            loaded_at=loaded_at,
        )

        if not table_observations:
            raise AdapterError("ExchangeRates daily history returned no close prices for configured date range")

        return FetchResult(
            table_observations=table_observations,
            loaded_at=loaded_at,
            raw_payload={
                "mode": "history_daily",
                "row_count": len(rows),
                "observation_count": len(table_observations),
                "duplicate_count": 0,
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat(),
                "requests": request_summaries,
            },
        )

    async def _fetch_history_json_page(
        self,
        *,
        client: httpx.AsyncClient,
        context: FetchContext,
        url: str,
        window_start: date,
        window_end: date,
        page: int,
        per_page: int,
        session_state: dict[str, str],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        from_param = str(extra.get("from_param") or "from")
        to_param = str(extra.get("to_param") or "to")
        page_param = str(extra.get("page_param") or "page")
        per_param = str(extra.get("per_param") or "per")
        page_retries = int(extra.get("history_page_retries", 5))
        retry_delay_seconds = float(extra.get("history_retry_delay_seconds", 10.0))
        params = dict(spec.params)
        params.update(
            {
                "ajax": str(extra.get("ajax") or "hist"),
                from_param: window_start.isoformat(),
                to_param: window_end.isoformat(),
                page_param: str(page),
                per_param: str(per_page),
            }
        )
        nonce = str(session_state.get("nonce") or extra.get("nonce") or context.settings.exchangerates_ajax_nonce or "")
        session_cookie = session_state.get("cookie")
        method = spec.method.upper()
        if method == "GET" and bool(extra.get("history_post", True)):
            method = "POST"

        session_refreshed = False
        if (bool(extra.get("history_refresh_session_before_request", True)) and not session_state) or not nonce:
            refreshed_nonce, refreshed_cookie = await self._refresh_history_session(client, context=context, url=url)
            session_refreshed = True
            if refreshed_nonce:
                nonce = refreshed_nonce
                session_state["nonce"] = refreshed_nonce
            if refreshed_cookie:
                session_cookie = refreshed_cookie
                session_state["cookie"] = refreshed_cookie

        if not nonce:
            raise AdapterError(
                "ExchangeRates daily history requires a nonce. Configure EXCHANGERATES_AJAX_NONCE "
                "or allow the adapter to refresh the page session."
            )

        def build_request_kwargs() -> dict[str, Any]:
            request_params = dict(params)
            request_params["nonce"] = nonce
            headers = self._page_headers(context, accept="application/json")
            headers["X-Requested-With"] = "XMLHttpRequest"
            headers["Origin"] = "https://www.exchangerates.org.uk"
            headers["X-Ajax-Nonce"] = nonce
            if session_cookie:
                headers["Cookie"] = session_cookie

            kwargs: dict[str, Any] = {
                "headers": headers,
                "params": request_params if method == "GET" else None,
            }
            if method == "POST":
                encoding = str(extra.get("history_form_encoding") or "multipart")
                if encoding == "multipart":
                    kwargs["files"] = {key: (None, str(value)) for key, value in request_params.items()}
                else:
                    kwargs["data"] = request_params
            return kwargs

        response: httpx.Response | None = None
        request_kwargs = build_request_kwargs()
        refresh_on_403 = bool(extra.get("history_refresh_session_on_403", True))
        for attempt in range(1, page_retries + 1):
            response = await client.request(
                method,
                url,
                **request_kwargs,
            )
            if response.status_code != 403:
                break
            if refresh_on_403 and not session_refreshed:
                refreshed_nonce, refreshed_cookie = await self._refresh_history_session(client, context=context, url=url)
                session_refreshed = True
                if refreshed_nonce:
                    nonce = refreshed_nonce
                    session_state["nonce"] = refreshed_nonce
                if refreshed_cookie:
                    session_cookie = refreshed_cookie
                    session_state["cookie"] = refreshed_cookie
                request_kwargs = build_request_kwargs()
                continue
            if attempt < page_retries:
                await asyncio.sleep(retry_delay_seconds * attempt)

        if response is None:
            raise AdapterError("ExchangeRates daily history request was not sent")
        self._raise_for_blocked_response(response, "ExchangeRates daily history")

        try:
            payload = response.json()
        except ValueError as exc:
            raise AdapterError(
                "ExchangeRates daily history returned non-json response: "
                f"status={response.status_code}, content_type={response.headers.get('content-type')}"
            ) from exc

        if not isinstance(payload, dict):
            raise AdapterError("ExchangeRates daily history JSON must be an object")

        return payload, {
            "url": str(response.url),
            "status_code": response.status_code,
            "from": window_start.isoformat(),
            "to": window_end.isoformat(),
            "page": page,
            "rows": len(payload.get("rows", [])) if isinstance(payload.get("rows"), list) else None,
            "pages": payload.get("pages"),
            "clamped": payload.get("clamped"),
            "session_refreshed": session_refreshed,
        }

    async def _refresh_history_session(
        self,
        client: httpx.AsyncClient,
        *,
        context: FetchContext,
        url: str,
    ) -> tuple[str | None, str | None]:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        headers = self._page_headers(context, accept="text/html")
        if not bool(extra.get("history_refresh_uses_configured_cookie", False)):
            headers.pop("Cookie", None)
        response = await client.get(url, headers=headers)
        self._raise_for_blocked_response(response, "ExchangeRates history session refresh")

        nonce = self._extract_ajax_nonce(response.text)
        cookie = self._cookie_header_from_client(client, url)
        return nonce, cookie

    @staticmethod
    def _extract_ajax_nonce(html: str) -> str | None:
        patterns = (
            r"(?:nonce|ajaxNonce|ajax_nonce|xAjaxNonce|x_ajax_nonce)['\"\s:=]+([a-fA-F0-9]{16,64})",
            r"\b([a-fA-F0-9]{32})\b",
        )
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _cookie_header_from_client(client: httpx.AsyncClient, url: str) -> str | None:
        cookie_header = client.build_request("GET", url).headers.get("cookie")
        return str(cookie_header) if cookie_header else None

    async def _fetch_live_html(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ExchangeRates scrape spec")

        extra = spec.extra
        series_code = str(spec.series_code or extra.get("series_code") or "EXCHANGERATES:URALSUSD:LIVE")
        interval_minutes = int(extra.get("interval_minutes", 15))
        url = str(spec.url or extra.get("live_url") or self.referer)
        fetched_at = datetime.now(timezone.utc)
        store_in_observations = bool(extra.get("store_in_observations", False))
        allowed, gate_payload = await self._live_capture_gate(fetched_at, context)
        if not allowed:
            return FetchResult(
                observations=[],
                table_observations=[],
                loaded_at=fetched_at,
                raw_payload={
                    "mode": "live_html",
                    "skipped": True,
                    **gate_payload,
                },
            )

        headers = self._page_headers(context, accept="text/html")
        if bool(extra.get("live_refresh_session_before_request", True)):
            headers.pop("Cookie", None)

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            response = await client.get(url, headers=headers, params=spec.params)
            if response.status_code == 403 and bool(extra.get("live_refresh_session_on_403", True)):
                headers.pop("Cookie", None)
                response = await client.get(url, headers=headers, params=spec.params)

        self._raise_for_blocked_response(response, "ExchangeRates live page")
        price, parsed_at = self._parse_live_html(response.text)
        observed_at = self._round_time(fetched_at, interval_minutes)
        last_observed = context.latest_observed_at_by_series.get(series_code)
        if last_observed is not None and observed_at <= last_observed:
            return FetchResult(
                observations=[],
                table_observations=[],
                loaded_at=fetched_at,
                raw_payload={
                    "mode": "live_html",
                    "skipped": True,
                    "skip_reason": "already_loaded",
                    "observed_at": observed_at.isoformat(),
                    "latest_observed_at": last_observed.isoformat(),
                },
            )

        parsed_observation = ParsedObservation(
            series_code=series_code,
            source_code=context.source.source_code,
            observed_at=observed_at,
            value_numeric=price,
            publication_at=fetched_at,
            kind=ObservationKind.QUOTE,
            raw_payload={
                "source": "exchangerates",
                "mode": "live_html",
                "url": str(response.url),
                "price": str(price),
                "parsed_at": parsed_at.isoformat() if parsed_at else None,
                "fetched_at": fetched_at.isoformat(),
                "interval_minutes": interval_minutes,
            },
        )

        return FetchResult(
            table_observations=[
                self._live_observation_to_table(
                    parsed_observation,
                    interval_minutes=interval_minutes,
                    extra=extra,
                )
            ],
            loaded_at=fetched_at,
            raw_payload={
                "url": str(response.url),
                "status_code": response.status_code,
                "mode": "live_html",
                "observed_at": observed_at.isoformat(),
                "published_at": fetched_at.isoformat(),
                **gate_payload,
            },
        )

    async def _live_capture_gate(
        self,
        fetched_at: datetime,
        context: FetchContext,
    ) -> tuple[bool, dict[str, Any]]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        exchange_tz = ZoneInfo(str(extra.get("exchange_timezone", self.default_exchange_timezone)))
        local_value = fetched_at.astimezone(exchange_tz)

        if not self._is_trading_session_time(local_value, extra):
            return False, {
                "skip_reason": "outside_trading_session",
                "exchange_timezone": str(exchange_tz),
                "local_time": local_value.isoformat(),
            }

        if bool(extra.get("russian_business_day_only", False)):
            calendar_client = IsDayOffClient(timeout_seconds=context.settings.request_timeout_seconds)
            try:
                day_type = await calendar_client.get_day_type(
                    local_value.date(),
                    country_code=str(extra.get("business_day_country_code") or "ru"),
                    include_short_days=bool(extra.get("business_day_include_short_days", True)),
                    six_day_week=bool(extra.get("business_day_six_day_week", False)),
                )
            except (IsDayOffError, httpx.HTTPError):
                if bool(extra.get("business_day_fail_closed", True)):
                    return False, {
                        "skip_reason": "business_calendar_unavailable",
                        "exchange_timezone": str(exchange_tz),
                        "local_date": local_value.date().isoformat(),
                    }
                raise

            if not day_type.is_working_day:
                return False, {
                    "skip_reason": "non_working_day_ru",
                    "exchange_timezone": str(exchange_tz),
                    "local_date": local_value.date().isoformat(),
                    "isdayoff_code": day_type.code,
                }
            return True, {
                "exchange_timezone": str(exchange_tz),
                "local_date": local_value.date().isoformat(),
                "isdayoff_code": day_type.code,
            }

        return True, {
            "exchange_timezone": str(exchange_tz),
            "local_date": local_value.date().isoformat(),
        }

    def _points_to_observations(
        self,
        *,
        points: list[dict[str, Any]],
        source_code: str,
        series_code: str,
        interval_minutes: int,
    ) -> list[ParsedObservation]:
        buckets: dict[datetime, dict[str, Any]] = {}
        for point in points:
            observed_at = self._parse_datetime(point.get("time"))
            value = self._parse_decimal(point.get("value"))
            if observed_at is None or value is None:
                continue

            bucket_at = self._round_time(observed_at, interval_minutes)
            bucket = buckets.setdefault(
                bucket_at,
                {
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "points": [],
                },
            )
            bucket["high"] = max(bucket["high"], value)
            bucket["low"] = min(bucket["low"], value)
            bucket["close"] = value
            bucket["points"].append({"time": observed_at.isoformat(), "value": str(value), "raw": point.get("raw")})

        return [
            ParsedObservation(
                series_code=series_code,
                source_code=source_code,
                observed_at=observed_at,
                value_numeric=bar["close"],
                kind=ObservationKind.QUOTE,
                raw_payload={
                    "source": "exchangerates",
                    "open": str(bar["open"]),
                    "high": str(bar["high"]),
                    "low": str(bar["low"]),
                    "close": str(bar["close"]),
                    "points": bar["points"],
                },
            )
            for observed_at, bar in sorted(buckets.items())
        ]

    def _extract_points(self, payload: Any) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        self._walk_payload(payload, points)
        if not points:
            raise AdapterError("ExchangeRates AJAX JSON has no recognizable time/value points")
        return points

    def _walk_payload(self, item: Any, points: list[dict[str, Any]]) -> None:
        if isinstance(item, dict):
            point = self._dict_to_point(item)
            if point is not None:
                points.append(point)
                return
            for value in item.values():
                self._walk_payload(value, points)
            return

        if isinstance(item, list):
            point = self._list_to_point(item)
            if point is not None:
                points.append(point)
                return
            for value in item:
                self._walk_payload(value, points)

    def _dict_to_point(self, item: dict[str, Any]) -> dict[str, Any] | None:
        time_value = (
            item.get("time")
            or item.get("timestamp")
            or item.get("date")
            or item.get("datetime")
            or item.get("x")
            or item.get("label")
        )
        value = item.get("value") or item.get("price") or item.get("close") or item.get("y")
        if self._parse_datetime(time_value) is None or self._parse_decimal(value) is None:
            return None
        return {"time": time_value, "value": value, "raw": item}

    def _list_to_point(self, item: list[Any]) -> dict[str, Any] | None:
        if len(item) < 2:
            return None
        if self._parse_datetime(item[0]) is not None and self._parse_decimal(item[1]) is not None:
            return {"time": item[0], "value": item[1], "raw": item}
        if self._parse_datetime(item[1]) is not None and self._parse_decimal(item[0]) is not None:
            return {"time": item[1], "value": item[0], "raw": item}
        return None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp /= 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)

        raw = str(value).strip()
        if not raw:
            return None
        if raw.isdigit():
            return ExchangeRatesAdapter._parse_datetime(float(raw))

        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d %b %Y %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d %b %Y",
        ):
            try:
                parsed = datetime.strptime(raw.replace("Z", "+0000"), fmt)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    @staticmethod
    def _parse_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        try:
            return Decimal(str(value).strip().replace(",", ""))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _round_time(value: datetime, interval_minutes: int) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        interval_seconds = max(1, interval_minutes) * 60
        rounded = int(value.timestamp()) // interval_seconds * interval_seconds
        return datetime.fromtimestamp(rounded, tz=timezone.utc)

    def _parse_history_json_rows(self, payload: dict[str, Any], *, pair: str) -> list[dict[str, Any]]:
        if payload.get("ok") is False:
            raise AdapterError(f"ExchangeRates daily history returned ok=false: {payload}")

        raw_rows = payload.get("rows", [])
        if not isinstance(raw_rows, list):
            raise AdapterError("ExchangeRates daily history JSON has no rows list")

        rows: list[dict[str, Any]] = []
        for raw_row in raw_rows:
            if not isinstance(raw_row, dict):
                continue
            row_pair = str(raw_row.get("pair") or pair)
            if pair and pair not in row_pair:
                continue
            parsed_date = self._parse_datetime(raw_row.get("date"))
            close = self._parse_decimal(raw_row.get("close"))
            if parsed_date is None or close is None:
                continue
            rows.append(
                {
                    "date": parsed_date,
                    "pair": row_pair,
                    "open": self._parse_decimal(raw_row.get("open")),
                    "high": self._parse_decimal(raw_row.get("high")),
                    "low": self._parse_decimal(raw_row.get("low")),
                    "close": close,
                }
            )

        return rows

    def _history_rows_to_observations(
        self,
        rows: list[dict[str, Any]],
        *,
        source_code: str,
        series_code: str,
    ) -> list[ParsedObservation]:
        return [
            ParsedObservation(
                series_code=series_code,
                source_code=source_code,
                observed_at=row["date"],
                value_numeric=row["close"],
                kind=ObservationKind.QUOTE,
                raw_payload={
                    "source": "exchangerates",
                    "mode": "history_daily",
                    "pair": row.get("pair"),
                    "open": str(row["open"]) if row.get("open") is not None else None,
                    "high": str(row["high"]) if row.get("high") is not None else None,
                    "low": str(row["low"]) if row.get("low") is not None else None,
                    "close": str(row["close"]),
                },
            )
            for row in rows
        ]

    def _history_rows_to_table_observations(
        self,
        rows: list[dict[str, Any]],
        *,
        source_code: str,
        series_code: str,
        loaded_at: datetime,
    ) -> list[ObservationIn]:
        observations: list[ObservationIn] = []
        for row in rows:
            reference_start = row["date"].replace(hour=0, minute=0, second=0, microsecond=0)
            reference_end = reference_start + timedelta(days=1) - timedelta(microseconds=1)
            if reference_end > loaded_at:
                continue
            observations.append(
                ObservationIn(
                    series_code=series_code,
                    source_code=source_code,
                    reference_date=reference_start.date(),
                    reference_start=reference_start,
                    reference_end=reference_end,
                    value=row["close"],
                    published_at=reference_end,
                )
            )
        return observations

    def _live_observation_to_table(
        self,
        observation: ParsedObservation,
        *,
        interval_minutes: int,
        extra: dict[str, Any],
    ) -> ObservationIn:
        exchange_tz = ZoneInfo(str(extra.get("exchange_timezone", self.default_exchange_timezone)))
        reference_start = observation.observed_at
        reference_end = reference_start + timedelta(minutes=max(1, interval_minutes)) - timedelta(microseconds=1)
        published_at = observation.publication_at or datetime.now(timezone.utc)
        return ObservationIn(
            series_code=observation.series_code,
            source_code=observation.source_code,
            reference_date=reference_start.astimezone(exchange_tz).date(),
            reference_start=reference_start,
            reference_end=reference_end,
            value=observation.value_numeric,
            published_at=published_at,
            compress_equal_runs=bool(extra.get("compress_equal_runs", False)),
        )

    def _is_trading_session_time(self, local_value: datetime, extra: dict[str, Any]) -> bool:
        if not self._is_trading_session_weekday(local_value, extra):
            return False
        local_time = local_value.time()
        session_open = self._trading_session_open(extra)
        session_close = self._daily_session_close(extra)
        if session_open <= session_close:
            return session_open <= local_time <= session_close
        return local_time >= session_open or local_time <= session_close

    def _trading_session_open(self, extra: dict[str, Any]) -> time:
        return self._parse_time_config(
            extra,
            "trading_session_open_time",
            self.default_trading_session_open,
        )

    def _daily_session_close(self, extra: dict[str, Any]) -> time:
        return self._parse_time_config(
            extra,
            "daily_session_close_time",
            self.default_daily_session_close,
        )

    @staticmethod
    def _is_trading_session_weekday(local_value: datetime, extra: dict[str, Any]) -> bool:
        raw_days = extra.get("trading_session_weekdays")
        if raw_days is None:
            return True
        if isinstance(raw_days, str):
            days = [part.strip().lower() for part in raw_days.split(",")]
        else:
            days = [str(part).strip().lower() for part in raw_days]
        aliases = {
            "mon": 0,
            "monday": 0,
            "tue": 1,
            "tuesday": 1,
            "wed": 2,
            "wednesday": 2,
            "thu": 3,
            "thursday": 3,
            "fri": 4,
            "friday": 4,
            "sat": 5,
            "saturday": 5,
            "sun": 6,
            "sunday": 6,
        }
        allowed: set[int] = set()
        for day in days:
            if day in aliases:
                allowed.add(aliases[day])
            elif day:
                allowed.add(int(day))
        return local_value.weekday() in allowed

    @staticmethod
    def _parse_time_config(extra: dict[str, Any], key: str, default: time) -> time:
        raw_value = extra.get(key)
        if raw_value is None:
            return default
        try:
            hour, minute = str(raw_value).split(":", maxsplit=1)
            return time(hour=int(hour), minute=int(minute))
        except ValueError as exc:
            raise AdapterError(f"invalid {key}: {raw_value}") from exc

    @staticmethod
    async def _sleep_between_history_requests(delay_min_seconds: float, delay_max_seconds: float) -> None:
        await asyncio.sleep(random.uniform(delay_min_seconds, delay_max_seconds))

    def _parse_history_rows(self, html: str, *, pair: str) -> list[dict[str, Any]]:
        rows = self._parse_history_tables(html, pair=pair)
        if not rows:
            rows = self._parse_history_text(html, pair=pair)
        if not rows:
            raise AdapterError("ExchangeRates daily history HTML has no recognizable Date/Close rows")

        deduped: dict[datetime, dict[str, Any]] = {}
        for row in rows:
            deduped[row["date"]] = row
        return [deduped[date] for date in sorted(deduped)]

    def _parse_history_tables(self, html: str, *, pair: str) -> list[dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        rows: list[dict[str, Any]] = []
        for table in soup.select("table"):
            headers: list[str] = []
            for tr in table.select("tr"):
                cells = [cell.get_text(" ", strip=True) for cell in tr.select("th,td")]
                if not cells:
                    continue
                if tr.select("th"):
                    headers = [self._normalize_header(cell) for cell in cells]
                    continue
                row = self._history_cells_to_row(cells, headers=headers, pair=pair)
                if row is not None:
                    rows.append(row)
        return rows

    def _history_cells_to_row(self, cells: list[str], *, headers: list[str], pair: str) -> dict[str, Any] | None:
        if headers and {"date", "close"}.issubset(set(headers)):
            by_header = {header: cells[index] for index, header in enumerate(headers) if index < len(cells)}
            row_pair = by_header.get("pair") or pair
            if pair and pair not in row_pair:
                return None
            raw_date = by_header.get("date")
            close = self._parse_decimal(by_header.get("close"))
            parsed_date = self._parse_history_date(raw_date)
            if parsed_date is None or close is None:
                return None
            return {
                "date": parsed_date,
                "pair": row_pair,
                "open": self._parse_decimal(by_header.get("open")),
                "high": self._parse_decimal(by_header.get("high")),
                "low": self._parse_decimal(by_header.get("low")),
                "close": close,
            }

        if len(cells) < 6:
            return None
        joined = " ".join(cells)
        if pair and pair not in joined:
            return None
        parsed_date = self._parse_history_date(cells[0])
        close = self._parse_decimal(cells[5])
        if parsed_date is None or close is None:
            return None
        return {
            "date": parsed_date,
            "pair": pair,
            "open": self._parse_decimal(cells[2]),
            "high": self._parse_decimal(cells[3]),
            "low": self._parse_decimal(cells[4]),
            "close": close,
        }

    def _parse_history_text(self, html: str, *, pair: str) -> list[dict[str, Any]]:
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        escaped_pair = re.escape(pair)
        number = r"[+-]?\d[\d,]*(?:\.\d+)?"
        pattern = re.compile(
            rf"(?P<date>\d{{2}}/\d{{2}}/(?:\d{{4}}|\d{{2}}))\s+"
            rf"(?:\d{{2}}/\d{{2}}/(?:\d{{4}}|\d{{2}})\s+)?"
            rf"{escaped_pair}\s+"
            rf"(?P<open>{number})\s+"
            rf"(?P<high>{number})\s+"
            rf"(?P<low>{number})\s+"
            rf"(?P<close>{number})"
        )
        rows: list[dict[str, Any]] = []
        for match in pattern.finditer(text):
            parsed_date = self._parse_history_date(match.group("date"))
            close = self._parse_decimal(match.group("close"))
            if parsed_date is None or close is None:
                continue
            rows.append(
                {
                    "date": parsed_date,
                    "pair": pair,
                    "open": self._parse_decimal(match.group("open")),
                    "high": self._parse_decimal(match.group("high")),
                    "low": self._parse_decimal(match.group("low")),
                    "close": close,
                }
            )
        return rows

    def _parse_live_html(self, html: str) -> tuple[Decimal, datetime | None]:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        price = self._parse_live_price(text)
        if price is None:
            points = self._extract_points_from_text(html)
            if points:
                last_point = max(
                    points,
                    key=lambda point: self._parse_datetime(point.get("time")) or datetime.fromtimestamp(0, tz=timezone.utc),
                )
                parsed_at = self._parse_datetime(last_point.get("time"))
                value = self._parse_decimal(last_point.get("value"))
                if value is not None:
                    return value, parsed_at
            raise AdapterError("ExchangeRates live HTML has no recognizable current URALS price")

        return price, self._parse_live_timestamp(text)

    def _extract_points_from_text(self, text: str) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        array_pattern = re.compile(
            r"\[\s*(?P<time>\d{10,13})\s*,\s*(?P<value>[+-]?\d[\d,]*(?:\.\d+)?)\s*\]"
        )
        for match in array_pattern.finditer(text):
            points.append({"time": match.group("time"), "value": match.group("value"), "raw": match.group(0)})
        return points

    def _parse_live_price(self, text: str) -> Decimal | None:
        patterns = (
            r"URALS/USD\s+(?P<price>\d[\d,]*(?:\.\d+)?)",
            r"Urals Oil[^0-9]{0,80}(?P<price>\d[\d,]*(?:\.\d+)?)\s*(?:USD|Dollars)",
            r"(?P<price>\d[\d,]*(?:\.\d+)?)\s*Dollars",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._parse_decimal(match.group("price"))
                if value is not None:
                    return value
        return None

    def _parse_live_timestamp(self, text: str) -> datetime | None:
        patterns = (
            r"(?:updated|last updated)[^0-9]{0,40}(?P<date>\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?)",
            r"(?P<date>\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})\s+\d{1,2}:\d{2}(?::\d{2})?)",
        )
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                parsed = self._parse_datetime(match.group("date"))
                if parsed is not None:
                    return parsed
        return None

    def _page_headers(self, context: FetchContext, *, accept: str) -> dict[str, str]:
        spec = context.source.scrape
        extra = spec.extra if spec is not None else {}
        browser_user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.3.1 Safari/605.1.15"
        )
        headers = {
            "Accept": accept,
            "Accept-Language": "en-GB,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": str(extra.get("referer") or self.referer),
            "User-Agent": str(extra.get("user_agent") or browser_user_agent),
        }
        if bool(extra.get("browser_headers", True)):
            headers.update(
                {
                    "Priority": "u=3, i",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
            )
        cookie = str(extra.get("cookie") or context.settings.exchangerates_cookie or "")
        if cookie:
            headers["Cookie"] = cookie
        if spec is not None:
            headers.update(spec.headers)
        return headers

    def _raise_for_blocked_response(self, response: httpx.Response, label: str) -> None:
        if response.status_code == 403 and "cloudflare" in response.text.lower():
            raise AdapterError(
                f"{label} is blocked by Cloudflare; set EXCHANGERATES_COOKIE from a browser session or run from an allowed network"
            )
        response.raise_for_status()

    @staticmethod
    def _infer_mode(url: str) -> str:
        if "history" in url:
            return "history_daily"
        return "live_ajax"

    @staticmethod
    def _normalize_header(value: str) -> str:
        normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")
        if normalized in {"change_change", "change"}:
            return "change"
        if normalized in {"change_percent", "percent_change", "change_pct"}:
            return "percent_change"
        return normalized

    @staticmethod
    def _normalize_datetime(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    @staticmethod
    def _parse_history_date(value: Any) -> datetime | None:
        if value is None:
            return None
        raw = str(value).strip()
        match = re.search(r"\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})", raw)
        if not match:
            return None
        raw_date = match.group(0)
        for fmt in ("%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(raw_date, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_date_only(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raw = str(value).strip()
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        return None

    @classmethod
    def _date_windows(cls, start: date, end: date, max_range_years: int) -> list[tuple[date, date]]:
        if end < start:
            return []

        windows: list[tuple[date, date]] = []
        current = start
        while current <= end:
            window_end = min(cls._add_years(current, max(1, max_range_years)) - timedelta(days=1), end)
            windows.append((current, window_end))
            current = window_end + timedelta(days=1)
        return windows

    @staticmethod
    def _add_years(value: date, years: int) -> date:
        try:
            return value.replace(year=value.year + years)
        except ValueError:
            return value.replace(month=2, day=28, year=value.year + years)

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
