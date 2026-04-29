from __future__ import annotations

import json
import re
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class TradingViewAdapter(BaseAdapter):
    name = "tradingview"
    scanner_url = "https://scanner.tradingview.com/symbol"
    scanner_url_fallback = "https://symbol-search.tradingview.com/symbol"
    scanner_fields = (
        "close",
        "change",
        "change_abs",
        "description",
        "exchange",
        "type",
        "update_mode",
    )

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None or spec.url is None:
            raise AdapterError(f"source {context.source.source_code} has no TradingView symbol url")

        ticker = str(spec.extra.get("ticker") or spec.series_code or "").strip()
        if not ticker:
            raise AdapterError("tradingview adapter requires scrape.extra.ticker or scrape.series_code")

        interval_minutes = int(spec.extra.get("interval_minutes", 15))
        headers = {
            "User-Agent": context.settings.request_user_agent,
            "Accept": "application/json,text/html;q=0.8,*/*;q=0.5",
        }
        headers.update(spec.headers)

        if bool(spec.extra.get("backfill_enabled", True)):
            history = await self._fetch_backfill(context, ticker, interval_minutes, headers)
            if history:
                return FetchResult(observations=history)

        max_retries = int(spec.extra.get("max_retries", 3))
        retry_delay_seconds = float(spec.extra.get("retry_delay_seconds", 0.6))
        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            try:
                quote, response = await self._fetch_quote(client, ticker, headers, spec.extra)
                price = self._extract_quote_price(quote)
            except (AdapterError, httpx.HTTPError, json.JSONDecodeError) as exc:
                fallback_headers = {
                    **headers,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }
                response = await self._request_with_retries(
                    client,
                    str(spec.url),
                    headers=fallback_headers,
                    params=spec.params,
                    max_retries=max_retries,
                    retry_delay_seconds=retry_delay_seconds,
                )
                response.raise_for_status()
                price = self._extract_price(response.text)
                quote = {
                    "close": str(price),
                    "fallback": "symbol_page",
                    "scanner_error": str(exc),
                }

        observed_at = self._round_time(datetime.now(timezone.utc), interval_minutes)
        last_observed = context.latest_observed_at_by_series.get(ticker)
        if last_observed is not None and observed_at <= last_observed:
            return FetchResult(observations=[])

        return FetchResult(
            observations=[
                RawObservationIn(
                    series_code=ticker,
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    value_numeric=price,
                    kind=ObservationKind.QUOTE,
                    raw_payload={
                        "url": str(response.url),
                        "ticker": ticker,
                        "interval_minutes": interval_minutes,
                        "quote": quote,
                    },
                )
            ],
            raw_payload={"url": str(response.url), "status_code": response.status_code},
        )

    async def _fetch_backfill(
        self,
        context: FetchContext,
        ticker: str,
        interval_minutes: int,
        headers: dict[str, str],
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None or spec.start_date is None:
            return []

        last_observed = context.latest_observed_at_by_series.get(ticker)
        start_from = spec.start_date if last_observed is None else last_observed + timedelta(minutes=interval_minutes)
        if start_from.tzinfo is None:
            start_from = start_from.replace(tzinfo=timezone.utc)

        end_at = self._round_time(datetime.now(timezone.utc), interval_minutes)
        if start_from > end_at:
            return []

        backfill_interval = str(spec.extra.get("backfill_interval") or f"{interval_minutes}m").strip().lower()
        interval_timedelta = self._interval_to_timedelta(backfill_interval)

        # Yahoo chart intraday retention is limited and older ranges return 422.
        # Clamp start date only when fetching intraday ranges so ingestion can continue.
        max_lookback_days = self._max_intraday_lookback_days_for_interval(backfill_interval)
        if max_lookback_days is not None:
            min_supported = end_at - timedelta(days=max_lookback_days)
            if start_from < min_supported:
                start_from = min_supported

        yahoo_symbol = str(spec.extra.get("yahoo_symbol") or "").strip()
        if not yahoo_symbol:
            yahoo_symbol = self._to_yahoo_symbol(ticker)
        if not yahoo_symbol:
            return []

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
        # Yahoo Chart API limits intraday intervals (e.g., 15m) to a relatively short period.
        # Pulling data in chunks allows long-range historical backfill from start_date.
        chunk_span = timedelta(days=int(spec.extra.get("backfill_chunk_days", 59)))
        observations: list[RawObservationIn] = []
        cursor = start_from

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            while cursor <= end_at:
                chunk_end = min(end_at, cursor + chunk_span)
                params = {
                    "interval": backfill_interval,
                    "period1": str(int(cursor.timestamp())),
                    "period2": str(int((chunk_end + interval_timedelta).timestamp())),
                    "includePrePost": "false",
                    "events": "div,splits",
                }
                response = await client.get(url, params=params, headers=headers)
                # Some symbols/ranges return 422 even within nominal limits.
                # Skip backfill on this chunk and let normal quote flow continue.
                if response.status_code == 422:
                    break
                response.raise_for_status()
                payload = response.json()

                result = payload.get("chart", {}).get("result", [])
                if not result:
                    cursor = chunk_end + interval_timedelta
                    continue

                entry = result[0]
                timestamps = entry.get("timestamp") or []
                closes = (((entry.get("indicators") or {}).get("quote") or [{}])[0]).get("close") or []
                for idx, ts in enumerate(timestamps):
                    if idx >= len(closes):
                        break
                    close = closes[idx]
                    if close is None:
                        continue
                    observed_at = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    if observed_at < start_from or observed_at > end_at:
                        continue
                    observations.append(
                        RawObservationIn(
                            series_code=ticker,
                            source_code=context.source.source_code,
                            observed_at=self._round_time(observed_at, interval_minutes),
                            value_numeric=Decimal(str(close)),
                            kind=ObservationKind.QUOTE,
                            raw_payload={"ticker": ticker, "yahoo_symbol": yahoo_symbol, "source": "yahoo_chart"},
                        )
                    )

                cursor = chunk_end + interval_timedelta

        return observations

    @staticmethod
    def _max_intraday_lookback_days_for_interval(interval: str) -> int | None:
        normalized = interval.strip().lower()
        if normalized.endswith("m"):
            try:
                minutes = int(normalized[:-1])
            except ValueError:
                return None
            if minutes < 60:
                return 60
            if minutes < 24 * 60:
                return 730
        return None

    @staticmethod
    def _interval_to_timedelta(interval: str) -> timedelta:
        normalized = interval.strip().lower()
        if normalized.endswith("m"):
            return timedelta(minutes=max(1, int(normalized[:-1])))
        if normalized.endswith("h"):
            return timedelta(hours=max(1, int(normalized[:-1])))
        if normalized.endswith("d"):
            return timedelta(days=max(1, int(normalized[:-1])))
        return timedelta(minutes=1)

    @staticmethod
    def _to_yahoo_symbol(ticker: str) -> str | None:
        normalized = ticker.split(":")[-1].upper()
        if len(normalized) == 6 and normalized.isalpha():
            return f"{normalized}=X"
        return None

    async def _fetch_quote(
        self,
        client: httpx.AsyncClient,
        ticker: str,
        headers: dict[str, str],
        extra: dict[str, Any],
    ) -> tuple[dict[str, Any], httpx.Response]:
        fields = extra.get("fields", self.scanner_fields)
        if isinstance(fields, (list, tuple)):
            fields = ",".join(str(field) for field in fields)

        max_retries = int(extra.get("max_retries", 3))
        retry_delay_seconds = float(extra.get("retry_delay_seconds", 0.6))
        scanner_urls = [
            str(extra.get("scanner_url") or self.scanner_url),
            str(extra.get("scanner_url_fallback") or self.scanner_url_fallback),
        ]

        last_error: Exception | None = None
        for scanner_url in scanner_urls:
            try:
                response = await self._request_with_retries(
                    client,
                    scanner_url,
                    headers=headers,
                    params={"symbol": ticker, "fields": str(fields)},
                    max_retries=max_retries,
                    retry_delay_seconds=retry_delay_seconds,
                )
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise AdapterError("TradingView scanner returned non-object payload")
                return payload, response
            except (AdapterError, httpx.HTTPError, json.JSONDecodeError) as exc:
                last_error = exc

        raise AdapterError(f"unable to fetch TradingView quote from scanner endpoints: {last_error}")

    async def _request_with_retries(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        headers: dict[str, str],
        params: dict[str, Any] | None,
        max_retries: int,
        retry_delay_seconds: float,
    ) -> httpx.Response:
        attempts = max(1, max_retries)
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code >= 500 or response.status_code == 429:
                    response.raise_for_status()
                return response
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt == attempts:
                    break
                await asyncio.sleep(retry_delay_seconds * attempt)

        if last_error is None:
            raise AdapterError("request failed without explicit error")
        raise last_error

    def _extract_quote_price(self, payload: dict[str, Any]) -> Decimal:
        candidate = payload.get("close")
        if candidate is None:
            raise AdapterError("TradingView scanner response has no close price")
        try:
            return Decimal(str(candidate))
        except InvalidOperation as exc:
            raise AdapterError(f"invalid TradingView close price: {candidate}") from exc

    def _extract_price(self, html: str) -> Decimal:
        ld_json = re.search(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S)
        if ld_json:
            try:
                payload = json.loads(ld_json.group(1))
                candidate = payload.get("offers", {}).get("price") if isinstance(payload, dict) else None
                if candidate is not None:
                    return Decimal(str(candidate))
            except (json.JSONDecodeError, InvalidOperation):
                pass

        regexes = [
            r'"last"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"price"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'data-last-price="([0-9]+(?:\.[0-9]+)?)"',
        ]
        for pattern in regexes:
            match = re.search(pattern, html)
            if match:
                try:
                    return Decimal(match.group(1))
                except InvalidOperation:
                    continue

        raise AdapterError("unable to parse TradingView price from page")

    @staticmethod
    def _round_time(value: datetime, interval_minutes: int) -> datetime:
        if interval_minutes <= 0:
            return value
        discard = timedelta(
            minutes=value.minute % interval_minutes,
            seconds=value.second,
            microseconds=value.microsecond,
        )
        return value - discard
