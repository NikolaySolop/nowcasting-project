from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class TradingViewAdapter(BaseAdapter):
    name = "tradingview"
    scanner_url = "https://scanner.tradingview.com/symbol"
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

        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, follow_redirects=True) as client:
            try:
                quote, response = await self._fetch_quote(client, ticker, headers, spec.extra)
                price = self._extract_quote_price(quote)
            except (AdapterError, httpx.HTTPError, json.JSONDecodeError) as exc:
                fallback_headers = {
                    **headers,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                }
                response = await client.get(str(spec.url), headers=fallback_headers, params=spec.params)
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

        response = await client.get(
            str(extra.get("scanner_url") or self.scanner_url),
            headers=headers,
            params={"symbol": ticker, "fields": str(fields)},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise AdapterError("TradingView scanner returned non-object payload")
        return payload, response

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
