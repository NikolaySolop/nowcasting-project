from __future__ import annotations

import json
import re
import asyncio
import base64
import os
import random
import ssl
import string
import struct
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
    history_ws_host = "data.tradingview.com"
    history_ws_path = "/socket.io/websocket"
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

        backfill_interval = str(
            spec.extra.get("history_interval")
            or spec.extra.get("backfill_interval")
            or "1d"
        ).strip()
        history_delta = self._backfill_interval_to_timedelta(backfill_interval, interval_minutes)
        configured_start = spec.start_date
        if configured_start.tzinfo is None:
            configured_start = configured_start.replace(tzinfo=timezone.utc)

        last_observed = context.latest_observed_at_by_series.get(ticker)
        start_from = (
            configured_start
            if last_observed is None or last_observed < configured_start
            else last_observed + history_delta
        )
        if start_from.tzinfo is None:
            start_from = start_from.replace(tzinfo=timezone.utc)

        end_at = self._round_time(datetime.now(timezone.utc), interval_minutes)
        if start_from > end_at:
            return []

        history = await self._fetch_tradingview_history(
            ticker=ticker,
            source_code=context.source.source_code,
            headers=headers,
            start_from=start_from,
            end_at=end_at,
            interval_minutes=interval_minutes,
            backfill_interval=backfill_interval,
            extra=spec.extra,
            timeout_seconds=context.settings.request_timeout_seconds,
        )
        return history

    async def _fetch_tradingview_history(
        self,
        *,
        ticker: str,
        source_code: str,
        headers: dict[str, str],
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        backfill_interval: str,
        extra: dict[str, Any],
        timeout_seconds: float,
    ) -> list[RawObservationIn]:
        chart_session = self._new_session("cs")
        quote_session = self._new_session("qs")
        symbol_ref = "symbol_1"
        series_ref = "s1"
        tv_interval = self._to_tradingview_interval(backfill_interval, interval_minutes)
        page_size = max(1, int(extra.get("backfill_page_size", extra.get("backfill_bar_count", 5000))))
        max_pages = self._resolve_backfill_max_pages(
            extra=extra,
            page_size=page_size,
            start_from=start_from,
            end_at=end_at,
            backfill_interval=backfill_interval,
            interval_minutes=interval_minutes,
        )
        read_timeout = float(extra.get("history_timeout_seconds", timeout_seconds))

        reader, writer = await self._open_history_socket(headers=headers, timeout_seconds=read_timeout)
        bars_by_time: dict[datetime, dict[str, Any]] = {}
        try:
            symbol_payload = {
                "symbol": ticker,
                "adjustment": str(extra.get("adjustment", "splits")),
                "session": str(extra.get("history_session", "regular")),
            }
            await self._send_tradingview_messages(
                writer,
                [
                    ("set_auth_token", ["unauthorized_user_token"]),
                    ("chart_create_session", [chart_session, ""]),
                    ("quote_create_session", [quote_session]),
                    ("quote_add_symbols", [quote_session, ticker]),
                    ("quote_fast_symbols", [quote_session, ticker]),
                    (
                        "resolve_symbol",
                        [chart_session, symbol_ref, "=" + json.dumps(symbol_payload, separators=(",", ":"))],
                    ),
                    ("create_series", [chart_session, series_ref, series_ref, symbol_ref, tv_interval, page_size]),
                ],
            )

            pages_loaded = 0
            requested_more = False
            earliest_observed: datetime | None = None
            pending_payloads: list[str] = []
            while pages_loaded < max_pages:
                if not pending_payloads:
                    try:
                        pending_payloads = await self._read_tradingview_payloads(
                            reader,
                            writer,
                            timeout_seconds=read_timeout,
                        )
                    except TimeoutError:
                        if bars_by_time:
                            break
                        raise
                    if not pending_payloads:
                        break
                payload = pending_payloads.pop(0)

                message = json.loads(payload)
                method = message.get("m")
                params = message.get("p") or []
                if method in {"symbol_error", "series_error", "critical_error"}:
                    raise AdapterError(f"TradingView history error for {ticker}: {params}")
                if method != "timescale_update":
                    continue

                new_count = self._merge_history_bars(
                    payload=message,
                    series_ref=series_ref,
                    start_from=start_from,
                    end_at=end_at,
                    interval_minutes=interval_minutes,
                    bars_by_time=bars_by_time,
                )
                if bars_by_time:
                    current_earliest = min(bars_by_time)
                    if earliest_observed is None or current_earliest < earliest_observed:
                        earliest_observed = current_earliest

                if earliest_observed is not None and earliest_observed <= start_from:
                    break
                if new_count == 0 and requested_more:
                    break

                pages_loaded += 1
                if pages_loaded >= max_pages:
                    break
                requested_more = True
                await self._send_tradingview_messages(
                    writer,
                    [("request_more_data", [chart_session, series_ref, page_size])],
                )

            if (
                bool(extra.get("require_full_backfill", True))
                and bars_by_time
                and min(bars_by_time)
                > start_from + self._backfill_interval_to_timedelta(backfill_interval, interval_minutes)
            ):
                earliest = min(bars_by_time)
                raise AdapterError(
                    "TradingView history did not reach configured start_date for "
                    f"{ticker}: earliest={earliest.isoformat()}, start_date={start_from.isoformat()}. "
                    "Increase the history interval, or set scrape.extra.require_full_backfill=false "
                    "to allow partial history."
                )

            return [
                RawObservationIn(
                    series_code=ticker,
                    source_code=source_code,
                    observed_at=observed_at,
                    value_numeric=Decimal(str(bar["close"])),
                    kind=ObservationKind.QUOTE,
                    raw_payload={
                        "ticker": ticker,
                        "source": "tradingview_history",
                        "interval": tv_interval,
                        "open": bar.get("open"),
                        "high": bar.get("high"),
                        "low": bar.get("low"),
                        "close": bar.get("close"),
                        "volume": bar.get("volume"),
                    },
                )
                for observed_at, bar in sorted(bars_by_time.items())
            ]
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=2)
            except (TimeoutError, OSError):
                pass

    def _merge_history_bars(
        self,
        *,
        payload: dict[str, Any],
        series_ref: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        bars_by_time: dict[datetime, dict[str, Any]],
    ) -> int:
        params = payload.get("p") or []
        if len(params) < 2 or not isinstance(params[1], dict):
            return 0
        series = params[1].get(series_ref) or {}
        raw_bars = series.get("s") or []

        added = 0
        for raw_bar in raw_bars:
            values = raw_bar.get("v") if isinstance(raw_bar, dict) else None
            if not isinstance(values, list) or len(values) < 5:
                continue
            close = values[4]
            if close is None:
                continue
            observed_at = datetime.fromtimestamp(int(float(values[0])), tz=timezone.utc)
            observed_at = self._round_time(observed_at, interval_minutes)
            if observed_at < start_from or observed_at > end_at:
                continue
            if observed_at not in bars_by_time:
                added += 1
            bars_by_time[observed_at] = {
                "open": values[1] if len(values) > 1 else None,
                "high": values[2] if len(values) > 2 else None,
                "low": values[3] if len(values) > 3 else None,
                "close": close,
                "volume": values[5] if len(values) > 5 else None,
            }
        return added

    async def _open_history_socket(
        self,
        *,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        ssl_context = ssl.create_default_context()
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.history_ws_host, 443, ssl=ssl_context, server_hostname=self.history_ws_host),
            timeout=timeout_seconds,
        )
        websocket_key = base64.b64encode(os.urandom(16)).decode("ascii")
        request_headers = {
            "Host": self.history_ws_host,
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": websocket_key,
            "Sec-WebSocket-Version": "13",
            "Origin": "https://www.tradingview.com",
            "User-Agent": headers.get("User-Agent", "nowcast-ingestion/0.1"),
        }
        request = [f"GET {self.history_ws_path} HTTP/1.1"]
        request.extend(f"{key}: {value}" for key, value in request_headers.items())
        request.append("")
        request.append("")
        writer.write("\r\n".join(request).encode("ascii"))
        await writer.drain()

        response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=timeout_seconds)
        status_line = response.split(b"\r\n", 1)[0]
        if b" 101 " not in status_line:
            raise AdapterError(f"TradingView history websocket upgrade failed: {status_line.decode(errors='replace')}")
        return reader, writer

    async def _send_tradingview_messages(
        self,
        writer: asyncio.StreamWriter,
        messages: list[tuple[str, list[Any]]],
    ) -> None:
        for method, params in messages:
            await self._send_ws_text(writer, self._pack_tradingview_message(method, params))

    async def _read_tradingview_payloads(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout_seconds: float,
    ) -> list[str]:
        while True:
            text = await self._recv_ws_text(reader, writer, timeout_seconds=timeout_seconds)
            if text is None:
                return []
            payloads: list[str] = []
            for payload in self._iter_tradingview_payloads(text):
                if payload.startswith("~h~"):
                    await self._send_ws_text(writer, f"~m~{len(payload)}~m~{payload}")
                    continue
                payloads.append(payload)
            if payloads:
                return payloads

    async def _send_ws_text(self, writer: asyncio.StreamWriter, text: str) -> None:
        payload = text.encode("utf-8")
        writer.write(self._build_ws_frame(payload=payload, opcode=0x1, masked=True))
        await writer.drain()

    async def _send_ws_pong(self, writer: asyncio.StreamWriter, payload: bytes) -> None:
        writer.write(self._build_ws_frame(payload=payload, opcode=0xA, masked=True))
        await writer.drain()

    async def _recv_ws_text(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        timeout_seconds: float,
    ) -> str | None:
        header = await asyncio.wait_for(reader.readexactly(2), timeout=timeout_seconds)
        first_byte, second_byte = header
        opcode = first_byte & 0x0F
        length = second_byte & 0x7F
        if length == 126:
            length = struct.unpack("!H", await asyncio.wait_for(reader.readexactly(2), timeout=timeout_seconds))[0]
        elif length == 127:
            length = struct.unpack("!Q", await asyncio.wait_for(reader.readexactly(8), timeout=timeout_seconds))[0]

        mask = await asyncio.wait_for(reader.readexactly(4), timeout=timeout_seconds) if second_byte & 0x80 else b""
        payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout_seconds) if length else b""
        if mask:
            payload = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))

        if opcode == 0x8:
            return None
        if opcode == 0x9:
            await self._send_ws_pong(writer, payload)
            return ""
        if opcode != 0x1:
            return ""
        return payload.decode("utf-8", errors="replace")

    @staticmethod
    def _build_ws_frame(*, payload: bytes, opcode: int, masked: bool) -> bytes:
        header = bytearray([0x80 | opcode])
        mask_bit = 0x80 if masked else 0
        if len(payload) < 126:
            header.append(mask_bit | len(payload))
        elif len(payload) < 65536:
            header.append(mask_bit | 126)
            header.extend(struct.pack("!H", len(payload)))
        else:
            header.append(mask_bit | 127)
            header.extend(struct.pack("!Q", len(payload)))

        if not masked:
            return bytes(header) + payload

        mask = os.urandom(4)
        masked_payload = bytes(byte ^ mask[idx % 4] for idx, byte in enumerate(payload))
        return bytes(header) + mask + masked_payload

    @staticmethod
    def _pack_tradingview_message(method: str, params: list[Any]) -> str:
        payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
        return f"~m~{len(payload)}~m~{payload}"

    @staticmethod
    def _iter_tradingview_payloads(text: str):
        cursor = 0
        while cursor < len(text):
            if not text.startswith("~m~", cursor):
                break
            cursor += 3
            length_end = text.find("~m~", cursor)
            if length_end == -1:
                break
            try:
                length = int(text[cursor:length_end])
            except ValueError:
                break
            cursor = length_end + 3
            yield text[cursor : cursor + length]
            cursor += length

    @staticmethod
    def _new_session(prefix: str) -> str:
        suffix = "".join(random.choice(string.ascii_lowercase) for _ in range(12))
        return f"{prefix}_{suffix}"

    @staticmethod
    def _to_tradingview_interval(interval: str, interval_minutes: int) -> str:
        normalized = interval.strip().lower()
        if normalized.endswith("mo"):
            return f"{max(1, int(normalized[:-2]))}M"
        if normalized.endswith("m"):
            return str(max(1, int(normalized[:-1])))
        if normalized.endswith("h"):
            return str(max(1, int(normalized[:-1])) * 60)
        if normalized.endswith("d"):
            return f"{max(1, int(normalized[:-1]))}D"
        if normalized.endswith("w"):
            return f"{max(1, int(normalized[:-1]))}W"
        if normalized.isdigit():
            return normalized
        return str(max(1, interval_minutes))

    @classmethod
    def _resolve_backfill_max_pages(
        cls,
        *,
        extra: dict[str, Any],
        page_size: int,
        start_from: datetime,
        end_at: datetime,
        backfill_interval: str,
        interval_minutes: int,
    ) -> int:
        if "backfill_max_pages" in extra:
            return max(1, int(extra["backfill_max_pages"]))

        interval = cls._backfill_interval_to_timedelta(backfill_interval, interval_minutes)
        estimated_bars = max(1, int((end_at - start_from).total_seconds() // interval.total_seconds()) + 1)
        estimated_pages = (estimated_bars + page_size - 1) // page_size
        max_pages_cap = max(1, int(extra.get("backfill_max_pages_cap", 100)))
        return max(1, min(estimated_pages, max_pages_cap))

    @staticmethod
    def _backfill_interval_to_timedelta(interval: str, interval_minutes: int) -> timedelta:
        normalized = interval.strip().lower()
        if normalized.endswith("m") and not normalized.endswith("mo"):
            return timedelta(minutes=max(1, int(normalized[:-1])))
        if normalized.endswith("h"):
            return timedelta(hours=max(1, int(normalized[:-1])))
        if normalized.endswith("d"):
            return timedelta(days=max(1, int(normalized[:-1])))
        if normalized.endswith("w"):
            return timedelta(weeks=max(1, int(normalized[:-1])))
        if normalized.isdigit():
            return timedelta(minutes=max(1, int(normalized)))
        return timedelta(minutes=max(1, interval_minutes))

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
