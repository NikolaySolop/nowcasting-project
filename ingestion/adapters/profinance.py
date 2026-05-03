from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import ssl
import struct
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class ProFinanceAdapter(BaseAdapter):
    name = "profinance"

    current_urals_url = "https://www.profinance.ru/urals/"
    legacy_urals_url = "https://www.profinance.ru/chart/urals/"

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no ProFinance scrape spec")

        extra = spec.extra
        ticker = str(extra.get("ticker") or spec.series_code or "Urals Med").strip()
        series_code = str(spec.series_code or extra.get("series_code") or ticker).strip()
        interval_minutes = int(extra.get("interval_minutes", 15))
        end_at = self._round_time(datetime.now(timezone.utc), interval_minutes)
        start_from = self._resolve_start(context, series_code, interval_minutes)

        headers = {
            "User-Agent": context.settings.request_user_agent,
            "Accept": "text/html,application/json,text/plain;q=0.9,*/*;q=0.5",
        }
        headers.update(spec.headers)

        page_metadata: dict[str, Any] = {}
        if spec.url is not None and bool(extra.get("discover_page", True)):
            page_metadata = await self._fetch_page_metadata(
                urls=self._page_urls(str(spec.url), extra),
                headers=headers,
                timeout_seconds=context.settings.request_timeout_seconds,
            )
            ticker = str(extra.get("ticker") or page_metadata.get("ticker") or ticker).strip()

        observations = await self._fetch_history(
            context=context,
            ticker=ticker,
            series_code=series_code,
            start_from=start_from,
            end_at=end_at,
            interval_minutes=interval_minutes,
            headers=headers,
            page_metadata=page_metadata,
        )

        return FetchResult(
            observations=observations,
            raw_payload={
                "ticker": ticker,
                "series_code": series_code,
                "interval_minutes": interval_minutes,
                "start_from": start_from.isoformat(),
                "end_at": end_at.isoformat(),
                "page_metadata": page_metadata,
            },
        )

    async def _fetch_history(
        self,
        *,
        context: FetchContext,
        ticker: str,
        series_code: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        headers: dict[str, str],
        page_metadata: dict[str, Any],
    ) -> list[RawObservationIn]:
        spec = context.source.scrape
        if spec is None:
            return []

        extra = spec.extra
        rows: list[dict[str, Any]] = []

        history_url = extra.get("history_url")
        if history_url:
            rows.extend(
                await self._fetch_http_history(
                    history_url=str(history_url),
                    ticker=ticker,
                    start_from=start_from,
                    end_at=end_at,
                    interval_minutes=interval_minutes,
                    headers=headers,
                    timeout_seconds=context.settings.request_timeout_seconds,
                    extra=extra,
                )
            )

        ws_url = extra.get("websocket_url") or page_metadata.get("websocket_url")
        if ws_url:
            rows.extend(
                await self._fetch_websocket_history(
                    ws_url=str(ws_url),
                    ticker=ticker,
                    start_from=start_from,
                    end_at=end_at,
                    interval_minutes=interval_minutes,
                    headers=headers,
                    timeout_seconds=float(extra.get("websocket_timeout_seconds", context.settings.request_timeout_seconds)),
                    extra=extra,
                )
            )

        if not rows:
            raise AdapterError(
                "ProFinance returned no candle data. Configure scrape.extra.history_url "
                "or scrape.extra.websocket_url/messages for the current ProFinance chart protocol."
            )

        observations = self._rows_to_observations(
            rows=rows,
            source_code=context.source.source_code,
            series_code=series_code,
            start_from=start_from,
            end_at=end_at,
            interval_minutes=interval_minutes,
        )
        if not observations:
            return []

        if bool(extra.get("require_full_backfill", False)) and observations[0].observed_at > start_from + timedelta(
            minutes=interval_minutes
        ):
            raise AdapterError(
                "ProFinance history did not reach configured start_date: "
                f"earliest={observations[0].observed_at.isoformat()}, start_date={start_from.isoformat()}"
            )
        return observations

    async def _fetch_page_metadata(
        self,
        *,
        urls: list[str],
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            for url in urls:
                try:
                    response = await client.get(url, headers=headers)
                    response.raise_for_status()
                    break
                except httpx.HTTPError as exc:
                    last_error = exc
            else:
                raise AdapterError(f"unable to fetch ProFinance chart page: {last_error}")

        html = response.text
        settings = self._extract_chart_settings(html)
        script_urls = re.findall(r"""<script[^>]+src=["']([^"']+)["']""", html, flags=re.IGNORECASE)
        ws_script = next((item for item in script_urls if "/ws_" in item or item.startswith("ws_")), None)
        return {
            "url": str(response.url),
            "status_code": response.status_code,
            "ticker": settings.get("ticker"),
            "charts_path": settings.get("chartsPath"),
            "charts_url": settings.get("chartsUrl"),
            "tic_type": settings.get("ticType"),
            "ws_script": ws_script,
            "websocket_url": self._extract_websocket_url(html),
        }

    async def _fetch_http_history(
        self,
        *,
        history_url: str,
        ticker: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        headers: dict[str, str],
        timeout_seconds: float,
        extra: dict[str, Any],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        max_pages = max(1, int(extra.get("history_pages", 1)))
        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            for page in range(max_pages):
                url = self._format_template(
                    history_url,
                    ticker=ticker,
                    start_from=start_from,
                    end_at=end_at,
                    interval_minutes=interval_minutes,
                    page=page,
                )
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                rows.extend(self._extract_candle_rows(response.text, response.headers.get("content-type", "")))
        return rows

    async def _fetch_websocket_history(
        self,
        *,
        ws_url: str,
        ticker: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        headers: dict[str, str],
        timeout_seconds: float,
        extra: dict[str, Any],
    ) -> list[dict[str, Any]]:
        reader, writer = await self._open_websocket(
            ws_url=ws_url,
            origin=str(extra.get("origin") or "https://www.profinance.ru"),
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
        rows: list[dict[str, Any]] = []
        try:
            for message in self._initial_ws_messages(
                extra=extra,
                ticker=ticker,
                start_from=start_from,
                end_at=end_at,
                interval_minutes=interval_minutes,
            ):
                await self._send_ws_text(writer, message)

            max_messages = max(1, int(extra.get("websocket_max_messages", 200)))
            idle_limit = max(1, int(extra.get("websocket_idle_messages", 3)))
            idle_count = 0
            for _ in range(max_messages):
                try:
                    text = await self._recv_ws_text(reader, writer, timeout_seconds=timeout_seconds)
                except TimeoutError:
                    break
                if text is None:
                    break
                found = self._extract_candle_rows(text, "application/json")
                if found:
                    rows.extend(found)
                    idle_count = 0
                    if self._has_reached_start(rows, start_from):
                        break
                    continue

                idle_count += 1
                if idle_count >= idle_limit:
                    break
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=2)
            except (TimeoutError, OSError):
                pass
        return rows

    def _initial_ws_messages(
        self,
        *,
        extra: dict[str, Any],
        ticker: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
    ) -> list[str]:
        raw_messages = extra.get("websocket_messages") or extra.get("initial_messages") or []
        if isinstance(raw_messages, (str, dict)):
            raw_messages = [raw_messages]

        messages: list[str] = []
        for raw_message in raw_messages:
            if isinstance(raw_message, dict):
                message = json.dumps(raw_message, separators=(",", ":"))
            else:
                message = str(raw_message)
            messages.append(
                self._format_template(
                    message,
                    ticker=ticker,
                    start_from=start_from,
                    end_at=end_at,
                    interval_minutes=interval_minutes,
                    page=0,
                )
            )
        return messages

    def _rows_to_observations(
        self,
        *,
        rows: list[dict[str, Any]],
        source_code: str,
        series_code: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
    ) -> list[RawObservationIn]:
        rows_by_time: dict[datetime, dict[str, Any]] = {}
        for row in rows:
            observed_at = self._parse_datetime(row.get("time") or row.get("t") or row.get("date"))
            close = row.get("close") if row.get("close") is not None else row.get("c")
            value = self._parse_decimal(close)
            if observed_at is None or value is None:
                continue

            observed_at = self._round_time(observed_at, interval_minutes)
            if observed_at < start_from or observed_at > end_at:
                continue

            rows_by_time[observed_at] = {
                "open": row.get("open") if row.get("open") is not None else row.get("o"),
                "high": row.get("high") if row.get("high") is not None else row.get("h"),
                "low": row.get("low") if row.get("low") is not None else row.get("l"),
                "close": value,
                "volume": row.get("volume") if row.get("volume") is not None else row.get("v"),
                "raw": row,
            }

        return [
            RawObservationIn(
                series_code=series_code,
                source_code=source_code,
                observed_at=observed_at,
                value_numeric=bar["close"],
                kind=ObservationKind.QUOTE,
                raw_payload={
                    "source": "profinance",
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": str(bar["close"]),
                    "volume": bar["volume"],
                    "raw": bar["raw"],
                },
            )
            for observed_at, bar in sorted(rows_by_time.items())
        ]

    def _extract_candle_rows(self, payload: str, content_type: str) -> list[dict[str, Any]]:
        text = payload.strip()
        if not text:
            return []

        rows: list[dict[str, Any]] = []
        json_payloads = self._json_payloads(text)
        for item in json_payloads:
            rows.extend(self._walk_json_for_candles(item))

        if rows:
            return rows
        if "json" not in content_type.lower():
            return self._parse_csvish_rows(text)
        return []

    def _json_payloads(self, text: str) -> list[Any]:
        try:
            return [json.loads(text)]
        except json.JSONDecodeError:
            pass

        payloads: list[Any] = []
        for match in re.finditer(r"(\{.*?\}|\[.*?\])", text):
            try:
                payloads.append(json.loads(match.group(1)))
            except json.JSONDecodeError:
                continue
        return payloads

    def _walk_json_for_candles(self, item: Any) -> list[dict[str, Any]]:
        if isinstance(item, dict):
            row = self._dict_to_candle(item)
            if row is not None:
                return [row]
            rows: list[dict[str, Any]] = []
            for value in item.values():
                rows.extend(self._walk_json_for_candles(value))
            return rows

        if isinstance(item, list):
            row = self._list_to_candle(item)
            if row is not None:
                return [row]
            rows: list[dict[str, Any]] = []
            for value in item:
                rows.extend(self._walk_json_for_candles(value))
            return rows

        return []

    def _dict_to_candle(self, item: dict[str, Any]) -> dict[str, Any] | None:
        has_time = any(key in item for key in ("time", "t", "timestamp", "date", "datetime"))
        has_close = any(key in item for key in ("close", "c", "last", "price"))
        if not has_time or not has_close:
            return None
        time_value = item.get("time") or item.get("t") or item.get("timestamp") or item.get("date") or item.get("datetime")
        close_value = item.get("close") or item.get("c") or item.get("last") or item.get("price")
        return {
            "time": time_value,
            "open": item.get("open") or item.get("o"),
            "high": item.get("high") or item.get("h"),
            "low": item.get("low") or item.get("l"),
            "close": close_value,
            "volume": item.get("volume") or item.get("v"),
        }

    def _list_to_candle(self, item: list[Any]) -> dict[str, Any] | None:
        if len(item) < 5 or self._parse_datetime(item[0]) is None or self._parse_decimal(item[4]) is None:
            return None
        return {
            "time": item[0],
            "open": item[1],
            "high": item[2],
            "low": item[3],
            "close": item[4],
            "volume": item[5] if len(item) > 5 else None,
        }

    def _parse_csvish_rows(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            parts = [part.strip() for part in re.split(r"[;,\t]", line.strip()) if part.strip()]
            if len(parts) < 5:
                continue
            if self._parse_datetime(parts[0]) is None or self._parse_decimal(parts[4]) is None:
                continue
            rows.append(
                {
                    "time": parts[0],
                    "open": parts[1],
                    "high": parts[2],
                    "low": parts[3],
                    "close": parts[4],
                    "volume": parts[5] if len(parts) > 5 else None,
                }
            )
        return rows

    async def _open_websocket(
        self,
        *,
        ws_url: str,
        origin: str,
        headers: dict[str, str],
        timeout_seconds: float,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        parsed = urlparse(ws_url)
        if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
            raise AdapterError(f"invalid ProFinance websocket url: {ws_url}")

        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        ssl_context = ssl.create_default_context() if parsed.scheme == "wss" else None
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(parsed.hostname, port, ssl=ssl_context, server_hostname=parsed.hostname if ssl_context else None),
            timeout=timeout_seconds,
        )

        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        websocket_key = base64.b64encode(os.urandom(16)).decode("ascii")
        request_headers = {
            "Host": parsed.netloc,
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": websocket_key,
            "Sec-WebSocket-Version": "13",
            "Origin": origin,
            "User-Agent": headers.get("User-Agent", "nowcast-ingestion/0.1"),
        }
        request = [f"GET {path} HTTP/1.1"]
        request.extend(f"{key}: {value}" for key, value in request_headers.items())
        request.append("")
        request.append("")
        writer.write("\r\n".join(request).encode("ascii"))
        await writer.drain()

        response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=timeout_seconds)
        status_line = response.split(b"\r\n", 1)[0]
        if b" 101 " not in status_line:
            raise AdapterError(f"ProFinance websocket upgrade failed: {status_line.decode(errors='replace')}")
        return reader, writer

    async def _send_ws_text(self, writer: asyncio.StreamWriter, text: str) -> None:
        writer.write(self._build_ws_frame(payload=text.encode("utf-8"), opcode=0x1, masked=True))
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

    @classmethod
    def _resolve_start(cls, context: FetchContext, series_code: str, interval_minutes: int) -> datetime:
        spec = context.source.scrape
        configured_start = spec.start_date if spec else None
        last_observed = context.latest_observed_at_by_series.get(series_code)
        interval = timedelta(minutes=max(1, interval_minutes))

        if configured_start is not None and configured_start.tzinfo is None:
            configured_start = configured_start.replace(tzinfo=timezone.utc)
        if last_observed is not None and last_observed.tzinfo is None:
            last_observed = last_observed.replace(tzinfo=timezone.utc)

        if configured_start is None:
            start_from = (last_observed + interval) if last_observed else datetime.now(timezone.utc) - interval
        elif last_observed is None:
            start_from = configured_start
        else:
            start_from = max(configured_start, last_observed + interval)

        return cls._round_time(start_from, interval_minutes)

    @staticmethod
    def _page_urls(primary_url: str, extra: dict[str, Any]) -> list[str]:
        raw_urls = extra.get("page_urls") or []
        if isinstance(raw_urls, str):
            raw_urls = [raw_urls]

        urls = [primary_url]
        urls.extend(str(url) for url in raw_urls)
        urls.extend(
            [
                ProFinanceAdapter.current_urals_url,
                ProFinanceAdapter.legacy_urals_url,
                "https://profinance.broker-obzor.com/chart/urals/",
            ]
        )

        deduplicated: list[str] = []
        for url in urls:
            if url and url not in deduplicated:
                deduplicated.append(url)
        return deduplicated

    @staticmethod
    def _extract_chart_settings(html: str) -> dict[str, Any]:
        match = re.search(r"window\.chart_settings\s*=\s*\{(?P<body>.*?)\};", html, flags=re.DOTALL)
        if not match:
            return {}

        body = re.sub(r"//.*", "", match.group("body"))
        settings: dict[str, Any] = {}
        for key in ("ticker", "chartsPath", "chartsUrl"):
            key_match = re.search(rf'["\']?{key}["\']?\s*:\s*["\'](?P<value>[^"\']+)["\']', body)
            if key_match:
                settings[key] = key_match.group("value")
        tic_match = re.search(r'["\']?ticType["\']?\s*:\s*(?P<value>\d+)', body)
        if tic_match:
            settings["ticType"] = int(tic_match.group("value"))
        return settings

    @staticmethod
    def _extract_websocket_url(html: str) -> str | None:
        match = re.search(r"""wss?://[^"'\s<>)]+""", html)
        return match.group(0) if match else None

    @staticmethod
    def _format_template(
        template: str,
        *,
        ticker: str,
        start_from: datetime,
        end_at: datetime,
        interval_minutes: int,
        page: int,
    ) -> str:
        return template.format(
            ticker=ticker,
            ticker_url=quote_plus(ticker),
            start=int(start_from.timestamp()),
            end=int(end_at.timestamp()),
            start_ms=int(start_from.timestamp() * 1000),
            end_ms=int(end_at.timestamp() * 1000),
            interval_minutes=interval_minutes,
            page=page,
        )

    @staticmethod
    def _has_reached_start(rows: list[dict[str, Any]], start_from: datetime) -> bool:
        parsed_times = [
            observed_at
            for row in rows
            if (observed_at := ProFinanceAdapter._parse_datetime(row.get("time") or row.get("t") or row.get("date")))
            is not None
        ]
        return bool(parsed_times) and min(parsed_times) <= start_from

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
        if re.fullmatch(r"\d+(\.\d+)?", raw):
            return ProFinanceAdapter._parse_datetime(float(raw))

        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%d",
            "%d.%m.%Y",
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
        normalized = str(value).strip().replace(" ", "").replace(",", ".")
        try:
            return Decimal(normalized)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _round_time(value: datetime, interval_minutes: int) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        interval_seconds = max(1, interval_minutes) * 60
        rounded = int(value.timestamp()) // interval_seconds * interval_seconds
        return datetime.fromtimestamp(rounded, tz=timezone.utc)
