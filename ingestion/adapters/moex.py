from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


class MoexAdapter(BaseAdapter):
    name = "moex"

    iss_base_url = "https://iss.moex.com/iss"
    default_exchange_timezone = "Europe/Moscow"
    default_session_anchor = "09:50:00"

    instrument_defaults: dict[str, dict[str, Any]] = {
        "CNYRUB_TOM": {
            "secid": "CNYRUB_TOM",
            "engine": "currency",
            "market": "selt",
            "board": "CETS",
        },
        "RUBCNY": {
            "secid": "CNYRUB_TOM",
            "engine": "currency",
            "market": "selt",
            "board": "CETS",
        },
        "IMOEX": {
            "secid": "IMOEX",
            "engine": "stock",
            "market": "index",
            "board": "SNDX",
        },
        "MOEX": {
            "secid": "IMOEX",
            "engine": "stock",
            "market": "index",
            "board": "SNDX",
        },
        "RTSI": {
            "secid": "RTSI",
            "engine": "stock",
            "market": "index",
            "board": "SNDX",
        },
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no MOEX scrape spec")

        extra = spec.extra or {}
        mode = str(extra.get("mode") or "history_daily").strip().lower()
        instruments = self._resolve_instruments(context)
        headers = {
            "User-Agent": context.settings.request_user_agent,
            "Accept": "application/json,text/plain;q=0.9,*/*;q=0.5",
        }
        headers.update(spec.headers)

        observations: list[RawObservationIn] = []
        raw_payload: dict[str, Any] = {"mode": mode, "instruments": []}
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            for instrument in instruments:
                if mode in {"history_daily", "daily", "history"}:
                    instrument_observations, metadata = await self._fetch_daily_history(
                        client=client,
                        context=context,
                        instrument=instrument,
                        headers=headers,
                    )
                elif mode in {"online_15m", "candles_15m", "intraday_15m", "15m"}:
                    instrument_observations, metadata = await self._fetch_online_15m(
                        client=client,
                        context=context,
                        instrument=instrument,
                        headers=headers,
                    )
                else:
                    raise AdapterError(f"unsupported MOEX mode: {mode}")

                observations.extend(instrument_observations)
                raw_payload["instruments"].append(metadata)

        return FetchResult(observations=observations, raw_payload=raw_payload)

    async def _fetch_daily_history(
        self,
        *,
        client: httpx.AsyncClient,
        context: FetchContext,
        instrument: dict[str, Any],
        headers: dict[str, str],
    ) -> tuple[list[RawObservationIn], dict[str, Any]]:
        spec = context.source.scrape
        if spec is None:
            return [], {}

        exchange_tz = self._exchange_timezone(instrument)
        start_from = self._daily_start(context, instrument)
        till = self._resolve_end_datetime(instrument.get("till") or instrument.get("to"))
        if till is None:
            till = datetime.now(exchange_tz)

        rows = await self._fetch_candle_rows(
            client=client,
            instrument=instrument,
            interval=24,
            from_value=start_from.date().isoformat(),
            till_value=till.date().isoformat(),
            headers=headers,
        )
        observations = self._rows_to_observations(
            rows=rows,
            context=context,
            instrument=instrument,
            interval_minutes=24 * 60,
            exchange_tz=exchange_tz,
            daily=True,
        )
        return observations, {
            "series_code": instrument["series_code"],
            "secid": instrument["secid"],
            "interval": 24,
            "from": start_from.date().isoformat(),
            "till": till.date().isoformat(),
            "rows": len(rows),
            "observations": len(observations),
        }

    async def _fetch_online_15m(
        self,
        *,
        client: httpx.AsyncClient,
        context: FetchContext,
        instrument: dict[str, Any],
        headers: dict[str, str],
    ) -> tuple[list[RawObservationIn], dict[str, Any]]:
        exchange_tz = self._exchange_timezone(instrument)
        interval_minutes = int(instrument.get("interval_minutes") or 15)
        if interval_minutes != 15:
            raise AdapterError("MOEX online mode currently supports only 15-minute aggregation")

        start_from = self._intraday_start(context, instrument, interval_minutes)
        now_utc = datetime.now(timezone.utc)
        safety_delay = timedelta(seconds=float(instrument.get("aggregation_delay_seconds", 90)))
        complete_before = now_utc - safety_delay
        till = self._resolve_end_datetime(instrument.get("till") or instrument.get("to")) or now_utc

        rows = await self._fetch_candle_rows(
            client=client,
            instrument=instrument,
            interval=1,
            from_value=start_from.astimezone(exchange_tz).date().isoformat(),
            till_value=till.astimezone(exchange_tz).date().isoformat(),
            headers=headers,
        )
        bars = self._aggregate_minute_rows(
            rows=rows,
            instrument=instrument,
            exchange_tz=exchange_tz,
            interval_minutes=interval_minutes,
            start_from=start_from,
            complete_before=complete_before,
        )
        observations = self._bars_to_observations(
            bars=bars,
            context=context,
            instrument=instrument,
            interval_minutes=interval_minutes,
        )
        return observations, {
            "series_code": instrument["series_code"],
            "secid": instrument["secid"],
            "interval": "1m_aggregated_to_15m",
            "from": start_from.isoformat(),
            "till": till.isoformat(),
            "minute_rows": len(rows),
            "observations": len(observations),
        }

    async def _fetch_candle_rows(
        self,
        *,
        client: httpx.AsyncClient,
        instrument: dict[str, Any],
        interval: int,
        from_value: str,
        till_value: str,
        headers: dict[str, str],
    ) -> list[dict[str, Any]]:
        url = self._candles_url(instrument)
        start = 0
        max_pages = int(instrument.get("max_pages") or 200)
        max_rows = int(instrument.get("max_rows") or 100_000)
        rows: list[dict[str, Any]] = []

        for _ in range(max_pages):
            params = {
                "from": from_value,
                "till": till_value,
                "interval": str(interval),
                "iss.meta": "off",
                "start": str(start),
            }
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            page_rows = self._table_rows(payload, "candles")
            if not page_rows:
                break

            rows.extend(page_rows)
            if len(rows) >= max_rows:
                rows = rows[:max_rows]
                break
            start += len(page_rows)

        return rows

    def _rows_to_observations(
        self,
        *,
        rows: list[dict[str, Any]],
        context: FetchContext,
        instrument: dict[str, Any],
        interval_minutes: int,
        exchange_tz: ZoneInfo,
        daily: bool,
    ) -> list[RawObservationIn]:
        observations: list[RawObservationIn] = []
        latest = context.latest_observed_at_by_series.get(instrument["series_code"])

        for row in rows:
            close = self._parse_decimal(row.get("close"))
            if close is None:
                continue

            begin = self._parse_moex_datetime(row.get("begin"), exchange_tz)
            end = self._parse_moex_datetime(row.get("end"), exchange_tz)
            if begin is None:
                continue

            if daily:
                observed_at = datetime.combine(begin.astimezone(exchange_tz).date(), time.min, tzinfo=timezone.utc)
            else:
                observed_at = begin

            if latest is not None and observed_at <= latest:
                continue

            observations.append(
                RawObservationIn(
                    series_code=instrument["series_code"],
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=begin,
                    period_end=end,
                    value_numeric=self._transform_value(close, instrument),
                    kind=ObservationKind.QUOTE,
                    raw_payload=self._raw_candle_payload(
                        row=row,
                        instrument=instrument,
                        interval_minutes=interval_minutes,
                        source="moex_iss",
                    ),
                )
            )

        return observations

    def _aggregate_minute_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        instrument: dict[str, Any],
        exchange_tz: ZoneInfo,
        interval_minutes: int,
        start_from: datetime,
        complete_before: datetime,
    ) -> dict[datetime, dict[str, Any]]:
        bars: dict[datetime, dict[str, Any]] = {}
        anchor = self._session_anchor(instrument)

        for row in rows:
            begin = self._parse_moex_datetime(row.get("begin"), exchange_tz)
            if begin is None:
                continue

            bucket_start = self._bucket_start(begin, interval_minutes, exchange_tz, anchor)
            bucket_end = bucket_start + timedelta(minutes=interval_minutes) - timedelta(seconds=1)
            if bucket_start < start_from or bucket_end > complete_before:
                continue

            open_value = self._parse_decimal(row.get("open"))
            high = self._parse_decimal(row.get("high"))
            low = self._parse_decimal(row.get("low"))
            close = self._parse_decimal(row.get("close"))
            value = self._parse_decimal(row.get("value"))
            volume = self._parse_decimal(row.get("volume"))
            if open_value is None or high is None or low is None or close is None:
                continue

            bar = bars.setdefault(
                bucket_start,
                {
                    "open": open_value,
                    "high": high,
                    "low": low,
                    "close": close,
                    "value": Decimal("0"),
                    "volume": Decimal("0"),
                    "begin": bucket_start,
                    "end": bucket_end,
                    "minute_count": 0,
                    "raw_rows": [],
                },
            )
            bar["high"] = max(bar["high"], high)
            bar["low"] = min(bar["low"], low)
            bar["close"] = close
            bar["value"] += value or Decimal("0")
            bar["volume"] += volume or Decimal("0")
            bar["minute_count"] += 1
            bar["raw_rows"].append(row)

        return bars

    def _bars_to_observations(
        self,
        *,
        bars: dict[datetime, dict[str, Any]],
        context: FetchContext,
        instrument: dict[str, Any],
        interval_minutes: int,
    ) -> list[RawObservationIn]:
        latest = context.latest_observed_at_by_series.get(instrument["series_code"])
        observations: list[RawObservationIn] = []

        for observed_at, bar in sorted(bars.items()):
            if latest is not None and observed_at <= latest:
                continue
            ohlc = self._transform_ohlc(
                open_value=bar["open"],
                high=bar["high"],
                low=bar["low"],
                close=bar["close"],
                instrument=instrument,
            )
            observations.append(
                RawObservationIn(
                    series_code=instrument["series_code"],
                    source_code=context.source.source_code,
                    observed_at=observed_at,
                    period_start=bar["begin"],
                    period_end=bar["end"],
                    value_numeric=self._transform_value(bar["close"], instrument),
                    kind=ObservationKind.QUOTE,
                    raw_payload={
                        "source": "moex_iss_1m_aggregate",
                        "secid": instrument["secid"],
                        "engine": instrument["engine"],
                        "market": instrument["market"],
                        "board": instrument["board"],
                        "interval_minutes": interval_minutes,
                        "open": self._format_decimal(ohlc["open"]),
                        "high": self._format_decimal(ohlc["high"]),
                        "low": self._format_decimal(ohlc["low"]),
                        "close": self._format_decimal(ohlc["close"]),
                        "value": str(bar["value"]),
                        "volume": str(bar["volume"]),
                        "minute_count": bar["minute_count"],
                    },
                )
            )

        return observations

    def _resolve_instruments(self, context: FetchContext) -> list[dict[str, Any]]:
        spec = context.source.scrape
        if spec is None:
            return []

        base_from_url = self._parse_iss_url(str(spec.url)) if spec.url is not None else {}
        extra = dict(spec.extra or {})
        raw_instruments = extra.get("instruments")

        if isinstance(raw_instruments, list) and raw_instruments:
            instruments = [item for item in raw_instruments if isinstance(item, dict)]
        else:
            instruments = [
                {
                    "series_code": spec.series_code
                    or extra.get("series_code")
                    or (context.source.series[0].series_code if context.source.series else context.source.source_code),
                    **extra,
                }
            ]

        resolved: list[dict[str, Any]] = []
        for raw in instruments:
            item = dict(base_from_url)
            item.update({key: value for key, value in extra.items() if key != "instruments"})
            item.update(raw)

            series_code = str(item.get("series_code") or item.get("secid") or "").strip()
            secid = str(item.get("secid") or series_code).strip()
            defaults = self.instrument_defaults.get(secid.upper()) or self.instrument_defaults.get(series_code.upper()) or {}
            merged = dict(defaults)
            merged.update({key: value for key, value in item.items() if value is not None})
            merged["series_code"] = series_code or str(merged.get("secid") or "").strip()
            merged["secid"] = str(merged.get("secid") or merged["series_code"]).strip()

            for required in ("engine", "market", "board", "secid", "series_code"):
                if not merged.get(required):
                    raise AdapterError(f"MOEX instrument requires {required}: {merged}")
            resolved.append(merged)

        return resolved

    def _candles_url(self, instrument: dict[str, Any]) -> str:
        return (
            f"{str(instrument.get('iss_base_url') or self.iss_base_url).rstrip('/')}"
            f"/engines/{instrument['engine']}"
            f"/markets/{instrument['market']}"
            f"/boards/{instrument['board']}"
            f"/securities/{instrument['secid']}/candles.json"
        )

    @staticmethod
    def _parse_iss_url(url: str) -> dict[str, str]:
        parts = [part for part in urlparse(url).path.split("/") if part]
        result: dict[str, str] = {}
        for key in ("engines", "markets", "boards", "securities"):
            if key in parts and parts.index(key) + 1 < len(parts):
                value = parts[parts.index(key) + 1]
                if value and not value.endswith(".json"):
                    result[key[:-1] if key != "securities" else "secid"] = value
        return result

    @staticmethod
    def _table_rows(payload: dict[str, Any], table_name: str) -> list[dict[str, Any]]:
        table = payload.get(table_name)
        if not isinstance(table, dict):
            raise AdapterError(f"MOEX response has no {table_name} table")
        columns = table.get("columns")
        data = table.get("data")
        if not isinstance(columns, list) or not isinstance(data, list):
            raise AdapterError(f"MOEX {table_name} table has invalid shape")
        return [dict(zip(columns, row)) for row in data if isinstance(row, list)]

    def _daily_start(self, context: FetchContext, instrument: dict[str, Any]) -> datetime:
        spec = context.source.scrape
        explicit = self._resolve_end_datetime(instrument.get("from") or instrument.get("start_date"))
        if explicit is not None:
            return explicit

        latest = context.latest_observed_at_by_series.get(instrument["series_code"])
        if latest is not None:
            return latest + timedelta(days=1)

        if spec is not None and spec.start_date is not None:
            return spec.start_date if spec.start_date.tzinfo else spec.start_date.replace(tzinfo=timezone.utc)

        return datetime.now(timezone.utc) - timedelta(days=int(instrument.get("lookback_days") or 30))

    def _intraday_start(
        self,
        context: FetchContext,
        instrument: dict[str, Any],
        interval_minutes: int,
    ) -> datetime:
        spec = context.source.scrape
        latest = context.latest_observed_at_by_series.get(instrument["series_code"])
        max_latest_age_days = int(instrument.get("max_latest_age_days") or 30)
        now_utc = datetime.now(timezone.utc)
        if latest is not None and latest >= now_utc - timedelta(days=max_latest_age_days):
            return latest + timedelta(minutes=interval_minutes)

        explicit = self._resolve_end_datetime(instrument.get("from") or instrument.get("start_date"))
        if explicit is not None and bool(instrument.get("backfill_from_start", False)):
            return explicit

        lookback_days = int(instrument.get("initial_lookback_days") or 5)
        start_from = now_utc - timedelta(days=lookback_days)
        if spec is not None and spec.start_date is not None and bool(instrument.get("backfill_from_start", False)):
            configured = spec.start_date if spec.start_date.tzinfo else spec.start_date.replace(tzinfo=timezone.utc)
            start_from = configured
        return start_from

    @staticmethod
    def _resolve_end_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        raw = str(value).strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y"):
            try:
                parsed = datetime.strptime(raw.replace("Z", "+0000"), fmt)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _exchange_timezone(self, instrument: dict[str, Any]) -> ZoneInfo:
        timezone_name = str(instrument.get("timezone") or self.default_exchange_timezone)
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise AdapterError(f"unknown MOEX timezone: {timezone_name}") from exc

    def _session_anchor(self, instrument: dict[str, Any]) -> time:
        raw = str(instrument.get("session_anchor_time") or self.default_session_anchor)
        try:
            return time.fromisoformat(raw)
        except ValueError as exc:
            raise AdapterError(f"invalid MOEX session_anchor_time: {raw}") from exc

    @staticmethod
    def _parse_moex_datetime(value: Any, exchange_tz: ZoneInfo) -> datetime | None:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.replace(tzinfo=exchange_tz).astimezone(timezone.utc)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=exchange_tz)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _bucket_start(value: datetime, interval_minutes: int, exchange_tz: ZoneInfo, anchor: time) -> datetime:
        local = value.astimezone(exchange_tz)
        anchor_at = datetime.combine(local.date(), anchor, tzinfo=exchange_tz)
        if local < anchor_at:
            anchor_at = datetime.combine(local.date(), time.min, tzinfo=exchange_tz)
        minutes = int((local - anchor_at).total_seconds() // 60)
        bucket = anchor_at + timedelta(minutes=(minutes // interval_minutes) * interval_minutes)
        return bucket.astimezone(timezone.utc)

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
    def _transform_value(value: Decimal, instrument: dict[str, Any]) -> Decimal:
        if bool(instrument.get("invert", False)):
            if value == 0:
                raise AdapterError("cannot invert zero MOEX price")
            return Decimal("1") / value
        return value

    def _transform_ohlc(
        self,
        *,
        open_value: Decimal | None,
        high: Decimal | None,
        low: Decimal | None,
        close: Decimal | None,
        instrument: dict[str, Any],
    ) -> dict[str, Decimal | None]:
        if not bool(instrument.get("invert", False)):
            return {"open": open_value, "high": high, "low": low, "close": close}
        return {
            "open": self._transform_value(open_value, instrument) if open_value is not None else None,
            "high": self._transform_value(low, instrument) if low is not None else None,
            "low": self._transform_value(high, instrument) if high is not None else None,
            "close": self._transform_value(close, instrument) if close is not None else None,
        }

    @staticmethod
    def _format_decimal(value: Decimal | None) -> str | None:
        return str(value) if value is not None else None

    def _raw_candle_payload(
        self,
        *,
        row: dict[str, Any],
        instrument: dict[str, Any],
        interval_minutes: int,
        source: str,
    ) -> dict[str, Any]:
        open_value = self._parse_decimal(row.get("open"))
        high = self._parse_decimal(row.get("high"))
        low = self._parse_decimal(row.get("low"))
        close = self._parse_decimal(row.get("close"))
        ohlc = self._transform_ohlc(
            open_value=open_value,
            high=high,
            low=low,
            close=close,
            instrument=instrument,
        )
        return {
            "source": source,
            "secid": instrument["secid"],
            "engine": instrument["engine"],
            "market": instrument["market"],
            "board": instrument["board"],
            "interval_minutes": interval_minutes,
            "open": self._format_decimal(ohlc["open"]),
            "high": self._format_decimal(ohlc["high"]),
            "low": self._format_decimal(ohlc["low"]),
            "close": self._format_decimal(ohlc["close"]),
            "value": self._format_decimal(self._parse_decimal(row.get("value"))),
            "volume": self._format_decimal(self._parse_decimal(row.get("volume"))),
            "begin": row.get("begin"),
            "end": row.get("end"),
        }
