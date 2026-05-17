import re
from calendar import monthrange
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn


class CbrTradeBalanceAdapter(BaseAdapter):
    name = "cbr_trade_balance"

    cbr_page_url = "https://www.cbr.ru/statistics/macro_itm/external_sector/etg/"
    cbr_trade_url = "https://www.cbr.ru/vfs/statistics/credit_statistics/trade/trade.xls"
    series_code = "RU_TRADE_BALANCE"

    months = {
        "янв": 1,
        "фев": 2,
        "мар": 3,
        "апр": 4,
        "май": 5,
        "июн": 6,
        "июл": 7,
        "авг": 8,
        "сен": 9,
        "сент": 9,
        "окт": 10,
        "ноя": 11,
        "дек": 12,
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        if spec is None:
            raise AdapterError(f"source {context.source.source_code} has no CBR trade scrape spec")

        url = str(spec.url or (spec.extra or {}).get("trade_url") or self.cbr_trade_url)
        page_url = str((spec.extra or {}).get("page_url") or self.cbr_page_url)
        extra = spec.extra or {}
        series_code = str(spec.series_code or extra.get("series_code") or self.series_code)
        start_date = spec.start_date.date() if spec.start_date else date(2015, 1, 1)
        latest = context.latest_observed_at_by_series.get(series_code)
        loaded_at = datetime.now(timezone.utc)

        headers = {"User-Agent": context.settings.request_user_agent}
        headers.update(spec.headers)
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
        ) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise AdapterError(
                    f"CBR trade balance XLS HTTP {exc.response.status_code}: {exc.response.text[:300]}"
                ) from exc
            except httpx.RequestError as exc:
                raise AdapterError(f"CBR trade balance XLS request failed: {type(exc).__name__}: {exc!r}") from exc

        source_rows = self._read_xls_rows(response.content, "Ежемесячные")
        file_update_at = self._publication_datetime(source_rows, response.headers.get("last-modified"))
        rows = self._parse_trade_balance_rows(source_rows, start_date=start_date)
        start_anchor = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        is_backfill = latest is None or latest < start_anchor
        observations = [
            self._to_observation(
                context.source.source_code,
                series_code,
                row,
                self._published_at(row, extra, loaded_at, is_backfill),
            )
            for row in rows
            if latest is None or row["reference_start"] > latest
        ]
        return FetchResult(
            table_observations=observations,
            loaded_at=loaded_at,
            raw_payload={
                "url": url,
                "page_url": page_url,
                "file_update_at": file_update_at.isoformat() if file_update_at else None,
                "row_count": len(rows),
                "observation_count": len(observations),
            },
        )

    def _read_xls_rows(self, content: bytes, sheet_name: str) -> list[list[Any]]:
        try:
            import xlrd
        except ImportError as exc:
            raise AdapterError("xlrd is required to parse CBR trade XLS files") from exc

        workbook = xlrd.open_workbook(file_contents=content)
        try:
            sheet = workbook.sheet_by_name(sheet_name)
        except xlrd.biffh.XLRDError as exc:
            raise AdapterError(f"CBR trade XLS sheet not found: {sheet_name}") from exc
        return [[sheet.cell_value(row, col) for col in range(sheet.ncols)] for row in range(sheet.nrows)]

    def _parse_trade_balance_rows(
        self,
        rows: list[list[Any]],
        *,
        start_date: date,
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in rows:
            if len(row) < 9:
                continue
            year = self._parse_year(row[0])
            month = self._parse_month(row[1])
            if year is None or month is None:
                continue

            period = date(year, month, 1)
            if period < start_date:
                continue

            exports = self._parse_decimal(row[2])
            imports = self._parse_decimal(row[8])
            if exports is None or imports is None:
                continue

            output.append(
                {
                    "period": period,
                    "reference_start": datetime(period.year, period.month, 1, tzinfo=timezone.utc),
                    "reference_end": self._month_end(period),
                    "value": exports - imports,
                    "export_goods_fob": exports,
                    "import_goods_fob": imports,
                }
            )
        return output

    @staticmethod
    def _to_observation(
        source_code: str,
        series_code: str,
        row: dict[str, Any],
        published_at: datetime,
    ) -> ObservationIn:
        return ObservationIn(
            series_code=series_code,
            source_code=source_code,
            reference_date=row["period"],
            reference_start=row["reference_start"],
            reference_end=row["reference_end"],
            value=row["value"],
            published_at=published_at,
        )

    def _published_at(
        self,
        row: dict[str, Any],
        extra: dict[str, Any],
        loaded_at: datetime,
        is_backfill: bool,
    ) -> datetime:
        if not is_backfill:
            return loaded_at

        period: date = row["period"]
        months_after = self._parse_int(extra.get("backfill_publication_months_after"), 2)
        publication_month = self._add_months(period, months_after)
        day = self._parse_int(extra.get("backfill_publication_day"), 13)
        hour, minute = self._parse_time(str(extra.get("backfill_publication_time", "10:00")))
        try:
            tz = ZoneInfo(str(extra.get("publication_timezone", "Europe/Moscow")))
        except ZoneInfoNotFoundError:
            tz = timezone.utc

        publication_day = min(max(day, 1), monthrange(publication_month.year, publication_month.month)[1])
        return datetime(
            publication_month.year,
            publication_month.month,
            publication_day,
            hour,
            minute,
            tzinfo=tz,
        )

    def _publication_datetime(self, rows: list[list[Any]], last_modified: str | None) -> datetime | None:
        for row in reversed(rows):
            if not row:
                continue
            parsed = self._parse_ru_update_datetime(row[0])
            if parsed is not None:
                return parsed
        return self._parse_http_datetime(last_modified)

    @staticmethod
    def _parse_year(value: object) -> int | None:
        if isinstance(value, (int, float)):
            year = int(value)
            return year if 1900 <= year <= 2100 else None
        return None

    def _parse_month(self, value: object) -> int | None:
        if value is None:
            return None
        cleaned = str(value).strip().lower().replace("ё", "е").replace(".", "")
        cleaned = re.sub(r"[^а-я]", "", cleaned)
        return self.months.get(cleaned)

    @staticmethod
    def _parse_decimal(value: object) -> Decimal | None:
        if value in (None, "", "...", "x", "х"):
            return None
        try:
            return Decimal(str(value).replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _month_end(period: date) -> datetime:
        last_day = monthrange(period.year, period.month)[1]
        return datetime(period.year, period.month, last_day, 23, 59, 59, 999999, tzinfo=timezone.utc)

    @staticmethod
    def _add_months(period: date, months: int) -> date:
        month_index = period.month - 1 + months
        year = period.year + month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    @staticmethod
    def _parse_int(value: object, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_time(value: str) -> tuple[int, int]:
        match = re.match(r"^(\d{1,2}):(\d{2})$", value.strip())
        if not match:
            return 10, 0
        hour = int(match.group(1))
        minute = int(match.group(2))
        if hour > 23 or minute > 59:
            return 10, 0
        return hour, minute

    @staticmethod
    def _parse_ru_update_datetime(value: object) -> datetime | None:
        text = str(value or "")
        match = re.search(
            r"(\d{1,2})\s+"
            r"(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)"
            r"\s+(\d{4})",
            text.lower(),
        )
        if not match:
            return None
        month_names = {
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
        return datetime(
            int(match.group(3)),
            month_names[match.group(2)],
            int(match.group(1)),
            tzinfo=timezone.utc,
        )

    @staticmethod
    def _parse_http_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None
