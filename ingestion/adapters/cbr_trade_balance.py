import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import RawObservationIn


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
        start_date = spec.start_date.date() if spec.start_date else date(2015, 1, 1)
        latest = context.latest_observed_at_by_series.get(self.series_code)

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
        publication_at = self._publication_datetime(source_rows, response.headers.get("last-modified"))
        rows = self._parse_trade_balance_rows(
            source_rows,
            start_date=start_date,
            latest_observed_at=latest.date() if latest else None,
        )
        observations = [
            self._to_observation(context.source.source_code, row, publication_at, url, page_url)
            for row in rows
        ]
        return FetchResult(
            observations=observations,
            raw_payload={
                "url": url,
                "page_url": page_url,
                "publication_at": publication_at.date().isoformat() if publication_at else None,
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
        latest_observed_at: date | None,
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
            if latest_observed_at is not None and period <= latest_observed_at:
                continue

            exports = self._parse_decimal(row[2])
            imports = self._parse_decimal(row[8])
            if exports is None or imports is None:
                continue

            output.append(
                {
                    "period": period,
                    "value": exports - imports,
                    "export_goods_fob": exports,
                    "import_goods_fob": imports,
                }
            )
        return output

    def _to_observation(
        self,
        source_code: str,
        row: dict[str, Any],
        publication_at: datetime | None,
        source_url: str,
        page_url: str,
    ) -> RawObservationIn:
        period = row["period"]
        vintage_at = publication_at or datetime.now(timezone.utc)
        return RawObservationIn(
            series_code=self.series_code,
            source_code=source_code,
            observed_at=datetime(period.year, period.month, period.day, tzinfo=timezone.utc),
            publication_at=publication_at,
            vintage_at=vintage_at,
            value_numeric=row["value"],
            raw_payload={
                "source_url": source_url,
                "cbr_page_url": page_url,
                "unit": "million USD",
                "measure": "goods_exports_fob_minus_goods_imports_fob",
                "export_goods_fob": str(row["export_goods_fob"]),
                "import_goods_fob": str(row["import_goods_fob"]),
                "publication_at_source": "workbook_update_date" if publication_at else "missing",
            },
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
