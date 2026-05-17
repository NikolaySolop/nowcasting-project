import re
import zipfile
from calendar import monthrange
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any
from xml.etree import ElementTree
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationIn


class RosstatCpiLiveAdapter(BaseAdapter):
    name = "rosstat_cpi_live"

    price_page_url = "https://rosstat.gov.ru/statistics/price"
    series_code = "RU_CPI_MOM_ROSSTAT_LIVE"

    months = {
        "январь": 1,
        "февраль": 2,
        "март": 3,
        "апрель": 4,
        "май": 5,
        "июнь": 6,
        "июль": 7,
        "август": 8,
        "сентябрь": 9,
        "октябрь": 10,
        "ноябрь": 11,
        "декабрь": 12,
    }
    ns = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        extra = spec.extra if spec else {}
        page_url = str(spec.url) if spec and spec.url else self.price_page_url
        series_code = str(spec.series_code or extra.get("series_code") or self.series_code) if spec else self.series_code
        start_date = spec.start_date.date() if spec and spec.start_date else date(1991, 1, 1)
        loaded_at = datetime.now(timezone.utc)

        headers = {"User-Agent": context.settings.request_user_agent}
        if spec:
            headers.update(spec.headers)

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
            verify=False,
        ) as client:
            page_html = await self._get_text(client, page_url)
            workbook_url = self._find_workbook_url(page_html, page_url)
            workbook_bytes = await self._get_bytes(client, workbook_url)

        rows = self._parse_workbook(workbook_bytes)
        if not rows:
            raise AdapterError("Rosstat CPI workbook has no monthly CPI rows")

        rows = [row for row in rows if row["period"] >= start_date]
        if not rows:
            return FetchResult(
                loaded_at=loaded_at,
                raw_payload={
                    "page_url": page_url,
                    "workbook_url": workbook_url,
                    "row_count": 0,
                    "observation_count": 0,
                    "start_date": start_date.isoformat(),
                },
            )
        if bool(extra.get("latest_only", True)):
            rows = [max(rows, key=lambda item: item["period"])]

        latest = context.latest_observed_at_by_series.get(series_code)
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
                "page_url": page_url,
                "workbook_url": workbook_url,
                "row_count": len(rows),
                "observation_count": len(observations),
            },
        )

    async def _get_text(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(url)
        response.raise_for_status()
        return response.text

    async def _get_bytes(self, client: httpx.AsyncClient, url: str) -> bytes:
        response = await client.get(url)
        response.raise_for_status()
        return response.content

    def _find_workbook_url(self, html: str, page_url: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[tuple[int, int, str]] = []
        for link in soup.find_all("a", href=True):
            href = str(link["href"])
            filename = href.rsplit("/", 1)[-1].lower()
            match = re.match(r"ipc_mes_(\d{2})-(\d{4})\.xlsx$", filename)
            if match:
                candidates.append((int(match.group(2)), int(match.group(1)), href))
        if not candidates:
            raise AdapterError("Rosstat price page has no ipc_mes_*.xlsx link")

        href = max(candidates)[2]
        if href.startswith("http"):
            return href
        base = page_url.split("/", 3)[:3]
        return "/".join(base) + href

    def _parse_workbook(self, content: bytes) -> list[dict[str, Any]]:
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            shared_strings = self._read_shared_strings(workbook)
            sheet_path = self._sheet_path(workbook, "01")
            rows = self._read_sheet_rows(workbook, sheet_path, shared_strings)

        if len(rows) < 6:
            raise AdapterError("Rosstat CPI workbook sheet 01 has unexpected shape")

        years = rows[3]
        output: list[dict[str, Any]] = []
        for row in rows[5:17]:
            if not row:
                continue
            month_name = str(row[0] or "").strip().lower()
            month = self.months.get(month_name)
            if month is None:
                continue
            for col_idx in range(1, min(len(row), len(years))):
                year = self._parse_year(years[col_idx])
                index_value = self._parse_decimal(row[col_idx])
                if year is None or index_value is None:
                    continue
                period = date(year, month, 1)
                reference_start = datetime(period.year, period.month, 1, tzinfo=timezone.utc)
                output.append(
                    {
                        "period": period,
                        "reference_start": reference_start,
                        "reference_end": self._month_end(reference_start),
                        "index_value": index_value,
                        "value": index_value - Decimal("100"),
                    }
                )
        return sorted(output, key=lambda item: item["period"])

    def _read_shared_strings(self, workbook: zipfile.ZipFile) -> list[str]:
        if "xl/sharedStrings.xml" not in workbook.namelist():
            return []
        root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
        strings: list[str] = []
        for item in root.findall("main:si", self.ns):
            strings.append("".join(text.text or "" for text in item.findall(".//main:t", self.ns)))
        return strings

    def _sheet_path(self, workbook: zipfile.ZipFile, sheet_name: str) -> str:
        workbook_root = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
        rels_root = ElementTree.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        relationships = {
            item.attrib["Id"]: item.attrib["Target"]
            for item in rels_root.findall("pkgrel:Relationship", self.ns)
        }
        for sheet in workbook_root.findall(".//main:sheet", self.ns):
            if sheet.attrib.get("name") != sheet_name:
                continue
            rel_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            target = relationships.get(str(rel_id))
            if target:
                return f"xl/{target}"
        raise AdapterError(f"Rosstat CPI workbook sheet not found: {sheet_name}")

    def _read_sheet_rows(
        self,
        workbook: zipfile.ZipFile,
        sheet_path: str,
        shared_strings: list[str],
    ) -> list[list[str | None]]:
        root = ElementTree.fromstring(workbook.read(sheet_path))
        rows: list[list[str | None]] = []
        for row in root.findall(".//main:sheetData/main:row", self.ns):
            values: list[str | None] = []
            for cell in row.findall("main:c", self.ns):
                col_idx = self._column_index(cell.attrib.get("r", ""))
                while len(values) < col_idx:
                    values.append(None)
                values.append(self._cell_value(cell, shared_strings))
            rows.append(values)
        return rows

    def _cell_value(self, cell: ElementTree.Element, shared_strings: list[str]) -> str | None:
        value = cell.find("main:v", self.ns)
        if value is None or value.text is None:
            return None
        raw = value.text.strip()
        if cell.attrib.get("t") == "s":
            try:
                return shared_strings[int(raw)]
            except (IndexError, ValueError) as exc:
                raise AdapterError(f"Rosstat CPI workbook has invalid shared string index: {raw}") from exc
        return raw

    @staticmethod
    def _column_index(cell_ref: str) -> int:
        match = re.match(r"([A-Z]+)", cell_ref.upper())
        if not match:
            return 0
        result = 0
        for char in match.group(1):
            result = result * 26 + ord(char) - ord("A") + 1
        return result - 1

    @staticmethod
    def _parse_year(value: object) -> int | None:
        if value is None:
            return None
        match = re.search(r"\d{4}", str(value).strip())
        if not match:
            return None
        year = int(match.group(0))
        return year if 1990 <= year <= 2100 else None

    @staticmethod
    def _parse_decimal(value: object) -> Decimal | None:
        if value in (None, ""):
            return None
        cleaned = re.sub(r"[^0-9,.\-]", "", str(value)).replace(",", ".")
        if cleaned in ("", "-", "."):
            return None
        try:
            return Decimal(cleaned)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _month_end(value: datetime) -> datetime:
        if value.month == 12:
            next_month = value.replace(year=value.year + 1, month=1)
        else:
            next_month = value.replace(month=value.month + 1)
        return next_month - datetime.resolution

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
        if period.month == 12:
            year = period.year + 1
            month = 1
        else:
            year = period.year
            month = period.month + 1

        day = self._parse_int(extra.get("backfill_publication_day_next_month"), 15)
        hour, minute = self._parse_time(str(extra.get("backfill_publication_time", "10:00")))
        try:
            tz = ZoneInfo(str(extra.get("publication_timezone", "Europe/Moscow")))
        except ZoneInfoNotFoundError:
            tz = timezone.utc

        publication_day = min(max(day, 1), monthrange(year, month)[1])
        return datetime(year, month, publication_day, hour, minute, tzinfo=tz)

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


class RosstatIndustrialProducerCpiLiveAdapter(RosstatCpiLiveAdapter):
    name = "rosstat_industrial_producer_cpi_live"

    series_code = "RU_CPI_INDUSTRIAL_PROD_MOM_ROSSTAT_LIVE"
    workbook_filename_pattern = re.compile(r"Proizvoditeli_Ind_VED_(\d{2})-(\d{4})\.xlsx$", re.IGNORECASE)

    def _find_workbook_url(self, html: str, page_url: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[tuple[int, int, str]] = []
        for link in soup.find_all("a", href=True):
            href = str(link["href"])
            filename = href.rsplit("/", 1)[-1]
            match = self.workbook_filename_pattern.match(filename)
            if match:
                candidates.append((int(match.group(2)), int(match.group(1)), href))
        if not candidates:
            raise AdapterError("Rosstat price page has no Proizvoditeli_Ind_VED_MM-YYYY.xlsx link")

        href = max(candidates)[2]
        if href.startswith("http"):
            return href
        base = page_url.split("/", 3)[:3]
        return "/".join(base) + href

    def _parse_workbook(self, content: bytes) -> list[dict[str, Any]]:
        parsed_by_period: dict[date, dict[str, Any]] = {}
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            shared_strings = self._read_shared_strings(workbook)
            sheet_names = self._sheet_names(workbook)

            if "2.1" in sheet_names:
                sheet_path = self._sheet_path(workbook, "2.1")
                rows = self._read_sheet_rows(workbook, sheet_path, shared_strings)
                parsed_by_period.update({row["period"]: row for row in self._parse_historical_industry_rows(rows)})

            for sheet_name in sheet_names:
                if not re.match(r"^\d+\.1$", sheet_name) or sheet_name in {"1.1", "2.1"}:
                    continue
                sheet_path = self._sheet_path(workbook, sheet_name)
                rows = self._read_sheet_rows(workbook, sheet_path, shared_strings)
                parsed_by_period.update({row["period"]: row for row in self._parse_annual_industry_rows(rows)})

        return sorted(parsed_by_period.values(), key=lambda item: item["period"])

    def _parse_historical_industry_rows(self, rows: list[list[str | None]]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        if len(rows) < 6:
            return output

        month_header_idx = self._find_row_index(rows, "К предыдущему месяцу")
        if month_header_idx is None or month_header_idx == 0:
            return output

        years = rows[month_header_idx - 1]
        for row in rows[month_header_idx + 1:month_header_idx + 13]:
            if not row:
                continue
            month = self._parse_month_name(row[0])
            if month is None:
                continue
            for col_idx in range(1, min(len(row), len(years))):
                year = self._parse_year(years[col_idx])
                index_value = self._parse_decimal(row[col_idx])
                if year is None or index_value is None:
                    continue
                output.append(self._row_from_index(year, month, index_value))
        return output

    def _parse_annual_industry_rows(self, rows: list[list[str | None]]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        if len(rows) < 5:
            return output

        year = self._parse_year(" ".join(str(value or "") for value in rows[0]))
        if year is None:
            return output

        header_idx = self._find_month_header_row(rows)
        industry_idx = self._find_industry_group_row(rows)
        if header_idx is None or industry_idx is None:
            return output

        values_row = self._find_russia_row(rows, industry_idx + 1)
        if values_row is None:
            return output

        headers = rows[header_idx]
        for col_idx in range(2, min(len(headers), len(values_row))):
            month = self._parse_month_name(headers[col_idx])
            index_value = self._parse_decimal(values_row[col_idx])
            if month is None or index_value is None:
                continue
            output.append(self._row_from_index(year, month, index_value))
        return output

    def _row_from_index(self, year: int, month: int, index_value: Decimal) -> dict[str, Any]:
        period = date(year, month, 1)
        reference_start = datetime(period.year, period.month, 1, tzinfo=timezone.utc)
        return {
            "period": period,
            "reference_start": reference_start,
            "reference_end": self._month_end(reference_start),
            "index_value": index_value,
            "value": index_value - Decimal("100"),
        }

    def _sheet_names(self, workbook: zipfile.ZipFile) -> list[str]:
        workbook_root = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
        return [str(sheet.attrib.get("name") or "") for sheet in workbook_root.findall(".//main:sheet", self.ns)]

    @staticmethod
    def _find_row_index(rows: list[list[str | None]], text: str) -> int | None:
        target = text.lower()
        for idx, row in enumerate(rows):
            if target in str(row[0] if row else "").lower():
                return idx
        return None

    def _find_month_header_row(self, rows: list[list[str | None]]) -> int | None:
        for idx, row in enumerate(rows[:10]):
            if sum(1 for value in row if self._parse_month_name(value) is not None) >= 3:
                return idx
        return None

    @staticmethod
    def _find_industry_group_row(rows: list[list[str | None]]) -> int | None:
        for idx, row in enumerate(rows):
            label = str(row[0] if row else "").lower()
            if "собирательная классификационная" in label and "промышленность" in label:
                return idx
        return None

    @staticmethod
    def _find_russia_row(rows: list[list[str | None]], start_idx: int) -> list[str | None] | None:
        for row in rows[start_idx:start_idx + 10]:
            if str(row[0] if row else "").strip().lower() == "российская федерация":
                return row
        return None

    def _parse_month_name(self, value: object) -> int | None:
        cleaned = re.sub(r"[^А-Яа-яЁё]", "", str(value or "")).lower().replace("ё", "е")
        months = {name.replace("ё", "е"): month for name, month in self.months.items()}
        return months.get(cleaned)
