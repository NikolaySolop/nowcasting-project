import re
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import RawObservationIn


class RosstatRetailSalesAdapter(BaseAdapter):
    name = "rosstat_retail_sales"

    rosstat_base_url = "https://rosstat.gov.ru"
    retail_page_url = "https://rosstat.gov.ru/statistics/roznichnayatorgovlya"
    series_code = "RU_RETAIL_SALES"

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
        page_url = str(spec.url) if spec and spec.url else self.retail_page_url
        start_date = spec.start_date.date() if spec and spec.start_date else date(2015, 1, 1)

        headers = {"User-Agent": context.settings.request_user_agent}
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
            verify=False,
        ) as client:
            page_html = await self._get_text(client, page_url)
            sources = self._find_sources(page_html, page_url)
            if not sources:
                raise AdapterError("Rosstat retail page has no Oborot XLS/XLSX sources")

            latest = context.latest_observed_at_by_series.get(self.series_code)
            rows: dict[date, dict[str, Any]] = {}
            for source in sources:
                content = await self._get_bytes(client, source["url"])
                for row in self.parse_workbook(content, source, start_date=start_date):
                    if latest is not None and row["period"] <= latest.date():
                        continue
                    rows[row["period"]] = row

        observations = [
            self._to_observation(context.source.source_code, row)
            for row in [rows[key] for key in sorted(rows)]
        ]
        return FetchResult(
            observations=observations,
            raw_payload={"page_url": page_url, "source_files": sources},
        )

    async def _get_text(self, client: httpx.AsyncClient, url: str) -> str:
        response = await client.get(url)
        response.raise_for_status()
        return response.text

    async def _get_bytes(self, client: httpx.AsyncClient, url: str) -> bytes:
        response = await client.get(url)
        response.raise_for_status()
        return response.content

    def _find_sources(self, html: str, page_url: str) -> list[dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        sources: dict[str, dict[str, str]] = {}
        for link in soup.find_all("a", href=True):
            href = str(link["href"])
            filename = href.rsplit("/", 1)[-1]
            lowered = filename.lower()
            if lowered.startswith("oborot_m_") and lowered.endswith(".xlsx"):
                kind = "current"
            elif lowered.startswith("oborot_") and lowered.endswith(".xls") and "_m_" not in lowered:
                kind = "historical"
            else:
                continue

            item = link.find_parent(class_="document-list__item")
            text = " ".join(item.get_text(" ", strip=True).split()) if item else ""
            updated_at = self._parse_document_list_date(text)
            url = href if href.startswith("http") else f"{self.rosstat_base_url}{href}"
            sources[url] = {
                "url": url,
                "filename": filename,
                "kind": kind,
                "updated_at": updated_at or "",
                "page_url": page_url,
            }

        order = {"historical": 0, "current": 1}
        return sorted(sources.values(), key=lambda item: (order[item["kind"]], item["url"]))

    def parse_workbook(
        self,
        content: bytes,
        source: dict[str, str],
        *,
        start_date: date,
    ) -> list[dict[str, Any]]:
        filename = source["filename"].lower()
        if filename.endswith(".xls") and not filename.endswith(".xlsx"):
            rows = self._read_xls_rows(content, "2")
        else:
            rows = self._read_xlsx_rows(content, "1")
        return self._parse_monthly_rows(rows, source, start_date=start_date)

    def _read_xls_rows(self, content: bytes, sheet_name: str) -> list[list[Any]]:
        try:
            import xlrd
        except ImportError as exc:
            raise AdapterError("xlrd is required to parse Rosstat retail XLS files") from exc

        workbook = xlrd.open_workbook(file_contents=content)
        try:
            sheet = workbook.sheet_by_name(sheet_name)
        except xlrd.biffh.XLRDError as exc:
            raise AdapterError(f"Rosstat XLS sheet not found: {sheet_name}") from exc
        return [[sheet.cell_value(row, col) for col in range(sheet.ncols)] for row in range(sheet.nrows)]

    def _read_xlsx_rows(self, content: bytes, sheet_name: str) -> list[list[str | float | None]]:
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            shared_strings = self._read_shared_strings(workbook)
            sheet_xml = workbook.read(self._sheet_path(workbook, sheet_name))

        root = ElementTree.fromstring(sheet_xml)
        rows: list[list[str | float | None]] = []
        for row in root.findall(".//main:sheetData/main:row", self.ns):
            values: list[str | float | None] = []
            for cell in row.findall("main:c", self.ns):
                idx = self._column_index(cell.attrib["r"])
                while len(values) <= idx:
                    values.append(None)
                values[idx] = self._cell_value(cell, shared_strings)
            rows.append(values)
        return rows

    def _parse_monthly_rows(
        self,
        rows: list[list[Any]],
        source: dict[str, str],
        *,
        start_date: date,
    ) -> list[dict[str, Any]]:
        current_year: int | None = None
        output: list[dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            label = row[0]
            parsed_year = self._parse_year(label)
            if parsed_year is not None:
                current_year = parsed_year
                continue

            month = self._parse_month(label)
            if current_year is None or month is None or len(row) < 2:
                continue

            value = self._parse_decimal(row[1])
            if value is None:
                continue

            period = date(current_year, month, 1)
            if period < start_date:
                continue
            output.append(
                {
                    "period": period,
                    "value": value.quantize(Decimal("0.1")),
                    "source_url": source["url"],
                    "source_filename": source["filename"],
                    "source_updated_at": source["updated_at"],
                    "page_url": source["page_url"],
                }
            )
        return output

    def _to_observation(self, source_code: str, row: dict[str, Any]) -> RawObservationIn:
        period = row["period"]
        source_updated_at = self._parse_iso_date(row["source_updated_at"])
        return RawObservationIn(
            series_code=self.series_code,
            source_code=source_code,
            observed_at=datetime(period.year, period.month, period.day, tzinfo=timezone.utc),
            publication_at=source_updated_at,
            vintage_at=source_updated_at or datetime.now(timezone.utc),
            value_numeric=row["value"],
            raw_payload={
                "source_url": row["source_url"],
                "source_filename": row["source_filename"],
                "rosstat_page_url": row["page_url"],
                "source_updated_at": row["source_updated_at"],
                "unit": "million RUB, current prices",
            },
        )

    def _read_shared_strings(self, workbook: zipfile.ZipFile) -> list[str]:
        try:
            payload = workbook.read("xl/sharedStrings.xml")
        except KeyError:
            return []
        root = ElementTree.fromstring(payload)
        values: list[str] = []
        for item in root.findall("main:si", self.ns):
            values.append("".join(node.text or "" for node in item.findall(".//main:t", self.ns)))
        return values

    def _sheet_path(self, workbook: zipfile.ZipFile, sheet_name: str) -> str:
        book = ElementTree.fromstring(workbook.read("xl/workbook.xml"))
        rels = ElementTree.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        rel_targets = {
            rel.attrib["Id"]: rel.attrib["Target"]
            for rel in rels.findall("pkgrel:Relationship", self.ns)
        }
        for sheet in book.findall("main:sheets/main:sheet", self.ns):
            if sheet.attrib.get("name") != sheet_name:
                continue
            rel_id = sheet.attrib[f"{{{self.ns['rel']}}}id"]
            target = rel_targets[rel_id]
            return target.lstrip("/") if target.startswith("xl/") else f"xl/{target.lstrip('/')}"
        raise AdapterError(f"Rosstat XLSX sheet not found: {sheet_name}")

    def _cell_value(self, cell: ElementTree.Element, shared_strings: list[str]) -> str | float | None:
        cell_type = cell.attrib.get("t")
        if cell_type == "inlineStr":
            return "".join(node.text or "" for node in cell.findall(".//main:t", self.ns))
        value_node = cell.find("main:v", self.ns)
        if value_node is None or value_node.text is None:
            return None
        raw = value_node.text
        if cell_type == "s":
            return shared_strings[int(raw)]
        if cell_type == "str":
            return raw
        try:
            value = float(raw)
        except ValueError:
            return raw
        return int(value) if value.is_integer() else value

    def _parse_month(self, value: object) -> int | None:
        if value is None:
            return None
        lowered = str(value).lower().replace("ё", "е")
        if "-" in lowered:
            return None
        cleaned = re.sub(r"[^а-я]", "", lowered)
        return self.months.get(cleaned)

    @staticmethod
    def _parse_year(value: object) -> int | None:
        if isinstance(value, (int, float)):
            year = int(value)
            return year if 2000 <= year <= 2100 else None
        if value is None:
            return None
        match = re.search(r"20\d{2}", str(value))
        return int(match.group(0)) if match else None

    @staticmethod
    def _parse_decimal(value: object) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value).replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _parse_document_list_date(text: str) -> str | None:
        match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not match:
            return None
        day, month, year = (int(part) for part in match.groups())
        return date(year, month, day).isoformat()

    @staticmethod
    def _parse_iso_date(value: str | None) -> datetime | None:
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _column_index(cell_ref: str) -> int:
        letters = re.match(r"([A-Z]+)", cell_ref).group(1)
        index = 0
        for char in letters:
            index = index * 26 + (ord(char) - ord("A") + 1)
        return index - 1
