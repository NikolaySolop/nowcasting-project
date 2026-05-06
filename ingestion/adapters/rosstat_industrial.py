import re
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from typing import Any
from xml.etree import ElementTree

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import RawObservationIn


class RosstatIndustrialAdapter(BaseAdapter):
    name = "rosstat_industrial"

    rosstat_base_url = "https://rosstat.gov.ru"
    industrial_page_url = "https://rosstat.gov.ru/enterprise_industrial"
    news_page_url = "https://rosstat.gov.ru/folder/313"
    series_code = "RU_INDUSTRIAL_PRODUCTION"

    months = {
        "январь": 1,
        "январе": 1,
        "февраль": 2,
        "феврале": 2,
        "март": 3,
        "марте": 3,
        "апрель": 4,
        "апреле": 4,
        "май": 5,
        "мае": 5,
        "июнь": 6,
        "июне": 6,
        "июль": 7,
        "июле": 7,
        "август": 8,
        "августе": 8,
        "сентябрь": 9,
        "сентябре": 9,
        "октябрь": 10,
        "октябре": 10,
        "ноябрь": 11,
        "ноябре": 11,
        "декабрь": 12,
        "декабре": 12,
    }
    publication_months = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "мартa": 3,
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
    ns = {
        "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "rel": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        "pkgrel": "http://schemas.openxmlformats.org/package/2006/relationships",
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        extra = spec.extra if spec else {}
        page_url = str(spec.url) if spec and spec.url else self.industrial_page_url
        news_pages = int(extra.get("news_pages", 12))

        headers = {"User-Agent": context.settings.request_user_agent}
        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
            verify=False,
        ) as client:
            page_html = await self._get_text(client, page_url)
            sources = self._find_index_sources(page_html, page_url)
            if not sources:
                raise AdapterError("Rosstat industrial page has no ind_baza XLSX sources")

            latest = context.latest_observed_at_by_series.get(self.series_code)

            rows: dict[date, dict[str, Any]] = {}
            for source in sources:
                content = await self._get_bytes(client, source["url"])
                for row in self._parse_xlsx(content, source):
                    if latest is not None and row["period"] <= latest.date():
                        continue
                    rows[row["period"]] = row

            publication_dates = await self._fetch_publication_dates(client, news_pages) if rows else {}

        observations = [
            self._to_observation(context.source.source_code, row, publication_dates.get(row["period"].isoformat()))
            for row in [rows[key] for key in sorted(rows)]
        ]

        return FetchResult(
            observations=observations,
            raw_payload={
                "page_url": page_url,
                "xlsx_sources": sources,
                "publication_dates_found": len(publication_dates),
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

    def _find_index_sources(self, html: str, page_url: str) -> list[dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        sources: dict[str, dict[str, str]] = {}
        for link in soup.find_all("a", href=True):
            href = str(link["href"])
            filename = href.rsplit("/", 1)[-1].lower()
            if not filename.startswith("ind_baza_") or not filename.endswith(".xlsx"):
                continue
            if "4kv" in filename:
                continue

            item = link.find_parent(class_="document-list__item")
            text = " ".join(item.get_text(" ", strip=True).split()) if item else ""
            updated_at = self._parse_document_list_date(text)
            url = href if href.startswith("http") else f"{self.rosstat_base_url}{href}"
            sources[url] = {
                "url": url,
                "updated_at": updated_at or "",
                "page_url": page_url,
            }
        return sorted(sources.values(), key=lambda item: item["url"])

    def _parse_document_list_date(self, text: str) -> str | None:
        match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if not match:
            return None
        day, month, year = (int(part) for part in match.groups())
        return date(year, month, day).isoformat()

    async def _fetch_publication_dates(self, client: httpx.AsyncClient, max_pages: int) -> dict[str, str]:
        candidates: dict[str, str] = {}
        for page in range(1, max_pages + 1):
            url = self.news_page_url if page == 1 else f"{self.news_page_url}?page={page}"
            try:
                html = await self._get_text(client, url)
            except httpx.HTTPError:
                continue

            soup = BeautifulSoup(html, "html.parser")
            for link in soup.find_all("a", href=True):
                title = " ".join(link.get_text(" ", strip=True).split())
                if "промышлен" not in title.lower() or "производств" not in title.lower():
                    continue
                period = self._period_from_news_title(title)
                if period is None:
                    continue
                href = str(link["href"])
                if "/folder/313/document/" not in href:
                    continue
                url = href if href.startswith("http") else f"{self.rosstat_base_url}{href}"
                candidates[url] = title

        publication_dates: dict[str, str] = {}
        for url, title in candidates.items():
            period = self._period_from_news_title(title)
            if period is None:
                continue
            try:
                html = await self._get_text(client, url)
            except httpx.HTTPError:
                continue
            published_at = self._publication_date_from_news_html(html)
            if published_at is None:
                continue
            period_key = period.isoformat()
            current = publication_dates.get(period_key)
            if current is None or published_at.isoformat() < current:
                publication_dates[period_key] = published_at.isoformat()
        return publication_dates

    def _period_from_news_title(self, title: str) -> date | None:
        lowered = title.lower().replace("ё", "е")
        if any(word in lowered for word in ("уточн", "комментари", "методолог")):
            return None
        year_match = re.search(r"(20\d{2})", lowered)
        if not year_match:
            return None
        year = int(year_match.group(1))

        if "i квартал" in lowered or "первый квартал" in lowered:
            return date(year, 3, 1)
        if "i полугод" in lowered or "первом полугод" in lowered:
            return date(year, 6, 1)

        month_hits: list[tuple[int, int]] = []
        for name, month in self.months.items():
            for match in re.finditer(rf"\b{name}\b", lowered):
                month_hits.append((match.start(), month))
        if month_hits:
            return date(year, sorted(month_hits)[-1][1], 1)

        if re.search(r"\b(год|году)\b", lowered):
            return date(year, 12, 1)
        return None

    def _publication_date_from_news_html(self, html: str) -> date | None:
        soup = BeautifulSoup(html, "html.parser")
        node = soup.select_one(".article-info__data")
        text = " ".join(node.get_text(" ", strip=True).split()).lower() if node else ""
        match = re.search(r"(\d{1,2})\s+([а-яa]+)\s+(20\d{2})", text)
        if match:
            month = self.publication_months.get(match.group(2).replace("a", "а"))
            if month is not None:
                return date(int(match.group(3)), month, int(match.group(1)))

        image = soup.select_one('meta[property="og:image"]')
        content = str(image.get("content", "")) if image else ""
        match = re.search(r"/storage/document_news/(20\d{2})/(\d{2})-(\d{2})/", content)
        if match:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return None

    def _parse_xlsx(self, content: bytes, source: dict[str, str]) -> list[dict[str, Any]]:
        rows = self._read_sheet_rows(content, "2")
        if len(rows) < 6:
            raise AdapterError(f"Rosstat XLSX sheet 2 has too few rows: {source['url']}")

        year_row = rows[3]
        month_row = rows[4]
        years: list[int | None] = []
        current_year: int | None = None
        for cell in year_row:
            parsed_year = self._parse_year(cell)
            if parsed_year is not None:
                current_year = parsed_year
            years.append(current_year)

        months = [self._parse_month(cell) for cell in month_row]
        data_row = next(
            (row for row in rows if len(row) > 1 and str(row[1] or "").strip() == "BCDE"),
            None,
        )
        if data_row is None:
            raise AdapterError(f"Rosstat XLSX has no BCDE industrial production row: {source['url']}")

        output: list[dict[str, Any]] = []
        for idx, value in enumerate(data_row):
            if not isinstance(value, (int, float)):
                continue
            year = years[idx] if idx < len(years) else None
            month = months[idx] if idx < len(months) else None
            if year is None or month is None:
                continue
            output.append(
                {
                    "period": date(year, month, 1),
                    "value": Decimal(str(value)).quantize(Decimal("0.1")),
                    "source_url": source["url"],
                    "source_updated_at": source["updated_at"],
                    "page_url": source["page_url"],
                }
            )
        return output

    def _to_observation(self, source_code: str, row: dict[str, Any], published_at: str | None) -> RawObservationIn:
        period = row["period"]
        vintage_at = self._parse_iso_date(row["source_updated_at"]) or datetime.now(timezone.utc)
        return RawObservationIn(
            series_code=self.series_code,
            source_code=source_code,
            observed_at=datetime(period.year, period.month, period.day, tzinfo=timezone.utc),
            publication_at=self._parse_iso_date(published_at),
            vintage_at=vintage_at,
            value_numeric=row["value"],
            raw_payload={
                "source_url": row["source_url"],
                "rosstat_page_url": row["page_url"],
                "source_updated_at": row["source_updated_at"],
            },
        )

    def _read_sheet_rows(self, content: bytes, sheet_name: str) -> list[list[str | float | None]]:
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
        cleaned = re.sub(r"[^а-яА-ЯёЁ-]", "", str(value)).lower().replace("ё", "е")
        for name, number in self.months.items():
            if cleaned.startswith(name):
                return number
        return None

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
