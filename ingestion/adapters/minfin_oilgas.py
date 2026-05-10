import csv
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import RawObservationIn


class MinfinOilGasAdapter(BaseAdapter):
    name = "minfin_oilgas"

    default_url = (
        "https://minfin.gov.ru/ru/document"
        "?id_4=122094-svedeniya_o_formirovanii_i_ispolzovanii_dopolnitelnykh_"
        "neftegazovykh_dokhodov_federalnogo_byudzheta_v_2018-2025_godakh"
    )
    default_historical_csv_path = Path("storage/exports/russia_fiscal_oilgas_monthly_from_2017_02.csv")
    oilgas_series_code = "RU_FISCAL_OILGAS_REVENUE"
    fx_series_code = "RU_FISCAL_FX_OPERATION_AMOUNT"

    months = {
        "янв": 1,
        "январь": 1,
        "января": 1,
        "фев": 2,
        "февраль": 2,
        "февраля": 2,
        "мар": 3,
        "март": 3,
        "марта": 3,
        "апр": 4,
        "апрель": 4,
        "апреля": 4,
        "май": 5,
        "мая": 5,
        "июн": 6,
        "июнь": 6,
        "июня": 6,
        "июл": 7,
        "июль": 7,
        "июля": 7,
        "авг": 8,
        "август": 8,
        "августа": 8,
        "сен": 9,
        "сент": 9,
        "сентябрь": 9,
        "сентября": 9,
        "окт": 10,
        "октябрь": 10,
        "октября": 10,
        "ноя": 11,
        "ноябрь": 11,
        "ноября": 11,
        "дек": 12,
        "декабрь": 12,
        "декабря": 12,
    }

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.scrape
        extra = spec.extra if spec else {}
        page_url = str(spec.url) if spec and spec.url else self.default_url
        headers = {"User-Agent": context.settings.request_user_agent}
        historical_csv_path = Path(extra.get("historical_csv_path", self.default_historical_csv_path))
        historical_rows = self.parse_historical_csv(historical_csv_path)

        async with httpx.AsyncClient(
            timeout=context.settings.request_timeout_seconds,
            headers=headers,
            follow_redirects=True,
        ) as client:
            response = await client.get(page_url)
            response.raise_for_status()

        live = self.parse_html(response.text, response.url)
        combined_rows = self._combine_rows(historical_rows, live["rows"])
        observations = self._to_observations(context, combined_rows, live["metadata"])
        return FetchResult(
            observations=observations,
            raw_payload={
                **live["metadata"],
                "historical_csv_path": str(historical_csv_path),
                "historical_row_count": sum(len(rows) for rows in historical_rows.values()),
                "live_row_count": sum(len(rows) for rows in live["rows"].values()),
            },
        )

    def parse_historical_csv(self, path: Path) -> dict[str, list[dict[str, Any]]]:
        if not path.exists():
            raise AdapterError(f"Minfin oil/gas historical CSV not found: {path}")

        output = {
            self.oilgas_series_code: [],
            self.fx_series_code: [],
        }
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                period = self._parse_iso_month(row.get("month"))
                if period is None:
                    continue
                publication_at = self._parse_iso_date(row.get("publication_at")) or self._next_month_start(period)
                oilgas_value = self._parse_decimal(row.get("oilgas_revenue_bln_rub"))
                if oilgas_value is not None:
                    output[self.oilgas_series_code].append(
                        {
                            "period": period,
                            "value": oilgas_value,
                            "label": "historical_csv:oilgas_revenue_bln_rub",
                            "origin": "historical_csv",
                            "publication_at": publication_at,
                        }
                    )
                fx_value = self._parse_decimal(row.get("fx_operation_amount_bln_rub"))
                if fx_value is not None:
                    output[self.fx_series_code].append(
                        {
                            "period": period,
                            "value": fx_value,
                            "label": "historical_csv:fx_operation_amount_bln_rub",
                            "origin": "historical_csv",
                            "publication_at": publication_at,
                        }
                    )
        return {series_code: rows for series_code, rows in output.items() if rows}

    def parse_html(self, html: str, page_url: str | httpx.URL) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if table is None:
            raise AdapterError("Minfin oil/gas page has no data table")

        header_row = table.find("thead").find("tr") if table.find("thead") else table.find("tr")
        if header_row is None:
            raise AdapterError("Minfin oil/gas table has no header row")

        headers = [self._clean_text(cell.get_text(" ", strip=True)) for cell in header_row.find_all(["th", "td"])]
        periods = [self._parse_month_header(header) for header in headers]
        rows = self._parse_rows(table, periods)
        if not rows:
            raise AdapterError("Minfin oil/gas table has no supported monthly rows")

        page_url_text = str(page_url)
        download_url = self._find_download_url(soup, page_url_text)
        return {
            "rows": rows,
            "metadata": {
                "page_url": page_url_text,
                "document_title": self._document_title(soup),
                "download_url": download_url,
                "published_at": self._find_labeled_date(soup, "Опубликовано"),
                "updated_at": self._find_labeled_date(soup, "Изменено"),
                "unit": "billion RUB",
            },
        }

    def _parse_rows(self, table, periods: list[date | None]) -> dict[str, list[dict[str, Any]]]:
        output = {
            self.oilgas_series_code: [],
            self.fx_series_code: [],
        }

        tbody = table.find("tbody") or table
        for row in tbody.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            label = self._clean_text(cells[0].get_text(" ", strip=True)).lower()
            series_code = self._series_code_for_label(label)
            if series_code is None:
                continue

            for idx, cell in enumerate(cells[1:], start=1):
                period = periods[idx] if idx < len(periods) else None
                if period is None:
                    continue
                value = self._parse_decimal(cell.get_text(" ", strip=True))
                if value is None:
                    continue
                output[series_code].append(
                    {
                        "period": period,
                        "value": value,
                        "label": label,
                        "origin": "live_minfin",
                    }
                )

        return {series_code: rows for series_code, rows in output.items() if rows}

    def _combine_rows(
        self,
        historical_rows: dict[str, list[dict[str, Any]]],
        live_rows: dict[str, list[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        combined: dict[str, dict[date, dict[str, Any]]] = {}
        for rows_by_series in (historical_rows, live_rows):
            for series_code, rows in rows_by_series.items():
                series_rows = combined.setdefault(series_code, {})
                for row in rows:
                    series_rows.setdefault(row["period"], row)
        return {
            series_code: [rows_by_period[period] for period in sorted(rows_by_period)]
            for series_code, rows_by_period in combined.items()
        }

    def _to_observations(
        self,
        context: FetchContext,
        rows_by_series: dict[str, list[dict[str, Any]]],
        metadata: dict[str, Any],
    ) -> list[RawObservationIn]:
        publication_at = self._parse_iso_date(metadata.get("updated_at") or metadata.get("published_at"))

        observations: list[RawObservationIn] = []
        for series_code, rows in rows_by_series.items():
            latest = context.latest_observed_at_by_series.get(series_code)
            for row in rows:
                period = row["period"]
                observed_at = datetime(period.year, period.month, 1, tzinfo=timezone.utc)
                if row["origin"] == "live_minfin" and latest is not None and observed_at <= latest:
                    continue
                row_publication_at = publication_at if row["origin"] == "live_minfin" else row.get("publication_at")
                vintage_at = row_publication_at or observed_at
                observations.append(
                    RawObservationIn(
                        series_code=series_code,
                        source_code=context.source.source_code,
                        observed_at=observed_at,
                        publication_at=row_publication_at,
                        vintage_at=vintage_at,
                        value_numeric=row["value"],
                        raw_payload={
                            "source_url": metadata["page_url"],
                            "download_url": metadata["download_url"],
                            "document_title": metadata["document_title"],
                            "published_at": metadata["published_at"],
                            "updated_at": metadata["updated_at"],
                            "unit": metadata["unit"],
                            "source_label": row["label"],
                            "origin": row["origin"],
                        },
                    )
                )
        return observations

    def _series_code_for_label(self, label: str) -> str | None:
        if "нефтегазовые доходы" in label and "всего" in label:
            return self.oilgas_series_code
        if "объем покупки" in label and "иностранной валюты" in label:
            return self.fx_series_code
        return None

    def _parse_month_header(self, value: str) -> date | None:
        lowered = value.lower().replace("ё", "е")
        if re.fullmatch(r"20\d{2}", lowered):
            return None

        match = re.search(r"([а-я]+)[\s.-]*(\d{2,4})", lowered)
        if match is None:
            return None

        month = self.months.get(match.group(1))
        if month is None:
            return None
        raw_year = int(match.group(2))
        year = 2000 + raw_year if raw_year < 100 else raw_year
        return date(year, month, 1)

    @staticmethod
    def _parse_decimal(value: object) -> Decimal | None:
        if value is None:
            return None
        raw = str(value).replace("\xa0", " ").strip()
        if not raw or raw in {"-", "–", "—"}:
            return None
        cleaned = raw.replace(" ", "").replace("+", "").replace(",", ".")
        cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
        if not cleaned or cleaned in {"-", ".", "-."}:
            return None
        try:
            return Decimal(cleaned).quantize(Decimal("0.1"))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _parse_iso_date(value: str | None) -> datetime | None:
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _parse_iso_month(value: str | None) -> date | None:
        if not value:
            return None
        try:
            parsed = date.fromisoformat(value.strip())
        except ValueError:
            return None
        return date(parsed.year, parsed.month, 1)

    @staticmethod
    def _next_month_start(value: date) -> datetime:
        year = value.year + (1 if value.month == 12 else 0)
        month = 1 if value.month == 12 else value.month + 1
        return datetime(year, month, 1, tzinfo=timezone.utc)

    @staticmethod
    def _find_labeled_date(soup: BeautifulSoup, label: str) -> str | None:
        text = soup.get_text(" ", strip=True)
        match = re.search(rf"{label}:\s*(\d{{2}})\.(\d{{2}})\.(\d{{4}})", text)
        if not match:
            return None
        day, month, year = (int(part) for part in match.groups())
        return date(year, month, day).isoformat()

    @staticmethod
    def _find_download_url(soup: BeautifulSoup, page_url: str) -> str | None:
        link = soup.select_one('a[href$=".xlsx"], a[href$=".xls"]')
        if link is None:
            return None
        return urljoin(page_url, str(link.get("href", "")))

    @staticmethod
    def _document_title(soup: BeautifulSoup) -> str:
        node = soup.select_one("h1") or soup.find("title")
        return " ".join(node.get_text(" ", strip=True).split()) if node else ""

    @staticmethod
    def _clean_text(value: str) -> str:
        return " ".join(value.replace("\xa0", " ").split())
