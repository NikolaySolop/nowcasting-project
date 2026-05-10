import csv
import json
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import httpx

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.schemas.observations import ObservationKind, RawObservationIn


DEFAULT_VALUE_COLUMNS = {
    "is_ru_business_day": "RU_TAX_RU_BUSINESS_DAY",
    "any_tax_due_dummy": "RU_TAX_ANY_DUE_DUMMY",
    "tax_due_count": "RU_TAX_DUE_COUNT",
    "any_tax_window_t_minus_3_to_t": "RU_TAX_ANY_WINDOW_T_MINUS_3_TO_T",
    "tax_week_dummy": "RU_TAX_WEEK_DUMMY",
    "any_tax_t_minus_3": "RU_TAX_ANY_T_MINUS_3",
    "any_tax_t_minus_2": "RU_TAX_ANY_T_MINUS_2",
    "any_tax_t_minus_1": "RU_TAX_ANY_T_MINUS_1",
    "any_tax_t0": "RU_TAX_ANY_T0",
    "vat_due_dummy": "RU_TAX_VAT_DUE_DUMMY",
    "vat_t_minus_3": "RU_TAX_VAT_T_MINUS_3",
    "vat_t_minus_2": "RU_TAX_VAT_T_MINUS_2",
    "vat_t_minus_1": "RU_TAX_VAT_T_MINUS_1",
    "vat_t0": "RU_TAX_VAT_T0",
    "vat_window_t_minus_3_to_t": "RU_TAX_VAT_WINDOW_T_MINUS_3_TO_T",
    "profit_tax_due_dummy": "RU_TAX_PROFIT_DUE_DUMMY",
    "profit_tax_t_minus_3": "RU_TAX_PROFIT_T_MINUS_3",
    "profit_tax_t_minus_2": "RU_TAX_PROFIT_T_MINUS_2",
    "profit_tax_t_minus_1": "RU_TAX_PROFIT_T_MINUS_1",
    "profit_tax_t0": "RU_TAX_PROFIT_T0",
    "profit_tax_window_t_minus_3_to_t": "RU_TAX_PROFIT_WINDOW_T_MINUS_3_TO_T",
    "ndpi_due_dummy": "RU_TAX_NDPI_DUE_DUMMY",
    "ndpi_t_minus_3": "RU_TAX_NDPI_T_MINUS_3",
    "ndpi_t_minus_2": "RU_TAX_NDPI_T_MINUS_2",
    "ndpi_t_minus_1": "RU_TAX_NDPI_T_MINUS_1",
    "ndpi_t0": "RU_TAX_NDPI_T0",
    "ndpi_window_t_minus_3_to_t": "RU_TAX_NDPI_WINDOW_T_MINUS_3_TO_T",
    "quarter_tax_payment_due_dummy": "RU_TAX_QTR_PAYMENT_DUE_DUMMY",
    "quarter_tax_payment_t_minus_3": "RU_TAX_QTR_PAYMENT_T_MINUS_3",
    "quarter_tax_payment_t_minus_2": "RU_TAX_QTR_PAYMENT_T_MINUS_2",
    "quarter_tax_payment_t_minus_1": "RU_TAX_QTR_PAYMENT_T_MINUS_1",
    "quarter_tax_payment_t0": "RU_TAX_QTR_PAYMENT_T0",
    "quarter_tax_payment_window_t_minus_3_to_t": "RU_TAX_QTR_PAYMENT_WINDOW_T_MINUS_3_TO_T",
    "calendar_quarter_end_last_business_day_dummy": "RU_TAX_QTR_END_LAST_BUSINESS_DAY",
    "calendar_quarter_end_window_t_minus_3_to_t": "RU_TAX_QTR_END_WINDOW_T_MINUS_3_TO_T",
}


EVENT_COLUMNS = {
    "vat": ("vat_due_dummy", "vat_event_date", "vat_t_minus_3", "vat_t_minus_2", "vat_t_minus_1", "vat_t0", "vat_window_t_minus_3_to_t"),
    "profit_tax": (
        "profit_tax_due_dummy",
        "profit_tax_event_date",
        "profit_tax_t_minus_3",
        "profit_tax_t_minus_2",
        "profit_tax_t_minus_1",
        "profit_tax_t0",
        "profit_tax_window_t_minus_3_to_t",
    ),
    "ndpi": ("ndpi_due_dummy", "ndpi_event_date", "ndpi_t_minus_3", "ndpi_t_minus_2", "ndpi_t_minus_1", "ndpi_t0", "ndpi_window_t_minus_3_to_t"),
    "quarter_tax_payment": (
        "quarter_tax_payment_due_dummy",
        None,
        "quarter_tax_payment_t_minus_3",
        "quarter_tax_payment_t_minus_2",
        "quarter_tax_payment_t_minus_1",
        "quarter_tax_payment_t0",
        "quarter_tax_payment_window_t_minus_3_to_t",
    ),
}


class RuTaxDummyCalendarAdapter(BaseAdapter):
    name = "ru_tax_dummy_calendar"

    async def fetch(self, context: FetchContext) -> FetchResult:
        spec = context.source.csv
        if spec is None or spec.path is None:
            raise AdapterError(f"source {context.source.source_code} has no local csv path")

        path = spec.path
        extra = context.source.metadata.get("auto_extend", {})
        if extra.get("enabled", True) and self._is_release_check_period(extra):
            await self._extend_csv_if_available(path, extra, context)

        observations = self._read_observations(path, context)
        return FetchResult(observations=observations)

    def _read_observations(self, path: Path, context: FetchContext) -> list[RawObservationIn]:
        if not path.exists():
            raise AdapterError(f"tax dummy calendar csv does not exist: {path}")

        value_columns = self._value_columns(context)
        observations: list[RawObservationIn] = []
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                observed_at = self._date_to_datetime(date.fromisoformat(row["date"]))
                for column, series_code in value_columns.items():
                    latest = context.latest_observed_at_by_series.get(series_code)
                    if latest is not None and observed_at <= latest:
                        continue
                    value = self._parse_decimal(row.get(column, ""))
                    if value is None:
                        continue
                    observations.append(
                        RawObservationIn(
                            series_code=series_code,
                            source_code=context.source.source_code,
                            observed_at=observed_at,
                            value_numeric=value,
                            kind=ObservationKind.CALENDAR,
                            raw_payload={"column": column, **row},
                        )
                    )
        return observations

    async def _extend_csv_if_available(self, path: Path, extra: dict[str, Any], context: FetchContext) -> None:
        current_year = datetime.now(timezone.utc).year
        target_year = int(extra.get("target_year") or current_year + 1)

        existing_rows, fieldnames = self._read_rows(path)
        max_year = max((int(row["year"]) for row in existing_rows), default=0)
        if max_year >= target_year:
            return

        calendar = await self._fetch_production_calendar(target_year, extra, context)
        if calendar is None:
            return

        generated_rows = self._build_year_rows(target_year, calendar, fieldnames)
        self._write_rows(path, existing_rows + generated_rows, fieldnames)

    async def _fetch_production_calendar(
        self,
        year: int,
        extra: dict[str, Any],
        context: FetchContext,
    ) -> dict[date, bool] | None:
        template = str(extra.get("production_calendar_url_template") or "https://xmlcalendar.ru/data/ru/{year}/calendar.json")
        url = template.format(year=year)
        headers = {"User-Agent": context.settings.request_user_agent}
        async with httpx.AsyncClient(timeout=context.settings.request_timeout_seconds, headers=headers) as client:
            response = await client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if int(payload.get("year", 0)) != year:
            raise AdapterError(f"unexpected production calendar year in {url}: {payload.get('year')}")
        return self._parse_xmlcalendar_payload(year, payload)

    @staticmethod
    def _parse_xmlcalendar_payload(year: int, payload: dict[str, Any]) -> dict[date, bool]:
        special_days: dict[date, str] = {}
        for month_info in payload.get("months", []):
            month = int(month_info["month"])
            for token in str(month_info.get("days") or "").split(","):
                token = token.strip()
                if not token:
                    continue
                day = int(token.rstrip("*+"))
                special_days[date(year, month, day)] = token[-1] if token[-1] in "*+" else ""

        business: dict[date, bool] = {}
        current = date(year, 1, 1)
        while current.year == year:
            marker = special_days.get(current)
            if marker == "*":
                business[current] = True
            elif marker is not None:
                business[current] = False
            else:
                business[current] = current.weekday() < 5
            current += timedelta(days=1)
        return business

    def _build_year_rows(self, year: int, business_days: dict[date, bool], fieldnames: list[str]) -> list[dict[str, str]]:
        rows = [self._base_row(day, fieldnames, business_days[day]) for day in self._iter_year(year)]
        by_date = {date.fromisoformat(row["date"]): row for row in rows}

        events = self._tax_events(year, business_days)
        for tax_name, event_dates in events.items():
            self._mark_tax_events(tax_name, event_dates, by_date, business_days)
        self._mark_any_tax_fields(by_date)
        self._mark_quarter_end_fields(year, by_date, business_days)
        return rows

    def _tax_events(self, year: int, business_days: dict[date, bool]) -> dict[str, list[tuple[date, date]]]:
        vat_day = 28 if year >= 2023 else 25
        ndpi_day = 28 if year >= 2023 else 25
        events: dict[str, list[tuple[date, date]]] = {
            "vat": [],
            "profit_tax": [],
            "ndpi": [],
            "quarter_tax_payment": [],
        }
        for month in range(1, 13):
            for tax_name, due_day in (("vat", vat_day), ("profit_tax", 28), ("ndpi", ndpi_day)):
                statutory = date(year, month, min(due_day, monthrange(year, month)[1]))
                adjusted = self._roll_forward_to_business_day(statutory, business_days)
                events[tax_name].append((statutory, adjusted))

        quarter_months = (1, 3, 4, 7, 10) if year >= 2023 else (1, 3, 4, 7, 10)
        for month in quarter_months:
            due_days = (28,) if year >= 2023 or month == 3 else (25, 28)
            for due_day in due_days:
                statutory = date(year, month, min(due_day, monthrange(year, month)[1]))
                adjusted = self._roll_forward_to_business_day(statutory, business_days)
                events["quarter_tax_payment"].append((statutory, adjusted))
        return events

    def _mark_tax_events(
        self,
        tax_name: str,
        event_dates: list[tuple[date, date]],
        by_date: dict[date, dict[str, str]],
        business_days: dict[date, bool],
    ) -> None:
        due_col, event_col, minus3_col, minus2_col, minus1_col, t0_col, window_col = EVENT_COLUMNS[tax_name]
        for statutory, adjusted in event_dates:
            if adjusted not in by_date:
                continue
            row = by_date[adjusted]
            row[due_col] = "1"
            row[t0_col] = "1"
            row[window_col] = "1"
            if event_col:
                event_value = f"{tax_name}:{statutory.isoformat()}->{adjusted.isoformat()}"
                row[event_col] = event_value if not row[event_col] else f"{row[event_col]};{event_value}"

            business_offsets = {
                minus3_col: self._shift_business_days(adjusted, -3, business_days),
                minus2_col: self._shift_business_days(adjusted, -2, business_days),
                minus1_col: self._shift_business_days(adjusted, -1, business_days),
            }
            for col, offset_day in business_offsets.items():
                if offset_day in by_date:
                    by_date[offset_day][col] = "1"
            for day in self._iter_dates(min(business_offsets.values()), adjusted):
                if day in by_date:
                    by_date[day][window_col] = "1"
            week_start = self._shift_business_days(adjusted, -4, business_days)
            for day in self._iter_dates(week_start, adjusted):
                if day in by_date:
                    by_date[day]["tax_week_dummy"] = "1"

    @staticmethod
    def _mark_any_tax_fields(by_date: dict[date, dict[str, str]]) -> None:
        tax_prefixes = ("vat", "profit_tax", "ndpi")
        for row in by_date.values():
            due_count = sum(int(row[f"{prefix}_due_dummy"]) for prefix in tax_prefixes)
            row["tax_due_count"] = str(due_count)
            row["any_tax_due_dummy"] = "1" if due_count else "0"
            row["any_tax_t_minus_3"] = "1" if any(row[f"{prefix}_t_minus_3"] == "1" for prefix in tax_prefixes) else "0"
            row["any_tax_t_minus_2"] = "1" if any(row[f"{prefix}_t_minus_2"] == "1" for prefix in tax_prefixes) else "0"
            row["any_tax_t_minus_1"] = "1" if any(row[f"{prefix}_t_minus_1"] == "1" for prefix in tax_prefixes) else "0"
            row["any_tax_t0"] = "1" if any(row[f"{prefix}_t0"] == "1" for prefix in tax_prefixes) else "0"
            row["any_tax_window_t_minus_3_to_t"] = (
                "1" if any(row[f"{prefix}_window_t_minus_3_to_t"] == "1" for prefix in tax_prefixes) else "0"
            )

    def _mark_quarter_end_fields(self, year: int, by_date: dict[date, dict[str, str]], business_days: dict[date, bool]) -> None:
        for month in (3, 6, 9, 12):
            current = date(year, month, monthrange(year, month)[1])
            while not business_days[current]:
                current -= timedelta(days=1)
            by_date[current]["calendar_quarter_end_last_business_day_dummy"] = "1"
            start = self._shift_business_days(current, -3, business_days)
            for day in self._iter_dates(start, current):
                by_date[day]["calendar_quarter_end_window_t_minus_3_to_t"] = "1"

    @staticmethod
    def _base_row(day: date, fieldnames: list[str], is_business_day: bool) -> dict[str, str]:
        row = {field: "0" for field in fieldnames}
        row["date"] = day.isoformat()
        row["year"] = str(day.year)
        row["month"] = str(day.month)
        row["quarter"] = str((day.month - 1) // 3 + 1)
        row["dow"] = day.strftime("%A")
        row["is_ru_business_day"] = "1" if is_business_day else "0"
        for field in ("vat_event_date", "profit_tax_event_date", "ndpi_event_date"):
            if field in row:
                row[field] = ""
        return row

    @staticmethod
    def _roll_forward_to_business_day(day: date, business_days: dict[date, bool]) -> date:
        current = day
        while not business_days.get(current, current.weekday() < 5):
            current += timedelta(days=1)
        return current

    @staticmethod
    def _shift_business_days(day: date, offset: int, business_days: dict[date, bool]) -> date:
        step = 1 if offset > 0 else -1
        remaining = abs(offset)
        current = day
        while remaining:
            current += timedelta(days=step)
            if business_days.get(current, current.weekday() < 5):
                remaining -= 1
        return current

    @staticmethod
    def _is_release_check_period(extra: dict[str, Any]) -> bool:
        today = datetime.now(timezone.utc).date()
        release_month = int(extra.get("release_month", 9))
        window_days = int(extra.get("release_window_days", 15))
        start = date(today.year, release_month, 1) - timedelta(days=window_days)
        end = date(today.year, release_month, monthrange(today.year, release_month)[1]) + timedelta(days=window_days)
        return start <= today <= end

    @staticmethod
    def _read_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
        if not path.exists():
            raise AdapterError(f"tax dummy calendar csv does not exist: {path}")
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise AdapterError(f"tax dummy calendar csv has no header: {path}")
            return list(reader), list(reader.fieldnames)

    @staticmethod
    def _write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def _iter_year(year: int):
        current = date(year, 1, 1)
        while current.year == year:
            yield current
            current += timedelta(days=1)

    @staticmethod
    def _iter_dates(start: date, end: date):
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    @staticmethod
    def _date_to_datetime(value: date) -> datetime:
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)

    @staticmethod
    def _parse_decimal(value: str | None) -> Decimal | None:
        if value is None:
            return None
        try:
            return Decimal(value.replace(" ", "").replace(",", "."))
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _value_columns(context: FetchContext) -> dict[str, str]:
        raw = context.source.metadata.get("value_columns")
        if raw is None:
            return DEFAULT_VALUE_COLUMNS
        if isinstance(raw, str):
            loaded = json.loads(raw)
            if not isinstance(loaded, dict):
                raise AdapterError("metadata.value_columns must be a dict")
            return {str(column): str(series_code) for column, series_code in loaded.items()}
        if isinstance(raw, dict):
            return {str(column): str(series_code) for column, series_code in raw.items()}
        raise AdapterError("metadata.value_columns must be a dict")
