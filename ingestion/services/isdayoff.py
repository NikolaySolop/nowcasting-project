from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Final

import httpx


class IsDayOffError(RuntimeError):
    """Raised when isdayoff.ru cannot classify a date."""


@dataclass(frozen=True)
class IsDayOffResult:
    date: date
    code: int
    country_code: str

    @property
    def is_working_day(self) -> bool:
        return self.code in WORKING_DAY_CODES


WORKING_DAY_CODES: Final[set[int]] = {0, 2, 4}
DAY_TYPE_LABELS: Final[dict[int, str]] = {
    0: "working_day",
    1: "non_working_day",
    2: "short_working_day",
    4: "working_day",
    8: "holiday",
    100: "invalid_date_or_country",
    101: "data_not_found",
    199: "service_error",
}


class IsDayOffClient:
    base_url = "https://isdayoff.ru"

    def __init__(self, *, timeout_seconds: float = 10.0, base_url: str | None = None) -> None:
        self.timeout_seconds = timeout_seconds
        self.base_url = (base_url or self.base_url).rstrip("/")

    async def get_day_type(
        self,
        day: date,
        *,
        country_code: str = "ru",
        include_short_days: bool = True,
        six_day_week: bool = False,
        client: httpx.AsyncClient | None = None,
    ) -> IsDayOffResult:
        close_client = client is None
        http_client = client or httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True)
        try:
            response = await http_client.get(
                f"{self.base_url}/{day:%Y%m%d}",
                params={
                    "cc": country_code.lower(),
                    "pre": "1" if include_short_days else "0",
                    "sd": "1" if six_day_week else "0",
                },
                headers={"Accept": "text/plain"},
            )
        finally:
            if close_client:
                await http_client.aclose()

        raw_code = response.text.strip()
        try:
            code = int(raw_code)
        except ValueError as exc:
            raise IsDayOffError(f"isdayoff.ru returned non-numeric response: {raw_code!r}") from exc

        if response.status_code != 200 or code >= 100:
            label = DAY_TYPE_LABELS.get(code, "unknown_error")
            raise IsDayOffError(
                f"isdayoff.ru failed for {day.isoformat()}: status={response.status_code}, code={code} ({label})"
            )

        return IsDayOffResult(date=day, code=code, country_code=country_code.lower())

    async def is_working_day(
        self,
        day: date,
        *,
        country_code: str = "ru",
        include_short_days: bool = True,
        six_day_week: bool = False,
        client: httpx.AsyncClient | None = None,
    ) -> bool:
        result = await self.get_day_type(
            day,
            country_code=country_code,
            include_short_days=include_short_days,
            six_day_week=six_day_week,
            client=client,
        )
        return result.is_working_day
