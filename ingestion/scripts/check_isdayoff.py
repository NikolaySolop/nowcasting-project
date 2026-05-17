from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime

from ingestion.services.isdayoff import DAY_TYPE_LABELS, IsDayOffClient


def _parse_date(raw: str | None) -> date:
    if not raw or raw == "today":
        return date.today()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"invalid date: {raw}")


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Check whether a date is a working day using isdayoff.ru")
    parser.add_argument("date", nargs="?", type=_parse_date, default=date.today())
    parser.add_argument("--country-code", default="ru")
    parser.add_argument("--six-day-week", action="store_true")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    client = IsDayOffClient(timeout_seconds=args.timeout)
    result = await client.get_day_type(
        args.date,
        country_code=args.country_code,
        six_day_week=args.six_day_week,
    )
    print(
        json.dumps(
            {
                "date": result.date.isoformat(),
                "country_code": result.country_code,
                "code": result.code,
                "label": DAY_TYPE_LABELS.get(result.code, "unknown"),
                "is_working_day": result.is_working_day,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(_main())
