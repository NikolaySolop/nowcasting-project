import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.core.config import Settings
from ingestion.scheduler.jobs import run_source_job
from ingestion.services.source_registry import SourceRegistry


logger = logging.getLogger(__name__)

WEEKDAY_NAMES = {
    0: "Sunday",
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
}
CRON_WEEKDAY_ALIASES = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}
AP_SCHEDULER_WEEKDAYS = {
    0: "sun",
    1: "mon",
    2: "tue",
    3: "wed",
    4: "thu",
    5: "fri",
    6: "sat",
}


class IngestionScheduler:
    timezone = ZoneInfo("Asia/Dubai")

    def __init__(
        self,
        registry: SourceRegistry,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.registry = registry
        self.settings = settings
        self.session_factory = session_factory
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)

    def configure(self, source_codes: list[str] | None = None) -> list[str]:
        if source_codes:
            sources = [self.registry.get_source(source_code) for source_code in source_codes]
            sources = [source for source in sources if source.enabled]
        else:
            sources = self.registry.list_sources(enabled_only=True)

        scheduled_source_codes: list[str] = []
        for source in sources:
            if not source.schedule_cron:
                continue
            self.scheduler.add_job(
                run_source_job,
                trigger="cron",
                args=[self.registry, source.source_code, self.settings, self.session_factory],
                id=f"ingest:{source.source_code}",
                replace_existing=True,
                **self._parse_cron(source.schedule_cron),
            )
            scheduled_source_codes.append(source.source_code)
            logger.info(
                "Scheduled %s: %s",
                source.source_code,
                self._humanize_cron(source.schedule_cron),
            )
        return scheduled_source_codes

    def start(self, source_codes: list[str] | None = None) -> list[str]:
        scheduled_source_codes = self.configure(source_codes)
        if scheduled_source_codes and not self.scheduler.running:
            self.scheduler.start()
        return scheduled_source_codes

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown()

    @staticmethod
    def _parse_cron(expression: str) -> dict[str, str]:
        minute, hour, day, month, day_of_week = IngestionScheduler._split_cron(expression)
        return {
            "minute": minute,
            "hour": hour,
            "day": day,
            "month": month,
            "day_of_week": IngestionScheduler._to_apscheduler_day_of_week(day_of_week),
        }

    @classmethod
    def _humanize_cron(cls, expression: str) -> str:
        minute, hour, day, month, day_of_week = cls._split_cron(expression)
        time_label = cls._humanize_time(minute, hour)
        weekday_values = cls._expand_weekdays(day_of_week)

        if day == "*" and month == "*":
            if weekday_values is None:
                if time_label.startswith("every "):
                    return f"{time_label} Asia/Dubai"
                return f"daily {time_label} Asia/Dubai"
            if weekday_values == {1, 2, 3, 4, 5}:
                return f"every weekday {time_label} Asia/Dubai"
            return f"every {cls._format_weekdays(weekday_values)} {time_label} Asia/Dubai"

        if day != "*" and month == "*" and weekday_values is None:
            return f"on day {day} of every month {time_label} Asia/Dubai"

        return f"cron '{expression}' Asia/Dubai"

    @staticmethod
    def _split_cron(expression: str) -> tuple[str, str, str, str, str]:
        parts = expression.split()
        if len(parts) != 5:
            raise ValueError(f"expected 5-field cron expression, got {len(parts)} fields: {expression}")
        minute, hour, day, month, day_of_week = parts
        return minute, hour, day, month, day_of_week

    @classmethod
    def _to_apscheduler_day_of_week(cls, field: str) -> str:
        weekdays = cls._expand_weekdays(field)
        if weekdays is None:
            return "*"
        return ",".join(AP_SCHEDULER_WEEKDAYS[weekday] for weekday in sorted(weekdays))

    @classmethod
    def _expand_weekdays(cls, field: str) -> set[int] | None:
        field = field.strip().lower()
        if field == "*":
            return None

        values: set[int] = set()
        for part in field.split(","):
            values.update(cls._expand_weekday_part(part.strip()))
        return values

    @classmethod
    def _expand_weekday_part(cls, part: str) -> set[int]:
        if "/" in part:
            base, step_raw = part.split("/", 1)
            step = int(step_raw)
        else:
            base = part
            step = 1

        if base == "*":
            start, end = 0, 6
        elif "-" in base:
            start_raw, end_raw = base.split("-", 1)
            start = cls._weekday_to_number(start_raw)
            end = cls._weekday_to_number(end_raw)
        else:
            return {cls._weekday_to_number(base)}

        if start <= end:
            return set(range(start, end + 1, step))
        return set(list(range(start, 7, step)) + list(range(0, end + 1, step)))

    @staticmethod
    def _weekday_to_number(value: str) -> int:
        normalized = value.strip().lower()
        if normalized in CRON_WEEKDAY_ALIASES:
            return CRON_WEEKDAY_ALIASES[normalized]
        number = int(normalized)
        if number == 7:
            return 0
        if 0 <= number <= 6:
            return number
        raise ValueError(f"invalid cron day-of-week value: {value}")

    @staticmethod
    def _humanize_time(minute: str, hour: str) -> str:
        minutes = IngestionScheduler._expand_number_field(minute, minimum=0, maximum=59)
        hours = IngestionScheduler._expand_number_field(hour, minimum=0, maximum=23)
        if minutes is not None and hours is not None:
            times = [f"{hour_value:02d}:{minute_value:02d}" for hour_value in sorted(hours) for minute_value in sorted(minutes)]
            if len(times) <= 8:
                return f"at {IngestionScheduler._format_list(times)}"

        if minute == "0" and hour == "*":
            return "every hour"
        if minute.startswith("*/") and hour == "*":
            return f"every {minute[2:]} minutes"
        if minute.startswith("*/") and hours is not None:
            hour_label = IngestionScheduler._format_list([f"{hour_value:02d}" for hour_value in sorted(hours)])
            return f"every {minute[2:]} minutes during hour {hour_label}"
        return f"at cron time {hour}:{minute}"

    @staticmethod
    def _expand_number_field(field: str, *, minimum: int, maximum: int) -> set[int] | None:
        field = field.strip()
        if field == "*":
            return None

        values: set[int] = set()
        for part in field.split(","):
            values.update(IngestionScheduler._expand_number_part(part.strip(), minimum=minimum, maximum=maximum))
        return values

    @staticmethod
    def _expand_number_part(part: str, *, minimum: int, maximum: int) -> set[int]:
        if "/" in part:
            base, step_raw = part.split("/", 1)
            step = int(step_raw)
        else:
            base = part
            step = 1

        if base == "*":
            start, end = minimum, maximum
        elif "-" in base:
            start_raw, end_raw = base.split("-", 1)
            start = int(start_raw)
            end = int(end_raw)
        else:
            value = int(base)
            if not minimum <= value <= maximum:
                raise ValueError(f"cron value {value} is outside {minimum}-{maximum}")
            return {value}

        if not minimum <= start <= maximum or not minimum <= end <= maximum:
            raise ValueError(f"cron range {part} is outside {minimum}-{maximum}")
        if start > end:
            raise ValueError(f"cron range {part} has start greater than end")
        return set(range(start, end + 1, step))

    @staticmethod
    def _format_weekdays(weekdays: set[int]) -> str:
        names = [WEEKDAY_NAMES[weekday] for weekday in sorted(weekdays)]
        return IngestionScheduler._format_list(names)

    @staticmethod
    def _format_list(items: list[str]) -> str:
        if len(items) == 1:
            return items[0]
        if len(items) == 2:
            return " and ".join(items)
        return f"{', '.join(items[:-1])}, and {items[-1]}"
