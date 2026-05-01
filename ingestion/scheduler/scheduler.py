from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.core.config import Settings
from ingestion.scheduler.jobs import run_source_job
from ingestion.services.source_registry import SourceRegistry


class IngestionScheduler:
    def __init__(
        self,
        registry: SourceRegistry,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self.registry = registry
        self.settings = settings
        self.session_factory = session_factory
        self.scheduler = AsyncIOScheduler()

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
        minute, hour, day, month, day_of_week = expression.split()
        return {
            "minute": minute,
            "hour": hour,
            "day": day,
            "month": month,
            "day_of_week": day_of_week,
        }
