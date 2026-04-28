from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ingestion.core.config import Settings
from ingestion.scheduler.jobs import run_source_job
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry


class IngestionScheduler:
    def __init__(self, registry: SourceRegistry, service: IngestionService, settings: Settings) -> None:
        self.registry = registry
        self.service = service
        self.settings = settings
        self.scheduler = AsyncIOScheduler()

    def configure(self) -> None:
        for source in self.registry.list_sources(enabled_only=True):
            if not source.schedule_cron:
                continue
            self.scheduler.add_job(
                run_source_job,
                trigger="cron",
                args=[self.service, source.source_code, self.settings],
                id=f"ingest:{source.source_code}",
                replace_existing=True,
                **self._parse_cron(source.schedule_cron),
            )

    def start(self) -> None:
        self.configure()
        self.scheduler.start()

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
