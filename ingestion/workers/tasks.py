from ingestion.core.config import settings
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry
from storage.db.session import async_session_factory


async def ingest_sources(source_codes: list[str] | None = None):
    registry = SourceRegistry()
    if settings.source_config_path is not None:
        registry.load_json(settings.source_config_path)

    async with async_session_factory() as session:
        service = IngestionService(session=session, registry=registry, settings=settings)
        return await service.run(source_codes)
