from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ingestion.core.config import Settings
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry


async def run_source_job(
    registry: SourceRegistry,
    source_code: str,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
):
    async with session_factory() as session:
        service = IngestionService(session=session, registry=registry, settings=settings)
        return await service.run([source_code])
