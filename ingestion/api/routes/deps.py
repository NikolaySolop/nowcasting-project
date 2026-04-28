from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.core.config import settings
from ingestion.services.source_registry import SourceRegistry
from storage.db.session import async_session_factory

registry = SourceRegistry()
if settings.source_config_path is not None:
    registry.load_json(settings.source_config_path)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


def get_registry() -> SourceRegistry:
    return registry
