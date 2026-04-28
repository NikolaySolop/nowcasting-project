from ingestion.core.config import Settings
from ingestion.services.ingestion_service import IngestionService


async def run_source_job(service: IngestionService, source_code: str, settings: Settings):
    return await service.run([source_code])
