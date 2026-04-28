from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.api.routes.deps import get_registry, get_session
from ingestion.core.config import settings
from ingestion.schemas.jobs import IngestionJobRequest, IngestionJobResult
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry

router = APIRouter()


@router.post("/run", response_model=IngestionJobResult)
async def run_job(
    request: IngestionJobRequest,
    session: AsyncSession = Depends(get_session),
    registry: SourceRegistry = Depends(get_registry),
) -> IngestionJobResult:
    service = IngestionService(session=session, registry=registry, settings=settings)
    return await service.run(request.source_codes)
