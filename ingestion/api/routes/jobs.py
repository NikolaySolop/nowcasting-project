from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.api.routes.deps import get_registry, get_session
from ingestion.core.config import settings
from ingestion.schemas.jobs import IngestionJobRequest, IngestionJobResult, IngestionJobStatus
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry

router = APIRouter()


@router.post("/run", response_model=IngestionJobResult)
async def run_job(
    request: Request,
    job_request: IngestionJobRequest,
    session: AsyncSession = Depends(get_session),
    registry: SourceRegistry = Depends(get_registry),
) -> IngestionJobResult:
    service = IngestionService(session=session, registry=registry, settings=settings)
    result = await service.run(job_request.source_codes)
    if result.status != IngestionJobStatus.SUCCEEDED:
        return result

    scheduler = getattr(request.app.state, "ingestion_scheduler", None)
    if scheduler is None:
        result.messages.append("scheduler is not configured for this application process")
        return result

    scheduled_source_codes = scheduler.start(job_request.source_codes)
    if scheduled_source_codes:
        result.messages.append(f"scheduler started for: {', '.join(scheduled_source_codes)}")
    return result
