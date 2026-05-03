from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.api.routes.deps import get_session
from ingestion.core.config import settings
from ingestion.schemas.exports import CsvExportRequest, CsvExportResult
from ingestion.services.csv_export_service import CsvExportService

router = APIRouter()


@router.post("/csv", response_model=CsvExportResult)
async def export_csv(
    request: CsvExportRequest,
    session: AsyncSession = Depends(get_session),
) -> CsvExportResult:
    service = CsvExportService(session=session, export_dir=settings.csv_export_dir)
    return await service.export(series_codes=request.series_codes, source_codes=request.source_codes)
