import csv
from io import StringIO

from fastapi import APIRouter, Depends
from fastapi.responses import Response

from ingestion.api.routes.deps import get_registry
from ingestion.schemas.sources import SourceDefinition
from ingestion.services.source_indicator_table import SourceIndicatorTableService
from ingestion.services.source_registry import SourceRegistry

router = APIRouter()


@router.get("", response_model=list[SourceDefinition])
async def list_sources(registry: SourceRegistry = Depends(get_registry)) -> list[SourceDefinition]:
    return registry.list_sources()


@router.get("/indicator-table")
async def list_source_indicator_table(
    enabled_only: bool = False,
    registry: SourceRegistry = Depends(get_registry),
) -> Response:
    service = SourceIndicatorTableService(registry)
    rows = service.build(enabled_only=enabled_only)

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "номер",
            "показатель из source",
            "с какой даты данные",
            "частота показателя на backfill",
            "частота показателя live",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.number,
                row.indicator,
                row.data_from.isoformat() if row.data_from else "",
                row.backfill_frequency or "",
                row.live_frequency or "",
            ]
        )

    return Response(
        content=output.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="source_indicator_table.csv"'},
    )
