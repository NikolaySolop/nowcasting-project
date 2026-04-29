from fastapi import APIRouter, Depends

from ingestion.api.routes.deps import get_registry
from ingestion.schemas.sources import SourceDefinition
from ingestion.services.source_registry import SourceRegistry

router = APIRouter()


@router.get("", response_model=list[SourceDefinition])
async def list_sources(registry: SourceRegistry = Depends(get_registry)) -> list[SourceDefinition]:
    return registry.list_sources()
