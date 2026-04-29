import asyncio
import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.core.config import Settings
from ingestion.schemas.jobs import IngestionJobResult
from ingestion.schemas.observations import RawObservationIn
from ingestion.schemas.sources import SourceDefinition, SourceKind
from ingestion.services.source_registry import SourceRegistry
from ingestion.services.validation_service import ValidationService
from storage.models.enums import SourceType
from storage.models.raw_obsevations import RawObservation
from storage.repositories.raw_observation import RawObservationRepository
from storage.repositories.series import SeriesRepository
from storage.repositories.source import DataSourceRepository

logger = logging.getLogger(__name__)


class IngestionService:
    def __init__(
        self,
        session: AsyncSession,
        registry: SourceRegistry,
        settings: Settings,
        validation: ValidationService | None = None,
    ) -> None:
        self.session = session
        self.registry = registry
        self.settings = settings
        self.validation = validation or ValidationService()
        self.sources = DataSourceRepository(session)
        self.series = SeriesRepository(session)
        self.observations = RawObservationRepository(session)

    async def run(self, source_codes: list[str] | None = None) -> IngestionJobResult:
        selected_sources = self._select_sources(source_codes)
        result = IngestionJobResult(source_codes=[source.source_code for source in selected_sources])
        result.start()

        for source in selected_sources:
            try:
                loaded_count, duplicate_count = await self._run_source(source)
                result.loaded_count += loaded_count
                result.duplicate_count += duplicate_count
            except Exception as exc:
                logger.exception("source ingestion failed", extra={"source_code": source.source_code})
                result.error_count += 1
                result.messages.append(f"{source.source_code}: {exc}")

        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            result.error_count += 1
            result.messages.append(f"database integrity error: {exc}")

        result.finish()
        return result

    def _select_sources(self, source_codes: list[str] | None) -> list[SourceDefinition]:
        if source_codes:
            return [self.registry.get_source(source_code) for source_code in source_codes]
        return self.registry.list_sources(enabled_only=True)

    async def _run_source(self, source: SourceDefinition) -> tuple[int, int]:
        adapter = self.registry.build_adapter(source)
        latest_observed = await self._latest_observed_at_by_series(source)
        fetch_result = await self._fetch_with_retries(source, adapter, latest_observed)
        for observation in fetch_result.observations:
            observation.vintage_at = fetch_result.loaded_at

        valid_observations, duplicate_count = self.validation.deduplicate_batch(fetch_result.observations)
        storage_source = await self._ensure_source(source)

        loaded_count = 0
        for observation in valid_observations:
            storage_series = await self._ensure_series(source, observation.series_code)
            if await self._already_loaded(storage_series.id, storage_source.id, observation):
                duplicate_count += 1
                continue
            await self.observations.insert(
                series_id=storage_series.id,
                source_id=storage_source.id,
                observed_at=observation.observed_at,
                period_start=observation.period_start,
                period_end=observation.period_end,
                value_numeric=observation.value_numeric,
                value_text=observation.value_text,
                publication_at=observation.publication_at,
                vintage_at=observation.vintage_at,
                is_revised=observation.is_revised,
                is_final=observation.is_final,
                raw_payload=observation.raw_payload,
            )
            loaded_count += 1

        logger.info(
            "source loaded",
            extra={
                "source_code": source.source_code,
                "loaded_count": loaded_count,
                "duplicate_count": duplicate_count,
                "loaded_at": fetch_result.loaded_at.isoformat(),
            },
        )
        return loaded_count, duplicate_count

    async def _fetch_with_retries(
        self,
        source: SourceDefinition,
        adapter: BaseAdapter,
        latest_observed_at_by_series: dict[str, datetime],
    ) -> FetchResult:
        context = FetchContext(
            source=source,
            settings=self.settings,
            latest_observed_at_by_series=latest_observed_at_by_series,
        )
        last_error: Exception | None = None
        for attempt in range(1, self.settings.retry_attempts + 1):
            try:
                return await adapter.fetch(context)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "source fetch attempt failed",
                    extra={"source_code": source.source_code, "attempt": attempt, "error": str(exc)},
                )
                if attempt < self.settings.retry_attempts:
                    await asyncio.sleep(self.settings.retry_backoff_seconds * attempt)
        raise AdapterError(f"source {source.source_code} failed after retries: {last_error}")

    async def _ensure_source(self, source: SourceDefinition):
        existing = await self.sources.get_by_source_code(source.source_code)
        if existing is not None:
            return existing
        return await self.sources.insert(
            source_code=source.source_code,
            source_name=source.source_name,
            source_type=self._to_storage_source_type(source.source_type),
        )

    async def _ensure_series(self, source: SourceDefinition, series_code: str):
        existing = await self.series.get_by_series_code(series_code)
        if existing is not None:
            return existing
        series_definition = next((item for item in source.series if item.series_code == series_code), None)
        return await self.series.insert(
            series_code=series_code,
            series_name=series_definition.series_name if series_definition and series_definition.series_name else series_code,
        )


    async def _latest_observed_at_by_series(self, source: SourceDefinition) -> dict[str, datetime]:
        storage_source = await self.sources.get_by_source_code(source.source_code)
        if storage_source is None:
            return {}
        return await self.observations.latest_observed_at_by_series(storage_source.id)

    async def _already_loaded(self, series_id, source_id, observation: RawObservationIn) -> bool:
        stmt = select(RawObservation).where(
            RawObservation.series_id == series_id,
            RawObservation.source_id == source_id,
            RawObservation.observed_at == observation.observed_at,
            RawObservation.publication_at == observation.publication_at,
            RawObservation.value_text == observation.value_text,
        )
        if observation.value_numeric is None:
            stmt = stmt.where(RawObservation.value_numeric.is_(None))
        else:
            stmt = stmt.where(RawObservation.value_numeric == Decimal(observation.value_numeric))
        return await self.session.scalar(stmt) is not None

    @staticmethod
    def _to_storage_source_type(source_type: SourceKind) -> SourceType:
        return SourceType(source_type.value)
