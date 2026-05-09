import asyncio
import logging
from datetime import datetime, timedelta, timezone
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
        loaded_count, duplicate_count = await self._store_observations(
            source,
            fetch_result.observations,
            loaded_at=fetch_result.loaded_at,
            commit=False,
        )
        loaded_count += fetch_result.persisted_loaded_count
        duplicate_count += fetch_result.persisted_duplicate_count

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
            observation_sink=lambda observations, loaded_at: self._store_observations(
                source,
                observations,
                loaded_at=loaded_at,
                commit=True,
            ),
        )
        last_error: Exception | None = None
        retry_attempts = self.settings.retry_attempts
        if source.scrape is not None and bool((source.scrape.extra or {}).get("streaming_persistence", False)):
            retry_attempts = 1
        for attempt in range(1, retry_attempts + 1):
            try:
                return await adapter.fetch(context)
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "source fetch attempt failed",
                    extra={"source_code": source.source_code, "attempt": attempt, "error": str(exc)},
                )
                if attempt < retry_attempts:
                    await asyncio.sleep(self.settings.retry_backoff_seconds * attempt)
        raise AdapterError(f"source {source.source_code} failed after retries: {last_error}")

    async def _store_observations(
        self,
        source: SourceDefinition,
        observations: list[RawObservationIn],
        *,
        loaded_at: datetime,
        commit: bool,
    ) -> tuple[int, int]:
        if not observations:
            return 0, 0

        preserve_vintage = bool(source.csv and source.csv.vintage_date_column)
        if source.scrape and bool((source.scrape.extra or {}).get("preserve_vintage_at", False)):
            preserve_vintage = True
        if not preserve_vintage:
            for observation in observations:
                observation.vintage_at = loaded_at

        valid_observations, duplicate_count = self.validation.deduplicate_batch(observations)
        storage_source = await self._ensure_source(source)
        series_by_code = await self._ensure_series_batch(
            source,
            {observation.series_code for observation in valid_observations},
        )
        existing_keys = await self._existing_observation_keys(storage_source.id, series_by_code, valid_observations)

        loaded_count = 0
        new_entities: list[RawObservation] = []
        for observation in valid_observations:
            storage_series = series_by_code[observation.series_code]
            key = self._observation_storage_key(storage_series.id, observation)
            if key in existing_keys:
                duplicate_count += 1
                continue
            existing_keys.add(key)
            new_entities.append(
                RawObservation(
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
            )
            loaded_count += 1

        if new_entities:
            self.session.add_all(new_entities)
            await self.session.flush()

        if commit:
            try:
                await self.session.commit()
            except IntegrityError:
                await self.session.rollback()
                raise

        return loaded_count, duplicate_count

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

    async def _ensure_series_batch(self, source: SourceDefinition, series_codes: set[str]) -> dict[str, object]:
        if not series_codes:
            return {}

        stmt = select(self.series.model).where(self.series.model.series_code.in_(series_codes))
        existing_rows = list((await self.session.scalars(stmt)).all())
        series_by_code = {row.series_code: row for row in existing_rows}

        missing_codes = series_codes - set(series_by_code)
        if missing_codes:
            definitions = {item.series_code: item for item in source.series}
            new_rows = []
            for series_code in missing_codes:
                definition = definitions.get(series_code)
                new_rows.append(
                    self.series.model(
                        series_code=series_code,
                        series_name=definition.series_name if definition and definition.series_name else series_code,
                    )
                )
            self.session.add_all(new_rows)
            await self.session.flush()
            series_by_code.update({row.series_code: row for row in new_rows})

        return series_by_code


    async def _latest_observed_at_by_series(self, source: SourceDefinition) -> dict[str, datetime]:
        use_global = bool((source.scrape.extra if source.scrape else {}).get("global_series_latest"))
        if use_global:
            series_codes = [s.series_code for s in source.series]
            existing: dict[str, datetime] = await self.observations.latest_observed_at_by_series_global(series_codes)
        else:
            storage_source = await self.sources.get_by_source_code(source.source_code)
            if storage_source is None:
                existing = {}
            else:
                existing = await self.observations.latest_observed_at_by_series(storage_source.id)

        start_date = source.scrape.start_date if source.scrape else None
        if start_date is None:
            return existing

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        interval_minutes = int((source.scrape.extra or {}).get("interval_minutes", 15))
        bootstrap_latest = start_date - timedelta(minutes=max(1, interval_minutes))
        for series in source.series:
            existing.setdefault(series.series_code, bootstrap_latest)
        return existing

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

    async def _existing_observation_keys(
        self,
        source_id,
        series_by_code: dict[str, object],
        observations: list[RawObservationIn],
    ) -> set[tuple[object, ...]]:
        if not observations:
            return set()

        series_ids = {series_by_code[observation.series_code].id for observation in observations}
        min_observed_at = min(observation.observed_at for observation in observations)
        max_observed_at = max(observation.observed_at for observation in observations)
        stmt = select(
            RawObservation.series_id,
            RawObservation.observed_at,
            RawObservation.publication_at,
            RawObservation.value_numeric,
            RawObservation.value_text,
        ).where(
            RawObservation.source_id == source_id,
            RawObservation.series_id.in_(series_ids),
            RawObservation.observed_at >= min_observed_at,
            RawObservation.observed_at <= max_observed_at,
        )
        rows = (await self.session.execute(stmt)).all()
        return {
            (
                series_id,
                observed_at,
                publication_at,
                Decimal(value_numeric) if value_numeric is not None else None,
                value_text,
            )
            for series_id, observed_at, publication_at, value_numeric, value_text in rows
        }

    @staticmethod
    def _observation_storage_key(series_id, observation: RawObservationIn) -> tuple[object, ...]:
        return (
            series_id,
            observation.observed_at,
            observation.publication_at,
            Decimal(observation.value_numeric) if observation.value_numeric is not None else None,
            observation.value_text,
        )

    @staticmethod
    def _to_storage_source_type(source_type: SourceKind) -> SourceType:
        return SourceType(source_type.value)
