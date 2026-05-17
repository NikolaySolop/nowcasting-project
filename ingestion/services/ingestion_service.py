import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ingestion.adapters.base import AdapterError, BaseAdapter, FetchContext, FetchResult
from ingestion.core.config import Settings
from ingestion.schemas.jobs import IngestionJobResult
from ingestion.schemas.observations import ObservationIn, RawObservationIn
from ingestion.schemas.sources import SeriesDefinition, SourceDefinition, SourceKind
from ingestion.services.source_registry import SourceRegistry
from ingestion.services.validation_service import ValidationService
from storage.models.enums import Frequency, SourceType, TransformType
from storage.models.observations import Observation
from storage.models.raw_obsevations import RawObservation
from storage.repositories.observation import ObservationRepository
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
        self.table_observations = ObservationRepository(session)

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
        await self._ensure_source(source)
        if source.series:
            await self._ensure_series_batch(source, {series.series_code for series in source.series})
        loaded_count, duplicate_count = await self._store_observations(
            source,
            fetch_result.observations,
            loaded_at=fetch_result.loaded_at,
            commit=False,
        )
        table_loaded_count, table_duplicate_count = await self._store_table_observations(
            source,
            fetch_result.table_observations,
            loaded_at=fetch_result.loaded_at,
            commit=False,
        )
        loaded_count += table_loaded_count
        duplicate_count += table_duplicate_count
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

    async def _store_table_observations(
        self,
        source: SourceDefinition,
        observations: list[ObservationIn],
        *,
        loaded_at: datetime,
        commit: bool,
    ) -> tuple[int, int]:
        if not observations:
            return 0, 0

        valid_observations, duplicate_count = self.validation.deduplicate_batch(observations)
        storage_source = await self._ensure_source(source)
        series_by_code = await self._ensure_series_batch(
            source,
            {observation.series_code for observation in valid_observations},
        )
        extra = source.scrape.extra if source.scrape is not None else {}
        if bool(extra.get("replace_table_observations", False)):
            await self.session.execute(
                delete(Observation).where(
                    Observation.source_id == storage_source.id,
                    Observation.series_id.in_([series.id for series in series_by_code.values()]),
                )
            )
        if bool(extra.get("delete_non_publication_observations", False)):
            await self.session.execute(
                delete(Observation).where(
                    Observation.source_id == storage_source.id,
                    Observation.series_id.in_([series.id for series in series_by_code.values()]),
                    Observation.reference_start != Observation.published_at,
                )
            )

        existing_rows = await self._existing_table_observation_rows(storage_source.id, series_by_code, valid_observations)
        latest_by_reference: dict[tuple[object, ...], tuple[datetime, Decimal]] = {}
        published_at_by_reference: dict[tuple[object, ...], set[datetime]] = {}
        latest_by_identity: dict[tuple[object, ...], tuple[object, datetime, Decimal]] = {}
        previous_value_by_series = await self._previous_table_value_by_series(
            storage_source.id,
            series_by_code,
            valid_observations,
        )
        for series_id, reference_date, reference_start, reference_end, published_at, value in existing_rows:
            reference_key = (series_id, reference_date, reference_start, reference_end)
            value = Decimal(value)
            latest = latest_by_reference.get(reference_key)
            if latest is None or published_at > latest[0]:
                latest_by_reference[reference_key] = (published_at, value)
            published_at_by_reference.setdefault(reference_key, set()).add(published_at)
            latest_by_identity[(series_id, reference_start, published_at)] = (reference_date, reference_end, value)

        loaded_count = 0
        new_entities: list[Observation] = []
        update_same_value_published_at = bool(
            extra.get("update_same_value_published_at", False)
        )
        update_same_published_reference_end = bool(
            extra.get("update_same_published_reference_end", False)
        )
        for observation in valid_observations:
            storage_series = series_by_code[observation.series_code]
            reference_key = (
                storage_series.id,
                observation.reference_date,
                observation.reference_start,
                observation.reference_end,
            )
            value = Decimal(observation.value)
            identity_key = (storage_series.id, observation.reference_start, observation.published_at)
            existing_identity = latest_by_identity.get(identity_key)
            if update_same_published_reference_end and existing_identity is not None:
                existing_reference_date, existing_reference_end, existing_value = existing_identity
                if (
                    existing_reference_date != observation.reference_date
                    or existing_reference_end != observation.reference_end
                    or existing_value != value
                ):
                    await self.session.execute(
                        update(Observation)
                        .where(
                            Observation.source_id == storage_source.id,
                            Observation.series_id == storage_series.id,
                            Observation.reference_start == observation.reference_start,
                            Observation.published_at == observation.published_at,
                        )
                        .values(
                            reference_date=observation.reference_date,
                            reference_end=observation.reference_end,
                            value=value,
                        )
                    )
                    latest_by_identity[identity_key] = (
                        observation.reference_date,
                        observation.reference_end,
                        value,
                    )
                    loaded_count += 1
                    continue
                duplicate_count += 1
                continue

            latest = latest_by_reference.get(reference_key)
            if latest is not None and latest[1] == value:
                existing_published_ats = published_at_by_reference.get(reference_key, set())
                if (
                    update_same_value_published_at
                    and latest[0] != observation.published_at
                    and observation.published_at not in existing_published_ats
                ):
                    await self.session.execute(
                        update(Observation)
                        .where(
                            Observation.source_id == storage_source.id,
                            Observation.series_id == storage_series.id,
                            Observation.reference_date == observation.reference_date,
                            Observation.reference_start == observation.reference_start,
                            Observation.reference_end == observation.reference_end,
                            Observation.published_at == latest[0],
                        )
                        .values(published_at=observation.published_at)
                    )
                    existing_published_ats.discard(latest[0])
                    existing_published_ats.add(observation.published_at)
                    latest_by_reference[reference_key] = (observation.published_at, value)
                    loaded_count += 1
                    continue
                duplicate_count += 1
                continue
            if (
                observation.skip_equal_to_previous
                and latest is None
                and previous_value_by_series.get(storage_series.id) == value
            ):
                duplicate_count += 1
                continue
            if await self._compress_equal_table_observation_run(
                source,
                storage_source.id,
                storage_series.id,
                observation,
                value,
                loaded_at,
            ):
                duplicate_count += 1
                continue
            previous_value_by_series[storage_series.id] = value
            published_at = observation.published_at
            if latest is not None:
                published_at = self._revision_published_at(
                    loaded_at,
                    published_at_by_reference.get(reference_key, set()),
                )
            published_at_by_reference.setdefault(reference_key, set()).add(published_at)
            latest_by_reference[reference_key] = (published_at, value)
            new_entities.append(
                Observation(
                    series_id=storage_series.id,
                    source_id=storage_source.id,
                    reference_date=observation.reference_date,
                    reference_start=observation.reference_start,
                    reference_end=observation.reference_end,
                    value=value,
                    published_at=published_at,
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
        series_definition = next((item for item in source.series if item.series_code == series_code), None)
        if existing is not None:
            await self._update_series_metadata(existing, series_definition)
            return existing
        return await self.series.insert(
            **self._series_values(series_code, series_definition),
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
                new_rows.append(self.series.model(**self._series_values(series_code, definition)))
            self.session.add_all(new_rows)
            await self.session.flush()
            series_by_code.update({row.series_code: row for row in new_rows})

        definitions = {item.series_code: item for item in source.series}
        for series_code, row in series_by_code.items():
            if series_code in definitions:
                await self._update_series_metadata(row, definitions[series_code])

        return series_by_code

    @staticmethod
    def _series_values(series_code: str, definition: SeriesDefinition | None) -> dict[str, object]:
        values: dict[str, object] = {
            "series_code": series_code,
            "series_name": definition.series_name if definition and definition.series_name else series_code,
        }
        if definition is None:
            return values
        for field in (
            "frequency",
            "group_code",
            "subgroup_code",
            "description",
            "units",
            "default_transform",
            "is_model_input",
        ):
            value = getattr(definition, field)
            if value is not None:
                if field == "frequency":
                    value = Frequency(value)
                elif field == "default_transform":
                    value = TransformType(value)
                values[field] = value
        return values

    async def _update_series_metadata(self, series: object, definition: SeriesDefinition | None) -> None:
        if definition is None:
            return

        changed = False
        for field, value in self._series_values(series.series_code, definition).items():
            if getattr(series, field) != value:
                setattr(series, field, value)
                changed = True

        if changed:
            await self.session.flush()

    async def _latest_observed_at_by_series(self, source: SourceDefinition) -> dict[str, datetime]:
        if self._stores_table_observations(source):
            default_revision_check = 5 if source.adapter_name in {"fred_observations", "fred_sofr"} else 0
            revision_check_observations = int(
                (source.scrape.extra if source.scrape else {}).get(
                    "revision_check_observations",
                    default_revision_check,
                )
            )
            return await self._latest_reference_start_by_series(
                source,
                revision_check_observations=revision_check_observations,
            )

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

    async def _latest_reference_start_by_series(
        self,
        source: SourceDefinition,
        *,
        revision_check_observations: int = 0,
    ) -> dict[str, datetime]:
        storage_source = await self.sources.get_by_source_code(source.source_code)
        if storage_source is None:
            existing: dict[str, datetime] = {}
        elif revision_check_observations > 0:
            revision_starts = await self.table_observations.revision_check_start_by_series(
                storage_source.id,
                revision_check_observations,
            )
            existing = {
                series_code: reference_start - timedelta(days=1)
                for series_code, reference_start in revision_starts.items()
            }
        else:
            existing = await self.table_observations.latest_reference_start_by_series(storage_source.id)

        start_date = source.scrape.start_date if source.scrape else None
        if start_date is None:
            return existing

        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        bootstrap_latest = start_date - timedelta(days=1)
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

    async def _existing_table_observation_rows(
        self,
        source_id,
        series_by_code: dict[str, object],
        observations: list[ObservationIn],
    ) -> list[tuple[object, ...]]:
        if not observations:
            return []

        series_ids = {series_by_code[observation.series_code].id for observation in observations}
        min_reference_start = min(observation.reference_start for observation in observations)
        max_reference_start = max(observation.reference_start for observation in observations)
        stmt = select(
            Observation.series_id,
            Observation.reference_date,
            Observation.reference_start,
            Observation.reference_end,
            Observation.published_at,
            Observation.value,
        ).where(
            Observation.source_id == source_id,
            Observation.series_id.in_(series_ids),
            Observation.reference_start >= min_reference_start,
            Observation.reference_start <= max_reference_start,
        )
        return list((await self.session.execute(stmt)).all())

    async def _previous_table_value_by_series(
        self,
        source_id,
        series_by_code: dict[str, object],
        observations: list[ObservationIn],
    ) -> dict[object, Decimal]:
        result: dict[object, Decimal] = {}
        observations_by_series: dict[str, list[ObservationIn]] = {}
        for observation in observations:
            if observation.skip_equal_to_previous:
                observations_by_series.setdefault(observation.series_code, []).append(observation)

        for series_code, series_observations in observations_by_series.items():
            storage_series = series_by_code[series_code]
            min_reference_start = min(observation.reference_start for observation in series_observations)
            stmt = (
                select(Observation.value)
                .where(
                    Observation.source_id == source_id,
                    Observation.series_id == storage_series.id,
                    Observation.reference_start < min_reference_start,
                )
                .order_by(Observation.reference_start.desc(), Observation.published_at.desc())
                .limit(1)
            )
            previous_value = await self.session.scalar(stmt)
            if previous_value is not None:
                result[storage_series.id] = Decimal(previous_value)
        return result

    async def _compress_equal_table_observation_run(
        self,
        source: SourceDefinition,
        source_id,
        series_id,
        observation: ObservationIn,
        value: Decimal,
        loaded_at: datetime,
    ) -> bool:
        if not observation.compress_equal_runs:
            return False

        current_reference_date = self._source_reference_date(source, loaded_at)
        if observation.reference_date != current_reference_date:
            return False

        stmt = (
            select(Observation)
            .where(
                Observation.source_id == source_id,
                Observation.series_id == series_id,
                Observation.reference_date == observation.reference_date,
                Observation.reference_start < observation.reference_start,
            )
            .order_by(Observation.reference_start.desc())
            .limit(2)
        )
        previous_rows = list((await self.session.scalars(stmt)).all())
        if len(previous_rows) < 2:
            return False

        latest_previous, first_previous = previous_rows
        if Decimal(latest_previous.value) != value or Decimal(first_previous.value) != value:
            return False

        await self.session.delete(latest_previous)
        return True

    @staticmethod
    def _source_reference_date(source: SourceDefinition, value: datetime):
        extra = source.scrape.extra if source.scrape is not None else {}
        timezone_name = str(extra.get("exchange_timezone", "UTC"))
        return value.astimezone(ZoneInfo(timezone_name)).date()

    @staticmethod
    def _revision_published_at(loaded_at: datetime, existing_published_at: set[datetime]) -> datetime:
        if loaded_at.tzinfo is None:
            loaded_at = loaded_at.replace(tzinfo=timezone.utc)

        published_at = loaded_at
        while published_at in existing_published_at:
            published_at += timedelta(microseconds=1)
        return published_at

    @staticmethod
    def _stores_table_observations(source: SourceDefinition) -> bool:
        if source.adapter_name in {"fred_observations", "fred_sofr", "tradingview"}:
            return True
        extra = source.scrape.extra if source.scrape is not None else {}
        return bool(extra.get("store_in_observations", False))

    @staticmethod
    def _to_storage_source_type(source_type: SourceKind) -> SourceType:
        return SourceType(source_type.value)
