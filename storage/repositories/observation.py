from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.observations import Observation
from storage.models.series import Series
from storage.repositories.base import BaseRepository


class ObservationRepository(BaseRepository[Observation]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session=session, model=Observation)

    async def latest_reference_start_by_series(self, source_id: UUID) -> dict[str, datetime]:
        stmt = (
            select(Observation.series_id, func.max(Observation.reference_start))
            .where(Observation.source_id == source_id)
            .group_by(Observation.series_id)
        )
        rows = (await self.session.execute(stmt)).all()
        if not rows:
            return {}

        series_ids = [series_id for series_id, _ in rows]
        series_stmt = select(Series.id, Series.series_code).where(Series.id.in_(series_ids))
        series_rows = (await self.session.execute(series_stmt)).all()
        id_to_code = {series_id: series_code for series_id, series_code in series_rows}
        return {id_to_code[series_id]: reference_start for series_id, reference_start in rows if series_id in id_to_code}

    async def revision_check_start_by_series(self, source_id: UUID, limit: int) -> dict[str, datetime]:
        if limit <= 0:
            return {}

        distinct_references = (
            select(
                Observation.series_id.label("series_id"),
                Observation.reference_start.label("reference_start"),
            )
            .where(Observation.source_id == source_id)
            .group_by(Observation.series_id, Observation.reference_start)
            .subquery()
        )
        ranked = (
            select(
                distinct_references.c.series_id,
                distinct_references.c.reference_start,
                func.row_number()
                .over(
                    partition_by=distinct_references.c.series_id,
                    order_by=distinct_references.c.reference_start.desc(),
                )
                .label("row_number"),
            )
            .subquery()
        )
        stmt = (
            select(Series.series_code, func.min(ranked.c.reference_start))
            .join(ranked, Series.id == ranked.c.series_id)
            .where(ranked.c.row_number <= limit)
            .group_by(Series.series_code)
        )
        rows = (await self.session.execute(stmt)).all()
        return {series_code: reference_start for series_code, reference_start in rows}
