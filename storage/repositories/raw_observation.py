from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.raw_obsevations import RawObservation
from storage.models.series import Series
from storage.repositories.base import BaseRepository


class RawObservationRepository(BaseRepository[RawObservation]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session=session, model=RawObservation)

    async def list_for_series(
        self,
        series_id: UUID,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RawObservation]:
        stmt = (
            select(RawObservation)
            .where(RawObservation.series_id == series_id)
            .offset(offset)
            .limit(limit)
        )
        result = await self.session.scalars(stmt)
        return list(result.all())

    async def latest_observed_at_by_series(self, source_id: UUID) -> dict[str, datetime]:
        stmt = (
            select(RawObservation.series_id, func.max(RawObservation.observed_at))
            .where(RawObservation.source_id == source_id)
            .group_by(RawObservation.series_id)
        )
        rows = (await self.session.execute(stmt)).all()
        if not rows:
            return {}

        series_ids = [series_id for series_id, _ in rows]
        series_stmt = select(Series.id, Series.series_code).where(Series.id.in_(series_ids))
        series_rows = (await self.session.execute(series_stmt)).all()
        id_to_code = {series_id: series_code for series_id, series_code in series_rows}
        return {id_to_code[series_id]: observed_at for series_id, observed_at in rows if series_id in id_to_code}
