from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.raw_obsevations import RawObservation
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
