from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.series import Series
from storage.repositories.base import BaseRepository


class SeriesRepository(BaseRepository[Series]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session=session, model=Series)

    async def get_by_series_code(self, series_code: str) -> Series | None:
        stmt = select(Series).where(Series.series_code == series_code)
        result = await self.session.scalar(stmt)
        return result
