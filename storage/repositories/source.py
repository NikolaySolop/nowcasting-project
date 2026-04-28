from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.models.source import DataSource
from storage.repositories.base import BaseRepository


class DataSourceRepository(BaseRepository[DataSource]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session=session, model=DataSource)

    async def get_by_source_code(self, source_code: str) -> DataSource | None:
        stmt = select(DataSource).where(DataSource.source_code == source_code)
        result = await self.session.scalar(stmt)
        return result
