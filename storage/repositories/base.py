from __future__ import annotations

from typing import Any, Generic, TypeVar
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from storage.db.base import Base

ModelT = TypeVar("ModelT", bound=Base)


class BaseRepository(Generic[ModelT]):
    def __init__(self, session: AsyncSession, model: type[ModelT]) -> None:
        self.session = session
        self.model = model

    async def get(self, entity_id: UUID) -> ModelT | None:
        return await self.session.get(self.model, entity_id)

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[ModelT]:
        stmt = select(self.model).offset(offset).limit(limit)
        result = await self.session.scalars(stmt)
        return list(result.all())

    async def find_by(self, **filters: Any) -> list[ModelT]:
        stmt = select(self.model).filter_by(**filters)
        result = await self.session.scalars(stmt)
        return list(result.all())

    async def insert(self, **values: Any) -> ModelT:
        entity = self.model(**values)
        self.session.add(entity)
        await self.session.flush()
        return entity

    async def update(self, entity_id: UUID, **values: Any) -> ModelT | None:
        entity = await self.get(entity_id)
        if entity is None:
            return None

        for field, value in values.items():
            setattr(entity, field, value)

        await self.session.flush()
        return entity

    async def delete(self, entity_id: UUID) -> bool:
        entity = await self.get(entity_id)
        if entity is None:
            return False

        await self.session.delete(entity)
        await self.session.flush()
        return True
