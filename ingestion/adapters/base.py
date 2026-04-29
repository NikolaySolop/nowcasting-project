from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ingestion.core.config import Settings
from ingestion.schemas.observations import RawObservationIn
from ingestion.schemas.sources import SourceDefinition


class AdapterError(RuntimeError):
    """Raised when a source adapter cannot fetch or parse a source."""


@dataclass(frozen=True)
class FetchContext:
    source: SourceDefinition
    settings: Settings


@dataclass
class FetchResult:
    observations: list[RawObservationIn] = field(default_factory=list)
    loaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_payload: dict[str, Any] | None = None


class BaseAdapter(ABC):
    name = "base"

    @abstractmethod
    async def fetch(self, context: FetchContext) -> FetchResult:
        raise NotImplementedError
