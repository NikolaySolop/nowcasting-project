from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from ingestion.core.config import Settings
from ingestion.schemas.observations import ObservationIn, RawObservationIn
from ingestion.schemas.sources import SourceDefinition


class AdapterError(RuntimeError):
    """Raised when a source adapter cannot fetch or parse a source."""


@dataclass(frozen=True)
class FetchContext:
    source: SourceDefinition
    settings: Settings
    latest_observed_at_by_series: dict[str, datetime] = field(default_factory=dict)
    observation_sink: Callable[[list[RawObservationIn], datetime], Awaitable[tuple[int, int]]] | None = None


@dataclass
class FetchResult:
    observations: list[RawObservationIn] = field(default_factory=list)
    table_observations: list[ObservationIn] = field(default_factory=list)
    loaded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    raw_payload: dict[str, Any] | None = None
    persisted_loaded_count: int = 0
    persisted_duplicate_count: int = 0


class BaseAdapter(ABC):
    name = "base"

    @abstractmethod
    async def fetch(self, context: FetchContext) -> FetchResult:
        raise NotImplementedError
