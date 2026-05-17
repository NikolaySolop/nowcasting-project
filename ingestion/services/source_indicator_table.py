from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from typing import TYPE_CHECKING, Any

from ingestion.schemas.source_table import SourceIndicatorTableRow
from ingestion.schemas.sources import SeriesDefinition, SourceDefinition

if TYPE_CHECKING:
    from ingestion.services.source_registry import SourceRegistry


BACKFILL_FREQUENCY_KEYS = (
    "backfill_frequency",
    "backfill_data_frequency",
    "history_frequency",
    "history_interval",
)
LIVE_FREQUENCY_KEYS = (
    "live_frequency",
    "live_data_frequency",
    "live_interval",
)


class SourceIndicatorTableService:
    def __init__(self, registry: SourceRegistry) -> None:
        self.registry = registry

    def build(self, *, enabled_only: bool = False) -> list[SourceIndicatorTableRow]:
        return build_source_indicator_table(self.registry.list_sources(enabled_only=enabled_only))


def build_source_indicator_table(sources: Iterable[SourceDefinition]) -> list[SourceIndicatorTableRow]:
    rows: list[SourceIndicatorTableRow] = []
    for source in sources:
        for series in _iter_source_series(source):
            base_frequency = series.frequency if series is not None else None
            rows.append(
                SourceIndicatorTableRow(
                    number=len(rows) + 1,
                    indicator=_indicator_name(source, series),
                    data_from=_data_from(source),
                    backfill_frequency=_backfill_frequency(source, base_frequency),
                    live_frequency=_live_frequency(source, base_frequency),
                )
            )
    return rows


def _iter_source_series(source: SourceDefinition) -> Iterable[SeriesDefinition | None]:
    if source.series:
        return source.series
    return (None,)


def _indicator_name(source: SourceDefinition, series: SeriesDefinition | None) -> str:
    if series is not None:
        return series.series_name or series.series_code
    if source.scrape is not None and source.scrape.series_code:
        return source.scrape.series_code
    if source.csv is not None and source.csv.series_code:
        return source.csv.series_code
    return source.source_name or source.source_code


def _data_from(source: SourceDefinition) -> date | None:
    if source.scrape is None or source.scrape.start_date is None:
        return None
    return source.scrape.start_date.date()


def _backfill_frequency(source: SourceDefinition, fallback: str | None) -> str | None:
    explicit_frequency = (
        _first_frequency(source.metadata, BACKFILL_FREQUENCY_KEYS)
        or _first_frequency(_scrape_extra(source), BACKFILL_FREQUENCY_KEYS)
    )
    if explicit_frequency is not None:
        return explicit_frequency
    if _data_from(source) is None:
        return None
    return _frequency_from_interval_minutes(_scrape_extra(source).get("interval_minutes")) or fallback


def _live_frequency(source: SourceDefinition, fallback: str | None) -> str | None:
    return (
        _first_frequency(source.metadata, LIVE_FREQUENCY_KEYS)
        or _first_frequency(_scrape_extra(source), LIVE_FREQUENCY_KEYS)
        or _frequency_from_interval_minutes(_scrape_extra(source).get("interval_minutes"))
        or fallback
    )


def _scrape_extra(source: SourceDefinition) -> Mapping[str, Any]:
    if source.scrape is None:
        return {}
    return source.scrape.extra


def _first_frequency(mapping: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = mapping.get(key)
        frequency = _normalize_frequency(value)
        if frequency is not None:
            return frequency
    return None


def _frequency_from_interval_minutes(value: Any) -> str | None:
    if value is None:
        return None
    try:
        minutes = int(value)
    except (TypeError, ValueError):
        return None
    if minutes <= 0:
        return None
    return f"{minutes}min"


def _normalize_frequency(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _frequency_from_interval_minutes(value)
    if not isinstance(value, str):
        return str(value)

    raw = value.strip()
    if not raw:
        return None

    normalized = raw.lower().replace("_", "").replace("-", "")
    aliases = {
        "15m": "15min",
        "15min": "15min",
        "15minute": "15min",
        "15minutes": "15min",
        "1d": "daily",
        "d": "daily",
        "day": "daily",
        "daily": "daily",
        "w": "weekly",
        "week": "weekly",
        "weekly": "weekly",
        "m": "monthly",
        "month": "monthly",
        "monthly": "monthly",
        "a": "annual",
        "y": "annual",
        "year": "annual",
        "yearly": "annual",
        "annual": "annual",
    }
    return aliases.get(normalized, raw)
