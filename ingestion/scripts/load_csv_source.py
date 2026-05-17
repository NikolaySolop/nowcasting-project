#!/usr/bin/env python3
"""Load one or more configured CSV sources into the database."""

import argparse
import asyncio
from pathlib import Path

from ingestion.core.config import settings
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry
from storage.db.session import async_session_factory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load configured CSV/manual sources into storage.")
    parser.add_argument("source_codes", nargs="+", help="Source code(s) to load, e.g. RU_TAX_DUMMY_CALENDAR")
    parser.add_argument(
        "--config",
        type=Path,
        default=settings.source_config_path,
        help="Source config JSON path. Defaults to INGESTION_SOURCE_CONFIG_PATH.",
    )
    return parser.parse_args()


async def run(source_codes: list[str], config_path: Path) -> None:
    registry = SourceRegistry()
    registry.load_json(config_path)

    async with async_session_factory() as session:
        service = IngestionService(session=session, registry=registry, settings=settings)
        result = await service.run(source_codes)

    print(result.model_dump_json(indent=2))
    if result.error_count:
        raise SystemExit(1)


def main() -> None:
    args = parse_args()
    if args.config is None:
        raise SystemExit("--config or INGESTION_SOURCE_CONFIG_PATH is required")
    asyncio.run(run(args.source_codes, args.config))


if __name__ == "__main__":
    main()
