#!/usr/bin/env python3
"""Run the live CBR RU_TRADE_BALANCE ingestion job once."""

import argparse
import asyncio
from pathlib import Path

from ingestion.core.config import settings
from ingestion.services.ingestion_service import IngestionService
from ingestion.services.source_registry import SourceRegistry
from storage.db.session import async_session_factory

SOURCE_CODE = "CBR_RU_TRADE_BALANCE"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Check the Bank of Russia trade workbook and insert new "
            "RU_TRADE_BALANCE observations after the latest value already stored in the database."
        )
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=settings.source_config_path,
        help="Source config JSON path. Defaults to INGESTION_SOURCE_CONFIG_PATH.",
    )
    return parser.parse_args()


async def run(config_path: Path) -> None:
    registry = SourceRegistry()
    registry.load_json(config_path)

    async with async_session_factory() as session:
        service = IngestionService(session=session, registry=registry, settings=settings)
        result = await service.run([SOURCE_CODE])

    print(result.model_dump_json(indent=2))
    if result.error_count:
        raise SystemExit(1)


def main() -> None:
    args = parse_args()
    if args.config is None:
        raise SystemExit("--config or INGESTION_SOURCE_CONFIG_PATH is required")
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
