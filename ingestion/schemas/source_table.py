from datetime import date

from pydantic import BaseModel


class SourceIndicatorTableRow(BaseModel):
    number: int
    indicator: str
    data_from: date | None = None
    backfill_frequency: str | None = None
    live_frequency: str | None = None
