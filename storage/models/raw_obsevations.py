from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Numeric,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from storage.db.base import Base, UUIDMixin, TimestampMixin


class RawObservation(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "raw_observation"

    __table_args__ = (
        UniqueConstraint(
            "series_id",
            "source_id",
            "observed_at",
            "vintage_at",
            name="uq_raw_observation_series_source_observed_vintage",
        ),
    )

    series_id: Mapped[UUID] = mapped_column(
        ForeignKey("series.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    source_id: Mapped[UUID] = mapped_column(
        ForeignKey("data_source.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )

    period_start: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    period_end: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    value_numeric: Mapped[Optional[Decimal]] = mapped_column(
        Numeric,
        nullable=True,
    )

    value_text: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )

    publication_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )

    vintage_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True,
    )

    is_revised: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
    )

    is_final: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
    )

    raw_payload: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
    )

    series: Mapped["Series"] = relationship(
        "Series",
        back_populates="observations",
    )

    source: Mapped["DataSource"] = relationship(
        "DataSource",
        back_populates="observations",
    )
