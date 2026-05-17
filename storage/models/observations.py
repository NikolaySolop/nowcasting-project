from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, PrimaryKeyConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from storage.db.base import Base


class Observation(Base):
    __tablename__ = "observations"

    __table_args__ = (
        PrimaryKeyConstraint(
            "series_id",
            "source_id",
            "reference_start",
            "published_at",
            name="pk_observations",
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

    reference_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        index=True,
    )

    reference_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )

    reference_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    value: Mapped[Decimal] = mapped_column(
        Numeric,
        nullable=False,
    )

    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
