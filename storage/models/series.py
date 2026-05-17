from sqlalchemy import (
    Boolean,
    Enum as SQLEnum,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from storage.db.base import Base, UUIDMixin, TimestampMixin

from storage.models.enums import Frequency, TransformType


class Series(Base, UUIDMixin, TimestampMixin):
    __tablename__ = "series"

    series_code: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        index=True,
        nullable=False
    )

    series_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
    frequency: Mapped[Frequency | None] = mapped_column(
        SQLEnum(
            Frequency,
            name="series_frequency_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )

    group_code: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    subgroup_code: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    description: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    units: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    default_transform: Mapped[TransformType | None] = mapped_column(
        SQLEnum(
            TransformType,
            name="transform_type_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )

    is_model_input: Mapped[bool] = mapped_column(
        Boolean,
        server_default="true",
        nullable=False,
    )

    observations: Mapped[list["RawObservation"]] = relationship(
        back_populates="series",
    )
