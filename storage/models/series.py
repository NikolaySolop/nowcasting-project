import enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Text,
    func,
    Enum as SQLEnum,
)
from sqlalchemy.orm import Mapped, mapped_column
from storage.models.base import Base, UUIDMixin, TimestampMixin

from storage.models.enums import BlockCode, Frequency, AssetClass, TransformType


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
    block_code: Mapped[BlockCode | None] = mapped_column(
        SQLEnum(
            BlockCode,
            name="block_code_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        )
    )
    target_frequency: Mapped[Frequency | None] = mapped_column(
        SQLEnum(
            Frequency,
            name="target_frequency_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        )
    )
    asset_class: Mapped[AssetClass | None] = mapped_column(
        SQLEnum(
            AssetClass,
            name="asset_class_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )
    unit: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    currency: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    country: Mapped[str | None] = mapped_column(
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

    is_market_data: Mapped[bool] = mapped_column(
        Boolean,
        server_default="false",
        nullable=False,
    )

    is_revision_prone: Mapped[bool] = mapped_column(
        Boolean,
        server_default="false",
        nullable=False,
    )

    is_model_input: Mapped[bool] = mapped_column(
        Boolean,
        server_default="true",
        nullable=False,
    )
