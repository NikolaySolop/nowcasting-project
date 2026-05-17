from sqlalchemy import Enum as SQLEnum, String
from sqlalchemy.orm import Mapped, mapped_column

from storage.db.base import Base, UUIDMixin, TimestampMixin

from storage.models.enums import SourceType


class DataSource(UUIDMixin, TimestampMixin, Base):
    __tablename__ = "data_source"

    source_code: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        index=True,
        nullable=False
    )
    source_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
    source_type: Mapped[SourceType | None] = mapped_column(
        SQLEnum(
            SourceType,
            name="source_type_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )
