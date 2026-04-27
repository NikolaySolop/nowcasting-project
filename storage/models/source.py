from sqlalchemy import DateTime, String, Text, func, Enum as SQLEnum
from sqlalchemy.orm import Mapped, mapped_column, relationship
from storage.models.base import UUIDMixin, TimestampMixin

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
    source_type: Mapped[str] = mapped_column(
        SQLEnum(
            SourceType,
            name="source_type_enum",
            values_callable=lambda enum_cls: [item.value for item in enum_cls],
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )


    observations: Mapped[list["RawObservation"]] = relationship(
        back_populates="source"
    )
