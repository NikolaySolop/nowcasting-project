"""create initial storage tables

Revision ID: 202604290001
Revises:
Create Date: 2026-04-29 00:01:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202604290001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "data_source",
        sa.Column("source_code", sa.String(length=50), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column(
            "source_type",
            sa.Enum(
                "api",
                "csv",
                "manual",
                "web",
                "vendor",
                "exchange",
                name="source_type_enum",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_data_source")),
        sa.UniqueConstraint("source_code", name=op.f("uq_data_source_source_code")),
    )
    op.create_index(
        op.f("ix_data_source_source_code"),
        "data_source",
        ["source_code"],
        unique=False,
    )

    op.create_table(
        "series",
        sa.Column("series_code", sa.String(length=50), nullable=False),
        sa.Column("series_name", sa.String(length=255), nullable=False),
        sa.Column(
            "block_code",
            sa.Enum(
                "A",
                "B",
                "C",
                "D",
                "E",
                "F",
                "G",
                "H",
                "I",
                name="block_code_enum",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column(
            "target_frequency",
            sa.Enum(
                "15min",
                "30min",
                "daily",
                "weekly",
                "monthly",
                "quarterly",
                "event",
                "model_step",
                name="target_frequency_enum",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column(
            "asset_class",
            sa.Enum(
                "fx",
                "rates",
                "oil",
                "macro",
                "event",
                "tax",
                "sanctions",
                name="asset_class_enum",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column("unit", sa.Text(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("country", sa.Text(), nullable=True),
        sa.Column(
            "default_transform",
            sa.Enum(
                "level",
                "log_return",
                "diff",
                "spread",
                "yoy",
                "mom",
                name="transform_type_enum",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=True,
        ),
        sa.Column(
            "is_market_data",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "is_revision_prone",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "is_model_input",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_series")),
        sa.UniqueConstraint("series_code", name=op.f("uq_series_series_code")),
    )
    op.create_index(
        op.f("ix_series_series_code"),
        "series",
        ["series_code"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_series_series_code"), table_name="series")
    op.drop_table("series")

    op.drop_index(op.f("ix_data_source_source_code"), table_name="data_source")
    op.drop_table("data_source")
