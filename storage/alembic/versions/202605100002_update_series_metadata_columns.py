"""update series metadata columns

Revision ID: 202605100002
Revises: 202605100001
Create Date: 2026-05-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "202605100002"
down_revision: Union[str, None] = "202605100001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


series_frequency_enum = sa.Enum(
    "15min",
    "daily",
    "weekly",
    "monthly",
    "annual",
    name="series_frequency_enum",
    native_enum=False,
    create_constraint=True,
)


def upgrade() -> None:
    op.add_column("series", sa.Column("frequency", series_frequency_enum, nullable=True))
    op.add_column("series", sa.Column("group_code", sa.Text(), nullable=True))
    op.add_column("series", sa.Column("description", sa.Text(), nullable=True))
    op.alter_column("series", "unit", new_column_name="units", existing_type=sa.Text(), existing_nullable=True)

    op.drop_column("series", "block_code")
    op.drop_column("series", "target_frequency")
    op.drop_column("series", "asset_class")
    op.drop_column("series", "currency")
    op.drop_column("series", "country")
    op.drop_column("series", "is_market_data")
    op.drop_column("series", "is_revision_prone")


def downgrade() -> None:
    op.add_column(
        "series",
        sa.Column("is_revision_prone", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "series",
        sa.Column("is_market_data", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column("series", sa.Column("country", sa.Text(), nullable=True))
    op.add_column("series", sa.Column("currency", sa.Text(), nullable=True))
    op.add_column(
        "series",
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
    )
    op.add_column(
        "series",
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
    )
    op.add_column(
        "series",
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
    )

    op.alter_column("series", "units", new_column_name="unit", existing_type=sa.Text(), existing_nullable=True)
    op.drop_column("series", "description")
    op.drop_column("series", "group_code")
    op.drop_column("series", "frequency")
