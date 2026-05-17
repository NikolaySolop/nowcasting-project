"""create observations table

Revision ID: 202605100001
Revises: 202604290001
Create Date: 2026-05-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "202605100001"
down_revision: Union[str, None] = "202604290001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "observations",
        sa.Column("series_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reference_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reference_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["series_id"],
            ["series.id"],
            name=op.f("fk_observations_series_id_series"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["data_source.id"],
            name=op.f("fk_observations_source_id_data_source"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "series_id",
            "source_id",
            "reference_start",
            "published_at",
            name="pk_observations",
        ),
    )
    op.create_index(op.f("ix_observations_published_at"), "observations", ["published_at"], unique=False)
    op.create_index(op.f("ix_observations_reference_start"), "observations", ["reference_start"], unique=False)
    op.create_index(op.f("ix_observations_series_id"), "observations", ["series_id"], unique=False)
    op.create_index(op.f("ix_observations_source_id"), "observations", ["source_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_observations_source_id"), table_name="observations")
    op.drop_index(op.f("ix_observations_series_id"), table_name="observations")
    op.drop_index(op.f("ix_observations_reference_start"), table_name="observations")
    op.drop_index(op.f("ix_observations_published_at"), table_name="observations")
    op.drop_table("observations")
