"""add reference_date to observations

Revision ID: 202605100004
Revises: 202605100003
Create Date: 2026-05-10 00:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "202605100004"
down_revision: Union[str, None] = "202605100003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("observations", sa.Column("reference_date", sa.Date(), nullable=True))
    op.execute("UPDATE observations SET reference_date = (reference_start AT TIME ZONE 'UTC')::date")
    op.alter_column("observations", "reference_date", existing_type=sa.Date(), nullable=False)
    op.create_index(op.f("ix_observations_reference_date"), "observations", ["reference_date"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_observations_reference_date"), table_name="observations")
    op.drop_column("observations", "reference_date")
