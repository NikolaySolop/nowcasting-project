"""drop raw observation table

Revision ID: 202605170001
Revises: 202605100004
Create Date: 2026-05-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "202605170001"
down_revision: Union[str, None] = "202605100004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS raw_observation")


def downgrade() -> None:
    pass
