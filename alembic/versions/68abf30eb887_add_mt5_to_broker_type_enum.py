"""add mt5 to broker_type enum

Revision ID: 68abf30eb887
Revises: 0ea08fb29273
Create Date: 2026-05-22 13:48:08.398140

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '68abf30eb887'
down_revision: Union[str, None] = '0ea08fb29273'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute(
        "ALTER TYPE broker_type ADD VALUE IF NOT EXISTS 'MT5';"
    )


def downgrade() -> None:
    pass
