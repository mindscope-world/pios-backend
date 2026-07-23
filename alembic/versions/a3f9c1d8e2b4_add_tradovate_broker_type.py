"""add tradovate broker type

Revision ID: a3f9c1d8e2b4
Revises: cd6df96a43dc
Create Date: 2026-07-23 10:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3f9c1d8e2b4'
down_revision: Union[str, None] = 'cd6df96a43dc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE broker_type ADD VALUE IF NOT EXISTS 'TRADOVATE'")


def downgrade() -> None:
    op.execute("ALTER TYPE broker_type DROP VALUE IF EXISTS 'TRADOVATE'")
