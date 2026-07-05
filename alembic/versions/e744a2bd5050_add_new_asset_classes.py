"""add new asset classes

Revision ID: e744a2bd5050
Revises: 68abf30eb887
Create Date: 2026-05-26 15:53:36.315369

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e744a2bd5050'
down_revision: Union[str, None] = '68abf30eb887'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE asset_class ADD VALUE IF NOT EXISTS 'commodity'")
    op.execute("ALTER TYPE asset_class ADD VALUE IF NOT EXISTS 'bond'")
    op.execute("ALTER TYPE asset_class ADD VALUE IF NOT EXISTS 'metal'")
    op.execute("ALTER TYPE asset_class ADD VALUE IF NOT EXISTS 'index'")


def downgrade() -> None:
    op.execute("ALTER TYPE asset_class DROP VALUE IF EXISTS 'commodity'")
    op.execute("ALTER TYPE asset_class DROP VALUE IF EXISTS 'bond'")
    op.execute("ALTER TYPE asset_class DROP VALUE IF EXISTS 'metal'")
    op.execute("ALTER TYPE asset_class DROP VALUE IF EXISTS 'index'")
