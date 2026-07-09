"""client_order_id idempotency constraint

Revision ID: a1b2c3d4e5f6
Revises: e744a2bd5050
Create Date: 2026-07-08 19:45:00.000000

order_service._risk_check enforces client_order_id idempotency with a
SELECT-then-INSERT check, which is not safe against two concurrent
requests carrying the same (user_id, client_order_id) -- both can pass
the SELECT before either commits, and both submit to the broker. This is
exactly the double-submission scenario a retried idempotency key is meant
to prevent, so the guarantee needs to be enforced at the database level:
a concurrent second INSERT hits this unique index and fails, and
order_service catches that IntegrityError and returns 409.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'e744a2bd5050'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_orders_user_client_order_id_unique",
        "orders",
        ["user_id", "client_order_id"],
        unique=True,
        postgresql_where=sa.text("client_order_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_orders_user_client_order_id_unique", table_name="orders")
