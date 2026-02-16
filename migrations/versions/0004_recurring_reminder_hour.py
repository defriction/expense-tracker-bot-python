"""add reminder hour to recurring expenses

Revision ID: 0004_recurring_reminder_hour
Revises: 0003_recurring_billing_instances
Create Date: 2026-02-16 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_recurring_reminder_hour"
down_revision = "0003_recurring_billing_instances"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "recurring_expenses",
        sa.Column("reminder_hour", sa.Integer(), nullable=False, server_default=sa.text("9")),
    )


def downgrade() -> None:
    op.drop_column("recurring_expenses", "reminder_hour")
