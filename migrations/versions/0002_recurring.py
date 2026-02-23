"""add recurring expenses and pending actions

Revision ID: 0002_recurring
Revises: 0001_init
Create Date: 2026-02-09 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "0002_recurring"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "recurring_expenses",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("recurrence_id", sa.String(length=64), nullable=False),
        sa.Column("normalized_merchant", sa.String(length=128), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(length=8), nullable=False),
        sa.Column("recurrence", sa.String(length=16), nullable=False),
        sa.Column("billing_day", sa.Integer(), nullable=True),
        sa.Column("billing_weekday", sa.Integer(), nullable=True),
        sa.Column("billing_month", sa.Integer(), nullable=True),
        sa.Column("anchor_date", sa.Date(), nullable=True),
        sa.Column("timezone", sa.String(length=64), nullable=False, server_default=sa.text("'America/Bogota'")),
        sa.Column("payment_link", sa.Text(), nullable=True),
        sa.Column("payment_reference", sa.Text(), nullable=True),
        sa.Column("remind_offsets", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("next_due", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("source_tx_id", sa.String(length=64), nullable=True),
        sa.Column("last_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="cascade"),
        sa.UniqueConstraint("user_id", "recurrence_id", name="uq_recurring_user_recurrence_id"),
    )
    op.create_index("ix_recurring_expenses_user_id", "recurring_expenses", ["user_id"])

    op.create_table(
        "recurring_events",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("recurring_id", sa.Integer(), nullable=False),
        sa.Column("reminder_date", sa.Date(), nullable=False),
        sa.Column("reminder_offset", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tx_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["recurring_id"], ["recurring_expenses.id"], ondelete="cascade"),
        sa.UniqueConstraint("recurring_id", "reminder_date", "reminder_offset", name="uq_recurring_event_once"),
    )
    op.create_index("ix_recurring_events_recurring_id", "recurring_events", ["recurring_id"])
    op.create_index("ix_recurring_events_reminder_date", "recurring_events", ["reminder_date"])

    op.create_table(
        "bot_pending_actions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(length=64), nullable=False),
        sa.Column("action_type", sa.String(length=64), nullable=False),
        sa.Column("state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="cascade"),
        sa.UniqueConstraint("user_id", "action_type", name="uq_pending_action_user_type"),
    )


def downgrade() -> None:
    op.drop_table("bot_pending_actions")
    op.drop_index("ix_recurring_events_reminder_date", table_name="recurring_events")
    op.drop_index("ix_recurring_events_recurring_id", table_name="recurring_events")
    op.drop_table("recurring_events")
    op.drop_index("ix_recurring_expenses_user_id", table_name="recurring_expenses")
    op.drop_table("recurring_expenses")
