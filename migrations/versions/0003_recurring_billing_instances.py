"""refactor recurring reminders to bill instances

Revision ID: 0003_recurring_billing_instances
Revises: 0002_recurring
Create Date: 2026-02-15 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003_recurring_billing_instances"
down_revision = "0002_recurring"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("recurring_expenses", sa.Column("service_name", sa.String(length=128), nullable=True))
    op.add_column(
        "recurring_expenses",
        sa.Column("auto_add_transaction", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )
    op.add_column("recurring_expenses", sa.Column("canceled_at", sa.DateTime(timezone=True), nullable=True))

    op.execute(
        """
        update recurring_expenses
        set service_name = coalesce(nullif(normalized_merchant, ''), nullif(description, ''), 'Pago recurrente')
        where service_name is null
        """
    )
    op.alter_column("recurring_expenses", "service_name", nullable=False)

    op.create_table(
        "bill_instances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("recurring_id", sa.Integer(), nullable=False),
        sa.Column("period_year", sa.Integer(), nullable=False),
        sa.Column("period_month", sa.Integer(), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("payment_link", sa.Text(), nullable=True),
        sa.Column("reference_number", sa.Text(), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tx_id", sa.String(length=64), nullable=True),
        sa.Column("follow_up_on", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["recurring_id"], ["recurring_expenses.id"], ondelete="cascade"),
        sa.UniqueConstraint("recurring_id", "period_year", "period_month", name="uq_bill_instance_period"),
    )
    op.create_index("ix_bill_instances_due_date", "bill_instances", ["due_date"])
    op.create_index("ix_bill_instances_status", "bill_instances", ["status"])

    op.create_table(
        "bill_instance_reminders",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("bill_instance_id", sa.Integer(), nullable=False),
        sa.Column("reminder_offset", sa.Integer(), nullable=False),
        sa.Column("scheduled_for", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["bill_instance_id"], ["bill_instances.id"], ondelete="cascade"),
        sa.UniqueConstraint(
            "bill_instance_id",
            "reminder_offset",
            "scheduled_for",
            name="uq_bill_reminder_once",
        ),
    )
    op.create_index("ix_bill_instance_reminders_scheduled_for", "bill_instance_reminders", ["scheduled_for"])


def downgrade() -> None:
    op.drop_index("ix_bill_instance_reminders_scheduled_for", table_name="bill_instance_reminders")
    op.drop_table("bill_instance_reminders")

    op.drop_index("ix_bill_instances_status", table_name="bill_instances")
    op.drop_index("ix_bill_instances_due_date", table_name="bill_instances")
    op.drop_table("bill_instances")

    op.drop_column("recurring_expenses", "canceled_at")
    op.drop_column("recurring_expenses", "auto_add_transaction")
    op.drop_column("recurring_expenses", "service_name")
