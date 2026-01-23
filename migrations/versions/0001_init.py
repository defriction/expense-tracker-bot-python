"""init schema

Revision ID: 0001_init
Revises: 
Create Date: 2026-01-23
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "user_identities",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.String(length=64), sa.ForeignKey("users.id", ondelete="cascade"), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("external_user_id", sa.String(length=128), nullable=False),
        sa.Column("external_chat_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ux_user_identity_channel_user", "user_identities", ["channel", "external_user_id"], unique=True)

    op.create_table(
        "invites",
        sa.Column("invite_token", sa.String(length=128), primary_key=True),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="unused"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("used_by_user_id", sa.String(length=64), sa.ForeignKey("users.id"), nullable=True),
    )

    op.create_table(
        "transactions",
        sa.Column("tx_id", sa.String(length=64), primary_key=True),
        sa.Column("user_id", sa.String(length=64), sa.ForeignKey("users.id", ondelete="cascade"), nullable=False),
        sa.Column("type", sa.String(length=16), nullable=False),
        sa.Column("transaction_kind", sa.String(length=16), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(length=8), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("normalized_merchant", sa.String(length=128), nullable=True),
        sa.Column("payment_method", sa.String(length=32), nullable=True),
        sa.Column("counterparty", sa.String(length=128), nullable=True),
        sa.Column("loan_role", sa.String(length=32), nullable=True),
        sa.Column("loan_id", sa.String(length=64), nullable=True),
        sa.Column("is_recurring", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("recurrence", sa.String(length=16), nullable=True),
        sa.Column("recurrence_id", sa.String(length=64), nullable=True),
        sa.Column("parse_confidence", sa.Numeric(4, 2), nullable=True),
        sa.Column("parser_version", sa.String(length=32), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=True),
        sa.Column("source_message_id", sa.String(length=64), nullable=True),
        sa.Column("raw_text", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("chat_id", sa.String(length=64), nullable=True),
    )
    op.create_index("ix_transactions_user_created", "transactions", ["user_id", "created_at"], unique=False)
    op.create_index("ix_transactions_user_date", "transactions", ["user_id", "date"], unique=False)

    op.create_table(
        "error_logs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("workflow", sa.String(length=128), nullable=False),
        sa.Column("node", sa.String(length=64), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("user_id", sa.String(length=64), nullable=True),
        sa.Column("chat_id", sa.String(length=64), nullable=True),
    )

    op.create_table(
        "audit_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_id", sa.String(length=128), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_user_id", sa.String(length=64), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=True),
    )
    op.create_index("ix_audit_entity", "audit_events", ["entity_type", "entity_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_audit_entity", table_name="audit_events")
    op.drop_table("audit_events")
    op.drop_table("error_logs")
    op.drop_index("ix_transactions_user_date", table_name="transactions")
    op.drop_index("ix_transactions_user_created", table_name="transactions")
    op.drop_table("transactions")
    op.drop_table("invites")
    op.drop_index("ux_user_identity_channel_user", table_name="user_identities")
    op.drop_table("user_identities")
    op.drop_table("users")
