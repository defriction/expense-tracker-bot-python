from __future__ import annotations

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, JSON, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=True)


class UserIdentity(Base):
    __tablename__ = "user_identities"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="cascade"), nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    external_user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    external_chat_id: Mapped[str] = mapped_column(String(128), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class Invite(Base):
    __tablename__ = "invites"

    invite_token: Mapped[str] = mapped_column(String(128), primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="unused")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=True)
    used_by_user_id: Mapped[str | None] = mapped_column(String(64), ForeignKey("users.id"), nullable=True)


class Transaction(Base):
    __tablename__ = "transactions"

    tx_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="cascade"), nullable=False)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    transaction_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    amount: Mapped[Numeric] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    date: Mapped[Date] = mapped_column(Date, nullable=True)
    normalized_merchant: Mapped[str] = mapped_column(String(128), nullable=True)
    payment_method: Mapped[str] = mapped_column(String(32), nullable=True)
    counterparty: Mapped[str] = mapped_column(String(128), nullable=True)
    loan_role: Mapped[str] = mapped_column(String(32), nullable=True)
    loan_id: Mapped[str] = mapped_column(String(64), nullable=True)
    is_recurring: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)
    recurrence: Mapped[str] = mapped_column(String(16), nullable=True)
    recurrence_id: Mapped[str] = mapped_column(String(64), nullable=True)
    parse_confidence: Mapped[Numeric] = mapped_column(Numeric(4, 2), nullable=True)
    parser_version: Mapped[str] = mapped_column(String(32), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=True)
    source_message_id: Mapped[str] = mapped_column(String(64), nullable=True)
    raw_text: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_deleted: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=False)
    deleted_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=True)
    chat_id: Mapped[str] = mapped_column(String(64), nullable=True)


class ErrorLog(Base):
    __tablename__ = "error_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    occurred_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    workflow: Mapped[str] = mapped_column(String(128), nullable=False)
    node: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[str] = mapped_column(String(64), nullable=True)
    chat_id: Mapped[str] = mapped_column(String(64), nullable=True)


class AuditEvent(Base):
    __tablename__ = "audit_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_id: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[JSON] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    actor_user_id: Mapped[str] = mapped_column(String(64), nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=True)


class RecurringExpense(Base):
    __tablename__ = "recurring_expenses"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="cascade"), nullable=False)
    service_name: Mapped[str] = mapped_column(String(128), nullable=False)
    recurrence_id: Mapped[str] = mapped_column(String(64), nullable=False)
    normalized_merchant: Mapped[str] = mapped_column(String(128), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    amount: Mapped[Numeric] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    recurrence: Mapped[str] = mapped_column(String(16), nullable=False)
    billing_day: Mapped[int | None] = mapped_column(nullable=True)
    billing_weekday: Mapped[int | None] = mapped_column(nullable=True)
    billing_month: Mapped[int | None] = mapped_column(nullable=True)
    anchor_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="America/Bogota")
    payment_link: Mapped[str] = mapped_column(Text, nullable=True)
    payment_reference: Mapped[str] = mapped_column(Text, nullable=True)
    remind_offsets: Mapped[JSON] = mapped_column(JSONB, nullable=False, default=list)
    reminder_hour: Mapped[int] = mapped_column(nullable=False, default=9)
    next_due: Mapped[Date | None] = mapped_column(Date, nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    auto_add_transaction: Mapped[Boolean] = mapped_column(Boolean, nullable=False, default=True)
    canceled_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_tx_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_confirmed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class BillInstance(Base):
    __tablename__ = "bill_instances"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    recurring_id: Mapped[int] = mapped_column(ForeignKey("recurring_expenses.id", ondelete="cascade"), nullable=False)
    period_year: Mapped[int] = mapped_column(nullable=False)
    period_month: Mapped[int] = mapped_column(nullable=False)
    due_date: Mapped[Date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    amount: Mapped[Numeric | None] = mapped_column(Numeric(18, 2), nullable=True)
    payment_link: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    paid_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    tx_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    follow_up_on: Mapped[Date | None] = mapped_column(Date, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class BillInstanceReminder(Base):
    __tablename__ = "bill_instance_reminders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    bill_instance_id: Mapped[int] = mapped_column(ForeignKey("bill_instances.id", ondelete="cascade"), nullable=False)
    reminder_offset: Mapped[int] = mapped_column(nullable=False)
    scheduled_for: Mapped[Date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    sent_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)


class BotPendingAction(Base):
    __tablename__ = "bot_pending_actions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id", ondelete="cascade"), nullable=False)
    action_type: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[JSON] = mapped_column(JSONB, nullable=False)
    expires_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
