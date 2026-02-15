from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import json

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from app.core.logging import logger


@dataclass
class PostgresRepo:
    engine: Engine

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _session(self) -> Session:
        return Session(self.engine)

    def find_user_by_channel(self, channel: str, external_user_id: str) -> Optional[Dict[str, Any]]:
        sql = text(
            """
            select u.id as user_id, u.status, u.last_seen_at, i.external_chat_id
            from user_identities i
            join users u on u.id = i.user_id
            where i.channel = :channel and i.external_user_id = :external_user_id
            """
        )
        with self._session() as session:
            row = session.execute(sql, {"channel": channel, "external_user_id": external_user_id}).mappings().first()
            if not row:
                return None
            return {
                "userId": row["user_id"],
                "status": row["status"],
                "lastSeenAt": row["last_seen_at"],
                "chatId": row["external_chat_id"],
            }

    def update_user_last_seen(self, channel: str, external_user_id: str, timestamp: Optional[str] = None) -> None:
        ts = timestamp or self._now_iso()
        with self._session() as session:
            session.execute(
                text(
                    """
                    update users u
                    set last_seen_at = :ts, updated_at = :ts
                    from user_identities i
                    where u.id = i.user_id and i.channel = :channel and i.external_user_id = :external_user_id
                    """
                ),
                {"ts": ts, "channel": channel, "external_user_id": external_user_id},
            )
            session.commit()

    def create_user(self, user_id: str, channel: str, external_user_id: str, chat_id: Optional[str]) -> None:
        now = self._now_iso()
        with self._session() as session:
            session.execute(
                text(
                    """
                    insert into users (id, status, created_at, updated_at, last_seen_at)
                    values (:user_id, 'active', :now, :now, :now)
                    """
                ),
                {"user_id": user_id, "now": now},
            )
            session.execute(
                text(
                    """
                    insert into user_identities (user_id, channel, external_user_id, external_chat_id, created_at)
                    values (:user_id, :channel, :external_user_id, :external_chat_id, :now)
                    """
                ),
                {
                    "user_id": user_id,
                    "channel": channel,
                    "external_user_id": external_user_id,
                    "external_chat_id": chat_id,
                    "now": now,
                },
            )
            session.execute(
                text(
                    """
                    insert into audit_events (entity_type, entity_id, action, payload, created_at)
                    values ('user', :user_id, 'create', cast(:payload as jsonb), :now)
                    """
                ),
                {"user_id": user_id, "payload": "{}", "now": now},
            )
            session.commit()

    def find_invite(self, invite_token: str) -> Optional[Dict[str, Any]]:
        sql = text("select invite_token, status, used_by_user_id from invites where invite_token = :token")
        with self._session() as session:
            row = session.execute(sql, {"token": invite_token}).mappings().first()
            if not row:
                return None
            return {
                "inviteToken": row["invite_token"],
                "status": row["status"],
                "usedByUserId": row["used_by_user_id"],
            }

    def create_invite(self, invite_token: str, actor_user_id: Optional[str] = None) -> Dict[str, Any]:
        now = self._now_iso()
        with self._session() as session:
            row = session.execute(
                text(
                    """
                    insert into invites (invite_token, status, created_at)
                    values (:token, 'unused', :now)
                    returning invite_token, status, used_by_user_id
                    """
                ),
                {"token": invite_token, "now": now},
            ).mappings().first()
            session.execute(
                text(
                    """
                    insert into audit_events (entity_type, entity_id, action, payload, created_at, actor_user_id, source)
                    values ('invite', :token, 'create', cast(:payload as jsonb), :now, :actor_user_id, 'admin_api')
                    """
                ),
                {"token": invite_token, "payload": "{}", "now": now, "actor_user_id": actor_user_id},
            )
            session.commit()
            return {
                "inviteToken": row["invite_token"],
                "status": row["status"],
                "usedByUserId": row["used_by_user_id"],
            }

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None:
        now = self._now_iso()
        with self._session() as session:
            session.execute(
                text(
                    """
                    update invites
                    set status = 'used', used_at = :now, used_by_user_id = :user_id
                    where invite_token = :token
                    """
                ),
                {"now": now, "user_id": used_by_user_id, "token": invite_token},
            )
            session.execute(
                text(
                    """
                    insert into audit_events (entity_type, entity_id, action, payload, created_at, actor_user_id)
                    values ('invite', :token, 'use', cast(:payload as jsonb), :now, :user_id)
                    """
                ),
                {"token": invite_token, "payload": "{}", "now": now, "user_id": used_by_user_id},
            )
            session.commit()

    def append_transaction(self, tx: Dict[str, Any]) -> None:
        now = self._now_iso()
        params = {
            "tx_id": str(tx.get("txId") or ""),
            "user_id": str(tx.get("userId") or ""),
            "type": tx.get("type"),
            "transaction_kind": tx.get("transactionKind"),
            "amount": tx.get("amount"),
            "currency": tx.get("currency"),
            "category": tx.get("category"),
            "description": tx.get("description"),
            "date": tx.get("date") or None,
            "normalized_merchant": tx.get("normalizedMerchant"),
            "payment_method": tx.get("paymentMethod"),
            "counterparty": tx.get("counterparty"),
            "loan_role": tx.get("loanRole"),
            "loan_id": tx.get("loanId"),
            "is_recurring": tx.get("isRecurring"),
            "recurrence": tx.get("recurrence"),
            "recurrence_id": tx.get("recurrenceId"),
            "parse_confidence": tx.get("parseConfidence"),
            "parser_version": tx.get("parserVersion"),
            "source": tx.get("source"),
            "source_message_id": str(tx.get("sourceMessageId") or ""),
            "raw_text": tx.get("rawText"),
            "created_at": tx.get("createdAt") or now,
            "updated_at": tx.get("updatedAt") or now,
            "is_deleted": tx.get("isDeleted"),
            "deleted_at": tx.get("deletedAt") or None,
            "chat_id": str(tx.get("chatId")) if tx.get("chatId") is not None else None,
        }
        with self._session() as session:
            session.execute(
                text(
                    """
                    insert into transactions (
                        tx_id, user_id, type, transaction_kind, amount, currency, category, description, date,
                        normalized_merchant, payment_method, counterparty, loan_role, loan_id, is_recurring,
                        recurrence, recurrence_id, parse_confidence, parser_version, source, source_message_id,
                        raw_text, created_at, updated_at, is_deleted, deleted_at, chat_id
                    ) values (
                        :tx_id, :user_id, :type, :transaction_kind, :amount, :currency, :category, :description, :date,
                        :normalized_merchant, :payment_method, :counterparty, :loan_role, :loan_id, :is_recurring,
                        :recurrence, :recurrence_id, :parse_confidence, :parser_version, :source, :source_message_id,
                        :raw_text, :created_at, :updated_at, :is_deleted, :deleted_at, :chat_id
                    )
                    """
                ),
                params,
            )
            session.execute(
                text(
                    """
                    insert into audit_events (entity_type, entity_id, action, payload, created_at, actor_user_id, source)
                    values ('transaction', :tx_id, 'create', cast(:payload as jsonb), :now, :user_id, :source)
                    """
                ),
                {
                    "tx_id": params["tx_id"],
                    "payload": "{}",
                    "now": now,
                    "user_id": params["user_id"],
                    "source": params["source"],
                },
            )
            session.commit()

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]:
        sql = text(
            """
            select * from transactions
            where user_id = :user_id
            and (:include_deleted = true or is_deleted = false)
            order by created_at desc
            """
        )
        with self._session() as session:
            rows = session.execute(sql, {"user_id": user_id, "include_deleted": include_deleted}).mappings().all()
            result = []
            for row in rows:
                result.append(
                    {
                        "txId": row["tx_id"],
                        "userId": row["user_id"],
                        "type": row["type"],
                        "transactionKind": row["transaction_kind"],
                        "amount": float(row["amount"]) if row["amount"] is not None else 0,
                        "currency": row["currency"],
                        "category": row["category"],
                        "description": row["description"],
                        "date": row["date"].isoformat() if row["date"] is not None else "",
                        "normalizedMerchant": row["normalized_merchant"],
                        "paymentMethod": row["payment_method"],
                        "counterparty": row["counterparty"],
                        "loanRole": row["loan_role"],
                        "loanId": row["loan_id"],
                        "isRecurring": bool(row["is_recurring"]),
                        "recurrence": row["recurrence"],
                        "recurrenceId": row["recurrence_id"],
                        "parseConfidence": float(row["parse_confidence"]) if row["parse_confidence"] is not None else 0.0,
                        "parserVersion": row["parser_version"],
                        "source": row["source"],
                        "sourceMessageId": row["source_message_id"],
                        "rawText": row["raw_text"],
                        "createdAt": row["created_at"].isoformat() if row["created_at"] else "",
                        "updatedAt": row["updated_at"].isoformat() if row["updated_at"] else "",
                        "isDeleted": bool(row["is_deleted"]),
                        "deletedAt": row["deleted_at"].isoformat() if row["deleted_at"] else "",
                        "chatId": row["chat_id"],
                    }
                )
            return result

    def mark_transaction_deleted(self, tx_id: str) -> None:
        now = self._now_iso()
        with self._session() as session:
            session.execute(
                text(
                    """
                    update transactions
                    set is_deleted = true, updated_at = :now, deleted_at = :now
                    where tx_id = :tx_id
                    """
                ),
                {"now": now, "tx_id": tx_id},
            )
            session.execute(
                text(
                    """
                    insert into audit_events (entity_type, entity_id, action, payload, created_at)
                    values ('transaction', :tx_id, 'delete', cast(:payload as jsonb), :now)
                    """
                ),
                {"tx_id": tx_id, "payload": "{}", "now": now},
            )
            session.commit()

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None:
        now = self._now_iso()
        with self._session() as session:
            session.execute(
                text(
                    """
                    insert into error_logs (occurred_at, workflow, node, message, user_id, chat_id)
                    values (:now, :workflow, :node, :message, :user_id, :chat_id)
                    """
                ),
                {
                    "now": now,
                    "workflow": workflow,
                    "node": node,
                    "message": message,
                    "user_id": user_id,
                    "chat_id": chat_id,
                },
            )
            session.commit()

    def find_recurring_by_recurrence_id(self, user_id: str, recurrence_id: str) -> Optional[Dict[str, Any]]:
        sql = text(
            """
            select * from recurring_expenses
            where user_id = :user_id and recurrence_id = :recurrence_id
            """
        )
        with self._session() as session:
            row = session.execute(sql, {"user_id": user_id, "recurrence_id": recurrence_id}).mappings().first()
            return dict(row) if row else None

    def create_recurring_expense(self, data: Dict[str, Any]) -> Dict[str, Any]:
        now = self._now_iso()
        params = {
            "user_id": data.get("user_id"),
            "service_name": data.get("service_name") or data.get("normalized_merchant") or data.get("description") or "Pago recurrente",
            "recurrence_id": data.get("recurrence_id"),
            "normalized_merchant": data.get("normalized_merchant"),
            "description": data.get("description"),
            "category": data.get("category"),
            "amount": data.get("amount"),
            "currency": data.get("currency"),
            "recurrence": data.get("recurrence"),
            "billing_day": data.get("billing_day"),
            "billing_weekday": data.get("billing_weekday"),
            "billing_month": data.get("billing_month"),
            "anchor_date": data.get("anchor_date"),
            "timezone": data.get("timezone") or "America/Bogota",
            "payment_link": data.get("payment_link"),
            "payment_reference": data.get("payment_reference"),
            "remind_offsets": json.dumps(data.get("remind_offsets") or [3, 1]),
            "next_due": data.get("next_due"),
            "status": data.get("status") or "pending",
            "auto_add_transaction": bool(data.get("auto_add_transaction", True)),
            "canceled_at": data.get("canceled_at"),
            "source_tx_id": data.get("source_tx_id"),
            "created_at": data.get("created_at") or now,
            "updated_at": data.get("updated_at") or now,
        }
        with self._session() as session:
            row = session.execute(
                text(
                    """
                    insert into recurring_expenses (
                        user_id, service_name, recurrence_id, normalized_merchant, description, category, amount, currency,
                        recurrence, billing_day, billing_weekday, billing_month, anchor_date, timezone, payment_link,
                        payment_reference, remind_offsets, next_due, status, auto_add_transaction, canceled_at,
                        source_tx_id, created_at, updated_at
                    ) values (
                        :user_id, :service_name, :recurrence_id, :normalized_merchant, :description, :category, :amount, :currency,
                        :recurrence, :billing_day, :billing_weekday, :billing_month, :anchor_date, :timezone, :payment_link,
                        :payment_reference, cast(:remind_offsets as jsonb), :next_due, :status, :auto_add_transaction, :canceled_at,
                        :source_tx_id, :created_at, :updated_at
                    )
                    returning *
                    """
                ),
                params,
            ).mappings().first()
            session.commit()
            return dict(row)

    def get_recurring_expense(self, recurring_id: int) -> Optional[Dict[str, Any]]:
        with self._session() as session:
            row = session.execute(
                text("select * from recurring_expenses where id = :id"),
                {"id": recurring_id},
            ).mappings().first()
            return dict(row) if row else None

    def update_recurring_expense(self, recurring_id: int, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        updates = dict(updates)
        if "remind_offsets" in updates and not isinstance(updates["remind_offsets"], str):
            updates["remind_offsets"] = json.dumps(updates["remind_offsets"])
        updates["updated_at"] = updates.get("updated_at") or self._now_iso()
        fields = []
        for key in updates.keys():
            if key == "remind_offsets":
                fields.append("remind_offsets = cast(:remind_offsets as jsonb)")
            else:
                fields.append(f"{key} = :{key}")
        sql = text(f"update recurring_expenses set {', '.join(fields)} where id = :id")
        updates["id"] = recurring_id
        with self._session() as session:
            session.execute(sql, updates)
            session.commit()

    def list_active_recurring_expenses(self) -> list[Dict[str, Any]]:
        sql = text("select * from recurring_expenses where status = 'active'")
        with self._session() as session:
            rows = session.execute(sql).mappings().all()
            return [dict(row) for row in rows]

    def list_recurring_expenses(self, user_id: str) -> list[Dict[str, Any]]:
        sql = text(
            """
            select * from recurring_expenses
            where user_id = :user_id
            order by created_at desc
            """
        )
        with self._session() as session:
            rows = session.execute(sql, {"user_id": user_id}).mappings().all()
            return [dict(row) for row in rows]

    def upsert_bill_instance(
        self,
        recurring_id: int,
        period_year: int,
        period_month: int,
        due_date: str,
        amount: Optional[float],
        payment_link: Optional[str],
        reference_number: Optional[str],
    ) -> Dict[str, Any]:
        now = self._now_iso()
        with self._session() as session:
            row = session.execute(
                text(
                    """
                    insert into bill_instances (
                        recurring_id, period_year, period_month, due_date, status, amount,
                        payment_link, reference_number, created_at, updated_at
                    ) values (
                        :recurring_id, :period_year, :period_month, :due_date, 'pending', :amount,
                        :payment_link, :reference_number, :now, :now
                    )
                    on conflict (recurring_id, period_year, period_month)
                    do update set due_date = excluded.due_date,
                                  amount = excluded.amount,
                                  payment_link = excluded.payment_link,
                                  reference_number = excluded.reference_number,
                                  updated_at = excluded.updated_at
                    returning *
                    """
                ),
                {
                    "recurring_id": recurring_id,
                    "period_year": period_year,
                    "period_month": period_month,
                    "due_date": due_date,
                    "amount": amount,
                    "payment_link": payment_link,
                    "reference_number": reference_number,
                    "now": now,
                },
            ).mappings().first()
            session.commit()
            return dict(row)

    def update_bill_instance(self, bill_instance_id: int, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        updates = dict(updates)
        updates["updated_at"] = updates.get("updated_at") or self._now_iso()
        fields = []
        for key in updates.keys():
            fields.append(f"{key} = :{key}")
        sql = text(f"update bill_instances set {', '.join(fields)} where id = :id")
        updates["id"] = bill_instance_id
        with self._session() as session:
            session.execute(sql, updates)
            session.commit()

    def get_bill_instance(self, bill_instance_id: int) -> Optional[Dict[str, Any]]:
        sql = text(
            """
            select b.*, r.user_id, r.service_name, r.amount as recurring_amount, r.currency, r.category, r.description,
                   r.normalized_merchant, r.recurrence, r.recurrence_id, r.auto_add_transaction
            from bill_instances b
            join recurring_expenses r on r.id = b.recurring_id
            where b.id = :bill_instance_id
            """
        )
        with self._session() as session:
            row = session.execute(sql, {"bill_instance_id": bill_instance_id}).mappings().first()
            return dict(row) if row else None

    def mark_overdue_bill_instances(self, today_iso: str) -> int:
        with self._session() as session:
            result = session.execute(
                text(
                    """
                    update bill_instances
                    set status = 'overdue', updated_at = :now
                    where status = 'pending' and due_date < :today
                    """
                ),
                {"today": today_iso, "now": self._now_iso()},
            )
            session.commit()
            return int(result.rowcount or 0)

    def list_due_follow_up_bill_instances(self, today_iso: str) -> list[Dict[str, Any]]:
        sql = text(
            """
            select b.*, r.user_id, r.service_name, r.currency, r.category, r.description,
                   r.normalized_merchant, r.recurrence, r.recurrence_id, r.auto_add_transaction,
                   r.payment_link as recurring_payment_link, r.payment_reference as recurring_payment_reference
            from bill_instances b
            join recurring_expenses r on r.id = b.recurring_id
            where b.status = 'pending'
              and b.follow_up_on = :today
              and r.status = 'active'
            """
        )
        with self._session() as session:
            rows = session.execute(sql, {"today": today_iso}).mappings().all()
            return [dict(row) for row in rows]

    def create_bill_reminder_if_missing(
        self,
        bill_instance_id: int,
        reminder_offset: int,
        scheduled_for: str,
    ) -> Optional[int]:
        now = self._now_iso()
        with self._session() as session:
            row = session.execute(
                text(
                    """
                    insert into bill_instance_reminders (
                        bill_instance_id, reminder_offset, scheduled_for, status, created_at, updated_at
                    ) values (
                        :bill_instance_id, :reminder_offset, :scheduled_for, 'pending', :now, :now
                    )
                    on conflict (bill_instance_id, reminder_offset, scheduled_for) do nothing
                    returning id
                    """
                ),
                {
                    "bill_instance_id": bill_instance_id,
                    "reminder_offset": reminder_offset,
                    "scheduled_for": scheduled_for,
                    "now": now,
                },
            ).scalar()
            session.commit()
            return int(row) if row else None

    def update_bill_reminder(self, reminder_id: int, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        updates = dict(updates)
        updates["updated_at"] = updates.get("updated_at") or self._now_iso()
        fields = []
        for key in updates.keys():
            fields.append(f"{key} = :{key}")
        sql = text(f"update bill_instance_reminders set {', '.join(fields)} where id = :id")
        updates["id"] = reminder_id
        with self._session() as session:
            session.execute(sql, updates)
            session.commit()

    def get_user_chat_id(self, user_id: str, channel: str = "telegram") -> Optional[str]:
        sql = text(
            """
            select external_chat_id
            from user_identities
            where user_id = :user_id and channel = :channel
            order by id desc
            limit 1
            """
        )
        with self._session() as session:
            row = session.execute(sql, {"user_id": user_id, "channel": channel}).mappings().first()
            return row["external_chat_id"] if row else None

    def upsert_pending_action(self, user_id: str, action_type: str, state: Dict[str, Any]) -> Dict[str, Any]:
        now = self._now_iso()
        with self._session() as session:
            row = session.execute(
                text(
                    """
                    insert into bot_pending_actions (user_id, action_type, state, created_at, updated_at)
                    values (:user_id, :action_type, cast(:state as jsonb), :now, :now)
                    on conflict (user_id, action_type)
                    do update set state = excluded.state, updated_at = excluded.updated_at
                    returning *
                    """
                ),
                {"user_id": user_id, "action_type": action_type, "state": json.dumps(state), "now": now},
            ).mappings().first()
            session.commit()
            return dict(row)

    def get_pending_action(self, user_id: str, action_type: str) -> Optional[Dict[str, Any]]:
        sql = text(
            """
            select * from bot_pending_actions
            where user_id = :user_id and action_type = :action_type
            """
        )
        with self._session() as session:
            row = session.execute(sql, {"user_id": user_id, "action_type": action_type}).mappings().first()
            return dict(row) if row else None

    def delete_pending_action(self, pending_id: int) -> None:
        with self._session() as session:
            session.execute(text("delete from bot_pending_actions where id = :id"), {"id": pending_id})
            session.commit()


def ensure_database(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text("select 1"))


@dataclass
class ResilientPostgresRepo:
    repo: PostgresRepo

    def find_user_by_channel(self, channel: str, external_user_id: str) -> Optional[Dict[str, Any]]:
        return self.repo.find_user_by_channel(channel, external_user_id)

    def update_user_last_seen(self, channel: str, external_user_id: str, timestamp: Optional[str] = None) -> None:
        return self.repo.update_user_last_seen(channel, external_user_id, timestamp)

    def create_user(self, user_id: str, channel: str, external_user_id: str, chat_id: Optional[str]) -> None:
        return self.repo.create_user(user_id, channel, external_user_id, chat_id)

    def find_invite(self, invite_token: str) -> Optional[Dict[str, Any]]:
        return self.repo.find_invite(invite_token)

    def create_invite(self, invite_token: str, actor_user_id: Optional[str] = None) -> Dict[str, Any]:
        return self.repo.create_invite(invite_token, actor_user_id)

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None:
        return self.repo.mark_invite_used(invite_token, used_by_user_id)

    def append_transaction(self, tx: Dict[str, Any]) -> None:
        return self.repo.append_transaction(tx)

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]:
        return self.repo.list_transactions(user_id, include_deleted)

    def mark_transaction_deleted(self, tx_id: str) -> None:
        return self.repo.mark_transaction_deleted(tx_id)

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None:
        try:
            return self.repo.append_error_log(workflow, node, message, user_id, chat_id)
        except Exception as exc:
            logger.warning("Failed to append error log to Postgres: %s", exc)

    def find_recurring_by_recurrence_id(self, user_id: str, recurrence_id: str) -> Optional[Dict[str, Any]]:
        return self.repo.find_recurring_by_recurrence_id(user_id, recurrence_id)

    def create_recurring_expense(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.repo.create_recurring_expense(data)

    def get_recurring_expense(self, recurring_id: int) -> Optional[Dict[str, Any]]:
        return self.repo.get_recurring_expense(recurring_id)

    def update_recurring_expense(self, recurring_id: int, updates: Dict[str, Any]) -> None:
        return self.repo.update_recurring_expense(recurring_id, updates)

    def list_active_recurring_expenses(self) -> list[Dict[str, Any]]:
        return self.repo.list_active_recurring_expenses()

    def list_recurring_expenses(self, user_id: str) -> list[Dict[str, Any]]:
        return self.repo.list_recurring_expenses(user_id)

    def upsert_bill_instance(
        self,
        recurring_id: int,
        period_year: int,
        period_month: int,
        due_date: str,
        amount: Optional[float],
        payment_link: Optional[str],
        reference_number: Optional[str],
    ) -> Dict[str, Any]:
        return self.repo.upsert_bill_instance(
            recurring_id,
            period_year,
            period_month,
            due_date,
            amount,
            payment_link,
            reference_number,
        )

    def update_bill_instance(self, bill_instance_id: int, updates: Dict[str, Any]) -> None:
        return self.repo.update_bill_instance(bill_instance_id, updates)

    def get_bill_instance(self, bill_instance_id: int) -> Optional[Dict[str, Any]]:
        return self.repo.get_bill_instance(bill_instance_id)

    def mark_overdue_bill_instances(self, today_iso: str) -> int:
        return self.repo.mark_overdue_bill_instances(today_iso)

    def list_due_follow_up_bill_instances(self, today_iso: str) -> list[Dict[str, Any]]:
        return self.repo.list_due_follow_up_bill_instances(today_iso)

    def create_bill_reminder_if_missing(
        self,
        bill_instance_id: int,
        reminder_offset: int,
        scheduled_for: str,
    ) -> Optional[int]:
        return self.repo.create_bill_reminder_if_missing(
            bill_instance_id,
            reminder_offset,
            scheduled_for,
        )

    def update_bill_reminder(self, reminder_id: int, updates: Dict[str, Any]) -> None:
        return self.repo.update_bill_reminder(reminder_id, updates)

    def get_user_chat_id(self, user_id: str, channel: str = "telegram") -> Optional[str]:
        return self.repo.get_user_chat_id(user_id, channel)

    def upsert_pending_action(self, user_id: str, action_type: str, state: Dict[str, Any]) -> Dict[str, Any]:
        return self.repo.upsert_pending_action(user_id, action_type, state)

    def get_pending_action(self, user_id: str, action_type: str) -> Optional[Dict[str, Any]]:
        return self.repo.get_pending_action(user_id, action_type)

    def delete_pending_action(self, pending_id: int) -> None:
        return self.repo.delete_pending_action(pending_id)
