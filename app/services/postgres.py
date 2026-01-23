from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

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
                    values ('user', :user_id, 'create', :payload::jsonb, :now)
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
                    values ('invite', :token, 'use', :payload::jsonb, :now, :user_id)
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
                    values ('transaction', :tx_id, 'create', :payload::jsonb, :now, :user_id, :source)
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
                    values ('transaction', :tx_id, 'delete', :payload::jsonb, :now)
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
