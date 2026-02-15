from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Protocol


@dataclass(frozen=True)
class InviteRecord:
    invite_token: str
    status: str
    used_by_user_id: Optional[str]


class DataRepo(Protocol):
    def find_user_by_channel(self, channel: str, external_user_id: str) -> Optional[Dict[str, Any]]: ...

    def update_user_last_seen(self, channel: str, external_user_id: str, timestamp: Optional[str] = None) -> None: ...

    def create_user(self, user_id: str, channel: str, external_user_id: str, chat_id: Optional[str]) -> None: ...

    def find_invite(self, invite_token: str) -> Optional[Dict[str, Any]]: ...

    def create_invite(self, invite_token: str, actor_user_id: Optional[str] = None) -> Dict[str, Any]: ...

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None: ...

    def append_transaction(self, tx: Dict[str, Any]) -> None: ...

    def append_transactions(self, txs: list[Dict[str, Any]]) -> None: ...

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]: ...

    def mark_transaction_deleted(self, tx_id: str) -> None: ...
    def mark_all_transactions_deleted(self, user_id: str) -> int: ...

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None: ...

    def find_recurring_by_recurrence_id(self, user_id: str, recurrence_id: str) -> Optional[Dict[str, Any]]: ...

    def create_recurring_expense(self, data: Dict[str, Any]) -> Dict[str, Any]: ...

    def get_recurring_expense(self, recurring_id: int) -> Optional[Dict[str, Any]]: ...

    def update_recurring_expense(self, recurring_id: int, updates: Dict[str, Any]) -> None: ...

    def list_active_recurring_expenses(self) -> list[Dict[str, Any]]: ...

    def list_recurring_expenses(self, user_id: str) -> list[Dict[str, Any]]: ...

    def upsert_bill_instance(
        self,
        recurring_id: int,
        period_year: int,
        period_month: int,
        due_date: str,
        amount: Optional[float],
        payment_link: Optional[str],
        reference_number: Optional[str],
    ) -> Dict[str, Any]: ...

    def update_bill_instance(self, bill_instance_id: int, updates: Dict[str, Any]) -> None: ...

    def get_bill_instance(self, bill_instance_id: int) -> Optional[Dict[str, Any]]: ...

    def mark_overdue_bill_instances(self, today_iso: str) -> int: ...
    def list_due_follow_up_bill_instances(self, today_iso: str) -> list[Dict[str, Any]]: ...

    def create_bill_reminder_if_missing(
        self,
        bill_instance_id: int,
        reminder_offset: int,
        scheduled_for: str,
    ) -> Optional[int]: ...

    def update_bill_reminder(self, reminder_id: int, updates: Dict[str, Any]) -> None: ...

    def get_user_chat_id(self, user_id: str, channel: str = "telegram") -> Optional[str]: ...

    def upsert_pending_action(
        self,
        user_id: str,
        action_type: str,
        state: Dict[str, Any],
        expires_at: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def get_pending_action(self, user_id: str, action_type: str) -> Optional[Dict[str, Any]]: ...

    def delete_pending_action(self, pending_id: int) -> None: ...


@dataclass
class CompositeRepo:
    primary: DataRepo
    secondary_writers: Iterable[DataRepo]

    def find_user_by_channel(self, channel: str, external_user_id: str) -> Optional[Dict[str, Any]]:
        return self.primary.find_user_by_channel(channel, external_user_id)

    def update_user_last_seen(self, channel: str, external_user_id: str, timestamp: Optional[str] = None) -> None:
        self.primary.update_user_last_seen(channel, external_user_id, timestamp)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.update_user_last_seen(channel, external_user_id, timestamp))

    def create_user(self, user_id: str, channel: str, external_user_id: str, chat_id: Optional[str]) -> None:
        self.primary.create_user(user_id, channel, external_user_id, chat_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.create_user(user_id, channel, external_user_id, chat_id))

    def find_invite(self, invite_token: str) -> Optional[Dict[str, Any]]:
        return self.primary.find_invite(invite_token)

    def create_invite(self, invite_token: str, actor_user_id: Optional[str] = None) -> Dict[str, Any]:
        return self.primary.create_invite(invite_token, actor_user_id)

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None:
        self.primary.mark_invite_used(invite_token, used_by_user_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.mark_invite_used(invite_token, used_by_user_id))

    def append_transaction(self, tx: Dict[str, Any]) -> None:
        self.primary.append_transaction(tx)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.append_transaction(tx))

    def append_transactions(self, txs: list[Dict[str, Any]]) -> None:
        if not txs:
            return
        self.primary.append_transactions(txs)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.append_transactions(txs))

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]:
        return self.primary.list_transactions(user_id, include_deleted)

    def mark_transaction_deleted(self, tx_id: str) -> None:
        self.primary.mark_transaction_deleted(tx_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.mark_transaction_deleted(tx_id))

    def mark_all_transactions_deleted(self, user_id: str) -> int:
        deleted_count = self.primary.mark_all_transactions_deleted(user_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.mark_all_transactions_deleted(user_id))
        return deleted_count

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None:
        self.primary.append_error_log(workflow, node, message, user_id, chat_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.append_error_log(workflow, node, message, user_id, chat_id))

    def find_recurring_by_recurrence_id(self, user_id: str, recurrence_id: str) -> Optional[Dict[str, Any]]:
        return self.primary.find_recurring_by_recurrence_id(user_id, recurrence_id)

    def create_recurring_expense(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return self.primary.create_recurring_expense(data)

    def get_recurring_expense(self, recurring_id: int) -> Optional[Dict[str, Any]]:
        return self.primary.get_recurring_expense(recurring_id)

    def update_recurring_expense(self, recurring_id: int, updates: Dict[str, Any]) -> None:
        return self.primary.update_recurring_expense(recurring_id, updates)

    def list_active_recurring_expenses(self) -> list[Dict[str, Any]]:
        return self.primary.list_active_recurring_expenses()

    def list_recurring_expenses(self, user_id: str) -> list[Dict[str, Any]]:
        return self.primary.list_recurring_expenses(user_id)

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
        return self.primary.upsert_bill_instance(
            recurring_id,
            period_year,
            period_month,
            due_date,
            amount,
            payment_link,
            reference_number,
        )

    def update_bill_instance(self, bill_instance_id: int, updates: Dict[str, Any]) -> None:
        return self.primary.update_bill_instance(bill_instance_id, updates)

    def get_bill_instance(self, bill_instance_id: int) -> Optional[Dict[str, Any]]:
        return self.primary.get_bill_instance(bill_instance_id)

    def mark_overdue_bill_instances(self, today_iso: str) -> int:
        return self.primary.mark_overdue_bill_instances(today_iso)

    def list_due_follow_up_bill_instances(self, today_iso: str) -> list[Dict[str, Any]]:
        return self.primary.list_due_follow_up_bill_instances(today_iso)

    def create_bill_reminder_if_missing(
        self,
        bill_instance_id: int,
        reminder_offset: int,
        scheduled_for: str,
    ) -> Optional[int]:
        return self.primary.create_bill_reminder_if_missing(
            bill_instance_id,
            reminder_offset,
            scheduled_for,
        )

    def update_bill_reminder(self, reminder_id: int, updates: Dict[str, Any]) -> None:
        return self.primary.update_bill_reminder(reminder_id, updates)

    def get_user_chat_id(self, user_id: str, channel: str = "telegram") -> Optional[str]:
        return self.primary.get_user_chat_id(user_id, channel)

    def upsert_pending_action(
        self,
        user_id: str,
        action_type: str,
        state: Dict[str, Any],
        expires_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self.primary.upsert_pending_action(user_id, action_type, state, expires_at)

    def get_pending_action(self, user_id: str, action_type: str) -> Optional[Dict[str, Any]]:
        return self.primary.get_pending_action(user_id, action_type)

    def delete_pending_action(self, pending_id: int) -> None:
        return self.primary.delete_pending_action(pending_id)


def _safe_call(fn) -> None:
    try:
        fn()
    except Exception:
        return
