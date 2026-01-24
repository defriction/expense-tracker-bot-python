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

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None: ...

    def append_transaction(self, tx: Dict[str, Any]) -> None: ...

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]: ...

    def mark_transaction_deleted(self, tx_id: str) -> None: ...

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None: ...


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

    def mark_invite_used(self, invite_token: str, used_by_user_id: Optional[str]) -> None:
        self.primary.mark_invite_used(invite_token, used_by_user_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.mark_invite_used(invite_token, used_by_user_id))

    def append_transaction(self, tx: Dict[str, Any]) -> None:
        self.primary.append_transaction(tx)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.append_transaction(tx))

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> list[Dict[str, Any]]:
        return self.primary.list_transactions(user_id, include_deleted)

    def mark_transaction_deleted(self, tx_id: str) -> None:
        self.primary.mark_transaction_deleted(tx_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.mark_transaction_deleted(tx_id))

    def append_error_log(self, workflow: str, node: str, message: str, user_id: Optional[str], chat_id: Optional[str]) -> None:
        self.primary.append_error_log(workflow, node, message, user_id, chat_id)
        for writer in self.secondary_writers:
            _safe_call(lambda: writer.append_error_log(workflow, node, message, user_id, chat_id))


def _safe_call(fn) -> None:
    try:
        fn()
    except Exception:
        return
