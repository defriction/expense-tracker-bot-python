from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

from app.core.config import Settings, load_service_account_info


@dataclass
class SheetRecord:
    row_number: int
    data: Dict[str, Any]


class SheetsRepo:
    def __init__(self, settings: Settings) -> None:
        info = load_service_account_info(settings)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = Credentials.from_service_account_info(info, scopes=scopes)
        client = gspread.authorize(credentials)
        self._spreadsheet = client.open_by_key(settings.google_sheets_id)

    def _worksheet(self, name: str) -> gspread.Worksheet:
        return self._spreadsheet.worksheet(name)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _get_records(ws: gspread.Worksheet) -> List[SheetRecord]:
        values = ws.get_all_values()
        if not values:
            return []
        headers = values[0]
        records: List[SheetRecord] = []
        for idx, row in enumerate(values[1:], start=2):
            data: Dict[str, Any] = {}
            for col_index, header in enumerate(headers):
                data[header] = row[col_index] if col_index < len(row) else ""
            records.append(SheetRecord(row_number=idx, data=data))
        return records

    @staticmethod
    def _find_record(records: List[SheetRecord], key: str, value: str) -> Optional[SheetRecord]:
        for record in records:
            if str(record.data.get(key, "")) == str(value):
                return record
        return None

    @staticmethod
    def _normalize_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1", "yes"}

    @staticmethod
    def _normalize_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(str(value).replace(",", "."))
        except ValueError:
            return default

    def find_user_by_telegram_id(self, telegram_user_id: str) -> Optional[Dict[str, Any]]:
        ws = self._worksheet("Users")
        records = self._get_records(ws)
        record = self._find_record(records, "telegramUserId", str(telegram_user_id))
        return record.data if record else None

    def update_user_last_seen(self, telegram_user_id: str, timestamp: Optional[str] = None) -> None:
        ws = self._worksheet("Users")
        records = self._get_records(ws)
        record = self._find_record(records, "telegramUserId", str(telegram_user_id))
        if not record:
            return
        ts = timestamp or self._now_iso()
        self._update_cells(ws, record.row_number, {"telegramUserId": telegram_user_id, "lastSeenAt": ts})

    def create_user(self, user_id: str, telegram_user_id: str, chat_id: str) -> None:
        ws = self._worksheet("Users")
        row = {
            "userId": user_id,
            "telegramUserId": telegram_user_id,
            "chatId": chat_id,
            "status": "active",
            "createdAt": self._now_iso(),
        }
        self._append_row(ws, row)

    def find_invite(self, invite_token: str) -> Optional[Dict[str, Any]]:
        ws = self._worksheet("Invites")
        records = self._get_records(ws)
        record = self._find_record(records, "inviteToken", invite_token)
        return record.data if record else None

    def mark_invite_used(self, invite_token: str) -> None:
        ws = self._worksheet("Invites")
        records = self._get_records(ws)
        record = self._find_record(records, "inviteToken", invite_token)
        if not record:
            return
        self._update_cells(ws, record.row_number, {"status": "used", "inviteToken": invite_token})

    def append_transaction(self, tx: Dict[str, Any]) -> None:
        ws = self._worksheet("Transactions")
        self._append_row(ws, tx)

    def list_transactions(self, user_id: str, include_deleted: bool = False) -> List[Dict[str, Any]]:
        ws = self._worksheet("Transactions")
        records = self._get_records(ws)
        result: List[Dict[str, Any]] = []
        for record in records:
            if str(record.data.get("userId")) != str(user_id):
                continue
            is_deleted = self._normalize_bool(record.data.get("isDeleted"))
            if not include_deleted and is_deleted:
                continue
            normalized = dict(record.data)
            normalized["isDeleted"] = is_deleted
            normalized["amount"] = self._normalize_float(record.data.get("amount"), 0.0)
            normalized["parseConfidence"] = self._normalize_float(record.data.get("parseConfidence"), 0.0)
            normalized["isRecurring"] = self._normalize_bool(record.data.get("isRecurring"))
            result.append(normalized)
        return result

    def mark_transaction_deleted(self, tx_id: str) -> None:
        ws = self._worksheet("Transactions")
        records = self._get_records(ws)
        record = self._find_record(records, "txId", tx_id)
        if not record:
            return
        now = self._now_iso()
        self._update_cells(
            ws,
            record.row_number,
            {"txId": tx_id, "isDeleted": "true", "updatedAt": now, "deletedAt": now},
        )

    def append_error_log(self, workflow: str, node: str, message: str) -> None:
        ws = self._worksheet("ErrorLogs")
        row = {
            "timestamp": self._now_iso(),
            "workflow": workflow,
            "node": node,
            "message": message,
        }
        self._append_row(ws, row)

    def _append_row(self, ws: gspread.Worksheet, row: Dict[str, Any]) -> None:
        headers = ws.row_values(1)
        values = [row.get(header, "") for header in headers]
        ws.append_row(values, value_input_option="USER_ENTERED")

    def _update_cells(self, ws: gspread.Worksheet, row_number: int, updates: Dict[str, Any]) -> None:
        headers = ws.row_values(1)
        for header, value in updates.items():
            if header not in headers:
                continue
            col = headers.index(header) + 1
            ws.update_cell(row_number, col, value)
