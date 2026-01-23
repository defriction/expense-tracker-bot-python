from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from telegram import Update
from telegram.ext import ContextTypes

from app.core.config import Settings
from app.services.groq import extract_json, groq_chat_completion, groq_transcribe
from app.bot.formatters import (
    HELP_MESSAGE,
    NON_TEXT_MESSAGE,
    ONBOARDING_SUCCESS_MESSAGE,
    UNAUTHORIZED_MESSAGE,
    format_add_tx_message,
    format_list_message,
    format_summary_message,
    format_undo_message,
)
from app.bot.parser import build_prompt, generate_tx_id, normalize_ai_response, normalize_types, parse_command
from app.services.sheets import SheetsRepo

INVALID_TOKEN_MESSAGE = "Token de invitación inválido o expirado."
INVALID_TX_MESSAGE = "Monto inválido o categoría faltante. Por favor intenta de nuevo."


class BotPipeline:
    def __init__(self, settings: Settings, sheets: SheetsRepo) -> None:
        self.settings = settings
        self.sheets = sheets

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat_id = message.chat_id if message else None
        telegram_user_id = message.from_user.id if message and message.from_user else None
        text = message.text if message else None

        if message and message.voice:
            audio_text = await self._transcribe_voice(message.voice.file_id, context)
            text = audio_text or text

        non_text_type = None
        if not text:
            if message and message.voice:
                non_text_type = "voice"
            elif message and message.photo:
                non_text_type = "photo"
            elif message and message.sticker:
                non_text_type = "sticker"
            else:
                non_text_type = "non_text"

        command = parse_command(text, chat_id, telegram_user_id, non_text_type)

        if command.route == "onboarding":
            await self._handle_onboarding(command, context)
            return

        if command.route == "help":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return

        if command.route == "non_text":
            await self._reply(chat_id, NON_TEXT_MESSAGE, context)
            return

        if not telegram_user_id:
            await self._reply(chat_id, UNAUTHORIZED_MESSAGE, context)
            return

        user = self.sheets.find_user_by_telegram_id(str(telegram_user_id))
        if not user or str(user.get("status")) != "active":
            await self._reply(chat_id, UNAUTHORIZED_MESSAGE, context)
            return

        self.sheets.update_user_last_seen(str(telegram_user_id))

        if command.route == "list":
            await self._handle_list(user, chat_id, context)
            return
        if command.route == "summary":
            await self._handle_summary(user, chat_id, context)
            return
        if command.route == "undo":
            await self._handle_undo(user, chat_id, context)
            return

        await self._handle_ai(command, user, chat_id, update, context)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        callback = update.callback_query
        if not callback:
            return
        await callback.answer()
        message = callback.message
        chat_id = message.chat_id if message else None
        telegram_user_id = callback.from_user.id if callback.from_user else None
        text = callback.data
        command = parse_command(text, chat_id, telegram_user_id, None)
        if command.route == "help":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return
        if command.route in {"list", "summary", "undo", "ai"}:
            user = self.sheets.find_user_by_telegram_id(str(telegram_user_id)) if telegram_user_id else None
            if not user or str(user.get("status")) != "active":
                await self._reply(chat_id, UNAUTHORIZED_MESSAGE, context)
                return
            self.sheets.update_user_last_seen(str(telegram_user_id))
            if command.route == "list":
                await self._handle_list(user, chat_id, context)
            elif command.route == "summary":
                await self._handle_summary(user, chat_id, context)
            elif command.route == "undo":
                await self._handle_undo(user, chat_id, context)
            else:
                await self._handle_ai(command, user, chat_id, update, context)

    async def _handle_onboarding(self, command, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = command.chat_id
        telegram_user_id = command.telegram_user_id
        if not telegram_user_id:
            await self._reply(chat_id, INVALID_TOKEN_MESSAGE, context)
            return

        invite = self.sheets.find_invite(command.invite_token)
        if not invite or str(invite.get("status")) != "unused":
            await self._reply(chat_id, INVALID_TOKEN_MESSAGE, context)
            return

        user_id = f"USR-{int(datetime.now(timezone.utc).timestamp() * 1000)}-{telegram_user_id}"
        self.sheets.create_user(user_id, str(telegram_user_id), str(chat_id))
        self.sheets.mark_invite_used(command.invite_token)
        await self._reply(chat_id, ONBOARDING_SUCCESS_MESSAGE, context)

    async def _handle_ai(self, command, user: Dict[str, Any], chat_id: Optional[int], update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        prompt = build_prompt(command.text_for_parsing or command.text, self.settings)
        content = await groq_chat_completion(self.settings, prompt)
        try:
            parsed = extract_json(content)
        except json.JSONDecodeError:
            await self._reply(chat_id, HELP_MESSAGE, context)
            return
        tx = normalize_ai_response(parsed, command.text, chat_id, self.settings)
        tx = normalize_types(tx)
        tx["chatId"] = chat_id
        tx["sourceMessageId"] = str(update.effective_message.message_id) if update.effective_message else ""

        intent = str(tx.get("intent", "add_tx")).lower()
        if intent == "help":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return
        if intent == "list":
            await self._handle_list(user, chat_id, context)
            return
        if intent == "summary":
            await self._handle_summary(user, chat_id, context)
            return

        if intent != "add_tx":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return

        if float(tx.get("amount", 0)) <= 0 or not str(tx.get("category")):
            await self._reply(chat_id, INVALID_TX_MESSAGE, context)
            return

        tx_id = generate_tx_id()
        tx["txId"] = tx_id
        tx["userId"] = user.get("userId")
        tx["paymentMethod"] = tx.get("paymentMethod") or "cash"
        tx["normalizedMerchant"] = tx.get("normalizedMerchant") or ""
        tx["transactionKind"] = tx.get("transactionKind") or "regular"
        tx["isRecurring"] = bool(tx.get("isRecurring"))
        tx["recurrence"] = tx.get("recurrence") or ""
        tx["recurrenceId"] = tx.get("recurrenceId") or ""
        tx["counterparty"] = tx.get("counterparty") or ""
        tx["loanRole"] = tx.get("loanRole") or ""
        tx["loanId"] = tx.get("loanId") or ""
        tx["parseConfidence"] = tx.get("parseConfidence") or 0.7
        tx["parserVersion"] = tx.get("parserVersion") or "mvp-v1"
        tx["source"] = tx.get("source") or "telegram"
        tx["sourceMessageId"] = tx.get("sourceMessageId") or ""
        tx["createdAt"] = tx.get("createdAt") or datetime.now(timezone.utc).isoformat()
        tx["updatedAt"] = datetime.now(timezone.utc).isoformat()
        tx["isDeleted"] = tx.get("isDeleted", False)
        tx["deletedAt"] = tx.get("deletedAt", "")

        self.sheets.append_transaction(tx)
        await self._reply(chat_id, format_add_tx_message(tx), context)

    async def _handle_list(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        txs = self.sheets.list_transactions(user.get("userId"))
        message = format_list_message(txs)
        await self._reply(chat_id, message, context)

    async def _handle_summary(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        txs = self.sheets.list_transactions(user.get("userId"))
        message = format_summary_message(txs)
        await self._reply(chat_id, message, context)

    async def _handle_undo(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        txs = self.sheets.list_transactions(user.get("userId"))
        picked = self._pick_latest(txs)
        if picked.get("ok"):
            self.sheets.mark_transaction_deleted(str(picked["txId"]))
        await self._reply(chat_id, format_undo_message(picked), context)

    @staticmethod
    def _pick_latest(transactions: list[Dict[str, Any]]) -> Dict[str, Any]:
        def to_ts(item: Dict[str, Any]) -> float:
            date_value = str(item.get("date") or "")
            if date_value and len(date_value) == 10:
                try:
                    return datetime.fromisoformat(date_value + "T00:00:00+00:00").timestamp()
                except ValueError:
                    pass
            created_at = str(item.get("createdAt") or "")
            try:
                return datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return float("-inf")

        valid = [tx for tx in transactions if tx.get("txId")]
        if not valid:
            return {"ok": False, "reason": "no_tx"}

        valid.sort(key=to_ts, reverse=True)
        tx = valid[0]
        return {
            "ok": True,
            "txId": str(tx.get("txId")),
            "amount": tx.get("amount"),
            "currency": tx.get("currency", "COP"),
            "category": tx.get("category", "misc"),
            "description": tx.get("description", ""),
            "date": tx.get("date", ""),
        }

    async def _transcribe_voice(self, file_id: str, context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
        file = await context.bot.get_file(file_id)
        audio_bytes = await file.download_as_bytearray()
        response = await groq_transcribe(self.settings, bytes(audio_bytes))
        return response.get("text") if isinstance(response, dict) else None

    async def _reply(self, chat_id: Optional[int], text: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not chat_id:
            return
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
