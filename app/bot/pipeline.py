from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Dict, Optional

from telegram import Update
from telegram.ext import ContextTypes

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
from app.bot.parser import (
    build_system_prompt,
    generate_tx_id,
    normalize_ai_response,
    normalize_types,
    parse_command,
)
from app.core.config import Settings
from app.core.logging import logger
from app.services.groq import GroqClient, extract_json
from app.services.sheets import ResilientSheetsRepo

INVALID_TOKEN_MESSAGE = "Token de invitación inválido o expirado."
INVALID_TX_MESSAGE = "Monto inválido o categoría faltante. Por favor intenta de nuevo."


class PipelineBase:
    def __init__(self, settings: Settings, sheets: Optional[ResilientSheetsRepo] = None, groq: Optional[GroqClient] = None) -> None:
        self.settings = settings
        self._sheets = sheets
        self._groq = groq

    def _get_sheets(self) -> ResilientSheetsRepo:
        if self._sheets is None:
            raise RuntimeError("Sheets repository not configured")
        return self._sheets

    def _get_groq(self) -> GroqClient:
        if self._groq is None:
            raise RuntimeError("Groq client not configured")
        return self._groq

    async def _reply(self, chat_id: Optional[int], text: str, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not chat_id:
            return
        await context.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@dataclass
class ActiveUserResult:
    user: Optional[Dict[str, Any]]
    error_message: Optional[str]


class AuthFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    def require_active_user(self, telegram_user_id: Optional[int]) -> ActiveUserResult:
        if not telegram_user_id:
            return ActiveUserResult(None, UNAUTHORIZED_MESSAGE)
        user = self.pipeline._get_sheets().find_user_by_telegram_id(str(telegram_user_id))
        if not user or str(user.get("status")) != "active":
            return ActiveUserResult(None, UNAUTHORIZED_MESSAGE)
        return ActiveUserResult(user, None)


class OnboardingFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle(self, command, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = command.chat_id
        telegram_user_id = command.telegram_user_id
        logger.info("Onboarding start chat_id=%s user_id=%s", chat_id, telegram_user_id)
        if not telegram_user_id:
            await self.pipeline._reply(chat_id, INVALID_TOKEN_MESSAGE, context)
            return

        sheets = self.pipeline._get_sheets()
        invite = sheets.find_invite(command.invite_token)
        if not invite or str(invite.get("status")) != "unused":
            logger.warning("Onboarding invalid token chat_id=%s user_id=%s", chat_id, telegram_user_id)
            await self.pipeline._reply(chat_id, INVALID_TOKEN_MESSAGE, context)
            return

        user_id = f"USR-{int(time.time() * 1000)}-{telegram_user_id}"
        sheets.create_user(user_id, str(telegram_user_id), str(chat_id))
        sheets.mark_invite_used(command.invite_token)
        logger.info("Onboarding success chat_id=%s user_id=%s", chat_id, telegram_user_id)
        await self.pipeline._reply(chat_id, ONBOARDING_SUCCESS_MESSAGE, context)


class CommandFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle_list(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.info("List command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        await self.pipeline._reply(chat_id, format_list_message(txs), context)

    async def handle_summary(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.info("Summary command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        await self.pipeline._reply(chat_id, format_summary_message(txs), context)

    async def handle_undo(self, user: Dict[str, Any], chat_id: Optional[int], context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.info("Undo command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        picked = BotPipeline._pick_latest(txs)
        if picked.get("ok"):
            self.pipeline._get_sheets().mark_transaction_deleted(str(picked["txId"]))
        await self.pipeline._reply(chat_id, format_undo_message(picked), context)


class AiFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle(self, command, user: Dict[str, Any], chat_id: Optional[int], update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        logger.info("AI parse start chat_id=%s user_id=%s", chat_id, user.get("userId"))
        system_prompt = build_system_prompt(self.pipeline.settings)
        user_message = command.text_for_parsing or command.text
        content = await self.pipeline._get_groq().chat_completion(system_prompt, user_message)
        parsed = extract_json(content)

        tx = normalize_ai_response(parsed, command.text, chat_id, self.pipeline.settings)
        tx = normalize_types(tx)
        tx["chatId"] = chat_id
        tx["sourceMessageId"] = str(update.effective_message.message_id) if update.effective_message else ""

        intent = str(tx.get("intent", "add_tx")).lower()
        if intent == "help":
            await self.pipeline._reply(chat_id, HELP_MESSAGE, context)
            return
        if intent == "list":
            await self.pipeline.command_flow.handle_list(user, chat_id, context)
            return
        if intent == "summary":
            await self.pipeline.command_flow.handle_summary(user, chat_id, context)
            return

        if intent != "add_tx":
            await self.pipeline._reply(chat_id, HELP_MESSAGE, context)
            return

        if float(tx.get("amount", 0)) <= 0 or not str(tx.get("category")):
            logger.warning("AI invalid tx chat_id=%s user_id=%s", chat_id, user.get("userId"))
            await self.pipeline._reply(chat_id, INVALID_TX_MESSAGE, context)
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
        tx["createdAt"] = tx.get("createdAt") or __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
        tx["updatedAt"] = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
        tx["isDeleted"] = tx.get("isDeleted", False)
        tx["deletedAt"] = tx.get("deletedAt", "")

        self.pipeline._get_sheets().append_transaction(tx)
        logger.info("AI tx saved chat_id=%s user_id=%s tx_id=%s", chat_id, user.get("userId"), tx_id)
        await self.pipeline._reply(chat_id, format_add_tx_message(tx), context)


class BotPipeline(PipelineBase):
    def __init__(self, settings: Settings, sheets: Optional[ResilientSheetsRepo] = None, groq: Optional[GroqClient] = None) -> None:
        super().__init__(settings, sheets, groq)
        self.auth_flow = AuthFlow(self)
        self.onboarding_flow = OnboardingFlow(self)
        self.command_flow = CommandFlow(self)
        self.ai_flow = AiFlow(self)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        chat_id = message.chat_id if message else None
        telegram_user_id = message.from_user.id if message and message.from_user else None
        text = message.text if message else None
        logger.info(
            "Incoming message route=pending chat_id=%s user_id=%s has_text=%s has_voice=%s",
            chat_id,
            telegram_user_id,
            bool(text),
            bool(message and message.voice),
        )

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

        logger.info(
            "Parsed command route=%s chat_id=%s user_id=%s",
            command.route,
            chat_id,
            telegram_user_id,
        )
        if command.route == "onboarding":
            await self.onboarding_flow.handle(command, context)
            return

        if command.route == "help":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return

        if command.route == "non_text":
            await self._reply(chat_id, NON_TEXT_MESSAGE, context)
            return

        auth_result = self.auth_flow.require_active_user(telegram_user_id)
        if not auth_result.user:
            logger.warning(
                "Unauthorized user chat_id=%s user_id=%s",
                chat_id,
                telegram_user_id,
            )
            await self._reply(chat_id, auth_result.error_message or UNAUTHORIZED_MESSAGE, context)
            return

        self._get_sheets().update_user_last_seen(str(telegram_user_id))

        if command.route == "list":
            await self.command_flow.handle_list(auth_result.user, chat_id, context)
            return
        if command.route == "summary":
            await self.command_flow.handle_summary(auth_result.user, chat_id, context)
            return
        if command.route == "undo":
            await self.command_flow.handle_undo(auth_result.user, chat_id, context)
            return

        await self.ai_flow.handle(command, auth_result.user, chat_id, update, context)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        callback = update.callback_query
        if not callback:
            return
        await callback.answer()
        message = callback.message
        chat_id = message.chat_id if message else None
        telegram_user_id = callback.from_user.id if callback.from_user else None
        text = callback.data
        logger.info(
            "Incoming callback chat_id=%s user_id=%s has_data=%s",
            chat_id,
            telegram_user_id,
            bool(text),
        )
        command = parse_command(text, chat_id, telegram_user_id, None)

        if command.route == "onboarding":
            await self.onboarding_flow.handle(command, context)
            return

        if command.route == "help":
            await self._reply(chat_id, HELP_MESSAGE, context)
            return

        if command.route in {"list", "summary", "undo", "ai"}:
            auth_result = self.auth_flow.require_active_user(telegram_user_id)
            if not auth_result.user:
                logger.warning(
                    "Unauthorized callback chat_id=%s user_id=%s",
                    chat_id,
                    telegram_user_id,
                )
                await self._reply(chat_id, auth_result.error_message or UNAUTHORIZED_MESSAGE, context)
                return
            self._get_sheets().update_user_last_seen(str(telegram_user_id))
            if command.route == "list":
                await self.command_flow.handle_list(auth_result.user, chat_id, context)
            elif command.route == "summary":
                await self.command_flow.handle_summary(auth_result.user, chat_id, context)
            elif command.route == "undo":
                await self.command_flow.handle_undo(auth_result.user, chat_id, context)
            else:
                await self.ai_flow.handle(command, auth_result.user, chat_id, update, context)

    async def _transcribe_voice(self, file_id: str, context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
        try:
            file = await context.bot.get_file(file_id)
            audio_bytes = await file.download_as_bytearray()
            response = await self._get_groq().transcribe(bytes(audio_bytes))
        except Exception as exc:
            logger.warning("Voice transcription failed: %s", exc)
            return None
        return response.get("text") if isinstance(response, dict) else None

    @staticmethod
    def _pick_latest(transactions: list[Dict[str, Any]]) -> Dict[str, Any]:
        def to_ts(item: Dict[str, Any]) -> float:
            date_value = str(item.get("date") or "")
            if date_value and len(date_value) == 10:
                try:
                    return __import__('datetime').datetime.fromisoformat(date_value + "T00:00:00+00:00").timestamp()
                except ValueError:
                    pass
            created_at = str(item.get("createdAt") or "")
            try:
                return __import__('datetime').datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
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
