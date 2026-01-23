from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Dict, Optional

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
from app.bot.ui_models import BotAction, BotInput, BotKeyboard, BotMessage
from app.core.config import Settings
from app.core.logging import logger
from app.services.groq import GroqClient, extract_json
from app.services.sheets import ResilientSheetsRepo

INVALID_TOKEN_MESSAGE = "Token de invitación inválido o expirado."
INVALID_TX_MESSAGE = "Monto inválido o categoría faltante. Por favor intenta de nuevo."

ACTION_LIST = BotAction("/list", "🧾 Movimientos")
ACTION_SUMMARY = BotAction("/summary", "📊 Resumen")
ACTION_UNDO = BotAction("/undo", "↩️ Deshacer")
ACTION_HELP = BotAction("/help", "ℹ️ Ayuda")


def _kb(*rows: list[BotAction]) -> BotKeyboard:
    return BotKeyboard(rows=[row for row in rows if row])


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

    def _make_message(self, text: str, keyboard: Optional[BotKeyboard] = None) -> BotMessage:
        return BotMessage(text=text, keyboard=keyboard, disable_web_preview=True)


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

    async def handle(self, command) -> BotMessage:
        chat_id = command.chat_id
        telegram_user_id = command.telegram_user_id
        logger.info("Onboarding start chat_id=%s user_id=%s", chat_id, telegram_user_id)
        if not telegram_user_id:
            return self.pipeline._make_message(INVALID_TOKEN_MESSAGE)

        sheets = self.pipeline._get_sheets()
        invite = sheets.find_invite(command.invite_token)
        if not invite or str(invite.get("status")) != "unused":
            logger.warning("Onboarding invalid token chat_id=%s user_id=%s", chat_id, telegram_user_id)
            return self.pipeline._make_message(INVALID_TOKEN_MESSAGE)

        user_id = f"USR-{int(time.time() * 1000)}-{telegram_user_id}"
        sheets.create_user(user_id, str(telegram_user_id), str(chat_id))
        sheets.mark_invite_used(command.invite_token)
        logger.info("Onboarding success chat_id=%s user_id=%s", chat_id, telegram_user_id)
        keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(ONBOARDING_SUCCESS_MESSAGE, keyboard)


class CommandFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle_list(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("List command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_UNDO, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(format_list_message(txs), keyboard)

    async def handle_summary(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Summary command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_LIST, ACTION_UNDO], [ACTION_HELP])
        return self.pipeline._make_message(format_summary_message(txs), keyboard)

    async def handle_undo(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Undo command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_sheets().list_transactions(user.get("userId"))
        picked = BotPipeline._pick_latest(txs)
        if picked.get("ok"):
            self.pipeline._get_sheets().mark_transaction_deleted(str(picked["txId"]))
        keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(format_undo_message(picked), keyboard)


class AiFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle(
        self,
        command,
        user: Dict[str, Any],
        chat_id: Optional[int],
        message_id: Optional[str],
        source: str,
    ) -> BotMessage:
        logger.info("AI parse start chat_id=%s user_id=%s", chat_id, user.get("userId"))
        system_prompt = build_system_prompt(self.pipeline.settings)
        user_message = command.text_for_parsing or command.text
        content = await self.pipeline._get_groq().chat_completion(system_prompt, user_message)
        parsed = extract_json(content)

        tx = normalize_ai_response(parsed, command.text, chat_id, self.pipeline.settings, source)
        tx = normalize_types(tx)
        tx["chatId"] = chat_id
        tx["sourceMessageId"] = str(message_id or "")

        intent = str(tx.get("intent", "add_tx")).lower()
        if intent == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)
        if intent == "list":
            return await self.pipeline.command_flow.handle_list(user, chat_id)
        if intent == "summary":
            return await self.pipeline.command_flow.handle_summary(user, chat_id)

        if intent != "add_tx":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)

        if float(tx.get("amount", 0)) <= 0 or not str(tx.get("category")):
            logger.warning("AI invalid tx chat_id=%s user_id=%s", chat_id, user.get("userId"))
            keyboard = _kb([ACTION_HELP])
            return self.pipeline._make_message(INVALID_TX_MESSAGE, keyboard)

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
        tx["source"] = tx.get("source") or source
        tx["sourceMessageId"] = tx.get("sourceMessageId") or ""
        tx["createdAt"] = tx.get("createdAt") or __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
        tx["updatedAt"] = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat()
        tx["isDeleted"] = tx.get("isDeleted", False)
        tx["deletedAt"] = tx.get("deletedAt", "")

        self.pipeline._get_sheets().append_transaction(tx)
        logger.info("AI tx saved chat_id=%s user_id=%s tx_id=%s", chat_id, user.get("userId"), tx_id)
        keyboard = _kb([ACTION_UNDO, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP])
        return self.pipeline._make_message(format_add_tx_message(tx), keyboard)


class BotPipeline(PipelineBase):
    def __init__(self, settings: Settings, sheets: Optional[ResilientSheetsRepo] = None, groq: Optional[GroqClient] = None) -> None:
        super().__init__(settings, sheets, groq)
        self.auth_flow = AuthFlow(self)
        self.onboarding_flow = OnboardingFlow(self)
        self.command_flow = CommandFlow(self)
        self.ai_flow = AiFlow(self)

    async def handle_message(self, request: BotInput) -> list[BotMessage]:
        chat_id = request.chat_id
        telegram_user_id = request.user_id
        text = request.text
        logger.info(
            "Incoming message route=pending chat_id=%s user_id=%s has_text=%s has_voice=%s",
            chat_id,
            telegram_user_id,
            bool(text),
            bool(request.audio_bytes),
        )

        if request.audio_bytes:
            audio_text = await self._transcribe_audio(request.audio_bytes)
            text = audio_text or text

        non_text_type = request.non_text_type
        if not text:
            non_text_type = non_text_type or "non_text"

        command = parse_command(text, chat_id, telegram_user_id, non_text_type)

        logger.info(
            "Parsed command route=%s chat_id=%s user_id=%s",
            command.route,
            chat_id,
            telegram_user_id,
        )
        if command.route == "onboarding":
            return [await self.onboarding_flow.handle(command)]

        if command.route == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
            return [self._make_message(HELP_MESSAGE, keyboard)]

        if command.route == "non_text":
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(NON_TEXT_MESSAGE, keyboard)]

        auth_result = self.auth_flow.require_active_user(telegram_user_id)
        if not auth_result.user:
            logger.warning(
                "Unauthorized user chat_id=%s user_id=%s",
                chat_id,
                telegram_user_id,
            )
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(auth_result.error_message or UNAUTHORIZED_MESSAGE, keyboard)]

        self._get_sheets().update_user_last_seen(str(telegram_user_id))

        if command.route == "list":
            return [await self.command_flow.handle_list(auth_result.user, chat_id)]
        if command.route == "summary":
            return [await self.command_flow.handle_summary(auth_result.user, chat_id)]
        if command.route == "undo":
            return [await self.command_flow.handle_undo(auth_result.user, chat_id)]

        response = await self.ai_flow.handle(
            command,
            auth_result.user,
            chat_id,
            request.message_id,
            request.channel,
        )
        return [response]

    async def handle_callback(self, request: BotInput) -> list[BotMessage]:
        chat_id = request.chat_id
        telegram_user_id = request.user_id
        text = request.text
        logger.info(
            "Incoming callback chat_id=%s user_id=%s has_data=%s",
            chat_id,
            telegram_user_id,
            bool(text),
        )
        command = parse_command(text, chat_id, telegram_user_id, None)

        if command.route == "onboarding":
            return [await self.onboarding_flow.handle(command)]

        if command.route == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
            return [self._make_message(HELP_MESSAGE, keyboard)]

        if command.route in {"list", "summary", "undo", "ai"}:
            auth_result = self.auth_flow.require_active_user(telegram_user_id)
            if not auth_result.user:
                logger.warning(
                    "Unauthorized callback chat_id=%s user_id=%s",
                    chat_id,
                    telegram_user_id,
                )
                keyboard = _kb([ACTION_HELP])
                return [self._make_message(auth_result.error_message or UNAUTHORIZED_MESSAGE, keyboard)]
            self._get_sheets().update_user_last_seen(str(telegram_user_id))
            if command.route == "list":
                return [await self.command_flow.handle_list(auth_result.user, chat_id)]
            elif command.route == "summary":
                return [await self.command_flow.handle_summary(auth_result.user, chat_id)]
            elif command.route == "undo":
                return [await self.command_flow.handle_undo(auth_result.user, chat_id)]
            else:
                response = await self.ai_flow.handle(
                    command,
                    auth_result.user,
                    chat_id,
                    request.message_id,
                    request.channel,
                )
                return [response]
        return []

    async def _transcribe_audio(self, audio_bytes: bytes) -> Optional[str]:
        try:
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
