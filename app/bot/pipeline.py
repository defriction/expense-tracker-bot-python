from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Dict, Optional

from app.bot.formatters import (
    HELP_MESSAGE,
    LONG_MESSAGE,
    NON_TEXT_MESSAGE,
    ONBOARDING_SUCCESS_MESSAGE,
    UNAUTHORIZED_MESSAGE,
    format_add_tx_message,
    format_list_message,
    format_recurring_list_message,
    format_summary_message,
    format_undo_message,
)
from app.bot.exporters import build_transactions_xlsx
from app.bot.parser import (
    build_system_prompt,
    generate_tx_id,
    normalize_ai_response,
    normalize_types,
    parse_command,
    sanitize_ai_payload,
)
from app.bot.ui_models import BotAction, BotInput, BotKeyboard, BotMessage
from app.bot.recurring_flow import (
    PENDING_RECURRING_ACTION,
    build_setup_question,
    build_setup_summary,
    compute_next_due,
    get_today,
    handle_setup_step,
    parse_remind_offsets,
)
from app.core.config import Settings
from app.core.logging import logger
from app.services.groq import GroqClient, extract_json
from app.services.repositories import DataRepo

INVALID_TOKEN_MESSAGE = "Token de invitación inválido o expirado."
INVALID_TX_MESSAGE = "Monto inválido o categoría faltante. Por favor intenta de nuevo."

ACTION_LIST = BotAction("/list", "🧾 Movimientos")
ACTION_SUMMARY = BotAction("/summary", "📊 Resumen")
ACTION_UNDO = BotAction("/undo", "↩️ Deshacer")
ACTION_HELP = BotAction("/help", "ℹ️ Ayuda")
ACTION_DOWNLOAD = BotAction("/download", "⬇️ Descargar")


def _kb(*rows: list[BotAction]) -> BotKeyboard:
    return BotKeyboard(rows=[row for row in rows if row])


class PipelineBase:
    def __init__(self, settings: Settings, repo: Optional[DataRepo] = None, groq: Optional[GroqClient] = None) -> None:
        self.settings = settings
        self._repo = repo
        self._groq = groq

    def _get_repo(self) -> DataRepo:
        if self._repo is None:
            raise RuntimeError("Data repository not configured")
        return self._repo

    def _get_groq(self) -> GroqClient:
        if self._groq is None:
            raise RuntimeError("Groq client not configured")
        return self._groq

    def _make_message(
        self,
        text: str,
        keyboard: Optional[BotKeyboard] = None,
        *,
        document_bytes: Optional[bytes] = None,
        document_name: Optional[str] = None,
        document_mime: Optional[str] = None,
    ) -> BotMessage:
        return BotMessage(
            text=text,
            keyboard=keyboard,
            disable_web_preview=True,
            document_bytes=document_bytes,
            document_name=document_name,
            document_mime=document_mime,
        )


@dataclass
class ActiveUserResult:
    user: Optional[Dict[str, Any]]
    error_message: Optional[str]


class AuthFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    def require_active_user(self, channel: str, external_user_id: Optional[str]) -> ActiveUserResult:
        if not external_user_id:
            return ActiveUserResult(None, UNAUTHORIZED_MESSAGE)
        user = self.pipeline._get_repo().find_user_by_channel(channel, str(external_user_id))
        if not user or str(user.get("status")) != "active":
            return ActiveUserResult(None, UNAUTHORIZED_MESSAGE)
        return ActiveUserResult(user, None)


class OnboardingFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle(self, command) -> BotMessage:
        chat_id = command.chat_id
        external_user_id = command.user_id
        logger.info("Onboarding start chat_id=%s user_id=%s", chat_id, external_user_id)
        if not external_user_id:
            return self.pipeline._make_message(INVALID_TOKEN_MESSAGE)

        repo = self.pipeline._get_repo()
        invite = repo.find_invite(command.invite_token)
        if not invite or str(invite.get("status")) != "unused":
            logger.warning("Onboarding invalid token chat_id=%s user_id=%s", chat_id, external_user_id)
            return self.pipeline._make_message(INVALID_TOKEN_MESSAGE)

        user_id = f"USR-{int(time.time() * 1000)}-{external_user_id}"
        repo.create_user(user_id, command.channel, str(external_user_id), str(chat_id) if chat_id is not None else None)
        repo.mark_invite_used(command.invite_token, user_id)
        logger.info("Onboarding success chat_id=%s user_id=%s", chat_id, external_user_id)
        keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(ONBOARDING_SUCCESS_MESSAGE, keyboard)


class CommandFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle_list(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("List command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_UNDO, ACTION_SUMMARY], [ACTION_DOWNLOAD, ACTION_HELP])
        return self.pipeline._make_message(format_list_message(txs), keyboard)

    async def handle_summary(self, user: Dict[str, Any], chat_id: Optional[int], channel: str) -> BotMessage:
        logger.info("Summary command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_LIST, ACTION_UNDO], [ACTION_DOWNLOAD, ACTION_HELP])
        compact = channel in {"evolution", "whatsapp"}
        return self.pipeline._make_message(format_summary_message(txs, compact=compact), keyboard)

    async def handle_recurrings(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Recurrings command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        items = self.pipeline._get_repo().list_recurring_expenses(user.get("userId"))
        keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(format_recurring_list_message(items), keyboard)

    async def handle_download(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Download command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        txs = [tx for tx in txs if not tx.get("isDeleted")]
        if not txs:
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
            return self.pipeline._make_message("📭 <b>Sin movimientos</b>\nNo hay transacciones para descargar.", keyboard)

        document_bytes, filename = build_transactions_xlsx(txs, self.pipeline.settings.timezone or "America/Bogota")
        text = f"📎 <b>Exportación lista</b>\nTransacciones: <b>{len(txs)}</b>"
        keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP])
        return self.pipeline._make_message(
            text,
            keyboard,
            document_bytes=document_bytes,
            document_name=filename,
            document_mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    async def handle_undo(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Undo command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        picked = BotPipeline._pick_latest(txs)
        if picked.get("ok"):
            self.pipeline._get_repo().mark_transaction_deleted(str(picked["txId"]))
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
        user_message = (command.text_for_parsing or command.text or "").strip()
        content = await self.pipeline._get_groq().chat_completion(system_prompt, user_message)
        try:
            parsed = extract_json(content)
        except Exception as exc:
            logger.warning("AI response invalid JSON chat_id=%s user_id=%s error=%s", chat_id, user.get("userId"), exc)
            keyboard = _kb([ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)
        parsed = sanitize_ai_payload(parsed)

        tx = normalize_ai_response(parsed, command.text, chat_id, self.pipeline.settings, source)
        tx = normalize_types(tx)
        tx["chatId"] = chat_id
        tx["sourceMessageId"] = str(message_id or "")

        intent = str(tx.get("intent", "add_tx")).lower()
        if intent == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_DOWNLOAD, ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)
        if intent == "list":
            return await self.pipeline.command_flow.handle_list(user, chat_id)
        if intent == "summary":
            return await self.pipeline.command_flow.handle_summary(user, chat_id, source)
        if intent == "download":
            return await self.pipeline.command_flow.handle_download(user, chat_id)

        if intent != "add_tx":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_DOWNLOAD, ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)

        if float(tx.get("amount", 0)) <= 0 or not str(tx.get("category")):
            logger.warning("AI invalid tx chat_id=%s user_id=%s", chat_id, user.get("userId"))
            keyboard = _kb([ACTION_DOWNLOAD, ACTION_HELP])
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

        self.pipeline._get_repo().append_transaction(tx)
        logger.info("AI tx saved chat_id=%s user_id=%s tx_id=%s", chat_id, user.get("userId"), tx_id)
        keyboard = _kb([ACTION_UNDO, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP])
        text = format_add_tx_message(tx)
        recurring_prompt = self.pipeline._start_recurring_setup(tx)
        if recurring_prompt:
            text = f"{text}\n\n{recurring_prompt}"
        return self.pipeline._make_message(text, keyboard)


class BotPipeline(PipelineBase):
    def __init__(self, settings: Settings, repo: Optional[DataRepo] = None, groq: Optional[GroqClient] = None) -> None:
        super().__init__(settings, repo, groq)
        self.auth_flow = AuthFlow(self)
        self.onboarding_flow = OnboardingFlow(self)
        self.command_flow = CommandFlow(self)
        self.ai_flow = AiFlow(self)

    async def handle_message(self, request: BotInput) -> list[BotMessage]:
        chat_id = request.chat_id
        external_user_id = request.user_id
        text = request.text
        settings = self.settings
        logger.info(
            "Incoming message route=pending chat_id=%s user_id=%s has_text=%s has_voice=%s",
            chat_id,
            external_user_id,
            bool(text),
            bool(request.audio_bytes),
        )

        if request.audio_bytes:
            audio_text = await self._transcribe_audio(request.audio_bytes)
            text = audio_text or text

        non_text_type = request.non_text_type
        if not text:
            non_text_type = non_text_type or "non_text"

        command = parse_command(text, chat_id, external_user_id, non_text_type, request.channel)

        logger.info(
            "Parsed command route=%s chat_id=%s user_id=%s",
            command.route,
            chat_id,
            external_user_id,
        )
        if command.route == "onboarding":
            return [await self.onboarding_flow.handle(command)]

        if command.route == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_DOWNLOAD, ACTION_HELP])
            return [self._make_message(HELP_MESSAGE, keyboard)]

        if command.route == "non_text":
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(NON_TEXT_MESSAGE, keyboard)]

        auth_result = self.auth_flow.require_active_user(
            request.channel,
            str(external_user_id) if external_user_id is not None else None,
        )
        if not auth_result.user:
            logger.warning(
                "Unauthorized user chat_id=%s user_id=%s",
                chat_id,
                external_user_id,
            )
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(auth_result.error_message or UNAUTHORIZED_MESSAGE, keyboard)]

        if external_user_id is not None:
            self._get_repo().update_user_last_seen(request.channel, str(external_user_id))

        pending_setup = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), PENDING_RECURRING_ACTION)
        if pending_setup and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_recurring_setup(auth_result.user, command.text)]

        pending_edit = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), "recurring_edit_reminders")
        if pending_edit and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_recurring_edit(auth_result.user, command.text, pending_edit)]

        if command.route == "list":
            return [await self.command_flow.handle_list(auth_result.user, chat_id)]
        if command.route == "summary":
            return [await self.command_flow.handle_summary(auth_result.user, chat_id, request.channel)]
        if command.route == "recurrings":
            return [await self.command_flow.handle_recurrings(auth_result.user, chat_id)]
        if command.route == "download":
            return [await self.command_flow.handle_download(auth_result.user, chat_id)]
        if command.route == "undo":
            return [await self.command_flow.handle_undo(auth_result.user, chat_id)]
        if command.route == "recurring_edit":
            return [self._handle_recurring_edit(auth_result.user, command.text)]
        if command.route == "recurring_toggle":
            return [self._handle_recurring_toggle(auth_result.user, command.text)]

        if len(command.text_for_parsing or "") > settings.max_input_chars:
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(LONG_MESSAGE, keyboard)]
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
        external_user_id = request.user_id
        text = request.text
        settings = self.settings
        logger.info(
            "Incoming callback chat_id=%s user_id=%s has_data=%s",
            chat_id,
            external_user_id,
            bool(text),
        )
        command = parse_command(text, chat_id, external_user_id, None, request.channel)

        if command.route == "onboarding":
            return [await self.onboarding_flow.handle(command)]

        if command.route == "help":
            keyboard = _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_DOWNLOAD, ACTION_HELP])
            return [self._make_message(HELP_MESSAGE, keyboard)]

        if command.route in {"list", "summary", "download", "undo", "ai", "recurring_action", "recurrings"}:
            auth_result = self.auth_flow.require_active_user(
                request.channel,
                str(external_user_id) if external_user_id is not None else None,
            )
            if not auth_result.user:
                logger.warning(
                    "Unauthorized callback chat_id=%s user_id=%s",
                    chat_id,
                    external_user_id,
                )
                keyboard = _kb([ACTION_HELP])
                return [self._make_message(auth_result.error_message or UNAUTHORIZED_MESSAGE, keyboard)]
            if external_user_id is not None:
                self._get_repo().update_user_last_seen(request.channel, str(external_user_id))
            if command.route == "list":
                return [await self.command_flow.handle_list(auth_result.user, chat_id)]
            elif command.route == "summary":
                return [await self.command_flow.handle_summary(auth_result.user, chat_id, request.channel)]
            elif command.route == "recurrings":
                return [await self.command_flow.handle_recurrings(auth_result.user, chat_id)]
            elif command.route == "download":
                return [await self.command_flow.handle_download(auth_result.user, chat_id)]
            elif command.route == "undo":
                return [await self.command_flow.handle_undo(auth_result.user, chat_id)]
            elif command.route == "recurring_action":
                return [self._handle_recurring_action(auth_result.user, command.text)]
            else:
                if len(command.text_for_parsing or "") > settings.max_input_chars:
                    keyboard = _kb([ACTION_HELP])
                    return [self._make_message(LONG_MESSAGE, keyboard)]
                response = await self.ai_flow.handle(
                    command,
                    auth_result.user,
                    chat_id,
                    request.message_id,
                    request.channel,
                )
                return [response]
        return []

    def _parse_iso_date(self, value: str):
        if not value:
            return None
        try:
            return __import__("datetime").date.fromisoformat(value)
        except ValueError:
            return None

    def _start_recurring_setup(self, tx: Dict[str, Any]) -> str:
        if not tx.get("isRecurring"):
            return ""
        recurrence_id = str(tx.get("recurrenceId") or "")
        if not recurrence_id:
            return ""
        user_id = str(tx.get("userId") or "")
        if not user_id:
            return ""
        existing = self._get_repo().find_recurring_by_recurrence_id(user_id, recurrence_id)
        if existing and str(existing.get("status")) == "active":
            return ""
        if existing:
            if not existing.get("anchor_date"):
                tx_date = self._parse_iso_date(str(tx.get("date") or "")) or get_today(self.settings)
                self._get_repo().update_recurring_expense(
                    int(existing.get("id")),
                    {"anchor_date": tx_date.isoformat(), "billing_month": tx_date.month},
                )
            pending_state = {
                "recurring_id": existing["id"],
                "step": "ask_billing_day",
                "recurrence": existing.get("recurrence") or "monthly",
            }
        else:
            tx_date = self._parse_iso_date(str(tx.get("date") or "")) or get_today(self.settings)
            recurring = self._get_repo().create_recurring_expense(
                {
                    "user_id": user_id,
                    "recurrence_id": recurrence_id,
                    "normalized_merchant": tx.get("normalizedMerchant"),
                    "description": tx.get("description"),
                    "category": tx.get("category") or "misc",
                    "amount": tx.get("amount") or 0,
                    "currency": tx.get("currency") or "COP",
                    "recurrence": tx.get("recurrence") or "monthly",
                    "billing_month": tx_date.month,
                    "anchor_date": tx_date.isoformat(),
                    "timezone": self.settings.timezone or "America/Bogota",
                    "remind_offsets": [3, 1, 0],
                    "status": "pending",
                    "source_tx_id": tx.get("txId"),
                }
            )
            pending_state = {
                "recurring_id": recurring["id"],
                "step": "ask_billing_day",
                "recurrence": recurring.get("recurrence") or "monthly",
            }
        self._get_repo().upsert_pending_action(user_id, PENDING_RECURRING_ACTION, pending_state)
        return build_setup_question("ask_billing_day", pending_state["recurrence"])

    def _handle_recurring_setup(self, user: Dict[str, Any], text: str) -> BotMessage:
        pending = self._get_repo().get_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION)
        if not pending:
            return self._make_message(HELP_MESSAGE, _kb([ACTION_HELP]))
        state = pending.get("state") or {}
        if isinstance(state, str):
            try:
                state = __import__("json").loads(state)
            except Exception:
                state = {}
        step = state.get("step") or "ask_billing_day"
        recurrence = state.get("recurrence") or "monthly"
        recurring_id = int(state.get("recurring_id") or 0)
        result = handle_setup_step(step, text or "", recurrence)
        if result.response:
            follow = build_setup_question(step, recurrence)
            return self._make_message(f"{result.response}\n\n{follow}")

        updates = result.updates or {}
        if updates:
            self._get_repo().update_recurring_expense(recurring_id, updates)

        if result.done:
            recurring = self._get_repo().get_recurring_expense(recurring_id)
            if recurring:
                today = get_today(self.settings)
                next_due = compute_next_due(
                    str(recurring.get("recurrence") or "monthly"),
                    today,
                    recurring.get("billing_day"),
                    recurring.get("billing_weekday"),
                    recurring.get("billing_month"),
                    self._parse_iso_date(str(recurring.get("anchor_date") or "")),
                )
                self._get_repo().update_recurring_expense(
                    recurring_id,
                    {"status": "active", "next_due": next_due},
                )
            self._get_repo().delete_pending_action(int(pending["id"]))
            if recurring:
                return self._make_message(build_setup_summary(recurring, self.settings))
            return self._make_message("✅ Recurrente activado.")

        next_step = result.next_step or step
        state["step"] = next_step
        self._get_repo().upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, state)
        return self._make_message(build_setup_question(next_step, recurrence))

    def _handle_recurring_edit(self, user: Dict[str, Any], text: str, pending: Optional[Dict[str, Any]] = None) -> BotMessage:
        if pending is None:
            pending = self._get_repo().get_pending_action(str(user.get("userId")), "recurring_edit_reminders")

        content = (text or "").strip()
        parts = content.split()
        recurring_id = None
        offsets_text = ""

        if pending:
            state = pending.get("state") or {}
            if isinstance(state, str):
                try:
                    state = __import__("json").loads(state)
                except Exception:
                    state = {}
            recurring_id = state.get("recurring_id")
            offsets_text = content
        else:
            if len(parts) < 2:
                return self._make_message("Uso: <code>recordatorios ID 3,1,0</code>")
            try:
                recurring_id = int(parts[1])
            except ValueError:
                return self._make_message("ID inválido.")
            offsets_text = " ".join(parts[2:]).strip()

        offsets = parse_remind_offsets(offsets_text)
        if not offsets:
            if not pending:
                self._get_repo().upsert_pending_action(
                    str(user.get("userId")),
                    "recurring_edit_reminders",
                    {"recurring_id": recurring_id},
                )
            return self._make_message("Envía los recordatorios. Ej: <code>3,1,0</code>")

        self._get_repo().update_recurring_expense(int(recurring_id), {"remind_offsets": offsets})
        if pending:
            self._get_repo().delete_pending_action(int(pending["id"]))
        return self._make_message("✅ Recordatorios actualizados.")

    def _handle_recurring_toggle(self, user: Dict[str, Any], text: str) -> BotMessage:
        content = (text or "").strip().lower()
        parts = content.split()
        if len(parts) < 2:
            return self._make_message("Uso: <code>pausar ID</code> o <code>activar ID</code>")
        action = parts[0]
        try:
            recurring_id = int(parts[1])
        except ValueError:
            return self._make_message("ID inválido.")
        recurring = self._get_repo().get_recurring_expense(recurring_id)
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message("No encontrado.")

        if action == "pausar":
            self._get_repo().update_recurring_expense(recurring_id, {"status": "paused"})
            return self._make_message("⏸ Recurrente pausado.")

        if action == "activar":
            today = get_today(self.settings)
            next_due = compute_next_due(
                str(recurring.get("recurrence") or "monthly"),
                today,
                recurring.get("billing_day"),
                recurring.get("billing_weekday"),
                recurring.get("billing_month"),
                self._parse_iso_date(str(recurring.get("anchor_date") or "")),
            )
            self._get_repo().update_recurring_expense(recurring_id, {"status": "active", "next_due": next_due})
            return self._make_message("▶️ Recurrente activado.")

        return self._make_message("Acción inválida.")

    def _handle_recurring_action(self, user: Dict[str, Any], data: str) -> BotMessage:
        parts = (data or "").split(":")
        if len(parts) != 3:
            return self._make_message("Acción inválida.")
        action = parts[1]
        try:
            event_id = int(parts[2])
        except ValueError:
            return self._make_message("Acción inválida.")
        event = self._get_repo().get_recurring_event(event_id)
        if not event or str(event.get("user_id")) != str(user.get("userId")):
            return self._make_message("Acción no autorizada.")

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        if action == "paid":
            tx_id = generate_tx_id()
            due_date = event.get("due_date")
            date_value = due_date.isoformat() if hasattr(due_date, "isoformat") else str(due_date)
            tx = {
                "txId": tx_id,
                "userId": user.get("userId"),
                "type": "expense",
                "transactionKind": "regular",
                "amount": event.get("amount") or 0,
                "currency": event.get("currency") or "COP",
                "category": event.get("category") or "misc",
                "description": event.get("description") or "Pago recurrente",
                "date": date_value,
                "normalizedMerchant": event.get("normalized_merchant") or "",
                "paymentMethod": "cash",
                "counterparty": "",
                "loanRole": "",
                "loanId": "",
                "isRecurring": True,
                "recurrence": event.get("recurrence") or "",
                "recurrenceId": event.get("recurrence_id") or "",
                "parseConfidence": 0.9,
                "parserVersion": "recurring-v1",
                "source": "recurring",
                "sourceMessageId": "",
                "rawText": "recurring:auto",
                "createdAt": now,
                "updatedAt": now,
                "isDeleted": False,
                "deletedAt": "",
                "chatId": user.get("chatId"),
            }
            self._get_repo().append_transaction(tx)
            self._get_repo().update_recurring_event(
                event_id,
                {"status": "paid", "paid_at": now, "tx_id": tx_id},
            )
            recurring = self._get_repo().get_recurring_expense(int(event.get("recurring_id")))
            if recurring:
                due = self._parse_iso_date(date_value) or get_today(self.settings)
                next_due = compute_next_due(
                    str(recurring.get("recurrence") or "monthly"),
                    due + __import__("datetime").timedelta(days=1),
                    recurring.get("billing_day"),
                    recurring.get("billing_weekday"),
                    recurring.get("billing_month"),
                    self._parse_iso_date(str(recurring.get("anchor_date") or "")),
                )
                self._get_repo().update_recurring_expense(
                    int(recurring.get("id")),
                    {"next_due": next_due, "last_confirmed_at": now},
                )
            return self._make_message("✅ Pago confirmado y registrado.")

        if action == "skip":
            self._get_repo().update_recurring_event(event_id, {"status": "skipped", "paid_at": now})
            return self._make_message("⏳ Pago marcado como no realizado.")

        return self._make_message("Acción inválida.")

    async def _transcribe_audio(self, audio_bytes: bytes) -> Optional[str]:
        try:
            response = await self._get_groq().transcribe(bytes(audio_bytes))
        except Exception as exc:
            logger.warning("Voice transcription failed: %s", exc)
            return None
        return response.get("text") if isinstance(response, dict) else None

    @staticmethod
    def _pick_latest(transactions: list[Dict[str, Any]]) -> Dict[str, Any]:
        valid = [tx for tx in transactions if tx.get("txId") and not tx.get("isDeleted")]
        if not valid:
            return {"ok": False, "reason": "no_tx"}

        def created_ts(item: Dict[str, Any]) -> float:
            created_at = str(item.get("createdAt") or "")
            try:
                return __import__("datetime").datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
            except ValueError:
                try:
                    return float(str(item.get("txId") or "0").replace("TX-", ""))
                except ValueError:
                    return float("-inf")

        valid.sort(key=created_ts, reverse=True)
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
