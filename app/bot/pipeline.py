from __future__ import annotations

from dataclasses import dataclass
import re
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
    format_multi_tx_preview_message,
    format_multi_tx_saved_message,
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
    split_multi_transaction_text,
)
from app.bot.ui_models import BotAction, BotInput, BotKeyboard, BotMessage
from app.bot.recurring_flow import (
    PENDING_RECURRING_ACTION,
    PENDING_RECURRING_OFFER_ACTION,
    build_setup_question,
    build_setup_summary,
    compute_next_due,
    get_today,
    handle_setup_step,
    is_affirmative,
    is_negative,
    parse_amount,
    parse_recurrence,
    parse_remind_offsets,
    parse_service_name,
)
from app.core.config import Settings
from app.core.logging import logger
from app.services.groq import GroqClient, extract_json
from app.services.repositories import DataRepo

INVALID_TOKEN_MESSAGE = "🔒 <b>Token inválido</b>\nEl token de invitación no es válido o ya expiró."
INVALID_TX_MESSAGE = "⚠️ <b>No pude guardar el movimiento</b>\nMonto inválido o categoría faltante. Inténtalo de nuevo."
RECURRING_NOT_FOUND_MESSAGE = "⚠️ <b>No encontrado</b>\nNo encontré un recurrente con ese ID."
RECURRING_INVALID_ID_MESSAGE = "⚠️ <b>ID inválido</b>\nEl ID debe ser un número."
RECURRING_INVALID_ACTION_MESSAGE = "⚠️ <b>Acción inválida</b>"
PENDING_MULTI_TX_CONFIRM = "multi_tx_confirm"
PENDING_CLEAR_ALL_CONFIRM = "clear_all_confirm"

ACTION_LIST = BotAction("/list", "🧾 Movimientos")
ACTION_SUMMARY = BotAction("/summary", "📊 Resumen")
ACTION_UNDO = BotAction("/undo", "↩️ Deshacer")
ACTION_HELP = BotAction("/help", "ℹ️ Ayuda")
ACTION_DOWNLOAD = BotAction("/download", "⬇️ Descargar")
ACTION_RECURRINGS = BotAction("/recurrings", "🔁 Recurrentes")


def _kb(*rows: list[BotAction]) -> BotKeyboard:
    return BotKeyboard(rows=[row for row in rows if row])


def _kb_main() -> BotKeyboard:
    return _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_RECURRINGS, ACTION_DOWNLOAD], [ACTION_HELP])


def _kb_after_save() -> BotKeyboard:
    return _kb([ACTION_UNDO, ACTION_LIST], [ACTION_SUMMARY, ACTION_RECURRINGS], [ACTION_HELP])


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
        keyboard = _kb_main()
        return self.pipeline._make_message(ONBOARDING_SUCCESS_MESSAGE, keyboard)


class CommandFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    async def handle_list(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("List command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_UNDO, ACTION_SUMMARY], [ACTION_RECURRINGS, ACTION_DOWNLOAD], [ACTION_HELP])
        return self.pipeline._make_message(format_list_message(txs), keyboard)

    async def handle_summary(self, user: Dict[str, Any], chat_id: Optional[int], channel: str) -> BotMessage:
        logger.info("Summary command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        keyboard = _kb([ACTION_LIST, ACTION_UNDO], [ACTION_RECURRINGS, ACTION_DOWNLOAD], [ACTION_HELP])
        compact = channel in {"evolution", "whatsapp"}
        return self.pipeline._make_message(format_summary_message(txs, compact=compact), keyboard)

    async def handle_recurrings(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Recurrings command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        items = self.pipeline._get_repo().list_recurring_expenses(user.get("userId"))
        keyboard = _kb_main()
        return self.pipeline._make_message(format_recurring_list_message(items), keyboard)

    async def handle_download(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Download command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        txs = [tx for tx in txs if not tx.get("isDeleted")]
        if not txs:
            keyboard = _kb_main()
            return self.pipeline._make_message("📭 <b>Sin movimientos</b>\nNo hay transacciones para descargar.", keyboard)

        document_bytes, filename = build_transactions_xlsx(txs, self.pipeline.settings.timezone or "America/Bogota")
        text = f"📎 <b>Exportación lista</b>\nTransacciones: <b>{len(txs)}</b>"
        keyboard = _kb_main()
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
        keyboard = _kb_main()
        return self.pipeline._make_message(format_undo_message(picked), keyboard)

    async def handle_clear_all(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Clear-all command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        txs = self.pipeline._get_repo().list_transactions(user.get("userId"))
        active_count = len([tx for tx in txs if not bool(tx.get("isDeleted"))])
        if active_count == 0:
            return self.pipeline._make_message("📭 <b>Sin movimientos</b>\nNo hay transacciones para eliminar.", _kb_main())
        self.pipeline._get_repo().upsert_pending_action(
            str(user.get("userId")),
            PENDING_CLEAR_ALL_CONFIRM,
            {"active_count": active_count},
        )
        return self.pipeline._make_message(
            (
                f"⚠️ <b>Vas a eliminar {active_count} transacciones</b>\n"
                "Esta acción no se puede deshacer con <code>/undo</code>.\n\n"
                "Responde <code>sí</code> para confirmar o <code>no</code> para cancelar."
            ),
            _kb_main(),
        )


class AiFlow:
    def __init__(self, pipeline: PipelineBase) -> None:
        self.pipeline = pipeline

    @staticmethod
    def _infer_default_type(text: str) -> str:
        raw = (text or "").lower()
        if re.search(r"\b(me pagaron|recibi|recibí|ingreso|gan[eé]|salario|reembolso)\b", raw):
            return "income"
        return "expense"

    def _finalize_tx(
        self,
        tx: Dict[str, Any],
        user: Dict[str, Any],
        chat_id: Optional[int],
        message_id: Optional[str],
        source: str,
    ) -> Dict[str, Any]:
        tx_id = generate_tx_id()
        out = dict(tx)
        out["txId"] = tx_id
        out["userId"] = user.get("userId")
        out["paymentMethod"] = out.get("paymentMethod") or "cash"
        out["normalizedMerchant"] = out.get("normalizedMerchant") or ""
        out["transactionKind"] = out.get("transactionKind") or "regular"
        out["isRecurring"] = bool(out.get("isRecurring"))
        out["recurrence"] = out.get("recurrence") or ""
        out["recurrenceId"] = out.get("recurrenceId") or ""
        out["counterparty"] = out.get("counterparty") or ""
        out["loanRole"] = out.get("loanRole") or ""
        out["loanId"] = out.get("loanId") or ""
        out["parseConfidence"] = out.get("parseConfidence") or 0.7
        out["parserVersion"] = out.get("parserVersion") or "mvp-v1"
        out["source"] = out.get("source") or source
        out["sourceMessageId"] = str(message_id or "")
        out["createdAt"] = out.get("createdAt") or __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        out["updatedAt"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        out["isDeleted"] = out.get("isDeleted", False)
        out["deletedAt"] = out.get("deletedAt", "")
        out["chatId"] = chat_id
        return out

    def _build_multi_preview(self, txs: list[Dict[str, Any]]) -> str:
        return format_multi_tx_preview_message(txs)

    async def _handle_multi_segments(
        self,
        system_prompt: str,
        segments: list[str],
        command,
        user: Dict[str, Any],
        chat_id: Optional[int],
        message_id: Optional[str],
        source: str,
    ) -> BotMessage:
        default_type = self._infer_default_type(command.text or "")
        candidates: list[Dict[str, Any]] = []
        low_confidence = False

        for segment in segments:
            content = await self.pipeline._get_groq().chat_completion(system_prompt, segment)
            try:
                parsed = extract_json(content)
            except Exception as exc:
                logger.warning("AI multi response invalid JSON chat_id=%s user_id=%s error=%s", chat_id, user.get("userId"), exc)
                keyboard = _kb([ACTION_HELP])
                return self.pipeline._make_message(HELP_MESSAGE, keyboard)
            parsed = sanitize_ai_payload(parsed)
            tx = normalize_ai_response(parsed, segment, chat_id, self.pipeline.settings, source)
            tx = normalize_types(tx)
            tx["chatId"] = chat_id
            tx["sourceMessageId"] = str(message_id or "")
            tx["intent"] = "add_tx"
            if str(tx.get("type") or "").lower() not in {"income", "expense"}:
                tx["type"] = default_type
            if float(tx.get("amount", 0)) <= 0:
                keyboard = _kb_main()
                return self.pipeline._make_message(
                    "No pude validar todos los montos en el mensaje. Envíalo separado o con formato más claro.",
                    keyboard,
                )
            confidence = float(tx.get("parseConfidence") or 0)
            if confidence < 0.55:
                low_confidence = True
            candidates.append(tx)

        if not candidates:
            keyboard = _kb([ACTION_HELP])
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)

        if low_confidence:
            self.pipeline._get_repo().upsert_pending_action(
                str(user.get("userId")),
                PENDING_MULTI_TX_CONFIRM,
                {"txs": candidates, "source_message_id": str(message_id or ""), "source": source},
            )
            return self.pipeline._make_message(
                self._build_multi_preview(candidates),
                _kb_after_save(),
            )

        finalized = [self._finalize_tx(tx, user, chat_id, message_id, source) for tx in candidates]
        self.pipeline._get_repo().append_transactions(finalized)
        logger.info("AI multi tx saved chat_id=%s user_id=%s count=%s", chat_id, user.get("userId"), len(finalized))
        return self.pipeline._make_message(
            format_multi_tx_saved_message(finalized),
            _kb_after_save(),
        )

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
        segments = split_multi_transaction_text(user_message)
        if len(segments) >= 2:
            return await self._handle_multi_segments(system_prompt, segments, command, user, chat_id, message_id, source)
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
            keyboard = _kb_main()
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)
        if intent == "list":
            return await self.pipeline.command_flow.handle_list(user, chat_id)
        if intent == "summary":
            return await self.pipeline.command_flow.handle_summary(user, chat_id, source)
        if intent == "download":
            return await self.pipeline.command_flow.handle_download(user, chat_id)

        if intent != "add_tx":
            keyboard = _kb_main()
            return self.pipeline._make_message(HELP_MESSAGE, keyboard)

        if float(tx.get("amount", 0)) <= 0 or not str(tx.get("category")):
            logger.warning("AI invalid tx chat_id=%s user_id=%s", chat_id, user.get("userId"))
            keyboard = _kb_main()
            return self.pipeline._make_message(INVALID_TX_MESSAGE, keyboard)

        tx = self._finalize_tx(tx, user, chat_id, message_id, source)
        self.pipeline._get_repo().append_transaction(tx)
        logger.info("AI tx saved chat_id=%s user_id=%s tx_id=%s", chat_id, user.get("userId"), tx["txId"])
        keyboard = _kb_after_save()
        text = format_add_tx_message(tx)
        recurring_prompt = self.pipeline._offer_recurring_setup(tx)
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
            keyboard = _kb_main()
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

        pending_offer = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), PENDING_RECURRING_OFFER_ACTION)
        if pending_offer and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_recurring_offer(auth_result.user, command.text, pending_offer)]

        pending_edit = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), "recurring_edit_reminders")
        if pending_edit and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_recurring_edit(auth_result.user, command.text, pending_edit)]

        pending_multi = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), PENDING_MULTI_TX_CONFIRM)
        if pending_multi and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_multi_tx_confirm(auth_result.user, command.text, pending_multi, chat_id, request.message_id, request.channel)]

        pending_clear_all = self._get_repo().get_pending_action(str(auth_result.user.get("userId")), PENDING_CLEAR_ALL_CONFIRM)
        if pending_clear_all and not (command.command and command.command.startswith("/")) and command.route == "ai":
            return [self._handle_clear_all_confirm(auth_result.user, command.text, pending_clear_all)]

        if command.route == "list":
            return [await self.command_flow.handle_list(auth_result.user, chat_id)]
        if command.route == "summary":
            return [await self.command_flow.handle_summary(auth_result.user, chat_id, request.channel)]
        if command.route == "recurrings":
            return [await self.command_flow.handle_recurrings(auth_result.user, chat_id)]
        if command.route == "recurring_create":
            return [self._start_recurring_from_text(auth_result.user, command.text)]
        if command.route == "download":
            return [await self.command_flow.handle_download(auth_result.user, chat_id)]
        if command.route == "undo":
            return [await self.command_flow.handle_undo(auth_result.user, chat_id)]
        if command.route == "clear_all":
            return [await self.command_flow.handle_clear_all(auth_result.user, chat_id)]
        if command.route == "recurring_edit":
            return [self._handle_recurring_edit(auth_result.user, command.text)]
        if command.route == "recurring_update_amount":
            return [self._handle_recurring_update_amount(auth_result.user, command.text)]
        if command.route == "recurring_cancel":
            return [self._handle_recurring_cancel(auth_result.user, command.text)]
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
            keyboard = _kb_main()
            return [self._make_message(HELP_MESSAGE, keyboard)]

        if command.route in {"list", "summary", "download", "undo", "clear_all", "ai", "recurring_action", "recurrings"}:
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
            elif command.route == "clear_all":
                return [await self.command_flow.handle_clear_all(auth_result.user, chat_id)]
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

    def _offer_recurring_setup(self, tx: Dict[str, Any]) -> str:
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
        state = {
            "tx": {
                "txId": tx.get("txId"),
                "date": tx.get("date"),
                "recurrenceId": tx.get("recurrenceId"),
                "normalizedMerchant": tx.get("normalizedMerchant"),
                "description": tx.get("description"),
                "category": tx.get("category"),
                "amount": tx.get("amount"),
                "currency": tx.get("currency"),
                "recurrence": tx.get("recurrence"),
            }
        }
        self._get_repo().upsert_pending_action(user_id, PENDING_RECURRING_OFFER_ACTION, state)
        return (
            "Detecté que este pago parece recurrente.\n"
            "¿Quieres crear recordatorio de pago para esta suscripción?\n\n"
            "Responde <code>sí</code> o <code>no</code>."
        )

    def _handle_multi_tx_confirm(
        self,
        user: Dict[str, Any],
        text: str,
        pending: Dict[str, Any],
        chat_id: Optional[int],
        message_id: Optional[str],
        source: str,
    ) -> BotMessage:
        answer = (text or "").strip()
        if is_negative(answer):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. No guardé esos movimientos.", _kb_main())
        if not is_affirmative(answer):
            return self._make_message(
                "Responde <code>sí</code> para guardar o <code>no</code> para cancelar.",
                _kb_main(),
            )

        state = pending.get("state") or {}
        if isinstance(state, str):
            try:
                state = __import__("json").loads(state)
            except Exception:
                state = {}
        txs = state.get("txs") or []
        if not isinstance(txs, list) or not txs:
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("⚠️ No encontré movimientos pendientes para confirmar.", _kb_main())

        finalized = [self.ai_flow._finalize_tx(dict(tx), user, chat_id, message_id, source) for tx in txs if isinstance(tx, dict)]
        if not finalized:
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("⚠️ No encontré movimientos válidos para confirmar.", _kb_main())
        self._get_repo().append_transactions(finalized)
        self._get_repo().delete_pending_action(int(pending["id"]))
        return self._make_message(
            format_multi_tx_saved_message(finalized),
            _kb_after_save(),
        )

    def _handle_clear_all_confirm(self, user: Dict[str, Any], text: str, pending: Dict[str, Any]) -> BotMessage:
        answer = (text or "").strip()
        if is_negative(answer):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. No eliminé ninguna transacción.", _kb_main())
        if not is_affirmative(answer):
            return self._make_message(
                "Responde <code>sí</code> para eliminar todo o <code>no</code> para cancelar.",
                _kb_main(),
            )

        deleted_count = self._get_repo().mark_all_transactions_deleted(str(user.get("userId")))
        self._get_repo().delete_pending_action(int(pending["id"]))
        if deleted_count <= 0:
            return self._make_message("📭 <b>Sin movimientos</b>\nNo había transacciones activas para eliminar.", _kb_main())
        return self._make_message(
            f"🗑️ <b>Listo</b>\nEliminé <b>{deleted_count}</b> transacciones.",
            _kb_main(),
        )

    def _create_recurring_from_tx(self, user_id: str, tx: Dict[str, Any]) -> Dict[str, Any]:
        recurrence_id = str(tx.get("recurrenceId") or "")
        existing = self._get_repo().find_recurring_by_recurrence_id(user_id, recurrence_id)
        tx_date = self._parse_iso_date(str(tx.get("date") or "")) or get_today(self.settings)
        if existing:
            if not existing.get("anchor_date"):
                self._get_repo().update_recurring_expense(
                    int(existing.get("id")),
                    {"anchor_date": tx_date.isoformat(), "billing_month": tx_date.month},
                )
            return existing

        return self._get_repo().create_recurring_expense(
            {
                "user_id": user_id,
                "service_name": tx.get("normalizedMerchant") or tx.get("description") or "Pago recurrente",
                "recurrence_id": recurrence_id,
                "normalized_merchant": tx.get("normalizedMerchant"),
                "description": tx.get("description"),
                "category": tx.get("category") or "misc",
                "amount": tx.get("amount") or 0,
                "currency": "COP",
                "recurrence": tx.get("recurrence") or "monthly",
                "billing_month": tx_date.month,
                "anchor_date": tx_date.isoformat(),
                "timezone": self.settings.timezone or "America/Bogota",
                "remind_offsets": [3, 1, 0],
                "status": "pending",
                "source_tx_id": tx.get("txId"),
            }
        )

    def _handle_recurring_offer(self, user: Dict[str, Any], text: str, pending: Dict[str, Any]) -> BotMessage:
        answer = (text or "").strip()
        if is_negative(answer):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. No crearé recordatorio para ese gasto.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))
        if not is_affirmative(answer):
            return self._make_message(
                "Responde <code>sí</code> para crear el recordatorio o <code>no</code> para omitir.",
                _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]),
            )

        state = pending.get("state") or {}
        if isinstance(state, str):
            try:
                state = __import__("json").loads(state)
            except Exception:
                state = {}
        tx = state.get("tx") or {}
        recurring = self._create_recurring_from_tx(str(user.get("userId")), tx)
        pending_state = {
            "recurring_id": recurring["id"],
            "step": "ask_billing_day",
            "recurrence": recurring.get("recurrence") or "monthly",
        }
        self._get_repo().delete_pending_action(int(pending["id"]))
        self._get_repo().upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, pending_state)
        return self._make_message(
            build_setup_question("ask_billing_day", pending_state["recurrence"]),
            _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]),
        )

    def _start_recurring_from_text(self, user: Dict[str, Any], text: str) -> BotMessage:
        content = text or ""
        recurrence = parse_recurrence(content)
        service_name = parse_service_name(content) or "Pago recurrente"
        billing_day = None
        if recurrence in {"monthly", "quarterly", "yearly"}:
            billing_day = self._parse_billing_day_from_text(content)
        recurrence_id = f"REC:{service_name.upper().replace(' ', '_')[:40]}"
        amount = parse_amount(content) or 0
        today = get_today(self.settings)
        existing = self._get_repo().find_recurring_by_recurrence_id(str(user.get("userId")), recurrence_id)
        if existing:
            recurring = existing
            self._get_repo().update_recurring_expense(
                int(existing["id"]),
                {
                    "service_name": service_name,
                    "recurrence": recurrence,
                    "billing_day": billing_day,
                    "amount": amount,
                    "status": "pending",
                },
            )
        else:
            recurring = self._get_repo().create_recurring_expense(
                {
                    "user_id": str(user.get("userId")),
                    "service_name": service_name,
                    "recurrence_id": recurrence_id,
                    "normalized_merchant": service_name,
                    "description": service_name,
                    "category": "utilities",
                    "amount": amount,
                    "currency": "COP",
                    "recurrence": recurrence,
                    "billing_day": billing_day,
                    "billing_month": today.month,
                    "anchor_date": today.isoformat(),
                    "timezone": self.settings.timezone or "America/Bogota",
                    "remind_offsets": [3, 1, 0],
                    "status": "pending",
                    "source_tx_id": None,
                }
            )
        step = "ask_reminders" if billing_day else "ask_billing_day"
        pending_state = {
            "recurring_id": recurring["id"],
            "step": step,
            "recurrence": recurrence,
        }
        self._get_repo().upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, pending_state)
        intro = f"✅ Perfecto. Voy a configurar el recordatorio para <b>{service_name}</b> ({recurrence})."
        return self._make_message(
            f"{intro}\n\n{build_setup_question(step, recurrence)}",
            _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]),
        )

    def _parse_billing_day_from_text(self, text: str) -> Optional[int]:
        import re

        match = re.search(r"(?:todos?\\s+los|cada)\\s+(\\d{1,2})\\b", (text or "").lower())
        if not match:
            return None
        try:
            value = int(match.group(1))
            if 1 <= value <= 31:
                return value
        except ValueError:
            return None
        return None

    def _handle_recurring_setup(self, user: Dict[str, Any], text: str) -> BotMessage:
        pending = self._get_repo().get_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION)
        if not pending:
            return self._make_message(HELP_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
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
            return self._make_message(f"{result.response}\n\n{follow}", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

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
                return self._make_message(build_setup_summary(recurring, self.settings), _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))
            return self._make_message("✅ Recurrente activado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

        next_step = result.next_step or step
        state["step"] = next_step
        self._get_repo().upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, state)
        return self._make_message(build_setup_question(next_step, recurrence), _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

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
                return self._make_message("ℹ️ Uso: <code>recordatorios ID 3,1,0</code>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
            try:
                recurring_id = int(parts[1])
            except ValueError:
                return self._make_message(RECURRING_INVALID_ID_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
            offsets_text = " ".join(parts[2:]).strip()

        offsets = parse_remind_offsets(offsets_text)
        if not offsets:
            if not pending:
                self._get_repo().upsert_pending_action(
                    str(user.get("userId")),
                    "recurring_edit_reminders",
                    {"recurring_id": recurring_id},
                )
            return self._make_message("ℹ️ Envía los recordatorios. Ej: <code>3,1,0</code>", _kb([ACTION_RECURRINGS, ACTION_HELP]))

        recurring = self._get_repo().get_recurring_expense(int(recurring_id))
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        self._get_repo().update_recurring_expense(int(recurring_id), {"remind_offsets": offsets})
        if pending:
            self._get_repo().delete_pending_action(int(pending["id"]))
        return self._make_message("✅ Recordatorios actualizados.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

    def _handle_recurring_toggle(self, user: Dict[str, Any], text: str) -> BotMessage:
        content = (text or "").strip().lower()
        parts = content.split()
        if len(parts) < 2:
            return self._make_message("ℹ️ Uso: <code>pausar ID</code> o <code>activar ID</code>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
        action = parts[0]
        try:
            recurring_id = int(parts[1])
        except ValueError:
            return self._make_message(RECURRING_INVALID_ID_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        recurring = self._get_repo().get_recurring_expense(recurring_id)
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))

        if action == "pausar":
            self._get_repo().update_recurring_expense(recurring_id, {"status": "paused"})
            return self._make_message("⏸ Recurrente pausado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

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
            return self._make_message("▶️ Recurrente activado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

        return self._make_message(RECURRING_INVALID_ACTION_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))

    def _handle_recurring_update_amount(self, user: Dict[str, Any], text: str) -> BotMessage:
        parts = (text or "").strip().split()
        if len(parts) < 3:
            return self._make_message("ℹ️ Uso: <code>monto ID 45000</code>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
        try:
            recurring_id = int(parts[1])
        except ValueError:
            return self._make_message(RECURRING_INVALID_ID_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        amount = parse_amount(" ".join(parts[2:]))
        if amount is None or amount < 0:
            return self._make_message("⚠️ <b>Monto inválido</b>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
        recurring = self._get_repo().get_recurring_expense(recurring_id)
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        self._get_repo().update_recurring_expense(recurring_id, {"amount": amount})
        return self._make_message("✅ Monto actualizado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

    def _handle_recurring_cancel(self, user: Dict[str, Any], text: str) -> BotMessage:
        parts = (text or "").strip().split()
        if len(parts) < 2:
            return self._make_message("ℹ️ Uso: <code>cancelar ID</code>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
        try:
            recurring_id = int(parts[1])
        except ValueError:
            return self._make_message(RECURRING_INVALID_ID_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        recurring = self._get_repo().get_recurring_expense(recurring_id)
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        self._get_repo().update_recurring_expense(recurring_id, {"status": "canceled", "canceled_at": now})
        return self._make_message("🛑 Recurrente cancelado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

    def _handle_recurring_action(self, user: Dict[str, Any], data: str) -> BotMessage:
        parts = (data or "").split(":")
        if len(parts) != 3:
            return self._make_message(RECURRING_INVALID_ACTION_MESSAGE)
        action = parts[1]
        try:
            bill_instance_id = int(parts[2])
        except ValueError:
            return self._make_message(RECURRING_INVALID_ACTION_MESSAGE)
        bill = self._get_repo().get_bill_instance(bill_instance_id)
        if not bill or str(bill.get("user_id")) != str(user.get("userId")):
            return self._make_message("🔒 <b>Acción no autorizada</b>")

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        if action == "paid":
            if str(bill.get("status")) == "paid":
                return self._make_message("ℹ️ Este pago ya estaba confirmado.")
            tx_id = generate_tx_id()
            due_date = bill.get("due_date")
            date_value = due_date.isoformat() if hasattr(due_date, "isoformat") else str(due_date)
            amount = bill.get("amount")
            if amount is None:
                amount = bill.get("recurring_amount") or 0
            tx = {
                "txId": tx_id,
                "userId": user.get("userId"),
                "type": "expense",
                "transactionKind": "regular",
                "amount": amount or 0,
                "currency": bill.get("currency") or "COP",
                "category": bill.get("category") or "misc",
                "description": bill.get("description") or bill.get("service_name") or "Pago recurrente",
                "date": date_value,
                "normalizedMerchant": bill.get("normalized_merchant") or bill.get("service_name") or "",
                "paymentMethod": "cash",
                "counterparty": "",
                "loanRole": "",
                "loanId": "",
                "isRecurring": True,
                "recurrence": bill.get("recurrence") or "",
                "recurrenceId": bill.get("recurrence_id") or "",
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
            if bool(bill.get("auto_add_transaction", True)):
                self._get_repo().append_transaction(tx)
            self._get_repo().update_bill_instance(
                bill_instance_id,
                {"status": "paid", "paid_at": now, "tx_id": tx_id, "follow_up_on": None},
            )
            recurring = self._get_repo().get_recurring_expense(int(bill.get("recurring_id")))
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

        if action == "later":
            follow_up = get_today(self.settings) + __import__("datetime").timedelta(days=1)
            self._get_repo().update_bill_instance(
                bill_instance_id,
                {"status": "pending", "follow_up_on": follow_up.isoformat()},
            )
            return self._make_message("⏳ Perfecto, te recordaré de nuevo mañana.")

        if action == "no":
            return self._make_message("❌ Entendido. Lo dejaré pendiente.")

        return self._make_message(RECURRING_INVALID_ACTION_MESSAGE)

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
