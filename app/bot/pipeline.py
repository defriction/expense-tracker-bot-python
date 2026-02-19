from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import time
import unicodedata
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
    parse_amount_in_context,
    parse_recurrence,
    parse_reminder_hour,
    parse_remind_offsets,
    parse_service_name,
    parse_weekday,
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
PENDING_CLEAR_RECURRINGS_CONFIRM = "clear_recurrings_confirm"
PENDING_DAILY_NUDGE_SET_HOUR = "daily_nudge_set_hour"
PENDING_RECURRING_CANCEL_CONFIRM = "recurring_cancel_confirm"
DAILY_NUDGE_PREFS_ACTION = "daily_nudge_prefs"
PENDING_ACTION_TTL_MINUTES = 20
PENDING_EXPIRED_MESSAGE = (
    "⌛ <b>Esta confirmación expiró</b>\n"
    "Repite la acción para continuar."
)
AI_UNAVAILABLE_FALLBACK_MESSAGE = (
    "🤖 <b>IA no disponible en este momento</b>\n"
    "Usa formato rápido: <code>concepto monto</code>\n"
    "Ejemplo: <code>almuerzo 15000</code>"
)

ACTION_LIST = BotAction("/list", "🧾 Movimientos")
ACTION_SUMMARY = BotAction("/summary", "📊 Resumen")
ACTION_UNDO = BotAction("/undo", "↩️ Deshacer")
ACTION_HELP = BotAction("/help", "ℹ️ Ayuda")
ACTION_DOWNLOAD = BotAction("/download", "⬇️ Descargar")
ACTION_RECURRINGS = BotAction("/recurrings", "🔁 Recurrentes")
ACTION_CONFIRM_YES = BotAction("confirm:yes", "✅ Sí")
ACTION_CONFIRM_NO = BotAction("confirm:no", "❌ No")


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
        existing_user = repo.find_user_by_channel(command.channel, str(external_user_id))
        if existing_user and str(existing_user.get("status")) == "active":
            logger.info("Onboarding idempotent success chat_id=%s user_id=%s", chat_id, external_user_id)
            keyboard = _kb_main()
            return self.pipeline._make_message(
                "✅ <b>Tu cuenta ya estaba activa</b>\nPuedes seguir usando el bot normalmente.",
                keyboard,
            )
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
        items = [item for item in items if str(item.get("status") or "").lower() == "active"]
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
        self.pipeline._upsert_pending_action(
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
            _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
        )

    async def handle_clear_recurrings(self, user: Dict[str, Any], chat_id: Optional[int]) -> BotMessage:
        logger.info("Clear-recurrings command chat_id=%s user_id=%s", chat_id, user.get("userId"))
        items = self.pipeline._get_repo().list_recurring_expenses(user.get("userId"))
        clearable = [item for item in items if str(item.get("status") or "").lower() != "canceled"]
        clearable_count = len(clearable)
        if clearable_count == 0:
            return self.pipeline._make_message("📭 <b>Sin recurrentes</b>\nNo hay recurrentes activos/pausados para eliminar.", _kb_main())
        self.pipeline._upsert_pending_action(
            str(user.get("userId")),
            PENDING_CLEAR_RECURRINGS_CONFIRM,
            {"clearable_count": clearable_count},
        )
        return self.pipeline._make_message(
            (
                f"⚠️ <b>Vas a cancelar {clearable_count} recurrentes</b>\n"
                "Esta acción detiene sus recordatorios futuros.\n\n"
                "Responde <code>sí</code> para confirmar o <code>no</code> para cancelar."
            ),
            _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
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
            self.pipeline._upsert_pending_action(
                str(user.get("userId")),
                PENDING_MULTI_TX_CONFIRM,
                {"txs": candidates, "source_message_id": str(message_id or ""), "source": source},
            )
            return self.pipeline._make_message(
                self._build_multi_preview(candidates),
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
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
            keyboard = _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_UNDO, ACTION_LIST], [ACTION_HELP])
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

        pending_response = self._handle_pending_actions(
            auth_result.user,
            command,
            chat_id,
            request.message_id,
            request.channel,
        )
        if pending_response is not None:
            return [pending_response]

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
        if command.route == "clear_recurrings":
            return [await self.command_flow.handle_clear_recurrings(auth_result.user, chat_id)]
        if command.route == "recurring_edit":
            return [self._handle_recurring_edit(auth_result.user, command.text)]
        if command.route == "recurring_update_amount":
            return [self._handle_recurring_update_amount(auth_result.user, command.text)]
        if command.route == "recurring_cancel":
            return [self._handle_recurring_cancel(auth_result.user, command.text)]
        if command.route == "recurring_toggle":
            return [self._handle_recurring_toggle(auth_result.user, command.text)]
        if command.route == "daily_nudge_action":
            return [self._handle_daily_nudge_action(auth_result.user, command.text)]
        if command.route == "ai":
            natural_ai = await self._try_handle_recurring_natural_ai(auth_result.user, command.text or "")
            if natural_ai is not None:
                return [natural_ai]
            natural = self._try_handle_recurring_natural(auth_result.user, command.text or "")
            if natural is not None:
                return [natural]

        if len(command.text_for_parsing or "") > settings.max_input_chars:
            keyboard = _kb([ACTION_HELP])
            return [self._make_message(LONG_MESSAGE, keyboard)]
        if not settings.groq_api_key:
            return [self._make_message(AI_UNAVAILABLE_FALLBACK_MESSAGE, _kb_main())]
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

        if command.route in {"list", "summary", "download", "undo", "clear_all", "clear_recurrings", "ai", "recurring_action", "recurrings", "daily_nudge_action"}:
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
            elif command.route == "clear_recurrings":
                return [await self.command_flow.handle_clear_recurrings(auth_result.user, chat_id)]
            elif command.route == "recurring_action":
                return [self._handle_recurring_action(auth_result.user, command.text)]
            elif command.route == "daily_nudge_action":
                return [self._handle_daily_nudge_action(auth_result.user, command.text)]
            else:
                if command.route == "ai":
                    pending_response = self._handle_pending_actions(
                        auth_result.user,
                        command,
                        chat_id,
                        request.message_id,
                        request.channel,
                    )
                    if pending_response is not None:
                        return [pending_response]
                    natural_ai = await self._try_handle_recurring_natural_ai(auth_result.user, command.text or "")
                    if natural_ai is not None:
                        return [natural_ai]
                    natural = self._try_handle_recurring_natural(auth_result.user, command.text or "")
                    if natural is not None:
                        return [natural]
                if len(command.text_for_parsing or "") > settings.max_input_chars:
                    keyboard = _kb([ACTION_HELP])
                    return [self._make_message(LONG_MESSAGE, keyboard)]
                if not settings.groq_api_key:
                    return [self._make_message(AI_UNAVAILABLE_FALLBACK_MESSAGE, _kb_main())]
                response = await self.ai_flow.handle(
                    command,
                    auth_result.user,
                    chat_id,
                    request.message_id,
                    request.channel,
                )
                return [response]
        return []

    @staticmethod
    def _pending_allowed(command) -> bool:
        return command.route == "ai" and not (command.command and command.command.startswith("/"))

    def _upsert_pending_action(
        self,
        user_id: str,
        action_type: str,
        state: Dict[str, Any],
        ttl_minutes: int = PENDING_ACTION_TTL_MINUTES,
    ) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=max(1, ttl_minutes))
        self._get_repo().upsert_pending_action(user_id, action_type, state, expires_at=expires_at.isoformat())

    @staticmethod
    def _parse_pending_expires_at(pending: Dict[str, Any]) -> Optional[datetime]:
        value = pending.get("expires_at")
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    def _get_pending_action_state(self, user_id: str, action_type: str) -> tuple[Optional[Dict[str, Any]], bool]:
        pending = self._get_repo().get_pending_action(user_id, action_type)
        if not pending:
            return None, False
        expires_at = self._parse_pending_expires_at(pending)
        if expires_at and expires_at <= datetime.now(timezone.utc):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return None, True
        return pending, False

    def _handle_pending_actions(
        self,
        user: Dict[str, Any],
        command,
        chat_id: Optional[int],
        message_id: Optional[str],
        channel: str,
    ) -> Optional[BotMessage]:
        if not self._pending_allowed(command):
            return None
        user_id = str(user.get("userId"))
        checks = [
            (PENDING_RECURRING_ACTION, lambda p: self._handle_recurring_setup(user, command.text)),
            (PENDING_RECURRING_OFFER_ACTION, lambda p: self._handle_recurring_offer(user, command.text, p)),
            ("recurring_edit_reminders", lambda p: self._handle_recurring_edit(user, command.text, p)),
            (
                PENDING_MULTI_TX_CONFIRM,
                lambda p: self._handle_multi_tx_confirm(user, command.text, p, chat_id, message_id, channel),
            ),
            (PENDING_CLEAR_ALL_CONFIRM, lambda p: self._handle_clear_all_confirm(user, command.text, p)),
            (
                PENDING_CLEAR_RECURRINGS_CONFIRM,
                lambda p: self._handle_clear_recurrings_confirm(user, command.text, p),
            ),
            (
                PENDING_RECURRING_CANCEL_CONFIRM,
                lambda p: self._handle_recurring_cancel_confirm(user, command.text, p),
            ),
            (
                PENDING_DAILY_NUDGE_SET_HOUR,
                lambda p: self._handle_daily_nudge_set_hour(user, command.text, p),
            ),
        ]
        for action_type, handler in checks:
            pending, expired = self._get_pending_action_state(user_id, action_type)
            if expired:
                return self._make_message(PENDING_EXPIRED_MESSAGE, _kb_main())
            if pending:
                return handler(pending)
        return None

    def _parse_iso_date(self, value: str):
        if not value:
            return None
        try:
            return __import__("datetime").date.fromisoformat(value)
        except ValueError:
            return None

    def _get_daily_nudge_prefs(self, user_id: str) -> Dict[str, Any]:
        pending = self._get_repo().get_pending_action(user_id, DAILY_NUDGE_PREFS_ACTION)
        state: Dict[str, Any] = {}
        if pending:
            raw = pending.get("state") or {}
            if isinstance(raw, str):
                try:
                    raw = __import__("json").loads(raw)
                except Exception:
                    raw = {}
            if isinstance(raw, dict):
                state = raw
        enabled = bool(state.get("enabled", True))
        try:
            hour = int(state.get("hour", 21))
        except (TypeError, ValueError):
            hour = 21
        if hour < 0 or hour > 23:
            hour = 21
        return {"enabled": enabled, "hour": hour}

    def _save_daily_nudge_prefs(self, user_id: str, enabled: bool, hour: int) -> None:
        safe_hour = max(0, min(23, int(hour)))
        self._get_repo().upsert_pending_action(
            user_id,
            DAILY_NUDGE_PREFS_ACTION,
            {"enabled": bool(enabled), "hour": safe_hour},
            expires_at=None,
        )

    @staticmethod
    def _hour_label(hour: int) -> str:
        return f"{int(hour):02d}:00"

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
            recurring_id = existing.get("id")
            return (
                "ℹ️ Este gasto ya tiene un recordatorio recurrente activo.\n"
                f"ID: <code>{recurring_id}</code>\n\n"
                "Comandos disponibles:\n"
                f"• <code>recordatorios {recurring_id} 3 días antes y el mismo día</code>\n"
                f"• <code>monto {recurring_id} 45000</code>\n"
                f"• <code>pausar {recurring_id}</code> / <code>activar {recurring_id}</code> / <code>cancelar {recurring_id}</code>\n"
                "• <code>/recurrings</code> para ver todo"
            )
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
        self._upsert_pending_action(user_id, PENDING_RECURRING_OFFER_ACTION, state)
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
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
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
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
            )

        deleted_count = self._get_repo().mark_all_transactions_deleted(str(user.get("userId")))
        self._get_repo().delete_pending_action(int(pending["id"]))
        if deleted_count <= 0:
            return self._make_message("📭 <b>Sin movimientos</b>\nNo había transacciones activas para eliminar.", _kb_main())
        return self._make_message(
            f"🗑️ <b>Listo</b>\nEliminé <b>{deleted_count}</b> transacciones.",
            _kb_main(),
        )

    def _handle_clear_recurrings_confirm(self, user: Dict[str, Any], text: str, pending: Dict[str, Any]) -> BotMessage:
        answer = (text or "").strip()
        if is_negative(answer):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. No cancelé ningún recurrente.", _kb_main())
        if not is_affirmative(answer):
            return self._make_message(
                "Responde <code>sí</code> para cancelar todos los recurrentes o <code>no</code> para mantenerlos.",
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_HELP]),
            )

        items = self._get_repo().list_recurring_expenses(str(user.get("userId")))
        clearable = [item for item in items if str(item.get("status") or "").lower() != "canceled"]
        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        for item in clearable:
            self._get_repo().update_recurring_expense(int(item["id"]), {"status": "canceled", "canceled_at": now})

        self._get_repo().delete_pending_action(int(pending["id"]))
        if not clearable:
            return self._make_message("📭 <b>Sin recurrentes</b>\nNo había recurrentes para cancelar.", _kb_main())
        return self._make_message(
            f"🗑️ <b>Listo</b>\nCancelé <b>{len(clearable)}</b> recurrentes.",
            _kb_main(),
        )

    @staticmethod
    def _norm_match(text: str) -> str:
        raw = (text or "").strip().lower()
        raw = unicodedata.normalize("NFD", raw)
        raw = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
        raw = re.sub(r"\s+", " ", raw)
        return raw

    def _find_recurring_by_text(self, user_id: str, text: str) -> list[Dict[str, Any]]:
        items = self._get_repo().list_recurring_expenses(user_id)
        norm_text = self._norm_match(text)
        scored: list[tuple[int, Dict[str, Any]]] = []
        for item in items:
            name = str(item.get("service_name") or item.get("normalized_merchant") or item.get("description") or "").strip()
            if not name:
                continue
            norm_name = self._norm_match(name)
            if not norm_name:
                continue
            score = 0
            if norm_name in norm_text:
                score += len(norm_name) + 20
            tokens = [tok for tok in norm_name.split() if len(tok) >= 4]
            for tok in tokens:
                if tok in norm_text:
                    score += len(tok)
            if score > 0:
                scored.append((score, item))
        scored.sort(key=lambda x: x[0], reverse=True)
        if not scored:
            return []
        top = scored[0][0]
        return [item for score, item in scored if score == top]

    @staticmethod
    def _extract_explicit_id(text: str) -> Optional[int]:
        match = re.search(r"\bid\s*#?\s*(\d+)\b", text or "", flags=re.IGNORECASE)
        if not match:
            return None
        try:
            return int(match.group(1))
        except ValueError:
            return None

    @staticmethod
    def _looks_like_recurring_request(text: str) -> bool:
        t = (text or "").strip().lower()
        if not t:
            return False
        patterns = [
            r"\b(recordatorio|recordatorios|recurrente|recurrentes|suscripcion|suscripciones)\b",
            r"\b(cada\s+mes|todos?\s+los\s+meses|mensual|semanal|quincenal|trimestral|anual)\b",
            r"\bid\s*#?\s*\d+\b.*\b(cambiar|actualizar|monto|valor|hora|pausar|activar|cancelar)\b",
            r"\b(a\s+las\s+\d{1,2}(:\d{2})?\s*(am|pm)?)\b",
        ]
        return any(re.search(pattern, t) for pattern in patterns)

    def _parse_billing_day_natural(self, text: str) -> Optional[int]:
        t = (text or "").lower()
        candidates = [
            re.search(r"\b(\d{1,2})\s+de\s+cada\s+mes\b", t),
            re.search(r"\bel\s+(\d{1,2})\s+de\s+cada\s+mes\b", t),
            re.search(r"\btodos?\s+los\s+(\d{1,2})\b", t),
            re.search(r"\bel\s+(\d{1,2})\b", t),
        ]
        for match in candidates:
            if not match:
                continue
            try:
                day = int(match.group(1))
            except ValueError:
                continue
            if 1 <= day <= 31:
                return day
        return None

    async def _try_handle_recurring_natural_ai(self, user: Dict[str, Any], text: str) -> Optional[BotMessage]:
        raw = (text or "").strip()
        if not raw or not self.settings.groq_api_key:
            return None
        if not self._looks_like_recurring_request(raw):
            return None
        try:
            content = await self._get_groq().chat_completion(
                (
                    "Eres un parser experto de recordatorios recurrentes de pago en español.\n"
                    "Objetivo: extraer intención + campos estructurados desde texto natural.\n"
                    "Responde SOLO JSON válido (sin markdown, sin comentarios).\n"
                    "No inventes datos: si no aparece en el texto, usa null o [] según corresponda.\n"
                    "Si no es un caso de recurrentes, usa intent=none.\n\n"
                    "Schema exacto:\n"
                    "{\n"
                    '  "intent": "none|create|update|pause|activate|cancel|list",\n'
                    '  "target_id": number|null,\n'
                    '  "service_name": string|null,\n'
                    '  "amount": number|null,\n'
                    '  "recurrence": "weekly|biweekly|monthly|quarterly|yearly"|null,\n'
                    '  "billing_day": number|null,\n'
                    '  "reminder_hour": number|null,\n'
                    '  "remind_offsets": number[],\n'
                    '  "includes_same_day": boolean,\n'
                    '  "confidence": number\n'
                    "}\n\n"
                    "Reglas de extracción:\n"
                    "- reminder_hour: entero 0..23. Ej: '6 pm'->18, '2:30 pm'->14.\n"
                    "- billing_day: día del mes 1..31 cuando aparezca ('16 de cada mes', 'el 5').\n"
                    "- remind_offsets: días antes del cobro en orden descendente, sin repetidos.\n"
                    "- includes_same_day=true cuando aparezca 'mismo día', 'el día', 'día del cobro', 'hoy', '0 días'.\n"
                    "- Si includes_same_day=true, asegúrate de incluir 0 en remind_offsets.\n"
                    "- amount: número absoluto (ej: 56k->56000).\n"
                    "- target_id: solo si el texto menciona ID explícito.\n\n"
                    "Ejemplos:\n"
                    "Input: 'ID 2 actualizar a las 3 pm, recordatorio 3 dias y el mismo dia'\n"
                    'Output: {"intent":"update","target_id":2,"service_name":null,"amount":null,"recurrence":null,"billing_day":null,"reminder_hour":15,"remind_offsets":[3,0],"includes_same_day":true,"confidence":0.93}\n'
                    "Input: 'pago recurrente de 56k para luz a las 6 pm'\n"
                    'Output: {"intent":"create","target_id":null,"service_name":"luz","amount":56000,"recurrence":"monthly","billing_day":null,"reminder_hour":18,"remind_offsets":[],"includes_same_day":false,"confidence":0.89}\n'
                    "Input: 'almuerzo 20000'\n"
                    'Output: {"intent":"none","target_id":null,"service_name":null,"amount":null,"recurrence":null,"billing_day":null,"reminder_hour":null,"remind_offsets":[],"includes_same_day":false,"confidence":0.98}'
                ),
                raw,
            )
            parsed = extract_json(content)
        except Exception as exc:
            logger.warning("Recurring natural AI parse failed user_id=%s error=%s", user.get("userId"), exc)
            return None
        if not isinstance(parsed, dict):
            return None
        intent = str(parsed.get("intent") or "none").lower()
        if intent in {"none", ""}:
            return None
        confidence = float(parsed.get("confidence") or 0)
        if confidence < 0.55:
            return self._make_message(
                "⚠️ No tuve suficiente claridad para aplicar cambios automáticos. Indícame el ID y el cambio puntual, por ejemplo: <code>ID 2 cambiar hora a 18:30</code>.",
                _kb([ACTION_RECURRINGS, ACTION_HELP]),
            )
        if intent == "list":
            return await self.command_flow.handle_recurrings(user, None)

        target_id = parsed.get("target_id")
        service_name = str(parsed.get("service_name") or "").strip()
        recurring_id: Optional[int] = None
        if isinstance(target_id, (int, float)) and int(target_id) > 0:
            recurring_id = int(target_id)
        elif service_name:
            matches = self._find_recurring_by_text(str(user.get("userId")), service_name)
            if len(matches) == 1:
                recurring_id = int(matches[0]["id"])

        if intent in {"pause", "activate", "cancel"}:
            if recurring_id is None:
                recurring_id, err = self._resolve_recurring_target(user, raw, allow_numeric_fallback=True)
                if err:
                    return err
            if intent == "pause":
                return self._handle_recurring_toggle(user, f"pausar {recurring_id}")
            if intent == "activate":
                return self._handle_recurring_toggle(user, f"activar {recurring_id}")
            return self._handle_recurring_cancel(user, f"cancelar {recurring_id}")

        if intent == "create":
            return self._start_recurring_from_text(user, raw)

        if intent != "update":
            return None

        if recurring_id is None:
            recurring_id, err = self._resolve_recurring_target(user, raw)
            if err:
                return err
        recurring = self._get_repo().get_recurring_expense(int(recurring_id))
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))

        updates: Dict[str, Any] = {}
        amount = parsed.get("amount")
        if isinstance(amount, (int, float)) and float(amount) >= 0:
            updates["amount"] = round(float(amount), 2)
        recurrence = str(parsed.get("recurrence") or "").lower()
        if recurrence in {"weekly", "biweekly", "monthly", "quarterly", "yearly"}:
            updates["recurrence"] = recurrence
        billing_day = parsed.get("billing_day")
        if isinstance(billing_day, (int, float)) and 1 <= int(billing_day) <= 31:
            updates["billing_day"] = int(billing_day)
        elif updates.get("recurrence") in {"monthly", "quarterly", "yearly"}:
            inferred_day = self._parse_billing_day_natural(raw)
            if inferred_day is not None:
                updates["billing_day"] = inferred_day

        reminder_hour = parsed.get("reminder_hour")
        if isinstance(reminder_hour, (int, float)) and 0 <= int(reminder_hour) <= 23:
            updates["reminder_hour"] = int(reminder_hour)
        elif re.search(r"\ba las\b|\bhora\b", self._norm_match(raw)):
            inferred_hour = parse_reminder_hour(raw)
            if inferred_hour is not None:
                updates["reminder_hour"] = inferred_hour

        offsets = parsed.get("remind_offsets")
        includes_same_day = bool(parsed.get("includes_same_day"))
        inferred_offsets = parse_remind_offsets(raw)
        if isinstance(offsets, list):
            clean_offsets = []
            for val in offsets:
                try:
                    iv = abs(int(val))
                except (TypeError, ValueError):
                    continue
                if 0 <= iv <= 30 and iv not in clean_offsets:
                    clean_offsets.append(iv)
            for iv in inferred_offsets:
                if 0 <= iv <= 30 and iv not in clean_offsets:
                    clean_offsets.append(iv)
            if includes_same_day and 0 not in clean_offsets:
                clean_offsets.append(0)
            clean_offsets.sort(reverse=True)
            if clean_offsets:
                updates["remind_offsets"] = clean_offsets
        elif inferred_offsets:
            clean_offsets = list(inferred_offsets)
            if includes_same_day and 0 not in clean_offsets:
                clean_offsets.append(0)
                clean_offsets.sort(reverse=True)
            updates["remind_offsets"] = clean_offsets

        if not updates:
            return self._make_message(
                "⚠️ Me faltan datos para actualizar ese recurrente. Ejemplo: <code>ID 2 cambiar monto a 56000 y hora 18:30</code>",
                _kb([ACTION_RECURRINGS, ACTION_HELP]),
            )

        self._get_repo().update_recurring_expense(int(recurring_id), updates)
        refreshed = self._get_repo().get_recurring_expense(int(recurring_id))
        if refreshed and str(refreshed.get("status") or "").lower() == "active":
            today = get_today(self.settings)
            next_due = compute_next_due(
                str(refreshed.get("recurrence") or "monthly"),
                today,
                refreshed.get("billing_day"),
                refreshed.get("billing_weekday"),
                refreshed.get("billing_month"),
                self._parse_iso_date(str(refreshed.get("anchor_date") or "")),
            )
            self._get_repo().update_recurring_expense(int(recurring_id), {"next_due": next_due})
            refreshed = self._get_repo().get_recurring_expense(int(recurring_id))
        if refreshed:
            return self._make_message(build_setup_summary(refreshed, self.settings), _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))
        return self._make_message("✅ Recurrente actualizado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

    @staticmethod
    def _format_amount_for_command(amount: float) -> str:
        if float(amount).is_integer():
            return str(int(amount))
        return str(round(float(amount), 2))

    def _resolve_recurring_target(
        self,
        user: Dict[str, Any],
        text: str,
        *,
        allow_numeric_fallback: bool = False,
    ) -> tuple[Optional[int], Optional[BotMessage]]:
        explicit_id = self._extract_explicit_id(text)
        if explicit_id is not None:
            recurring = self._get_repo().get_recurring_expense(explicit_id)
            if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
                return None, self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
            return explicit_id, None

        if allow_numeric_fallback:
            numerics = re.findall(r"\b\d+\b", text or "")
            if len(numerics) == 1:
                try:
                    candidate = int(numerics[0])
                except ValueError:
                    candidate = 0
                if candidate > 0:
                    recurring = self._get_repo().get_recurring_expense(candidate)
                    if recurring and str(recurring.get("user_id")) == str(user.get("userId")):
                        return candidate, None

        matches = self._find_recurring_by_text(str(user.get("userId")), text or "")
        if len(matches) == 1:
            return int(matches[0]["id"]), None
        if len(matches) > 1:
            options = ", ".join([f"<code>{item.get('id')}</code>" for item in matches[:5]])
            return None, self._make_message(
                "⚠️ Encontré más de un recurrente que coincide.\n"
                f"IDs posibles: {options}\n"
                "Escríbelo con ID. Ej: <code>monto ID 45000</code>.",
                _kb([ACTION_RECURRINGS, ACTION_HELP]),
            )
        return None, self._make_message(
            "⚠️ No pude identificar cuál recurrente quieres editar.\n"
            "Primero usa <code>/recurrings</code> y luego envía el ID.",
            _kb([ACTION_RECURRINGS, ACTION_HELP]),
        )

    def _try_handle_recurring_natural(self, user: Dict[str, Any], text: str) -> Optional[BotMessage]:
        raw = (text or "").strip()
        if not raw:
            return None
        norm = self._norm_match(raw)
        user_id = str(user.get("userId"))
        has_explicit_id = self._extract_explicit_id(raw) is not None
        matched_targets = self._find_recurring_by_text(user_id, raw)
        has_target_match = bool(matched_targets)
        has_recurring_hint = bool(re.search(r"\b(recurrente|recurrentes|suscripcion|suscripciones|recordatorio|recordatorios)\b", norm))

        if re.search(r"\b(recordame|recuerdame|avisame)\b", norm) and re.search(
            r"\b(pagar|pago|factura|recibo|cobro|suscripcion)\b",
            norm,
        ):
            return self._start_recurring_from_text(user, raw)

        if re.search(r"\b(nuevo|crear|crea|agregar|agrega)\b.*\b(recordatorio|recurrente|suscripcion)\b", norm):
            return self._start_recurring_from_text(user, raw)

        if re.search(r"\b(recordatorios?|avisos?)\b", norm) and re.search(r"\d+\s*,\s*\d+", raw):
            offsets = parse_remind_offsets(raw)
            if offsets:
                if not (has_explicit_id or has_target_match or has_recurring_hint):
                    return None
                recurring_id, err = self._resolve_recurring_target(user, raw)
                if err:
                    return err
                offsets_text = ",".join([str(v) for v in offsets])
                return self._handle_recurring_edit(user, f"recordatorios {recurring_id} {offsets_text}")

        if parse_amount_in_context(raw) is not None and re.search(r"\b(monto|valor|sube|subir|baja|bajar|ajusta|ajustar|cambia|cambiar|actualiza|actualizar)\b", norm):
            if not (has_explicit_id or has_target_match or has_recurring_hint):
                return None
            recurring_id, err = self._resolve_recurring_target(user, raw)
            if err:
                return err
            amount = parse_amount_in_context(raw)
            if amount is None:
                return self._make_message("⚠️ <b>Monto inválido</b>", _kb([ACTION_RECURRINGS, ACTION_HELP]))
            return self._handle_recurring_update_amount(user, f"monto {recurring_id} {self._format_amount_for_command(amount)}")

        if re.search(r"\b(cancela|cancelar|elimina|eliminar)\b", norm):
            if not (has_explicit_id or has_target_match or has_recurring_hint):
                return None
            recurring_id, err = self._resolve_recurring_target(user, raw, allow_numeric_fallback=True)
            if err:
                return err
            return self._handle_recurring_cancel(user, f"cancelar {recurring_id}")

        if re.search(r"\b(pausa|pausar|deten|detener|frena|desactiva|desactivar)\b", norm):
            if not (has_explicit_id or has_target_match or has_recurring_hint):
                return None
            recurring_id, err = self._resolve_recurring_target(user, raw, allow_numeric_fallback=True)
            if err:
                return err
            return self._handle_recurring_toggle(user, f"pausar {recurring_id}")

        if re.search(r"\b(activa|activar|reanuda|reanudar)\b", norm):
            if not (has_explicit_id or has_target_match or has_recurring_hint):
                return None
            recurring_id, err = self._resolve_recurring_target(user, raw, allow_numeric_fallback=True)
            if err:
                return err
            return self._handle_recurring_toggle(user, f"activar {recurring_id}")

        return None

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
            if existing.get("reminder_hour") is None:
                self._get_repo().update_recurring_expense(int(existing.get("id")), {"reminder_hour": 9})
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
                "reminder_hour": 9,
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
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_RECURRINGS, ACTION_HELP]),
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
        self._upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, pending_state)
        return self._make_message(
            build_setup_question("ask_billing_day", pending_state["recurrence"]),
            _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]),
        )

    def _start_recurring_from_text(self, user: Dict[str, Any], text: str) -> BotMessage:
        content = text or ""
        recurrence = parse_recurrence(content)
        service_name = parse_service_name(content) or "Pago recurrente"
        billing_day = self._parse_billing_day_from_text(content) if recurrence in {"monthly", "quarterly", "yearly"} else None
        billing_weekday = parse_weekday(content) if recurrence in {"weekly", "biweekly"} else None
        reminder_hour = parse_reminder_hour(content)
        offsets = [3, 1, 0]
        if re.search(r"\b(recordatorio|recordatorios|av[ií]same|avisa|avisar|d[ií]as?\s+antes|mismo\s+d[ií]a|d[ií]a\s+del\s+cobro)\b", content.lower()):
            parsed_offsets = parse_remind_offsets(content)
            if parsed_offsets:
                offsets = parsed_offsets
        recurrence_id = f"REC:{service_name.upper().replace(' ', '_')[:40]}"
        parsed_amount = parse_amount_in_context(content)
        amount = parsed_amount if parsed_amount is not None else 0
        link_match = re.search(r"(https?://[^\s]+|www\.[^\s]+)", content, flags=re.IGNORECASE)
        payment_link = link_match.group(1)[:500] if link_match else ""
        payment_reference = ""
        ref_match = re.search(r"\b(?:ref(?:erencia)?|convenio|cuenta)\s*[:#-]?\s*([A-Za-z0-9\-_.]{4,64})\b", content, flags=re.IGNORECASE)
        if ref_match:
            payment_reference = ref_match.group(1)[:500]
        today = get_today(self.settings)
        existing = self._get_repo().find_recurring_by_recurrence_id(str(user.get("userId")), recurrence_id)
        if existing:
            recurring = existing
            self._get_repo().update_recurring_expense(
                int(existing["id"]),
                {
                    "service_name": service_name,
                    "recurrence": recurrence,
                    "billing_day": billing_day if billing_day is not None else existing.get("billing_day"),
                    "billing_weekday": billing_weekday if billing_weekday is not None else existing.get("billing_weekday"),
                    "amount": amount if parsed_amount is not None else (existing.get("amount") or 0),
                    "reminder_hour": reminder_hour if reminder_hour is not None else (existing.get("reminder_hour") or 9),
                    "remind_offsets": offsets,
                    "payment_link": payment_link or (existing.get("payment_link") or ""),
                    "payment_reference": payment_reference or (existing.get("payment_reference") or ""),
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
                    "billing_weekday": billing_weekday,
                    "billing_month": today.month,
                    "anchor_date": today.isoformat(),
                    "timezone": self.settings.timezone or "America/Bogota",
                    "remind_offsets": offsets,
                    "reminder_hour": reminder_hour if reminder_hour is not None else 9,
                    "payment_link": payment_link,
                    "payment_reference": payment_reference,
                    "status": "pending",
                    "source_tx_id": None,
                }
            )

        recurring = self._get_repo().get_recurring_expense(int(recurring["id"])) or recurring
        effective_billing_day = recurring.get("billing_day")
        effective_billing_weekday = recurring.get("billing_weekday")

        has_schedule = False
        if recurrence in {"weekly", "biweekly"}:
            has_schedule = effective_billing_weekday is not None
        else:
            has_schedule = effective_billing_day is not None

        if has_schedule:
            next_due = compute_next_due(
                recurrence,
                today,
                effective_billing_day,
                effective_billing_weekday,
                recurring.get("billing_month"),
                self._parse_iso_date(str(recurring.get("anchor_date") or "")),
            )
            self._get_repo().update_recurring_expense(
                int(recurring["id"]),
                {"status": "active", "next_due": next_due},
            )
            refreshed = self._get_repo().get_recurring_expense(int(recurring["id"]))
            if refreshed:
                return self._make_message(build_setup_summary(refreshed, self.settings), _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))
            return self._make_message("✅ Recurrente activado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))
        step = "ask_billing_day"
        pending_state = {
            "recurring_id": recurring["id"],
            "step": step,
            "recurrence": recurrence,
        }
        self._upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, pending_state)
        recurrence_label = {
            "weekly": "semanal",
            "biweekly": "quincenal",
            "monthly": "mensual",
            "quarterly": "trimestral",
            "yearly": "anual",
        }.get(str(recurrence), str(recurrence))
        intro = f"✅ Perfecto. Voy a configurar el recordatorio para <b>{service_name}</b> ({recurrence_label})."
        return self._make_message(
            f"{intro}\n\n{build_setup_question(step, recurrence)}",
            _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]),
        )

    def _parse_billing_day_from_text(self, text: str) -> Optional[int]:
        import re

        lower = (text or "").lower()
        matches = [
            re.search(r"(?:todos?\s+los|cada)\s+(\d{1,2})\b", lower),
            re.search(r"\b(\d{1,2})\s+de\s+cada\s+mes\b", lower),
            re.search(r"\bel\s+(\d{1,2})\s+de\s+cada\s+mes\b", lower),
        ]
        for match in matches:
            if not match:
                continue
            try:
                value = int(match.group(1))
            except ValueError:
                continue
            if 1 <= value <= 31:
                return value
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
        try:
            recurring_id = int(state.get("recurring_id") or 0)
        except (TypeError, ValueError):
            recurring_id = 0
        result = handle_setup_step(step, text or "", recurrence)
        if result.response:
            follow = build_setup_question(step, recurrence)
            keyboard = _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP])
            if step in {"ask_reminder_hour"}:
                keyboard = _kb([ACTION_CONFIRM_NO], [ACTION_RECURRINGS, ACTION_HELP])
            return self._make_message(f"{result.response}\n\n{follow}", keyboard)

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
        self._upsert_pending_action(str(user.get("userId")), PENDING_RECURRING_ACTION, state)
        keyboard = _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP])
        if next_step in {"ask_reminder_hour"}:
            keyboard = _kb([ACTION_CONFIRM_NO], [ACTION_RECURRINGS, ACTION_HELP])
        return self._make_message(build_setup_question(next_step, recurrence), keyboard)

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
                return self._make_message(
                    "ℹ️ Dime el ID y cuándo avisarte.\nEjemplo: <code>recordatorios 12 tres días antes y el mismo día</code>.",
                    _kb([ACTION_RECURRINGS, ACTION_HELP]),
                )
            try:
                recurring_id = int(parts[1])
            except ValueError:
                return self._make_message(RECURRING_INVALID_ID_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))
            offsets_text = " ".join(parts[2:]).strip()

        offsets = parse_remind_offsets(offsets_text)
        if not offsets:
            if not pending:
                self._upsert_pending_action(
                    str(user.get("userId")),
                    "recurring_edit_reminders",
                    {"recurring_id": recurring_id},
                )
            return self._make_message(
                "ℹ️ No te entendí cuándo avisar.\nPuedes escribir: <code>3 días antes y el mismo día</code>.",
                _kb([ACTION_RECURRINGS, ACTION_HELP]),
            )

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
        if action in {"pausa", "pause"}:
            action = "pausar"
        elif action in {"activa", "activate"}:
            action = "activar"
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
        service_name = str(recurring.get("service_name") or recurring.get("normalized_merchant") or recurring.get("description") or f"ID {recurring_id}")
        self._upsert_pending_action(
            str(user.get("userId")),
            PENDING_RECURRING_CANCEL_CONFIRM,
            {"recurring_id": recurring_id},
        )
        return self._make_message(
            "⚠️ Vas a cancelar este recurrente:\n"
            f"<b>{service_name}</b> (ID <code>{recurring_id}</code>)\n\n"
            "Responde <code>sí</code> para confirmar o <code>no</code> para mantenerlo activo.",
            _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_RECURRINGS, ACTION_HELP]),
        )

    def _handle_recurring_cancel_confirm(self, user: Dict[str, Any], text: str, pending: Dict[str, Any]) -> BotMessage:
        answer = (text or "").strip()
        if is_negative(answer):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. No cancelé ese recurrente.", _kb_main())
        if not is_affirmative(answer):
            return self._make_message(
                "Responde <code>sí</code> para cancelar o <code>no</code> para conservarlo.",
                _kb([ACTION_CONFIRM_YES, ACTION_CONFIRM_NO], [ACTION_RECURRINGS, ACTION_HELP]),
            )

        state = pending.get("state") or {}
        if isinstance(state, str):
            try:
                state = __import__("json").loads(state)
            except Exception:
                state = {}
        try:
            recurring_id = int(state.get("recurring_id") or 0)
        except (TypeError, ValueError):
            recurring_id = 0
        recurring = self._get_repo().get_recurring_expense(recurring_id)
        if not recurring or str(recurring.get("user_id")) != str(user.get("userId")):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message(RECURRING_NOT_FOUND_MESSAGE, _kb([ACTION_RECURRINGS, ACTION_HELP]))

        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
        self._get_repo().update_recurring_expense(recurring_id, {"status": "canceled", "canceled_at": now})
        self._get_repo().delete_pending_action(int(pending["id"]))
        return self._make_message("🛑 Recurrente cancelado.", _kb([ACTION_RECURRINGS, ACTION_LIST], [ACTION_SUMMARY, ACTION_HELP]))

    def _handle_daily_nudge_set_hour(self, user: Dict[str, Any], text: str, pending: Dict[str, Any]) -> BotMessage:
        content = (text or "").strip()
        if is_negative(content):
            self._get_repo().delete_pending_action(int(pending["id"]))
            return self._make_message("✅ Entendido. Mantengo la hora actual.", _kb_main())
        hour = parse_reminder_hour(content)
        if hour is None:
            return self._make_message(
                "🕖 Envíame la nueva hora del recordatorio.\nEjemplos: <code>21</code>, <code>9 pm</code>, <code>21:30</code>.",
                _kb([ACTION_CONFIRM_NO], [ACTION_HELP]),
            )
        user_id = str(user.get("userId"))
        self._save_daily_nudge_prefs(user_id, enabled=True, hour=int(hour))
        self._get_repo().delete_pending_action(int(pending["id"]))
        return self._make_message(
            f"✅ Listo. Te preguntaré por gastos cada día a las <b>{self._hour_label(int(hour))}</b>.",
            _kb_main(),
        )

    def _handle_daily_nudge_action(self, user: Dict[str, Any], data: str) -> BotMessage:
        parts = (data or "").split(":")
        if len(parts) < 2:
            return self._make_message(RECURRING_INVALID_ACTION_MESSAGE)
        action = parts[1].strip().lower()
        user_id = str(user.get("userId"))
        prefs = self._get_daily_nudge_prefs(user_id)
        current_hour = int(prefs.get("hour", 21))

        if action == "silence":
            self._save_daily_nudge_prefs(user_id, enabled=False, hour=current_hour)
            return self._make_message(
                "🔕 Recordatorio diario silenciado.\nSi quieres reactivarlo, pulsa el botón.",
                _kb([BotAction("dailynudge:enable", "🔔 Activar recordatorio")], [ACTION_HELP]),
            )

        if action == "examples":
            return self._make_message(
                "✍️ <b>Ejemplos rápidos</b>\n"
                "• <code>almuerzo 18000</code>\n"
                "• <code>uber 12500</code>\n"
                "• <code>supermercado 85k</code>\n"
                "• <code>me pagaron 2m</code>\n\n"
                "También puedes enviar varios en un mensaje:\n"
                "<code>almuerzo 18k y taxi 12k</code>",
                _kb([ACTION_LIST, ACTION_SUMMARY], [ACTION_HELP, BotAction("dailynudge:set_hour", "🕖 Cambiar hora")]),
            )

        if action == "enable":
            self._save_daily_nudge_prefs(user_id, enabled=True, hour=current_hour)
            return self._make_message(
                f"🔔 Recordatorio diario activado a las <b>{self._hour_label(current_hour)}</b>.",
                _kb([BotAction("dailynudge:set_hour", "🕖 Cambiar hora")], [ACTION_HELP]),
            )

        if action == "set_hour":
            self._upsert_pending_action(user_id, PENDING_DAILY_NUDGE_SET_HOUR, {"from": "daily_nudge"}, ttl_minutes=60)
            return self._make_message(
                "🕖 ¿A qué hora quieres el recordatorio diario?\nResponde con una hora. Ej: <code>21</code> o <code>9 pm</code>.",
                _kb([ACTION_CONFIRM_NO], [ACTION_HELP]),
            )

        return self._make_message(RECURRING_INVALID_ACTION_MESSAGE)

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
