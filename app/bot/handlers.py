from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from telegram.ext import CallbackQueryHandler, MessageHandler, filters

from app.bot.pipeline import BotPipeline
from app.bot.formatters import RATE_LIMIT_MESSAGE
from app.bot.parser import parse_command
from app.bot.ui_models import BotInput
from app.channels.telegram_adapter import download_voice_bytes, send_bot_message
from app.core.config import Settings, load_settings
from app.core.logging import get_client_ip, get_trace_id, logger, set_log_context, setup_logging, set_trace_id
from app.core.rate_limit import rate_limiter
from app.services.groq import GroqClient
from app.services.data_repo import build_data_repo

ERROR_WORKFLOW_NAME = "Finance Bot v2"
USER_ERROR_MESSAGE = "⚠️ <b>Ocurrió un error</b>\nPor favor inténtalo más tarde."

_settings: Optional[Settings] = None
_pipeline: Optional[BotPipeline] = None


@dataclass
class ErrorNotifier:
    settings: Settings

    async def notify(self, update, context, message: str) -> None:
        if update and update.effective_chat:
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=USER_ERROR_MESSAGE,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass

        if self.settings.admin_telegram_chat_id:
            text = (
                "🚨 <b>Error en el bot</b>\n\n"
                f"<b>Workflow:</b> <code>{ERROR_WORKFLOW_NAME}</code>\n"
                "<b>Nodo:</b> <code>handler</code>\n\n"
                "<b>Detalle:</b>\n"
                f"<pre>{message}</pre>"
            )
            try:
                await context.bot.send_message(
                    chat_id=self.settings.admin_telegram_chat_id,
                    text=text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
            except Exception:
                pass


class PipelineFactory:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def build(self) -> BotPipeline:
        return BotPipeline(self.settings, build_data_repo(self.settings), GroqClient(self.settings))


def _get_pipeline() -> BotPipeline:
    global _settings, _pipeline
    if _pipeline is not None:
        return _pipeline
    _settings = load_settings()
    setup_logging()
    _pipeline = PipelineFactory(_settings).build()
    return _pipeline


def _set_trace_from_update(update) -> None:
    if get_trace_id() != "-":
        return
    trace = None
    message_id = None
    chat_id = None
    if update is not None and getattr(update, "effective_message", None):
        message = update.effective_message
        message_id = getattr(message, "message_id", None)
        chat_id = getattr(message, "chat_id", None)
        if message_id is not None:
            if chat_id is not None:
                trace = f"tg-{chat_id}-{message_id}"
            else:
                trace = f"tg-{message_id}"
    if trace is None and update is not None and hasattr(update, "update_id"):
        trace = f"tg-{update.update_id}"
    set_trace_id(trace)


def get_handlers():
    return [
        MessageHandler(filters.ALL, _handle_message_safe),
        CallbackQueryHandler(_handle_callback_safe),
    ]


async def _handle_message_safe(update, context) -> None:
    _set_trace_from_update(update)
    pipeline = _get_pipeline()
    settings = pipeline.settings
    try:
        message = update.effective_message
        chat_id = message.chat_id if message else None
        telegram_user_id = message.from_user.id if message and message.from_user else None
        text = message.text if message else None
        if not text and message:
            text = message.caption
        message_id = str(message.message_id) if message else None
        set_log_context("telegram", chat_id, telegram_user_id, message_id)
        logger.debug("Handler message start channel=telegram")
        audio_bytes = None
        non_text_type = None

        if message and message.voice:
            try:
                audio_bytes = await download_voice_bytes(context, message.voice.file_id)
            except Exception as exc:
                logger.warning("Failed to download voice file: %s", exc)
                non_text_type = "voice"
        elif message and message.photo:
            non_text_type = "photo"
        elif message and message.sticker:
            non_text_type = "sticker"

        request = BotInput(
            channel="telegram",
            chat_id=chat_id,
            user_id=telegram_user_id,
            text=text,
            message_id=message_id,
            audio_bytes=audio_bytes,
            non_text_type=non_text_type,
        )

        command = parse_command(text, chat_id, telegram_user_id, non_text_type, "telegram")
        if command.route == "onboarding" and settings.rate_limit_onboarding_per_min > 0:
            limiter_key = f"onboard:{request.channel}:{telegram_user_id or chat_id or 'unknown'}"
            if not rate_limiter.allow(limiter_key, settings.rate_limit_onboarding_per_min, 60):
                logger.warning("Onboarding rate limited key=%s", limiter_key)
                await _notify_admin_rate_limit(context, settings, "onboarding", telegram_user_id, chat_id)
                await send_bot_message(context, chat_id, pipeline._make_message(RATE_LIMIT_MESSAGE))
                return

        if settings.rate_limit_per_user_per_min > 0:
            limiter_key = None
            if telegram_user_id is not None:
                limiter_key = f"user:{telegram_user_id}"
            elif chat_id is not None:
                limiter_key = f"chat:{chat_id}"
            if limiter_key and not rate_limiter.allow(limiter_key, settings.rate_limit_per_user_per_min, 60):
                logger.warning("Rate limit exceeded key=%s", limiter_key)
                await _notify_admin_rate_limit(context, settings, "user", telegram_user_id, chat_id)
                await send_bot_message(context, chat_id, pipeline._make_message(RATE_LIMIT_MESSAGE))
                return

        responses = await pipeline.handle_message(request)
        logger.debug("Handler message responses channel=telegram count=%s", len(responses))
        for response in responses:
            await send_bot_message(context, chat_id, response)
        logger.debug("Handler message done channel=telegram")
    except Exception as exc:
        await _notify_error(update, context, exc)


async def _handle_callback_safe(update, context) -> None:
    _set_trace_from_update(update)
    pipeline = _get_pipeline()
    settings = pipeline.settings
    try:
        callback = update.callback_query
        if not callback:
            return
        await callback.answer()
        message = callback.message
        chat_id = message.chat_id if message else None
        telegram_user_id = callback.from_user.id if callback.from_user else None
        message_id = str(message.message_id) if message else None
        text = callback.data
        set_log_context("telegram", chat_id, telegram_user_id, message_id)
        logger.debug("Handler callback start channel=telegram")

        request = BotInput(
            channel="telegram",
            chat_id=chat_id,
            user_id=telegram_user_id,
            text=text,
            message_id=message_id,
            audio_bytes=None,
            non_text_type=None,
        )
        command = parse_command(text, chat_id, telegram_user_id, None, "telegram")
        if command.route == "onboarding" and settings.rate_limit_onboarding_per_min > 0:
            limiter_key = f"onboard:{request.channel}:{telegram_user_id or chat_id or 'unknown'}"
            if not rate_limiter.allow(limiter_key, settings.rate_limit_onboarding_per_min, 60):
                logger.warning("Onboarding rate limited key=%s", limiter_key)
                await _notify_admin_rate_limit(context, settings, "onboarding", telegram_user_id, chat_id)
                await send_bot_message(context, chat_id, pipeline._make_message(RATE_LIMIT_MESSAGE))
                return
        if settings.rate_limit_per_user_per_min > 0:
            limiter_key = None
            if telegram_user_id is not None:
                limiter_key = f"user:{telegram_user_id}"
            elif chat_id is not None:
                limiter_key = f"chat:{chat_id}"
            if limiter_key and not rate_limiter.allow(limiter_key, settings.rate_limit_per_user_per_min, 60):
                logger.warning("Rate limit exceeded key=%s", limiter_key)
                await _notify_admin_rate_limit(context, settings, "user", telegram_user_id, chat_id)
                await send_bot_message(context, chat_id, pipeline._make_message(RATE_LIMIT_MESSAGE))
                return
        responses = await pipeline.handle_callback(request)
        logger.debug("Handler callback responses channel=telegram count=%s", len(responses))
        for response in responses:
            await send_bot_message(context, chat_id, response)
        logger.debug("Handler callback done channel=telegram")
    except Exception as exc:
        await _notify_error(update, context, exc)


async def _notify_error(update, context, exc: Exception) -> None:
    message = str(exc) or "Unknown error"
    chat_id = update.effective_chat.id if update and update.effective_chat else None
    user_id = update.effective_user.id if update and update.effective_user else None
    message_id = update.effective_message.message_id if update and update.effective_message else None
    set_log_context("telegram", chat_id, user_id, message_id)
    logger.exception("Unhandled error chat_id=%s user_id=%s error=%s", chat_id, user_id, message)
    try:
        pipeline = _get_pipeline()
        pipeline._get_repo().append_error_log(ERROR_WORKFLOW_NAME, "handler", message, str(user_id) if user_id else None, str(chat_id) if chat_id else None)
    except Exception:
        logger.warning("Failed to write error log to data store")

    if _settings:
        await ErrorNotifier(_settings).notify(update, context, message)


async def _notify_admin_rate_limit(context, settings: Settings, reason: str, user_id, chat_id) -> None:
    if not settings.admin_telegram_chat_id:
        return
    ip = get_client_ip()
    text = (
        "🚨 <b>Bloqueo por seguridad</b>\n\n"
        f"<b>Motivo:</b> <code>{reason}</code>\n"
        f"<b>User:</b> <code>{user_id or '-'}</code>\n"
        f"<b>Chat:</b> <code>{chat_id or '-'}</code>\n"
        f"<b>IP:</b> <code>{ip}</code>"
    )
    try:
        await context.bot.send_message(
            chat_id=settings.admin_telegram_chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception:
        logger.warning("Failed to notify admin on rate limit")


async def error_handler(update, context) -> None:
    _set_trace_from_update(update)
    message = str(context.error) if context.error else "Unknown error"
    await _notify_error(update, context, Exception(message))
