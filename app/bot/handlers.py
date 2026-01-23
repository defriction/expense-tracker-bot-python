from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from telegram.ext import CallbackQueryHandler, MessageHandler, filters

from app.bot.pipeline import BotPipeline
from app.bot.ui_models import BotInput
from app.channels.telegram_adapter import download_voice_bytes, send_bot_message
from app.core.config import Settings, load_settings
from app.core.logging import logger, setup_logging, set_trace_id
from app.services.groq import GroqClient
from app.services.sheets import build_sheets_repo

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
        return BotPipeline(self.settings, build_sheets_repo(self.settings), GroqClient(self.settings))


def _get_pipeline() -> BotPipeline:
    global _settings, _pipeline
    if _pipeline is not None:
        return _pipeline
    _settings = load_settings()
    setup_logging()
    _pipeline = PipelineFactory(_settings).build()
    return _pipeline


def _set_trace_from_update(update) -> None:
    trace = None
    if update is not None and hasattr(update, "update_id"):
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
    try:
        message = update.effective_message
        chat_id = message.chat_id if message else None
        telegram_user_id = message.from_user.id if message and message.from_user else None
        text = message.text if message else None
        message_id = str(message.message_id) if message else None
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

        responses = await pipeline.handle_message(request)
        for response in responses:
            await send_bot_message(context, chat_id, response)
    except Exception as exc:
        await _notify_error(update, context, exc)


async def _handle_callback_safe(update, context) -> None:
    _set_trace_from_update(update)
    pipeline = _get_pipeline()
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

        request = BotInput(
            channel="telegram",
            chat_id=chat_id,
            user_id=telegram_user_id,
            text=text,
            message_id=message_id,
            audio_bytes=None,
            non_text_type=None,
        )
        responses = await pipeline.handle_callback(request)
        for response in responses:
            await send_bot_message(context, chat_id, response)
    except Exception as exc:
        await _notify_error(update, context, exc)


async def _notify_error(update, context, exc: Exception) -> None:
    message = str(exc) or "Unknown error"
    chat_id = update.effective_chat.id if update and update.effective_chat else None
    user_id = update.effective_user.id if update and update.effective_user else None
    logger.exception("Unhandled error chat_id=%s user_id=%s error=%s", chat_id, user_id, message)
    try:
        pipeline = _get_pipeline()
        pipeline._get_sheets().append_error_log(ERROR_WORKFLOW_NAME, "handler", message)
    except Exception:
        logger.warning("Failed to write error log to Sheets")

    if _settings:
        await ErrorNotifier(_settings).notify(update, context, message)


async def error_handler(update, context) -> None:
    _set_trace_from_update(update)
    message = str(context.error) if context.error else "Unknown error"
    await _notify_error(update, context, Exception(message))
