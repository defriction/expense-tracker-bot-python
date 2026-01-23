from telegram.ext import CallbackQueryHandler, MessageHandler, filters

from app.core.config import load_settings
from app.bot.pipeline import BotPipeline
from app.services.sheets import SheetsRepo

_settings = load_settings()
_sheets = SheetsRepo(_settings)
_pipeline = BotPipeline(_settings, _sheets)

ERROR_WORKFLOW_NAME = "Finance Bot v2"


def get_handlers():
    return [
        MessageHandler(filters.ALL, _pipeline.handle_message),
        CallbackQueryHandler(_pipeline.handle_callback),
    ]


async def error_handler(update, context) -> None:
    message = str(context.error) if context.error else "Unknown error"
    _sheets.append_error_log(ERROR_WORKFLOW_NAME, "handler", message)
    if not _settings.admin_telegram_chat_id:
        return
    text = (
        "🚨 <b>Error en el bot</b>\n\n"
        f"<b>Workflow:</b> <code>{ERROR_WORKFLOW_NAME}</code>\n"
        "<b>Nodo:</b> <code>handler</code>\n\n"
        "<b>Detalle:</b>\n"
        f"<pre>{message}</pre>"
    )
    await context.bot.send_message(
        chat_id=_settings.admin_telegram_chat_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
