from __future__ import annotations

from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from app.bot.ui_models import BotKeyboard, BotMessage


async def send_bot_message(context: ContextTypes.DEFAULT_TYPE, chat_id: Optional[int], message: BotMessage) -> None:
    if not chat_id:
        return
    keyboard = _build_keyboard(message.keyboard)
    await context.bot.send_message(
        chat_id=chat_id,
        text=message.text,
        parse_mode="HTML",
        disable_web_page_preview=message.disable_web_preview,
        reply_markup=keyboard,
    )


async def download_voice_bytes(context: ContextTypes.DEFAULT_TYPE, file_id: str) -> bytes:
    file = await context.bot.get_file(file_id)
    audio_bytes = await file.download_as_bytearray()
    return bytes(audio_bytes)


def _build_keyboard(keyboard: Optional[BotKeyboard]) -> Optional[InlineKeyboardMarkup]:
    if not keyboard or not keyboard.rows:
        return None
    rows = []
    for row in keyboard.rows:
        buttons = [InlineKeyboardButton(text=action.label, callback_data=action.id) for action in row]
        if buttons:
            rows.append(buttons)
    return InlineKeyboardMarkup(rows) if rows else None
