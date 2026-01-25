from __future__ import annotations

import base64
from typing import Any, Dict, Optional

import httpx

from app.bot.ui_models import BotInput, BotMessage
from app.core.logging import logger
from app.services.evolution import EvolutionClient


async def send_evolution_message(client: EvolutionClient, to: str, message: BotMessage) -> None:
    if not to:
        return

    # Si hay teclado, intentamos poll. Si falla, mandamos texto plano.
    if message.keyboard and message.keyboard.rows:
        options: list[str] = []
        for row in message.keyboard.rows:
            for action in row:
                options.append(action.id)

        try:
            await client.send_poll(to, message.text, options, selectable_count=1)
            return
        except httpx.HTTPError:
            # Fallback: texto con opciones
            text = message.text + "\n\n" + "\n".join(f"- {opt}" for opt in options)
            await client.send_text(to, text, link_preview=False)
            return

    await client.send_text(to, message.text, link_preview=False)


def parse_evolution_webhook(data: Dict[str, Any]) -> Optional[BotInput]:
    event = (data.get("event") or "").strip().lower()
    if event != "messages.upsert":
        return None

    payload = data.get("data", {}) or {}
    key = payload.get("key", {}) or {}
    message = payload.get("message", {}) or {}

    # Ignore self messages
    if key.get("fromMe"):
        return None

    remote_jid = key.get("remoteJid", "") or ""

    text = (
        message.get("conversation")
        or (message.get("extendedTextMessage", {}) or {}).get("text")
        or (message.get("imageMessage", {}) or {}).get("caption")
        or (message.get("videoMessage", {}) or {}).get("caption")
        or ""
    )

    audio_bytes = None
    non_text_type = None

    if "audioMessage" in message:
        b64_data = message.get("base64")
        if b64_data:
            try:
                audio_bytes = base64.b64decode(b64_data)
            except Exception:
                pass
        non_text_type = "voice"
    elif "imageMessage" in message:
        non_text_type = "photo"

    return BotInput(
        channel="evolution",
        chat_id=remote_jid,
        user_id=remote_jid,
        text=text,
        message_id=key.get("id"),
        audio_bytes=audio_bytes,
        non_text_type=non_text_type,
    )
