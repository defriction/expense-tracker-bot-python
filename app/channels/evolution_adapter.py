from __future__ import annotations

import base64
from typing import Any, Dict, Optional, List

import httpx

from app.bot.ui_models import BotInput, BotMessage
from app.core.logging import logger
from app.services.evolution import EvolutionClient


def _jid_to_e164(to: str) -> str:
    """
    Evolution sendText/sendPoll suele esperar E.164 con '+' :contentReference[oaicite:5]{index=5}
    Convierte:
      - "573001234567@s.whatsapp.net" -> "+573001234567"
      - "203714711277568@lid"         -> "+203714711277568"
      - "+573001234567"               -> "+573001234567"
      - "573001234567"                -> "+573001234567"
    """
    raw = (to or "").strip()
    if not raw:
        return raw

    base = raw.split("@", 1)[0].strip()
    if base.startswith("+"):
        return base

    # dejar solo dígitos
    digits = "".join(ch for ch in base if ch.isdigit())
    if not digits:
        return base  # fallback
    return f"+{digits}"


def _poll_values_from_keyboard(message: BotMessage) -> List[str]:
    # Polls necesitan opciones como texto; usa label, NO id
    values: List[str] = []
    for row in message.keyboard.rows:
        for action in row:
            label = (action.label or "").strip()
            if label:
                values.append(label)
    return values[:12]  # límite práctico


async def send_evolution_message(client: EvolutionClient, to: str, message: BotMessage) -> None:
    if not to:
        return

    number = _jid_to_e164(to)

    # Si hay teclado, intentamos poll; si falla, texto plano
    if message.keyboard and message.keyboard.rows:
        values = _poll_values_from_keyboard(message)
        if not values:
            # si no hay labels, manda texto
            try:
                await client.send_text(number, message.text, link_preview=False)
            except httpx.HTTPError:
                return
            return

        try:
            await client.send_poll(number, message.text, values, selectable_count=1)
            return
        except httpx.HTTPError:
            # fallback a texto enumerado
            text = message.text + "\n\n" + "\n".join(f"{i+1}. {v}" for i, v in enumerate(values))
            try:
                await client.send_text(number, text, link_preview=False)
            except httpx.HTTPError:
                return
            return

    # Texto
    try:
        await client.send_text(number, message.text, link_preview=False)
    except httpx.HTTPError:
        return


def parse_evolution_webhook(data: Dict[str, Any]) -> Optional[BotInput]:
    event = (data.get("event") or "").strip().lower().replace("_", ".")
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
