from __future__ import annotations

import base64
from typing import Any, Dict, Optional, List

from app.bot.ui_models import BotInput, BotMessage
from app.services.evolution import EvolutionClient


def _normalize_number(to: str) -> str:
    """
    Evolution sendPoll NO acepta JID.
    Convierte:
    - 573001234567@s.whatsapp.net -> 573001234567
    - 203714711277568@lid -> 203714711277568
    - +573001234567 -> 573001234567
    """
    base = to.split("@", 1)[0].strip()
    if base.startswith("+"):
        base = base[1:]
    return base


def _poll_values_from_keyboard(message: BotMessage) -> List[str]:
    """
    Polls de WhatsApp requieren textos cortos.
    Usamos label (NO id).
    """
    values: List[str] = []
    for row in message.keyboard.rows:
        for action in row:
            label = (action.label or "").strip()
            if label:
                values.append(label)

    # WhatsApp permite hasta 12 opciones
    return values[:12]


async def send_evolution_message(
    client: EvolutionClient,
    to: str,
    message: BotMessage,
) -> None:
    if not to:
        return

    # Si hay teclado → intentamos poll
    if message.keyboard and message.keyboard.rows:
        number = _normalize_number(to)
        values = _poll_values_from_keyboard(message)

        # Fallback seguro
        if not values:
            await client.send_message(to, message.text)
            return

        try:
            await client.send_poll(
                number=number,
                name=message.text,
                values=values,
                selectable_count=1,
            )
        except Exception:
            # Si Evolution responde 400, degradamos a texto
            text = message.text + "\n\n" + "\n".join(
                f"{i+1}. {v}" for i, v in enumerate(values)
            )
            await client.send_message(to, text)
        return

    # Texto plano
    await client.send_message(to, message.text)


def parse_evolution_webhook(data: Dict[str, Any]) -> Optional[BotInput]:
    """
    Normalizado para Evolution v2
    """
    event = (data.get("event") or "").strip().lower().replace("_", ".")
    if event != "messages.upsert":
        return None

    payload = data.get("data", {})
    key = payload.get("key", {})
    message = payload.get("message", {})

    # Ignorar mensajes propios
    if key.get("fromMe"):
        return None

    remote_jid = key.get("remoteJid", "")

    text = (
        message.get("conversation")
        or message.get("extendedTextMessage", {}).get("text")
        or message.get("imageMessage", {}).get("caption")
        or message.get("videoMessage", {}).get("caption")
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
