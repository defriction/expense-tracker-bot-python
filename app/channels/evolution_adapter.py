from __future__ import annotations

import base64
from typing import Any, Dict, Optional, List

import httpx

from app.bot.ui_models import BotInput, BotMessage
from app.core.logging import logger
from app.services.evolution import EvolutionClient

import re

def html_to_whatsapp(text: str) -> str:
    if not text:
        return text

    s = text

    # line breaks
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")

    # bold / italic
    s = re.sub(r"</?b>", "*", s, flags=re.IGNORECASE)
    s = re.sub(r"</?strong>", "*", s, flags=re.IGNORECASE)
    s = re.sub(r"</?i>", "_", s, flags=re.IGNORECASE)
    s = re.sub(r"</?em>", "_", s, flags=re.IGNORECASE)

    # code
    s = re.sub(r"</?code>", "```", s, flags=re.IGNORECASE)

    # remove any remaining tags
    s = re.sub(r"<[^>]+>", "", s)

    return s


def _safe_str(v: Any) -> str:
    try:
        return str(v)
    except Exception:
        return "<unprintable>"


def _extract_reply_to(payload: Dict[str, Any]) -> str:
    """
    Intenta resolver el destinatario correcto para responder.

    Casos:
    - key.remoteJid = "57...@s.whatsapp.net"  -> OK
    - key.remoteJid = "...@g.us"              -> OK (grupo)
    - key.remoteJid = "...@lid"               -> NO sirve para enviar
      Entonces intentamos participant/senderJid/etc.
    """
    key = (payload.get("key") or {})
    remote_jid = (key.get("remoteJid") or "")

    # Si es un JID normal, úsalo
    if remote_jid.endswith("@s.whatsapp.net") or remote_jid.endswith("@g.us"):
        return remote_jid

    # Si es LID, intenta otras pistas
    if remote_jid.endswith("@lid"):
        # Algunos payloads traen participant en key
        participant = (key.get("participant") or "")
        if participant.endswith("@s.whatsapp.net"):
            return participant

        # A veces viene afuera en payload
        sender = (payload.get("sender") or payload.get("senderJid") or payload.get("participant") or "")
        if isinstance(sender, str) and sender.endswith("@s.whatsapp.net"):
            return sender

        # Último intento: si el "from" viene como jid
        frm = payload.get("from")
        if isinstance(frm, str) and (frm.endswith("@s.whatsapp.net") or frm.endswith("@g.us")):
            return frm

    return remote_jid


def _poll_values_from_keyboard(message: BotMessage) -> List[str]:
    values: List[str] = []
    for row in message.keyboard.rows:
        for action in row:
            label = (action.label or "").strip()
            if label:
                values.append(label)
    return values[:12]


async def send_evolution_message(client: EvolutionClient, to: str, message: BotMessage) -> None:
    if not to:
        return

    # Si es LID, no intentes enviar
    if to.endswith("@lid"):
        logger.warning("Skipping reply to LID jid=%s (cannot sendText/sendPoll to LID)", to)
        return

    text = html_to_whatsapp(message.text)

    if message.keyboard and message.keyboard.rows:
        values = _poll_values_from_keyboard(message)
        values = [html_to_whatsapp(v) for v in values]

        # Poll requiere mínimo 2 opciones
        if len(values) < 2:
            try:
                await client.send_text(to, text, link_preview=False)
            except httpx.HTTPError:
                return
            return

        try:
            await client.send_poll(to, text, values, selectable_count=1)
            return
        except httpx.HTTPError:
            fallback = text + "\n\n" + "\n".join(f"{i+1}. {v}" for i, v in enumerate(values))
            try:
                await client.send_text(to, fallback, link_preview=False)
            except httpx.HTTPError:
                return
            return

    try:
        await client.send_text(to, text, link_preview=False)
    except httpx.HTTPError:
        return


def parse_evolution_webhook(data: Dict[str, Any]) -> Optional[BotInput]:
    event = (data.get("event") or "").strip().lower().replace("_", ".")
    if event != "messages.upsert":
        return None

    payload = data.get("data", {}) or {}
    key = payload.get("key", {}) or {}
    message = payload.get("message", {}) or {}

    if key.get("fromMe"):
        return None

    # 🔎 LOGS: para entender por qué llega @lid y dónde viene el JID real
    # (deja estos logs mientras estabilizas; luego los quitas)
    try:
        logger.info(
            "EV webhook key.remoteJid=%s key.participant=%s payload.keys=%s",
            _safe_str(key.get("remoteJid")),
            _safe_str(key.get("participant")),
            list(payload.keys()),
        )
    except Exception:
        pass

    reply_to = _extract_reply_to(payload)

    # Extraer texto
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
        chat_id=reply_to,      # 👈 aquí guardamos el JID correcto para responder
        user_id=reply_to,
        text=text,
        message_id=key.get("id"),
        audio_bytes=audio_bytes,
        non_text_type=non_text_type,
    )
