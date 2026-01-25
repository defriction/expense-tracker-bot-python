from __future__ import annotations

import base64
import re
from typing import Any, Dict, List, Optional

import httpx

from app.bot.ui_models import BotInput, BotMessage
from app.core.logging import logger
from app.services.evolution import EvolutionClient


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
    key = (payload.get("key") or {})
    remote_jid = (key.get("remoteJid") or "")

    if remote_jid.endswith("@s.whatsapp.net") or remote_jid.endswith("@g.us"):
        return remote_jid

    if remote_jid.endswith("@lid"):
        participant = (key.get("participant") or "")
        if participant.endswith("@s.whatsapp.net"):
            return participant

        sender = (payload.get("sender") or payload.get("senderJid") or payload.get("participant") or "")
        if isinstance(sender, str) and sender.endswith("@s.whatsapp.net"):
            return sender

        frm = payload.get("from")
        if isinstance(frm, str) and (frm.endswith("@s.whatsapp.net") or frm.endswith("@g.us")):
            return frm

    return remote_jid


def _rows_from_keyboard(message: BotMessage) -> List[Dict[str, str]]:
    """
    keyboard -> rows para sendList.
    rowId: ideal action.id (comando); si no existe, usa label.
    Nota: Evolution (tu instancia) exige row.description NO vacío.
    """
    rows: List[Dict[str, str]] = []
    for row in message.keyboard.rows:
        for action in row:
            label = html_to_whatsapp((action.label or "").strip())
            if not label:
                continue

            action_id = getattr(action, "id", None)
            row_id = (action_id or label).strip()
            row_id = html_to_whatsapp(row_id)

            rows.append(
                {
                    "title": label,
                    "description": " ",  # requerido por validación de Evolution
                    "rowId": row_id,
                }
            )
    return rows


def _commands_fallback(text: str, rows: List[Dict[str, str]]) -> str:
    """
    Fallback sin encuesta: comandos para tocar/copiar/enviar.
    """
    lines: List[str] = []
    for r in rows:
        cmd = (r.get("rowId") or "").strip()
        title = (r.get("title") or "").strip()
        if not cmd or not title:
            continue
        lines.append(f"• `{cmd}` – {title}")

    if not lines:
        return text

    return (
        f"{text}\n\n"
        "👉 *Si no ves el menú, escribe o toca un comando:*\n"
        + "\n".join(lines)
    )


async def send_evolution_message(client: EvolutionClient, to: str, message: BotMessage) -> None:
    if not to:
        return

    if to.endswith("@lid"):
        logger.warning("Skipping reply to LID jid=%s (cannot sendText/sendList to LID)", to)
        return

    text = html_to_whatsapp(message.text)

    if message.keyboard and message.keyboard.rows:
        rows = _rows_from_keyboard(message)

        # mínimo 2 opciones
        if len(rows) < 2:
            try:
                await client.send_text(to, text, link_preview=False)
            except httpx.HTTPError:
                return
            return

        sections = [{"title": "Opciones", "rows": rows}]

        # 1) Siempre intentar LIST (tipo Claro)
        try:
            await client.send_list(
                to,
                title="Menú",
                description=text,
                button_text="Abrir",
                sections=sections,
            )
            return
        except httpx.HTTPError:
            pass

        # 2) Fallback: comandos tocables (no encuesta)
        fallback = _commands_fallback(text, rows)
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

    # LIST response (tipo Claro)
    selected_row_id = (
        (message.get("listResponseMessage", {}) or {})
        .get("singleSelectReply", {})
        .get("selectedRowId")
    )

    # texto normal
    text = selected_row_id or (
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
        chat_id=reply_to,
        user_id=reply_to,
        text=text,
        message_id=key.get("id"),
        audio_bytes=audio_bytes,
        non_text_type=non_text_type,
    )
