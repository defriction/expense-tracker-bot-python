from __future__ import annotations

import base64
import html
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

    # preformatted blocks
    s = re.sub(r"</?pre>", "```", s, flags=re.IGNORECASE)

    # bold / italic
    s = re.sub(r"</?b>", "*", s, flags=re.IGNORECASE)
    s = re.sub(r"</?strong>", "*", s, flags=re.IGNORECASE)
    s = re.sub(r"</?i>", "_", s, flags=re.IGNORECASE)
    s = re.sub(r"</?em>", "_", s, flags=re.IGNORECASE)

    # code
    s = re.sub(r"</?code>", "```", s, flags=re.IGNORECASE)

    # remove any remaining tags
    s = re.sub(r"<[^>]+>", "", s)

    return html.unescape(s)


def _safe_str(v: Any) -> str:
    try:
        return str(v)
    except Exception:
        return "<unprintable>"


def _is_addressable_jid(jid: str) -> bool:
    return jid.endswith("@s.whatsapp.net") or jid.endswith("@g.us")


def _extract_jid_candidates(payload: Dict[str, Any]) -> List[str]:
    key = (payload.get("key") or {})
    raw_candidates = [
        key.get("remoteJid"),
        key.get("remoteJidAlt"),
        payload.get("remoteJidAlt"),
        key.get("participant"),
        payload.get("sender"),
        payload.get("senderJid"),
        payload.get("participant"),
        payload.get("from"),
    ]

    result: List[str] = []
    for value in raw_candidates:
        if not isinstance(value, str):
            continue
        jid = value.strip()
        if not jid or jid in result:
            continue
        result.append(jid)
    return result


def _extract_chat_and_user_jid(payload: Dict[str, Any]) -> tuple[str, str]:
    key = (payload.get("key") or {})
    remote_jid = str(key.get("remoteJid") or "").strip()
    remote_jid_alt = str(key.get("remoteJidAlt") or payload.get("remoteJidAlt") or "").strip()
    candidates = _extract_jid_candidates(payload)

    if _is_addressable_jid(remote_jid):
        chat_jid = remote_jid
    elif _is_addressable_jid(remote_jid_alt):
        chat_jid = remote_jid_alt
    else:
        chat_jid = next((jid for jid in candidates if _is_addressable_jid(jid)), remote_jid or remote_jid_alt)

    is_group_chat = chat_jid.endswith("@g.us")

    if is_group_chat:
        user_jid = next((jid for jid in candidates if jid.endswith("@s.whatsapp.net")), chat_jid)
    else:
        user_jid = next((jid for jid in candidates if jid.endswith("@s.whatsapp.net")), chat_jid)

    # Evolution no permite sendText/sendList a @lid.
    if chat_jid.endswith("@lid") and user_jid.endswith("@s.whatsapp.net"):
        chat_jid = user_jid

    return chat_jid, user_jid


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
        logger.info("EV send skipped reason=missing_to")
        return

    if to.endswith("@lid"):
        logger.warning("Skipping reply to LID jid=%s (cannot sendText/sendList to LID)", to)
        return

    text = html_to_whatsapp(message.text)

    logger.info("EV send start to=%s has_keyboard=%s text_len=%s", to, bool(message.keyboard), len(message.text or ""))

    if message.document_bytes:
        media_b64 = base64.b64encode(message.document_bytes).decode("utf-8")
        try:
            await client.send_media(
                to,
                mediatype="document",
                mimetype=message.document_mime,
                media=media_b64,
                file_name=message.document_name,
                caption=text,
            )
            logger.info("EV send ok to=%s kind=document", to)
            return
        except httpx.HTTPError as exc:
            logger.warning("EV send failed to=%s kind=document error=%s", to, exc)
            return

    if message.keyboard and message.keyboard.rows:
        rows = _rows_from_keyboard(message)

        if len(rows) < 2:
            try:
                await client.send_text(to, text, link_preview=False)
                logger.info("EV send ok to=%s kind=text", to)
                return
            except httpx.HTTPError as exc:
                logger.warning("EV send failed to=%s kind=text error=%s", to, exc)
                return

        # Disable inline buttons
        #sections = [{"title": "Opciones", "rows": rows}]

        # try:
        #     await client.send_list(
        #         to,
        #         title="Menú",
        #         description=text,
        #         button_text="Abrir",
        #         sections=sections,
        #     )
        #     return
        # except httpx.HTTPError:
        #     pass

        fallback = _commands_fallback(text, rows)
        try:
            await client.send_text(to, fallback, link_preview=False)
            logger.info("EV send ok to=%s kind=text", to)
            return
        except httpx.HTTPError as exc:
            logger.warning("EV send failed to=%s kind=text error=%s", to, exc)
            return

    try:
        await client.send_text(to, text, link_preview=False)
        logger.info("EV send ok to=%s kind=text", to)
    except httpx.HTTPError as exc:
        logger.warning("EV send failed to=%s kind=text error=%s", to, exc)
        return


async def parse_evolution_webhook(data: Dict[str, Any], client: EvolutionClient) -> Optional[BotInput]:
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

    chat_jid, user_jid = _extract_chat_and_user_jid(payload)
    logger.info(
        "EV webhook jid.resolve chat_jid=%s user_jid=%s remoteJid=%s remoteJidAlt=%s",
        _safe_str(chat_jid),
        _safe_str(user_jid),
        _safe_str(key.get("remoteJid")),
        _safe_str(key.get("remoteJidAlt") or payload.get("remoteJidAlt")),
    )

    selected_row_id = (
        (message.get("listResponseMessage", {}) or {})
        .get("singleSelectReply", {})
        .get("selectedRowId")
    )

    text = selected_row_id or (
        message.get("conversation")
        or (message.get("extendedTextMessage", {}) or {}).get("text")
        or (message.get("imageMessage", {}) or {}).get("caption")
        or (message.get("videoMessage", {}) or {}).get("caption")
        or ""
    )

    audio_bytes: Optional[bytes] = None
    non_text_type = None

    if "audioMessage" in message:
        non_text_type = "voice"

        b64_data = (
            message.get("base64")
            or (message.get("audioMessage", {}) or {}).get("base64")
            or (message.get("audioMessage", {}) or {}).get("file")
        )
        if b64_data:
            try:
                audio_bytes = base64.b64decode(b64_data)
            except Exception:
                audio_bytes = None

        if audio_bytes is None:
            try:
                media = await client.get_base64_from_media_message(
                    key=key,
                    message=message,
                    convert_to_mp4=True,  # best compatibility for STT
                )
                media_b64 = (media or {}).get("base64")
                if media_b64:
                    audio_bytes = base64.b64decode(media_b64)
            except Exception as exc:
                logger.warning("EV getBase64FromMediaMessage failed: %s", exc)
    elif "imageMessage" in message:
        non_text_type = "photo"

    return BotInput(
        channel="evolution",
        chat_id=chat_jid,
        user_id=user_jid,
        text=text,
        message_id=key.get("id"),
        audio_bytes=audio_bytes,
        non_text_type=non_text_type,
    )
