from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any, Optional, Tuple

from fastapi import APIRouter, Header, Request

from app.bot.pipeline import BotPipeline
from app.channels.evolution_adapter import parse_evolution_webhook, send_evolution_message
from app.core.config import Settings
from app.core.logging import logger, set_client_ip, set_log_context, set_trace_id
from app.services.evolution import EvolutionClient


def build_evolution_router(
    pipeline: BotPipeline,
    evolution_client: EvolutionClient,
    settings: Settings,
) -> APIRouter:
    router = APIRouter()

    @router.post("/evolution/webhook")
    async def evolution_webhook(request: Request, apikey: Optional[str] = Header(None)):
        ctx = _init_request_context(request)
        _safe_set_log_context(ctx)

        _log_request_received(ctx, apikey)

        if _is_unauthorized(apikey, settings):
            _log_unauthorized(ctx, apikey)
            return {"ok": False, "error": "unauthorized"}

        data, json_ok, body_hash = await _read_json_safely_with_hash(request)
        if not json_ok:
            _log_invalid_json(ctx, request, body_hash)
            return {"ok": False, "error": "invalid_json"}

        event, raw_message = _extract_event_and_message(data)
        meta = _extract_meta(data)

        logger.debug(
            "EV webhook parsed event=%s body_hash=%s instanceId=%s remoteJid=%s key.id=%s msgTs=%s msgType=%s has_raw_message=%s",
            event,
            body_hash,
            meta.get("instanceId"),
            meta.get("remoteJid"),
            meta.get("keyId"),
            meta.get("messageTimestamp"),
            meta.get("messageType"),
            bool(raw_message),
        )

        if event == "messages.update":
            _log_update_summary(data, meta)
            return {"ok": True}

        if event != "messages.upsert":
            logger.debug("EV webhook ignored event=%s", event)
            return {"ok": True}

        if raw_message is None:
            _log_upsert_missing_message(data, meta)

        bot_input = await _parse_bot_input(data, evolution_client, event, meta, body_hash)
        if bot_input is None:
            return {"ok": True}

        _safe_set_log_context_from_bot_input(bot_input)

        ok = await _handle_and_send_responses(pipeline, evolution_client, bot_input)
        if not ok:
            return {"ok": True}

        logger.debug("EV webhook done")
        return {"ok": True}

    return router


def _init_request_context(request: Request) -> dict[str, str]:
    client_host = request.client.host if request.client else "unknown"
    request_method = getattr(request, "method", "UNKNOWN")
    request_path = str(getattr(getattr(request, "url", None), "path", "/evolution/webhook"))

    trace_id = _safe_trace_id()

    return {
        "trace_id": trace_id,
        "client_host": client_host,
        "method": request_method,
        "path": request_path,
    }


def _safe_trace_id() -> str:
    try:
        return f"tx-{uuid.uuid4().hex}"
    except Exception:
        return f"tx-fallback-{int(time.time() * 1000)}"


def _safe_set_log_context(ctx: dict[str, str]) -> None:
    try:
        set_trace_id(ctx["trace_id"])
        set_client_ip(ctx["client_host"])
        set_log_context("evolution", None, None, None)
    except Exception:
        pass


def _log_request_received(ctx: dict[str, str], apikey: Optional[str]) -> None:
    try:
        logger.info(
            "EV webhook received trace_id=%s ip=%s method=%s path=%s apikey_present=%s",
            ctx["trace_id"],
            ctx["client_host"],
            ctx["method"],
            ctx["path"],
            bool(apikey),
        )
    except Exception:
        try:
            logger.info("EV webhook received (logging_error)")
        except Exception:
            pass


def _is_unauthorized(apikey: Optional[str], settings: Settings) -> bool:
    return bool(settings.evolution_api_key) and apikey != settings.evolution_api_key


def _log_unauthorized(ctx: dict[str, str], apikey: Optional[str]) -> None:
    try:
        logger.warning(
            "EV webhook unauthorized trace_id=%s ip=%s apikey_present=%s",
            ctx["trace_id"],
            ctx["client_host"],
            bool(apikey),
        )
    except Exception:
        pass


async def _read_json_safely_with_hash(request: Request) -> Tuple[dict[str, Any], bool, str]:
    try:
        raw = await request.body()
        body_hash = hashlib.sha256(raw).hexdigest()[:12] if raw else "empty"
    except Exception:
        body_hash = "unavailable"

    try:
        data = await request.json()
        if isinstance(data, dict):
            return data, True, body_hash
        return {}, False, body_hash
    except Exception:
        return {}, False, body_hash


def _log_invalid_json(ctx: dict[str, str], request: Request, body_hash: str) -> None:
    try:
        content_length = request.headers.get("content-length", "unknown")
    except Exception:
        content_length = "unknown"

    try:
        logger.warning(
            "EV webhook invalid_json trace_id=%s ip=%s content_length=%s body_hash=%s",
            ctx["trace_id"],
            ctx["client_host"],
            content_length,
            body_hash,
        )
    except Exception:
        pass


def _extract_event_and_message(data: dict[str, Any]) -> Tuple[str, Any]:
    try:
        event = (data.get("event") or "").strip().lower().replace("_", ".")
    except Exception:
        event = ""

    try:
        raw_message = ((data.get("data") or {}).get("message"))
    except Exception:
        raw_message = None

    return event, raw_message


def _extract_meta(data: dict[str, Any]) -> dict[str, Any]:
    d = data.get("data") or {}

    key = d.get("key") or {}
    meta = {
        "instanceId": d.get("instanceId") or data.get("instanceId"),
        "source": d.get("source") or data.get("source"),
        "remoteJid": key.get("remoteJid") or d.get("key", {}).get("remoteJid"),
        "participant": key.get("participant"),
        "keyId": key.get("id"),
        "messageTimestamp": d.get("messageTimestamp"),
        "messageType": d.get("messageType"),
        "pushName": d.get("pushName"),
        "status": d.get("status"),
    }

    msg = d.get("message") or {}
    conv = None
    try:
        conv = msg.get("conversation")
    except Exception:
        conv = None
    if conv:
        meta["conversation"] = conv

    return meta


def _log_update_summary(data: dict[str, Any], meta: dict[str, Any]) -> None:
    d = data.get("data") or {}

    update_keys = []
    try:
        update_keys = list(d.keys())
    except Exception:
        update_keys = ["unavailable"]

    key = d.get("key") or {}
    logger.debug(
        "EV webhook update summary update_keys=%s remoteJid=%s key.id=%s status=%s msgTs=%s msgType=%s",
        update_keys,
        meta.get("remoteJid") or key.get("remoteJid"),
        meta.get("keyId") or key.get("id"),
        meta.get("status"),
        meta.get("messageTimestamp"),
        meta.get("messageType"),
    )

    has_message = False
    try:
        has_message = bool(d.get("message"))
    except Exception:
        has_message = False

    if has_message:
        msg_keys = []
        try:
            msg_keys = list((d.get("message") or {}).keys())
        except Exception:
            msg_keys = ["unavailable"]
        logger.debug(
            "EV webhook update contains message (!) message_keys=%s remoteJid=%s key.id=%s",
            msg_keys,
            meta.get("remoteJid"),
            meta.get("keyId"),
        )


def _log_upsert_missing_message(data: dict[str, Any], meta: dict[str, Any]) -> None:
    d = data.get("data") or {}
    d_keys = []
    try:
        d_keys = list(d.keys())
    except Exception:
        d_keys = ["unavailable"]

    candidates = []
    if "messages" in d:
        try:
            m = d.get("messages")
            candidates.append(f"messages_type={type(m).__name__}")
            if isinstance(m, list) and m:
                candidates.append(f"messages[0]_keys={list((m[0] or {}).keys())[:20]}")
        except Exception:
            candidates.append("messages_parse_error")

    logger.debug(
        "EV webhook upsert missing raw_message d_keys=%s candidates=%s remoteJid=%s key.id=%s msgType=%s",
        d_keys,
        candidates,
        meta.get("remoteJid"),
        meta.get("keyId"),
        meta.get("messageType"),
    )


async def _parse_bot_input(
    data: dict[str, Any],
    evolution_client: EvolutionClient,
    event: str,
    meta: dict[str, Any],
    body_hash: str,
):
    try:
        bot_input = await parse_evolution_webhook(data, evolution_client)
        if not bot_input:
            logger.debug(
                "EV webhook discarded reason=parse_evolution_webhook_returned_none event=%s body_hash=%s remoteJid=%s key.id=%s msgType=%s msgTs=%s",
                event,
                body_hash,
                meta.get("remoteJid"),
                meta.get("keyId"),
                meta.get("messageType"),
                meta.get("messageTimestamp"),
            )
            return None
        return bot_input
    except Exception:
        logger.exception(
            "EV parse_evolution_webhook failed event=%s body_hash=%s remoteJid=%s key.id=%s",
            event,
            body_hash,
            meta.get("remoteJid"),
            meta.get("keyId"),
        )
        return None


def _safe_set_log_context_from_bot_input(bot_input) -> None:
    try:
        set_log_context("evolution", bot_input.chat_id, bot_input.user_id, bot_input.message_id)
    except Exception:
        pass


async def _handle_and_send_responses(
    pipeline: BotPipeline,
    evolution_client: EvolutionClient,
    bot_input,
) -> bool:
    try:
        responses = await pipeline.handle_message(bot_input)
        logger.debug("EV webhook responses count=%s", len(responses))

        for response in responses:
            await send_evolution_message(evolution_client, str(bot_input.chat_id), response)

        return True
    except Exception:
        logger.exception("EV pipeline/send failed")
        return False
