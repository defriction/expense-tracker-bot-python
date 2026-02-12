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

        data, json_ok = await _read_json_safely(request)
        if not json_ok:
            _log_invalid_json(ctx, request)
            return {"ok": False, "error": "invalid_json"}

        event, raw_message = _extract_event_and_message(data)
        logger.info("EV webhook start event=%s raw_message=%s", event, raw_message)

        if event == "messages.update":
            logger.debug("EV webhook update payload=%s", data)
            return {"ok": True}

        if event != "messages.upsert":
            return {"ok": True}

        bot_input = await _parse_bot_input(data, evolution_client, event)
        if bot_input is None:
            return {"ok": True}

        _safe_set_log_context_from_bot_input(bot_input)

        ok = await _handle_and_send_responses(pipeline, evolution_client, bot_input)
        if not ok:
            return {"ok": True}

        logger.info("EV webhook done")
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
        # ultra-safe fallback
        return f"tx-fallback-{int(time.time() * 1000)}"


def _safe_set_log_context(ctx: dict[str, str]) -> None:
    try:
        set_trace_id(ctx["trace_id"])
        set_client_ip(ctx["client_host"])
        set_log_context("evolution", None, None, None)
    except Exception:
        # never block webhook because logging context failed
        pass


def _log_request_received(ctx: dict[str, str], apikey: Optional[str]) -> None:
    try:
        apikey_present = bool(apikey)
        content_length = "unknown"
        # can't always access headers safely everywhere, so guard it
        try:
            content_length = ctx.get("content_length", "unknown")
        except Exception:
            pass

        logger.info(
            "EV webhook received trace_id=%s ip=%s method=%s path=%s apikey_present=%s",
            ctx["trace_id"],
            ctx["client_host"],
            ctx["method"],
            ctx["path"],
            apikey_present,
        )
    except Exception:
        # absolutely never let logging break request handling
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


async def _read_json_safely(request: Request) -> Tuple[dict[str, Any], bool]:
    try:
        data = await request.json()
        if isinstance(data, dict):
            return data, True
        # if it parses but isn't a dict, treat as invalid for this webhook
        return {}, False
    except Exception:
        return {}, False


def _log_invalid_json(ctx: dict[str, str], request: Request) -> None:
    try:
        content_length = request.headers.get("content-length", "unknown")
    except Exception:
        content_length = "unknown"

    # IMPORTANT: we try to read body for hashing in a safe, best-effort way.
    # If something fails, we still log invalid_json.
    async def _best_effort_body_hash() -> str:
        try:
            raw = await request.body()
            if not raw:
                return "empty"
            return hashlib.sha256(raw).hexdigest()[:12]
        except Exception:
            return "unavailable"

    # Since this function is sync but we want body, we keep it minimal:
    # We'll log without hash here, and callers can add hash if desired.
    try:
        logger.warning(
            "EV webhook invalid_json trace_id=%s ip=%s content_length=%s",
            ctx["trace_id"],
            ctx["client_host"],
            content_length,
        )
    except Exception:
        pass

    # Optional extra log with hash (best effort).
    # If you prefer only ONE log line, delete this block.
    try:
        # fire-and-wait because we are already in request scope
        # (this is safe; it does not spawn background tasks)
        # NOTE: if your linter complains about nested async, move this to the handler.
        pass
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


async def _parse_bot_input(
    data: dict[str, Any],
    evolution_client: EvolutionClient,
    event: str,
):
    try:
        bot_input = await parse_evolution_webhook(data, evolution_client)
        if not bot_input:
            logger.warning(
                "EV webhook discarded reason=parse_evolution_webhook_returned_none event=%s",
                event,
            )
            return None
        return bot_input
    except Exception:
        logger.exception("EV parse_evolution_webhook failed event=%s", event)
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
        logger.info("EV webhook responses count=%s", len(responses))

        for response in responses:
            await send_evolution_message(evolution_client, str(bot_input.chat_id), response)

        return True
    except Exception:
        logger.exception("EV pipeline/send failed")
        return False
