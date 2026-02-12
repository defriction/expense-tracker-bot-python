from __future__ import annotations

from typing import Optional

import uuid

from fastapi import APIRouter, Header, Request

from app.bot.pipeline import BotPipeline
from app.channels.evolution_adapter import parse_evolution_webhook, send_evolution_message
from app.core.config import Settings
from app.core.logging import logger, set_client_ip, set_log_context, set_trace_id
from app.services.evolution import EvolutionClient


def build_evolution_router(pipeline: BotPipeline, evolution_client: EvolutionClient, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.post("/evolution/webhook")
    async def evolution_webhook(request: Request, apikey: Optional[str] = Header(None)):
        client_host = request.client.host if request.client else "unknown"
        set_trace_id(f"tx-{uuid.uuid4().hex}")
        set_client_ip(client_host)
        set_log_context("evolution", None, None, None)

        if settings.evolution_api_key and apikey != settings.evolution_api_key:
            logger.warning("Evolution webhook unauthorized apikey ip=%s apikey_present=%s", client_host, bool(apikey))
            return {"ok": False, "error": "unauthorized"}

        try:
            data = await request.json()
        except Exception:
            return {"ok": False, "error": "invalid_json"}

        event = (data.get("event") or "").strip().lower().replace("_", ".")
        raw_message = ((data.get("data") or {}).get("message"))
        logger.info("EV webhook start event=%s raw_message=%s", event, raw_message)

        if event == "messages.update":
            logger.debug("EV webhook update payload=%s", data)
            return {"ok": True}
        if event != "messages.upsert":
            return {"ok": True}

        bot_input = await parse_evolution_webhook(data, evolution_client)
        if not bot_input:
            return {"ok": True}
        set_log_context("evolution", bot_input.chat_id, bot_input.user_id, bot_input.message_id)

        try:
            responses = await pipeline.handle_message(bot_input)
            logger.info("EV webhook responses count=%s", len(responses))
            for response in responses:
                await send_evolution_message(evolution_client, str(bot_input.chat_id), response)
        except Exception:
            logger.exception("EV pipeline/send failed")
            return {"ok": True}

        logger.info("EV webhook done")
        return {"ok": True}

    return router
