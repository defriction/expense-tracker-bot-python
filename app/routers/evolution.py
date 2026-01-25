from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Header, Request

from app.bot.pipeline import BotPipeline
from app.channels.evolution_adapter import parse_evolution_webhook, send_evolution_message
from app.core.config import Settings
from app.core.logging import logger, set_client_ip, set_trace_id
from app.services.evolution import EvolutionClient


def build_evolution_router(pipeline: BotPipeline, evolution_client: EvolutionClient, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.post("/evolution/webhook")
    async def evolution_webhook(request: Request, apikey: Optional[str] = Header(None)):
        client_host = request.client.host if request.client else "unknown"
        set_trace_id(f"ev-{client_host}")
        set_client_ip(client_host)

        if settings.evolution_api_key and apikey != settings.evolution_api_key:
            logger.warning("Evolution webhook unauthorized apikey ip=%s apikey_present=%s", client_host, bool(apikey))
            return {"ok": False, "error": "unauthorized"}

        try:
            data = await request.json()
            # ===== EV DEBUG (RAW WEBHOOK) =====
            try:
                payload = data.get("data") or {}
                key = (payload.get("key") or {}) if isinstance(payload, dict) else {}
                logger.warning("EV DEBUG raw_event=%s", data.get("event"))
                logger.warning("EV DEBUG raw_key=%s", key)
                if isinstance(payload, dict):
                    logger.warning("EV DEBUG raw_payload_keys=%s", list(payload.keys()))
                    logger.warning(
                        "EV DEBUG ids remoteJid=%s participant(key)=%s participant(payload)=%s sender=%s senderJid=%s from=%s",
                        key.get("remoteJid"),
                        key.get("participant"),
                        payload.get("participant"),
                        payload.get("sender"),
                        payload.get("senderJid"),
                        payload.get("from"),
                    )
            except Exception:
                logger.exception("EV DEBUG logging failed")
            # ==================================

        except Exception:
            return {"ok": False, "error": "invalid_json"}

        event = (data.get("event") or "").strip().lower().replace("_", ".")
        logger.warning("EV webhook event=%s", event)

        if event != "messages.upsert":
            return {"ok": True}

        bot_input = parse_evolution_webhook(data)
        if not bot_input:
            return {"ok": True}

        try:
            responses = await pipeline.handle_message(bot_input)
            for response in responses:
                await send_evolution_message(evolution_client, str(bot_input.chat_id), response)
        except Exception:
            logger.exception("EV pipeline/send failed")
            return {"ok": True}

        return {"ok": True}

    return router
