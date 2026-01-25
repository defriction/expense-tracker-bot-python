from __future__ import annotations

from fastapi import APIRouter, Request, Header
from typing import Optional
from app.core.config import Settings
from app.core.logging import logger, set_client_ip, set_trace_id
from app.bot.pipeline import BotPipeline
from app.channels.evolution_adapter import parse_evolution_webhook, send_evolution_message
from app.services.evolution import EvolutionClient

def build_evolution_router(pipeline: BotPipeline, evolution_client: EvolutionClient, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.post("/evolution/webhook")
    async def evolution_webhook(request: Request, apikey: Optional[str] = Header(None)):
        client_host = request.client.host if request.client else "unknown"
        set_trace_id(f"ev-{client_host}")
        set_client_ip(client_host)
        
        # Security check: Evolution can send apikey in headers
        if settings.evolution_api_key and apikey != settings.evolution_api_key:
            logger.warning("Evolution webhook unauthorized apikey ip=%s", client_host)
            # Not raising 401 to avoid Evolution retries if misconfigured
            return {"ok": False, "error": "unauthorized"}

        try:
            data = await request.json()
        except Exception:
            return {"ok": False, "error": "invalid_json"}

        event = data.get("event")
        # Handle message upsert (new messages)
        if event == "messages.upsert":
            bot_input = parse_evolution_webhook(data)
            if not bot_input:
                return {"ok": True}
            
            logger.info("Evolution message received from=%s", bot_input.chat_id)
            
            responses = await pipeline.handle_message(bot_input)
            for response in responses:
                await send_evolution_message(evolution_client, str(bot_input.chat_id), response)
                
        # Handle poll updates (button-like selection)
        elif event == "messages.update":
            # This is where poll responses (votes) often land in Evolution
            # Detailed implementation would be needed to extract the vote
            # For now, we focus on text and audio.
            pass

        return {"ok": True}

    return router
