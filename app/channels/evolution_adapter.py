from __future__ import annotations

import base64
from typing import Any, Dict, Optional
from app.bot.ui_models import BotInput, BotMessage
from app.services.evolution import EvolutionClient

async def send_evolution_message(client: EvolutionClient, to: str, message: BotMessage) -> None:
    if not to:
        return
    
    # Evolution API doesn't support interactive buttons well in some versions/clients
    # We'll use a poll for selection if there's a keyboard, otherwise text.
    if message.keyboard and message.keyboard.rows:
        options = []
        for row in message.keyboard.rows:
            for action in row:
                options.append(action.id) # We use ID as the option text for simplicity in parsing
        
        # Evolution poll is often more reliable than buttons
        await client.send_poll(to, message.text, options)
    else:
        await client.send_message(to, message.text)

def parse_evolution_webhook(data: Dict[str, Any]) -> Optional[BotInput]:
    # Typical Evolution API Webhook structure for MESSAGES_UPSERT
    # { "event": "messages.upsert", "data": { "key": { "remoteJid": "..." }, "message": { ... }, "pushName": "..." } }
    
    event = data.get("event")
    if event != "messages.upsert":
        return None
        
    payload = data.get("data", {})
    key = payload.get("key", {})
    message = payload.get("message", {})
    
    # Ignore self messages
    if key.get("fromMe"):
        return None
        
    remote_jid = key.get("remoteJid", "")
    # remote_jid looks like "573001234567@s.whatsapp.net"
    
    # Extract text from different possible locations
    text = (
        message.get("conversation") or 
        message.get("extendedTextMessage", {}).get("text") or 
        message.get("imageMessage", {}).get("caption") or
        message.get("videoMessage", {}).get("caption") or
        ""
    )
    
    # Check for poll response (Evolution specific)
    # The event might be "poll.update" for poll responses, but let's handle text mostly.
    
    audio_bytes = None
    non_text_type = None
    
    if "audioMessage" in message:
        # Evolution often sends base64 in "base64" field if configured
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
        non_text_type=non_text_type
    )
