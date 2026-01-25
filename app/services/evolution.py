from __future__ import annotations

import httpx
from typing import Any, Dict, Optional
from app.core.config import Settings
from app.core.logging import logger

class EvolutionClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base_url = settings.evolution_api_url
        self.api_key = settings.evolution_api_key
        self.instance = settings.evolution_instance_name

    def _get_headers(self) -> Dict[str, str]:
        return {
            "apikey": self.api_key or "",
            "Content-Type": "application/json"
        }

    async def send_message(self, to: str, text: str) -> bool:
        if not self.base_url or not self.instance:
            logger.error("Evolution API URL or Instance name not configured")
            return False
        
        url = f"{self.base_url}/message/sendText/{self.instance}"
        payload = {
            "number": to,
            "options": {
                "delay": 1200,
                "presence": "composing",
                "linkPreview": False
            },
            "text": text
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=self._get_headers())
                response.raise_for_status()
                return True
        except Exception as exc:
            logger.error("Failed to send Evolution message: %s", exc)
            return False

    async def send_poll(self, to: str, text: str, options: list[str]) -> bool:
        """Evolution API poll as a replacement for buttons (which are often blocked/buggy)"""
        if not self.base_url or not self.instance:
            return False
        
        url = f"{self.base_url}/message/sendPoll/{self.instance}"
        payload = {
            "number": to,
            "poll": {
                "name": text,
                "selectableOptionsCount": 1,
                "values": options
            }
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, headers=self._get_headers())
                response.raise_for_status()
                return True
        except Exception as exc:
            logger.error("Failed to send Evolution poll: %s", exc)
            return False

    async def download_media(self, message_payload: Dict[str, Any]) -> Optional[bytes]:
        """Evolution API base64 media downloader"""
        if not self.base_url or not self.instance:
            return None
            
        # Evolution usually provides base64 in the webhook for some media types
        # or we might need to fetch it.
        # For now, let's assume we handle base64 from the webhook if available.
        # This is a placeholder for actual media fetch if needed.
        return None
