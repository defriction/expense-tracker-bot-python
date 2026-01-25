from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import httpx

from app.core.config import Settings
from app.core.logging import logger


class EvolutionClient:
    """
    Retrocompatible:
      - EvolutionClient(settings)
      - EvolutionClient(base_url, api_key, instance_name)
    """

    def __init__(
        self,
        settings_or_base_url: Union[Settings, str],
        api_key: Optional[str] = None,
        instance_name: Optional[str] = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        if isinstance(settings_or_base_url, Settings):
            settings = settings_or_base_url
            self.base_url = settings.evolution_api_url.rstrip("/")
            self.api_key = settings.evolution_api_key
            self.instance_name = settings.evolution_instance_name
        else:
            base_url = settings_or_base_url
            if not api_key or not instance_name:
                raise TypeError("EvolutionClient(base_url, api_key, instance_name) requires api_key and instance_name")
            self.base_url = base_url.rstrip("/")
            self.api_key = api_key
            self.instance_name = instance_name

        self.timeout_seconds = timeout_seconds
        self._headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            resp = await client.post(url, headers=self._headers, json=payload)

        if resp.status_code >= 400:
            logger.error(
                "Evolution API error status=%s url=%s response=%s payload=%s",
                resp.status_code,
                url,
                resp.text[:3000],
                payload,
            )
            resp.raise_for_status()

        data = resp.json()
        return data if isinstance(data, dict) else {"data": data}

    async def send_text(
        self,
        to: str,
        text: str,
        *,
        link_preview: bool = False,
    ) -> Dict[str, Any]:
        """
        Body v2 común:
          { "number": "<jid o +E164>", "text": "..." }
        """
        payload: Dict[str, Any] = {
            "number": to,
            "text": text,
            "linkPreview": link_preview,
        }
        return await self._post(f"message/sendText/{self.instance_name}", payload)

    async def send_poll(
        self,
        to: str,
        name: str,
        values: List[str],
        *,
        selectable_count: int = 1,
    ) -> Dict[str, Any]:
        """
        Body típico:
          { "number": "<jid o +E164>", "name": "...", "values": [...], "selectableCount": 1 }
        """
        payload: Dict[str, Any] = {
            "number": to,
            "name": name,
            "values": values,
            "selectableCount": selectable_count,
        }
        return await self._post(f"message/sendPoll/{self.instance_name}", payload)

    # Alias para compatibilidad
    async def send_message(self, to: str, text: str) -> Dict[str, Any]:
        return await self.send_text(to, text, link_preview=False)
