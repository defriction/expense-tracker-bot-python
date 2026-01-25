from __future__ import annotations

from typing import Any, Dict, List

import httpx
from app.core.config import Settings


class EvolutionClient:
    def __init__(self, settings: Settings) -> None:
        self.base_url = settings.evolution_api_url.rstrip("/")
        self.api_key = settings.evolution_api_key
        self.instance = settings.evolution_instance_name

        self._headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json",
        }

    async def _post(self, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=self._headers, json=payload)
            resp.raise_for_status()
            return resp.json()

    async def send_message(self, to: str, text: str) -> Any:
        payload = {
            "number": to.split("@", 1)[0],
            "text": text,
        }
        return await self._post(
            f"/message/sendText/{self.instance}",
            payload,
        )

    async def send_poll(
        self,
        number: str,
        name: str,
        values: List[str],
        selectable_count: int = 1,
    ) -> Any:
        """
        Payload EXACTO que espera Evolution API
        """
        payload = {
            "number": number,
            "name": name,
            "selectableCount": selectable_count,
            "values": values,
        }
        return await self._post(
            f"/message/sendPoll/{self.instance}",
            payload,
        )
