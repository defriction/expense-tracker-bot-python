from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import httpx

from app.core.logging import logger


@dataclass(frozen=True)
class EvolutionClient:
    base_url: str
    api_key: str
    instance_name: str
    timeout_seconds: float = 20.0

    async def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"
        headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            resp = await client.post(url, headers=headers, json=payload)

        # Si falla, loguea el body para que el 400 te diga qué campo faltó
        if resp.status_code >= 400:
            body = resp.text
            logger.error(
                "Evolution API error status=%s url=%s response=%s payload=%s",
                resp.status_code,
                url,
                body[:2000],
                payload,
            )
            resp.raise_for_status()

        data = resp.json()
        return data if isinstance(data, dict) else {"data": data}

    async def send_text(
        self,
        number_or_jid: str,
        text: str,
        *,
        delay_ms: int = 0,
        presence: str = "composing",
        link_preview: bool = False,
    ) -> Dict[str, Any]:
        payload = {
            "number": number_or_jid,
            "textMessage": {"text": text},
            "options": {
                "delay": delay_ms,
                "presence": presence,
                "linkPreview": link_preview,
            },
        }
        return await self._post(f"message/sendText/{self.instance_name}", payload)

    async def send_poll(
        self,
        number_or_jid: str,
        name: str,
        values: list[str],
        *,
        selectable_count: int = 1,
        delay_ms: int = 0,
        presence: str = "composing",
    ) -> Dict[str, Any]:
        payload = {
            "number": number_or_jid,
            "name": name,
            "selectableCount": selectable_count,
            "values": values,
            "options": {
                "delay": delay_ms,
                "presence": presence,
            },
        }
        return await self._post(f"message/sendPoll/{self.instance_name}", payload)
