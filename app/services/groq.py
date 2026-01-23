from __future__ import annotations

import logging
from typing import Any, Dict

import httpx

from app.core.config import Settings
from app.core.circuit_breaker import CircuitBreaker, guarded_call
from app.core.retry import async_retry
from app.core.logging import logger

GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_TRANSCRIBE_URL = "https://api.groq.com/openai/v1/audio/transcriptions"


class GroqClient:
    def __init__(self, settings: Settings, retries: int = 2, backoff_seconds: float = 0.5) -> None:
        self.settings = settings
        self._breaker = CircuitBreaker(on_state_change=self._on_breaker_change)
        self._retries = retries
        self._backoff = backoff_seconds

    def _on_breaker_change(self, old: str, new: str) -> None:
        logger.warning("Groq circuit breaker transition %s -> %s", old, new)

    async def chat_completion(self, system_prompt: str, user_message: str) -> str:
        if not self.settings.groq_api_key:
            raise RuntimeError("GROQ_API_KEY is required for AI parsing")

        async def do_call():
            if not self._breaker.allow():
                raise RuntimeError("Groq circuit breaker is open")
            headers = {
                "Authorization": f"Bearer {self.settings.groq_api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message},
                ],
                "temperature": 0,
            }
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(GROQ_CHAT_URL, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
            self._breaker.record_success()
            return data["choices"][0]["message"]["content"]

        async def wrapped():
            try:
                return await do_call()
            except Exception as exc:
                self._breaker.record_failure()
                raise exc

        return await async_retry(
            wrapped,
            retries=self._retries,
            backoff_seconds=self._backoff,
            on_retry=lambda attempt, exc: logger.warning(
                "Groq chat retry (attempt %s/%s): %s", attempt, self._retries + 1, exc
            ),
        )

    async def transcribe(self, audio_bytes: bytes) -> Dict[str, Any]:
        if not self.settings.groq_api_key:
            raise RuntimeError("GROQ_API_KEY is required for audio transcription")

        async def do_call():
            if not self._breaker.allow():
                raise RuntimeError("Groq circuit breaker is open")
            headers = {"Authorization": f"Bearer {self.settings.groq_api_key}"}
            files = {
                "file": ("audio.ogg", audio_bytes, "audio/ogg"),
            }
            data = {
                "model": "whisper-large-v3",
                "response_format": "json",
            }
            async with httpx.AsyncClient(timeout=60) as client:
                response = await client.post(GROQ_TRANSCRIBE_URL, headers=headers, files=files, data=data)
                response.raise_for_status()
                return response.json()

        async def wrapped():
            try:
                result = await do_call()
            except Exception as exc:
                self._breaker.record_failure()
                raise exc
            self._breaker.record_success()
            return result

        return await async_retry(
            wrapped,
            retries=self._retries,
            backoff_seconds=self._backoff,
            on_retry=lambda attempt, exc: logger.warning(
                "Groq transcribe retry (attempt %s/%s): %s", attempt, self._retries + 1, exc
            ),
        )


def extract_json(content: str) -> Dict[str, Any]:
    clean = content.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)
