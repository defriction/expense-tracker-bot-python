from __future__ import annotations

import json
from typing import Any, Dict

import httpx

from app.core.config import Settings

GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_TRANSCRIBE_URL = "https://api.groq.com/openai/v1/audio/transcriptions"


async def groq_chat_completion(settings: Settings, prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {settings.groq_api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(GROQ_CHAT_URL, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
    return data["choices"][0]["message"]["content"]


async def groq_transcribe(settings: Settings, audio_bytes: bytes) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {settings.groq_api_key}"}
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


def extract_json(content: str) -> Dict[str, Any]:
    clean = content.replace("```json", "").replace("```", "").strip()
    return json.loads(clean)
