from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class Settings:
    bot_token: str
    groq_api_key: Optional[str]
    google_sheets_id: str
    google_service_account_json: Optional[str]
    google_service_account_file: Optional[str]
    admin_telegram_chat_id: Optional[str]
    database_url: Optional[str]
    timezone: str = "America/Bogota"


DEFAULT_SHEETS_ID = "1IuxBa1o0LyvgoHjpYavcYCx3JSympYQ7gElXLQP8PPI"


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name, default)
    if value is None:
        return None
    value = value.strip()
    return value if value else None


def load_settings() -> Settings:
    bot_token = _get_env("BOT_TOKEN")
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")

    return Settings(
        bot_token=bot_token,
        groq_api_key=_get_env("GROQ_API_KEY"),
        google_sheets_id=_get_env("GOOGLE_SHEETS_ID", DEFAULT_SHEETS_ID) or DEFAULT_SHEETS_ID,
        google_service_account_json=_get_env("GOOGLE_SERVICE_ACCOUNT_JSON"),
        google_service_account_file=_get_env("GOOGLE_SERVICE_ACCOUNT_FILE"),
        admin_telegram_chat_id=_get_env("ADMIN_TELEGRAM_CHAT_ID"),
        database_url=_get_env("DATABASE_URL"),
    )


def load_service_account_info(settings: Settings) -> Dict[str, Any]:
    if settings.google_service_account_json:
        return json.loads(settings.google_service_account_json)

    if settings.google_service_account_file:
        with open(settings.google_service_account_file, "r", encoding="utf-8") as handle:
            return json.load(handle)

    raise RuntimeError(
        "Google service account credentials missing. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE."
    )
