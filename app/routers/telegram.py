from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from telegram import Update
from telegram.ext import Application

from app.core.config import Settings
from app.core.rate_limit import rate_limiter


def build_telegram_router(telegram_app: Application, settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.post("/webhook")
    async def telegram_webhook(request: Request):
        if settings.telegram_webhook_secret:
            token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if token != settings.telegram_webhook_secret:
                raise HTTPException(status_code=401, detail="Unauthorized")
        client_host = request.client.host if request.client else "unknown"
        if settings.rate_limit_per_ip_per_min > 0:
            if not rate_limiter.allow(f"ip:{client_host}", settings.rate_limit_per_ip_per_min, 60):
                raise HTTPException(status_code=429, detail="Too Many Requests")
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return {"ok": True}

    return router
