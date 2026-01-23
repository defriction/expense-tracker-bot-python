from __future__ import annotations

from fastapi import APIRouter, Request
from telegram import Update
from telegram.ext import Application


def build_telegram_router(telegram_app: Application) -> APIRouter:
    router = APIRouter()

    @router.post("/webhook")
    async def telegram_webhook(request: Request):
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return {"ok": True}

    return router
