from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from telegram import Update
from telegram.ext import Application

from app.core.config import Settings
from app.core.logging import logger, set_client_ip, set_trace_id
from app.core.rate_limit import rate_limiter


def build_telegram_router(telegram_app: Application, settings: Settings) -> APIRouter:
    router = APIRouter()

    async def _notify_admin(reason: str, client_ip: str) -> None:
        if not settings.admin_telegram_chat_id:
            return
        text = (
            "🚨 <b>Bloqueo por seguridad</b>\n\n"
            f"<b>Motivo:</b> <code>{reason}</code>\n"
            f"<b>IP:</b> <code>{client_ip}</code>"
        )
        try:
            await telegram_app.bot.send_message(
                chat_id=settings.admin_telegram_chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception:
            logger.warning("Failed to notify admin on security block")

    @router.post("/webhook")
    async def telegram_webhook(request: Request):
        client_host = request.client.host if request.client else "unknown"
        set_trace_id(f"wh-{client_host}")
        set_client_ip(client_host)
        if settings.telegram_webhook_secret:
            token = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if token != settings.telegram_webhook_secret:
                logger.warning("Webhook unauthorized token ip=%s", client_host)
                await _notify_admin("unauthorized_webhook", client_host)
                raise HTTPException(status_code=401, detail="Unauthorized")
        if settings.rate_limit_per_ip_per_min > 0:
            if not rate_limiter.allow(f"ip:{client_host}", settings.rate_limit_per_ip_per_min, 60):
                logger.warning("Webhook rate limited ip=%s", client_host)
                await _notify_admin("rate_limited_ip", client_host)
                raise HTTPException(status_code=429, detail="Too Many Requests")
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return {"ok": True}

    return router
