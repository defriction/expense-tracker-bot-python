import os
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import Application

from .handlers import get_handlers

BOT_TOKEN = os.environ["BOT_TOKEN"]

app = FastAPI()

telegram_app = Application.builder().token(BOT_TOKEN).build()

for handler in get_handlers():
    telegram_app.add_handler(handler)

@app.on_event("startup")
async def on_startup() -> None:
    await telegram_app.initialize()

@app.on_event("shutdown")
async def on_shutdown() -> None:
    await telegram_app.shutdown()

@app.post("/webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, telegram_app.bot)
    await telegram_app.process_update(update)
    return {"ok": True}
