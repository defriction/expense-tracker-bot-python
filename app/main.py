from fastapi import FastAPI
from app.core.config import load_settings
from app.bot.handlers import error_handler, get_handlers
from app.routers.telegram import build_telegram_router
from app.services.telegram import build_telegram_app

settings = load_settings()

app = FastAPI()
telegram_app = build_telegram_app(settings.bot_token, get_handlers(), error_handler)

app.include_router(build_telegram_router(telegram_app, settings))


@app.on_event("startup")
async def on_startup() -> None:
    await telegram_app.initialize()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await telegram_app.shutdown()
