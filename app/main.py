from fastapi import FastAPI
from app.core.config import load_settings
from app.core.logging import setup_logging
from app.bot.handlers import error_handler, get_handlers, PipelineFactory
from app.routers.telegram import build_telegram_router
from app.services.telegram import build_telegram_app
from app.routers.evolution import build_evolution_router
from app.services.evolution import EvolutionClient

settings = load_settings()

app = FastAPI()
telegram_app = build_telegram_app(settings.bot_token, get_handlers(), error_handler)
pipeline = PipelineFactory(settings).build()

evolution_client = EvolutionClient(settings)

app.include_router(build_telegram_router(telegram_app, settings))
app.include_router(build_evolution_router(pipeline, evolution_client, settings))


@app.on_event("startup")
async def on_startup() -> None:
    setup_logging()
    await telegram_app.initialize()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await telegram_app.shutdown()
