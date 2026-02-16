from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from app.core.config import load_settings
from app.bot.handlers import error_handler, get_handlers, PipelineFactory
from app.routers.telegram import build_telegram_router
from app.services.telegram import build_telegram_app
from app.routers.evolution import build_evolution_router
from app.routers.admin import build_admin_router
from app.services.evolution import EvolutionClient
from app.services.recurring_scheduler import process_recurring_reminders

settings = load_settings()

app = FastAPI()
telegram_app = build_telegram_app(settings.bot_token, get_handlers(), error_handler)
pipeline = PipelineFactory(settings).build()

evolution_client = None
if settings.evolution_api_url and settings.evolution_api_key and settings.evolution_instance_name:
    evolution_client = EvolutionClient(settings)

app.include_router(build_telegram_router(telegram_app, settings))
if evolution_client:
    app.include_router(build_evolution_router(pipeline, evolution_client, settings))
app.include_router(build_admin_router(pipeline._get_repo(), settings))


@app.on_event("startup")
async def on_startup() -> None:
    await telegram_app.initialize()
    scheduler = AsyncIOScheduler(timezone=settings.timezone or "America/Bogota")
    scheduler.add_job(
        process_recurring_reminders,
        CronTrigger(minute=0),
        args=[pipeline._get_repo(), telegram_app.bot, settings, evolution_client],
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    app.state.recurring_scheduler = scheduler


@app.on_event("shutdown")
async def on_shutdown() -> None:
    scheduler = getattr(app.state, "recurring_scheduler", None)
    if scheduler:
        scheduler.shutdown(wait=False)
    await telegram_app.shutdown()
