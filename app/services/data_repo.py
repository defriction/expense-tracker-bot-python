from __future__ import annotations

from sqlalchemy import create_engine

from app.core.config import Settings
from app.core.logging import logger
from app.services.postgres import PostgresRepo, ResilientPostgresRepo
from app.services.repositories import CompositeRepo, DataRepo
from app.services.sheets import build_sheets_repo


def build_data_repo(settings: Settings) -> DataRepo:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")

    engine = create_engine(settings.database_url, pool_pre_ping=True)
    primary = ResilientPostgresRepo(PostgresRepo(engine))
    sheets = build_sheets_repo(settings)
    logger.info("Data repo initialized primary=postgres secondary=sheets")
    return CompositeRepo(primary=primary, secondary_writers=[sheets])
