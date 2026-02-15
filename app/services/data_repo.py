from __future__ import annotations

from sqlalchemy import create_engine

from app.core.config import Settings
from app.services.postgres import PostgresRepo, ResilientPostgresRepo
from app.services.repositories import CompositeRepo, DataRepo


def build_data_repo(settings: Settings) -> DataRepo:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")

    connect_args = {}
    if settings.db_schema:
        connect_args["options"] = f"-csearch_path={settings.db_schema}"
    engine = create_engine(settings.database_url, pool_pre_ping=True, connect_args=connect_args)
    primary = ResilientPostgresRepo(PostgresRepo(engine))
    return CompositeRepo(primary=primary, secondary_writers=[])
