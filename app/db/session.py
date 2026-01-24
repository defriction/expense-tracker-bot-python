from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from app.core.config import Settings


@dataclass(frozen=True)
class Database:
    engine: Engine


def build_engine(settings: Settings) -> Engine:
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required")
    return create_engine(settings.database_url, pool_pre_ping=True)


def build_database(settings: Settings) -> Database:
    return Database(engine=build_engine(settings))
