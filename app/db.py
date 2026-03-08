from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings


class Base(DeclarativeBase):
    """Base class for all ORM models."""



def _build_engine() -> AsyncEngine:
    settings = get_settings()
    return create_async_engine(settings.database_url, echo=False)


engine: AsyncEngine = _build_engine()
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db() -> None:
    """Create database schema if it does not exist."""

    from . import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
