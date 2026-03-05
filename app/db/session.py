from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base


_engine = None
SessionLocal = None


def get_engine():
    global _engine, SessionLocal
    if _engine is None:
        from app.settings import get_settings

        settings = get_settings()
        _engine = create_engine(settings.database_url, future=True)
        SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    return _engine


def create_schema() -> None:
    get_engine()
    import app.db.models  # noqa: F401
    Base.metadata.create_all(bind=_engine)


@contextmanager
def get_db() -> Iterator:
    if SessionLocal is None:
        get_engine()
    assert SessionLocal is not None
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_db_session() -> Iterator:
    with get_db() as db:
        yield db
