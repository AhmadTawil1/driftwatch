import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


def get_engine():
    url = os.environ["WATCHDOG_DATABASE_URL"]
    return create_engine(url)


_SessionLocal: sessionmaker[Session] | None = None


def get_session() -> Session:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine())
    return _SessionLocal()
