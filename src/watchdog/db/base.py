from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class CreatedAtMixin:
    """created_at set by Postgres itself (server_default), not Python —
    so it's correct even if a row is inserted through something other than
    this ORM (e.g. a raw SQL backfill script)."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
