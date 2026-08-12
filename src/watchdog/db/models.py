"""The nine tables from the plan (PLAN.md section 3.4). Money is stored as
Numeric, never float — token costs are small enough that float rounding
error would actually show up in a 90-day cost total.

Deliberately no Postgres ENUM types for things like `runs.status` or
`tasks.scoring_method` — plain String instead. A native ENUM needs an
ALTER TYPE migration every time a new value shows up (e.g. adding a new
run status later), which is friction this project doesn't need yet. A
CHECK constraint would give the same safety without that cost, but even
that's deferred until the set of values actually stabilizes.

No indexes here beyond what correctness requires (the UNIQUE constraint
on `results`, needed for idempotency), except the one on `drift_checks`
added in day 5 — that query pattern (latest checks for a model/category)
is explicit in the plan from day 5 itself, not something to defer. The
performance indexes on `results` are still added later, on day 6, on
purpose — that's when EXPLAIN output proves whether they're needed.
"""

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from watchdog.db.base import Base, CreatedAtMixin


class Provider(Base, CreatedAtMixin):
    __tablename__ = "providers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    base_url: Mapped[str] = mapped_column(Text, nullable=False)
    # Name of the env var holding the API key — never the key itself (F1).
    credential_env_var: Mapped[str] = mapped_column(String(128), nullable=False)
    concurrency_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=5)

    models: Mapped[list["Model"]] = relationship(back_populates="provider")


class Model(Base, CreatedAtMixin):
    __tablename__ = "models"
    __table_args__ = (UniqueConstraint("provider_id", "name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    provider_id: Mapped[int] = mapped_column(ForeignKey("providers.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    provider: Mapped["Provider"] = relationship(back_populates="models")
    prices: Mapped[list["ModelPrice"]] = relationship(back_populates="model")


class ModelPrice(Base, CreatedAtMixin):
    __tablename__ = "model_prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"), nullable=False)
    input_price_per_million: Mapped[Numeric] = mapped_column(Numeric(12, 6), nullable=False)
    output_price_per_million: Mapped[Numeric] = mapped_column(Numeric(12, 6), nullable=False)
    effective_from: Mapped[Date] = mapped_column(Date, nullable=False)

    model: Mapped["Model"] = relationship(back_populates="prices")


class Suite(Base, CreatedAtMixin):
    __tablename__ = "suites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    git_sha: Mapped[str] = mapped_column(String(40), nullable=False)

    tasks: Mapped[list["Task"]] = relationship(back_populates="suite")


class Task(Base, CreatedAtMixin):
    __tablename__ = "tasks"
    __table_args__ = (UniqueConstraint("suite_id", "external_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    suite_id: Mapped[int] = mapped_column(ForeignKey("suites.id"), nullable=False)
    # The human-assigned "id" field from the suite YAML (e.g. "extract_01"),
    # distinct from this row's own surrogate primary key.
    external_id: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    scoring_method: Mapped[str] = mapped_column(String(32), nullable=False)
    expected: Mapped[str | None] = mapped_column(Text, nullable=True)
    rubric: Mapped[str | None] = mapped_column(Text, nullable=True)
    prompt_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    suite: Mapped["Suite"] = relationship(back_populates="tasks")


class Run(Base, CreatedAtMixin):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    suite_id: Mapped[int] = mapped_column(ForeignKey("suites.id"), nullable=False)
    # running | completed | aborted_cost_cap | failed
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Numeric(14, 10): 4 integer digits is far more than a nightly run
    # will ever cost, and 10 decimal digits keeps the sum of many
    # sub-micro-dollar calls exact rather than rounded.
    total_cost_usd: Mapped[Numeric] = mapped_column(Numeric(14, 10), nullable=False, default=0)
    pipeline_git_sha: Mapped[str] = mapped_column(String(40), nullable=False)

    suite: Mapped["Suite"] = relationship()


class Result(Base, CreatedAtMixin):
    __tablename__ = "results"
    __table_args__ = (
        # What makes re-running a date idempotent — enforced by the
        # database, not by application logic (BUILD-WEEK day 1).
        UniqueConstraint("run_id", "model_id", "task_id", "repeat_index"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id"), nullable=False)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"), nullable=False)
    task_id: Mapped[int] = mapped_column(ForeignKey("tasks.id"), nullable=False)
    repeat_index: Mapped[int] = mapped_column(Integer, nullable=False)

    output_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_usd: Mapped[Numeric | None] = mapped_column(Numeric(14, 10), nullable=True)
    provider_model_version: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    score: Mapped[Numeric | None] = mapped_column(Numeric(6, 4), nullable=True)
    scorer: Mapped[str | None] = mapped_column(String(32), nullable=True)
    grader_model_id: Mapped[int | None] = mapped_column(ForeignKey("models.id"), nullable=True)

    # Provenance (N7) — copied at write time rather than joined later, so
    # a result still shows what was true when it ran even if the task's
    # prompt or the suite version moves on afterward.
    suite_version: Mapped[int] = mapped_column(Integer, nullable=False)
    prompt_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    git_sha: Mapped[str] = mapped_column(String(40), nullable=False)

    backfilled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    run: Mapped["Run"] = relationship()
    model: Mapped["Model"] = relationship(foreign_keys=[model_id])
    task: Mapped["Task"] = relationship()
    grader_model: Mapped["Model | None"] = relationship(foreign_keys=[grader_model_id])


class DriftCheck(Base, CreatedAtMixin):
    __tablename__ = "drift_checks"
    __table_args__ = (
        # Serves the plan's own query pattern from day 5: "the most
        # recent checks for this model and category" — history queries
        # and the notifier both filter on (model_id, category) and want
        # the newest rows first.
        Index("ix_drift_checks_model_category_created_at", "model_id", "category", text("created_at DESC")),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_id: Mapped[int] = mapped_column(ForeignKey("models.id"), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    tonight_mean: Mapped[Numeric] = mapped_column(Numeric(12, 6), nullable=False)
    # Null on a cold-start or version-reset row — there's no baseline to
    # report a mean/stdev/delta/z for when no comparison was actually
    # made. A row still gets written either way (every attempt is
    # recorded), but NULL here is what distinguishes "we checked and
    # found nothing" from "we didn't have enough history to check."
    window_mean: Mapped[Numeric | None] = mapped_column(Numeric(12, 6), nullable=True)
    window_stdev: Mapped[Numeric | None] = mapped_column(Numeric(12, 6), nullable=True)
    delta: Mapped[Numeric | None] = mapped_column(Numeric(12, 6), nullable=True)
    z_score: Mapped[Numeric | None] = mapped_column(Numeric(12, 6), nullable=True)
    fired: Mapped[bool] = mapped_column(Boolean, nullable=False)

    model: Mapped["Model"] = relationship()


class Alert(Base, CreatedAtMixin):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    drift_check_id: Mapped[int] = mapped_column(ForeignKey("drift_checks.id"), nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    sent_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    drift_check: Mapped["DriftCheck"] = relationship()
