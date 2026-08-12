"""Rolling-window drift detection: per model x category, compare
tonight's mean score against the trailing window's own mean and
standard deviation — normalized by the window's own variance, not a
fixed percentage, because a naturally noisy category should need a
bigger move to fire than a naturally stable one.

"Tonight" here means "one run" — in production there's exactly one run
per calendar night, so a trailing window of runs and a trailing window
of nights are the same thing.
"""

from dataclasses import dataclass
from datetime import datetime
from statistics import mean, stdev

from sqlalchemy import func
from sqlalchemy.orm import Session

from watchdog.db.models import DriftCheck, Result, Run, Task

WINDOW_NIGHTS = 14
MIN_NIGHTS_FOR_BASELINE = 7
# Tuned against synthetic drift/noise data in the next section — this
# is a starting value, not a final one.
Z_THRESHOLD = 3.0
# A window with zero historical variance would otherwise divide by
# zero; flooring it keeps z finite (and very large) instead of NaN.
STDEV_EPSILON = 1e-6

METRIC_NAME = "mean_score"


@dataclass
class DriftComparison:
    model_id: int
    category: str
    metric: str
    tonight_mean: float
    window_mean: float | None
    window_stdev: float | None
    delta: float | None
    z_score: float | None
    fired: bool
    reason: str  # "compared" | "cold_start" | "version_change_reset"


def _nightly_aggregate(session: Session, model_id: int, category: str, run_id: int):
    """One row: this run's mean score and dominant provider_model_version
    for this model x category. None if there's nothing scored yet."""
    return (
        session.query(
            func.avg(Result.score).label("mean_score"),
            func.mode().within_group(Result.provider_model_version).label("version"),
        )
        .join(Task, Task.id == Result.task_id)
        .filter(
            Result.run_id == run_id,
            Result.model_id == model_id,
            Task.category == category,
            Result.score.isnot(None),
        )
        .one()
    )


def _prior_nights(session: Session, model_id: int, category: str, before: datetime):
    """Up to WINDOW_NIGHTS previous runs for this model x category,
    most recent first — each with its own mean score and dominant
    version, exactly like _nightly_aggregate but for many runs at once."""
    return (
        session.query(
            Result.run_id,
            Run.started_at,
            func.avg(Result.score).label("mean_score"),
            func.mode().within_group(Result.provider_model_version).label("version"),
        )
        .join(Run, Run.id == Result.run_id)
        .join(Task, Task.id == Result.task_id)
        .filter(
            Result.model_id == model_id,
            Task.category == category,
            Result.score.isnot(None),
            Run.started_at < before,
        )
        .group_by(Result.run_id, Run.started_at)
        .order_by(Run.started_at.desc())
        .limit(WINDOW_NIGHTS)
        .all()
    )


def compare(session: Session, model_id: int, category: str, run_id: int) -> DriftComparison:
    tonight = _nightly_aggregate(session, model_id, category, run_id)
    if tonight.mean_score is None:
        raise ValueError(f"no scored results for model={model_id} category={category} run={run_id}")
    tonight_mean = float(tonight.mean_score)
    tonight_version = tonight.version

    run = session.get(Run, run_id)
    candidates = _prior_nights(session, model_id, category, run.started_at)

    # Walk backward from most recent; stop at the first version mismatch
    # so an old baseline never gets averaged in with a new one.
    window = []
    for night in candidates:
        if night.version != tonight_version:
            break
        window.append(night)

    if len(window) < MIN_NIGHTS_FOR_BASELINE:
        reason = "version_change_reset" if len(window) < len(candidates) else "cold_start"
        return DriftComparison(
            model_id=model_id,
            category=category,
            metric=METRIC_NAME,
            tonight_mean=tonight_mean,
            window_mean=None,
            window_stdev=None,
            delta=None,
            z_score=None,
            fired=False,
            reason=reason,
        )

    window_scores = [float(n.mean_score) for n in window]
    window_mean = mean(window_scores)
    window_stdev = stdev(window_scores)
    delta = tonight_mean - window_mean
    z_score = delta / max(window_stdev, STDEV_EPSILON)
    fired = abs(z_score) >= Z_THRESHOLD

    return DriftComparison(
        model_id=model_id,
        category=category,
        metric=METRIC_NAME,
        tonight_mean=tonight_mean,
        window_mean=window_mean,
        window_stdev=window_stdev,
        delta=delta,
        z_score=z_score,
        fired=fired,
        reason="compared",
    )


def detect_for_run(session: Session, run_id: int) -> list[DriftComparison]:
    """Every (model, category) pair with scored results in this run,
    compared and written as a drift_checks row — fired or not, cold
    start or not. Recording the non-events is what makes the detector's
    false-positive rate measurable later (day 5, "prove it works")."""
    pairs = (
        session.query(Result.model_id, Task.category)
        .join(Task, Task.id == Result.task_id)
        .filter(Result.run_id == run_id, Result.score.isnot(None))
        .distinct()
        .all()
    )

    comparisons = []
    for model_id, category in pairs:
        comparison = compare(session, model_id, category, run_id)
        session.add(
            DriftCheck(
                model_id=comparison.model_id,
                category=comparison.category,
                metric=comparison.metric,
                tonight_mean=comparison.tonight_mean,
                window_mean=comparison.window_mean,
                window_stdev=comparison.window_stdev,
                delta=comparison.delta,
                z_score=comparison.z_score,
                fired=comparison.fired,
            )
        )
        comparisons.append(comparison)

    session.commit()
    return comparisons
