"""Generates a realistic volume of historical results purely to make
the index-decision measurement (EXPLAIN before/after) honest — with
only a single real night of data, Postgres would correctly choose a
sequential scan regardless of any index, since scanning a few hundred
rows is trivially cheap either way. No provider calls, no real cost;
every row this creates is marked pipeline_git_sha='synthetic-load-test'
so it's trivially identifiable and removable afterward.

Uses one bulk INSERT...SELECT per night (cross-joining the real models
and suite-1 tasks with a repeat-index series) rather than one INSERT
per row — 150 nights this way is ~150 round trips, not ~72,000.

Run with: uv run python scripts/generate_synthetic_load.py --nights 150
Clean up with: uv run python scripts/generate_synthetic_load.py --cleanup
"""

import argparse
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from sqlalchemy import text

from watchdog.db.session import get_engine

LOAD_TEST_MARKER = "synthetic-load-test"


def generate(nights: int) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        suite_id = conn.execute(text("SELECT id FROM suites WHERE version = 1")).scalar()
        if suite_id is None:
            raise RuntimeError("suite version 1 not found — run suite_loader.py first")

        base_time = datetime.now(timezone.utc) - timedelta(days=nights)

        for night in range(nights):
            started_at = base_time + timedelta(days=night)
            run_id = conn.execute(
                text(
                    """
                    INSERT INTO runs (suite_id, status, started_at, finished_at, total_cost_usd, pipeline_git_sha, created_at)
                    VALUES (:suite_id, 'completed', :started_at, :started_at, 0, :marker, :started_at)
                    RETURNING id
                    """
                ),
                {"suite_id": suite_id, "started_at": started_at, "marker": LOAD_TEST_MARKER},
            ).scalar()

            conn.execute(
                text(
                    """
                    INSERT INTO results (
                        run_id, model_id, task_id, repeat_index, output_text, latency_ms,
                        input_tokens, output_tokens, cost_usd, provider_model_version,
                        score, scorer, suite_version, prompt_hash, git_sha, backfilled, created_at
                    )
                    SELECT
                        :run_id,
                        m.id,
                        t.id,
                        rep.repeat_index,
                        'synthetic',
                        (50 + random() * 3000)::int,
                        (10 + random() * 200)::int,
                        (1 + random() * 100)::int,
                        (random() * 0.01)::numeric(14, 10),
                        m.name || '-synthetic',
                        (CASE WHEN random() < 0.9 THEN 1.0 ELSE 0.0 END)::numeric(6, 4),
                        t.scoring_method,
                        1,
                        t.prompt_hash,
                        :marker,
                        false,
                        :started_at
                    FROM models m
                    CROSS JOIN tasks t
                    CROSS JOIN generate_series(0, 2) AS rep(repeat_index)
                    WHERE t.suite_id = :suite_id AND m.active = true
                    """
                ),
                {"run_id": run_id, "suite_id": suite_id, "marker": LOAD_TEST_MARKER, "started_at": started_at},
            )

    print(f"generated {nights} synthetic nights")


def cleanup() -> None:
    engine = get_engine()
    with engine.begin() as conn:
        run_ids = conn.execute(
            text("SELECT id FROM runs WHERE pipeline_git_sha = :marker"), {"marker": LOAD_TEST_MARKER}
        ).scalars().all()
        if not run_ids:
            print("nothing to clean up")
            return
        conn.execute(text("DELETE FROM results WHERE run_id = ANY(:run_ids)"), {"run_ids": run_ids})
        conn.execute(text("DELETE FROM runs WHERE pipeline_git_sha = :marker"), {"marker": LOAD_TEST_MARKER})
    print(f"deleted {len(run_ids)} synthetic runs and their results")


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--nights", type=int, default=150)
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    cleanup() if args.cleanup else generate(args.nights)
