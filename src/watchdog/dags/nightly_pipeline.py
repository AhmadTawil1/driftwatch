"""load_registry -> execute (one mapped task per active model) ->
score_deterministic -> score_graded -> detect_drift -> notify -> finalize_run.

All five stages from the plan's architecture (PLAN.md 3.1) are now
wired in. Scoring never re-calls the model *under test* once its
result is written. Graded scoring is the one scoring stage that does
call a provider — the pinned grader — which is exactly why it runs as
its own stage, after deterministic scoring, so its cost stays
separable (day 4). detect_drift and notify never call a provider at
all — they only ever read back what scoring already wrote (day 5) and
what detect_drift already decided (day 6).

Idempotent per calendar date (N1): load_registry refuses to start a
second run for a date that already has a completed or aborted_cost_cap
run, raising AirflowSkipException — which propagates as a clean skip
to execute, not a failure, and lets the rest of the DAG's all_done
stages no-op safely on the empty result.
"""

import asyncio
import os
from datetime import date, datetime, timezone
from decimal import Decimal

from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from watchdog.db.models import Model, ModelPrice, Provider, Result, Run, Suite, Task
from watchdog.db.session import get_session
from watchdog.drift.detector import detect_for_run
from watchdog.git_info import git_sha
from watchdog.notify.notifier import notify_fired_checks
from watchdog.providers.client import ProviderClient
from watchdog.providers.pricing import Price
from watchdog.scoring.graded import GRADER_MODEL_NAME, score_graded
from watchdog.scoring.scorers import DETERMINISTIC_SCORERS

REPEATS = 3


def _current_total(session, run_id: int) -> Decimal:
    total = (
        session.query(func.coalesce(func.sum(Result.cost_usd), 0))
        .filter(Result.run_id == run_id)
        .scalar()
    )
    return Decimal(total)


def _mark_aborted(session, run_id: int) -> None:
    session.query(Run).filter(Run.id == run_id, Run.status == "running").update(
        {"status": "aborted_cost_cap"}
    )
    session.commit()


@dag(schedule=None, catchup=False, tags=["driftwatch"])
def nightly_pipeline():
    @task
    def load_registry() -> list[dict]:
        session = get_session()
        today = date.today()

        # Idempotency (N1): a finished run for today already means
        # tonight happened. Without this, re-triggering (by accident,
        # or via Airflow's own rerun/backfill tooling) would create a
        # second parallel run and spend a second night's cost for the
        # same night — the unique constraint on results only stops
        # duplicate rows *within* one run, not a whole second run.
        already_ran = (
            session.query(Run)
            .filter(func.date(Run.started_at) == today, Run.status.in_(["completed", "aborted_cost_cap"]))
            .first()
        )
        if already_ran is not None:
            session.close()
            raise AirflowSkipException(
                f"a run for {today} already exists (run_id={already_ran.id}, status={already_ran.status}) — skipping"
            )

        suite = session.query(Suite).order_by(Suite.version.desc()).first()
        if suite is None:
            raise RuntimeError("no suite loaded — run suite_loader.py first")

        run = Run(
            suite_id=suite.id,
            status="running",
            started_at=datetime.now(timezone.utc),
            pipeline_git_sha=git_sha(),
        )
        session.add(run)
        session.commit()

        tasks = [
            {"id": t.id, "prompt": t.prompt, "prompt_hash": t.prompt_hash}
            for t in suite.tasks
        ]

        work_items = []
        models = session.query(Model).join(Provider).filter(Model.active.is_(True)).all()
        for model in models:
            price_row = (
                session.query(ModelPrice)
                .filter(ModelPrice.model_id == model.id, ModelPrice.effective_from <= today)
                .order_by(ModelPrice.effective_from.desc())
                .first()
            )
            if price_row is None:
                # No price on file as of today — skip rather than guess a cost.
                continue

            work_items.append(
                {
                    "run_id": run.id,
                    "suite_version": suite.version,
                    "model_id": model.id,
                    "model_name": model.name,
                    "provider_name": model.provider.name,
                    "provider_base_url": model.provider.base_url,
                    "provider_credential_env_var": model.provider.credential_env_var,
                    "provider_concurrency_limit": model.provider.concurrency_limit,
                    "input_price_per_million": str(price_row.input_price_per_million),
                    "output_price_per_million": str(price_row.output_price_per_million),
                    "tasks": tasks,
                    "git_sha": run.pipeline_git_sha,
                }
            )

        session.close()
        if not work_items:
            raise RuntimeError("no active models with a current price — nothing to run")
        return work_items

    @task
    def execute(model_work: dict) -> dict:
        return asyncio.run(_execute_async(model_work))

    async def _execute_async(model_work: dict) -> dict:
        session = get_session()
        run_id = model_work["run_id"]
        model_id = model_work["model_id"]
        price = Price(
            input_per_million=Decimal(model_work["input_price_per_million"]),
            output_per_million=Decimal(model_work["output_price_per_million"]),
        )
        cost_cap = os.environ.get("NIGHTLY_COST_CAP_USD")
        cost_cap_usd = Decimal(cost_cap) if cost_cap else None

        api_key = os.environ[model_work["provider_credential_env_var"]]
        written = 0
        skipped_cost_cap = 0

        async with ProviderClient(
            model_work["provider_name"],
            model_work["provider_base_url"],
            api_key,
            concurrency_limit=model_work["provider_concurrency_limit"],
        ) as client:
            for task_def in model_work["tasks"]:
                for repeat_index in range(REPEATS):
                    if cost_cap_usd is not None and _current_total(session, run_id) >= cost_cap_usd:
                        skipped_cost_cap += 1
                        continue

                    # One bad call must not kill this model's whole
                    # dispatch loop (N8) — client.call() already turns
                    # provider-side failures into a CallResult with
                    # `error` set rather than raising, so this only
                    # catches genuinely unexpected failures (e.g. a bug,
                    # or the DB write itself failing).
                    try:
                        result = await client.call(model_work["model_name"], task_def["prompt"], price)
                        error_text = result.error
                    except Exception as exc:  # noqa: BLE001
                        result = None
                        error_text = f"{type(exc).__name__}: {exc}"

                    row = Result(
                        run_id=run_id,
                        model_id=model_id,
                        task_id=task_def["id"],
                        repeat_index=repeat_index,
                        output_text=result.output_text if result else None,
                        latency_ms=result.latency_ms if result else None,
                        input_tokens=result.input_tokens if result else None,
                        output_tokens=result.output_tokens if result else None,
                        cost_usd=result.cost_usd if result else None,
                        provider_model_version=result.provider_model_version if result else None,
                        error=error_text,
                        suite_version=model_work["suite_version"],
                        prompt_hash=task_def["prompt_hash"],
                        git_sha=model_work["git_sha"],
                    )
                    session.add(row)
                    session.commit()  # written on completion, never batched
                    written += 1

                    if cost_cap_usd is not None and _current_total(session, run_id) >= cost_cap_usd:
                        _mark_aborted(session, run_id)

        session.close()
        return {"run_id": run_id, "model_id": model_id, "written": written, "skipped_cost_cap": skipped_cost_cap}

    @task(trigger_rule="all_done")
    def score_deterministic(execute_results: list[dict]) -> dict:
        results = [r for r in execute_results if r]
        if not results:
            return {"run_id": None, "scored": 0}
        run_id = results[0]["run_id"]

        session = get_session()
        unscored = (
            session.query(Result)
            .join(Task, Task.id == Result.task_id)
            .options(joinedload(Result.task))
            .filter(Result.run_id == run_id, Result.score.is_(None), Task.scoring_method != "graded")
            .all()
        )
        scored_count = 0
        for row in unscored:
            scorer = DETERMINISTIC_SCORERS.get(row.task.scoring_method)
            if scorer is None:
                continue
            score_result = scorer(row.output_text, row.task.expected)
            row.score = score_result.score
            row.scorer = row.task.scoring_method
            scored_count += 1
        session.commit()
        session.close()
        print(f"run {run_id}: scored {scored_count} deterministic rows — no provider calls")
        return {"run_id": run_id, "scored": scored_count}

    @task(trigger_rule="all_done")
    def score_graded_stage(deterministic_result: dict) -> dict:
        run_id = deterministic_result.get("run_id")
        if run_id is None:
            return {"run_id": None, "graded": 0, "cost_usd": "0"}

        session = get_session()
        unscored = (
            session.query(Result)
            .join(Task, Task.id == Result.task_id)
            .options(joinedload(Result.task))
            .filter(Result.run_id == run_id, Result.score.is_(None), Task.scoring_method == "graded")
            .all()
        )
        if not unscored:
            session.close()
            return {"run_id": run_id, "graded": 0, "cost_usd": "0"}

        grader_model = session.query(Model).filter_by(name=GRADER_MODEL_NAME).one()
        provider = grader_model.provider
        today = date.today()
        price_row = (
            session.query(ModelPrice)
            .filter(ModelPrice.model_id == grader_model.id, ModelPrice.effective_from <= today)
            .order_by(ModelPrice.effective_from.desc())
            .first()
        )
        grader_price = Price(price_row.input_price_per_million, price_row.output_price_per_million)

        total_cost = Decimal("0")
        graded_count = 0

        async def _grade_all() -> None:
            nonlocal total_cost, graded_count
            api_key = os.environ[provider.credential_env_var]
            async with ProviderClient(
                provider.name, provider.base_url, api_key, concurrency_limit=provider.concurrency_limit
            ) as client:
                for row in unscored:
                    graded = await score_graded(row.output_text, row.task.rubric, client, grader_price)
                    row.score = graded.score_result.score
                    row.scorer = "graded"
                    row.grader_model_id = grader_model.id
                    if graded.grader_cost_usd:
                        total_cost += graded.grader_cost_usd
                    graded_count += 1

        asyncio.run(_grade_all())
        session.commit()
        session.close()
        print(f"run {run_id}: graded {graded_count} rows — cost ${total_cost} (grader: {GRADER_MODEL_NAME})")
        return {"run_id": run_id, "graded": graded_count, "cost_usd": str(total_cost)}

    @task(trigger_rule="all_done")
    def detect_drift(graded_result: dict) -> dict:
        run_id = graded_result.get("run_id")
        if run_id is None:
            return {**graded_result, "comparisons": 0, "fired": 0}

        session = get_session()
        comparisons = detect_for_run(session, run_id)
        session.close()

        fired = sum(1 for c in comparisons if c.fired)
        print(f"run {run_id}: {len(comparisons)} drift comparisons, {fired} fired")
        return {**graded_result, "comparisons": len(comparisons), "fired": fired}

    @task(trigger_rule="all_done")
    def notify(drift_result: dict) -> dict:
        session = get_session()
        sent = notify_fired_checks(session)
        session.close()
        print(f"notify: {sent} alerts sent")
        return {**drift_result, "alerts_sent": sent}

    @task(trigger_rule="all_done")
    def finalize_run(notify_result: dict) -> None:
        run_id = notify_result.get("run_id")
        if run_id is None:
            return

        session = get_session()
        run = session.get(Run, run_id)
        task_cost = _current_total(session, run_id)
        grading_cost = Decimal(notify_result.get("cost_usd") or "0")
        run.total_cost_usd = task_cost + grading_cost
        run.finished_at = datetime.now(timezone.utc)
        if run.status == "running":
            run.status = "completed"
        session.commit()

        print(
            f"run {run_id}: status={run.status} total_cost_usd={run.total_cost_usd} "
            f"(task_cost={task_cost} grading_cost={grading_cost}) "
            f"drift_fired={notify_result.get('fired')}/{notify_result.get('comparisons')} "
            f"alerts_sent={notify_result.get('alerts_sent')}"
        )
        session.close()

    execute_results = execute.expand(model_work=load_registry())
    finalize_run(notify(detect_drift(score_graded_stage(score_deterministic(execute_results)))))


nightly_pipeline()
