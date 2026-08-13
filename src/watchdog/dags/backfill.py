"""Separate from nightly_pipeline (PLAN.md 3.2) — re-scores stored
outputs for a task whose scoring method changed, across retained
history, without ever calling a provider again.

The scenario: a task's prompt is unchanged (same prompt_hash) but its
scoring_method was upgraded — e.g. from `graded` to a deterministic
method, once it turns out the answer has a checkable shape after all.
Every historical result recorded against the *old* task definition
with that same prompt_hash gets a new row: same output, same model,
same run, scored under the *new* definition, marked `backfilled=True`
so it's never mistaken for something a live nightly run produced.

cost_usd is always 0 on a backfilled row — no provider call happened
to create it, and counting the source row's original cost again here
would double it in any SUM(cost_usd) grouped by run_id (the cost cap
check, the run's own total_cost_usd). input_tokens/output_tokens/
latency_ms are still copied from the source, as informational
provenance about the original call.

Trigger with a specific task id via dag_run.conf:
  airflow dags trigger backfill --conf '{"task_id": 96}'
"""

from airflow.sdk import dag, task
from airflow.sdk import get_current_context

from watchdog.db.models import Result, Task
from watchdog.db.session import get_session
from watchdog.git_info import git_sha
from watchdog.scoring.scorers import DETERMINISTIC_SCORERS


@dag(schedule=None, catchup=False, tags=["driftwatch"])
def backfill():
    @task
    def backfill_task() -> dict:
        context = get_current_context()
        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        new_task_id = conf.get("task_id")
        if new_task_id is None:
            raise ValueError('backfill requires --conf \'{"task_id": N}\'')

        session = get_session()
        new_task = session.get(Task, new_task_id)
        if new_task is None:
            raise ValueError(f"no task with id {new_task_id}")
        scorer = DETERMINISTIC_SCORERS.get(new_task.scoring_method)
        if scorer is None:
            raise ValueError(
                f"task {new_task_id} has scoring_method={new_task.scoring_method!r} — "
                "backfill only supports deterministic methods, never a provider call"
            )
        suite_version = new_task.suite.version

        # Already-backfilled (run, model, repeat) triples for this
        # specific new task — running this twice must not duplicate.
        already_done = {
            (r.run_id, r.model_id, r.repeat_index)
            for r in session.query(Result.run_id, Result.model_id, Result.repeat_index)
            .filter(Result.task_id == new_task_id)
            .all()
        }

        source_rows = (
            session.query(Result)
            .join(Task, Task.id == Result.task_id)
            .filter(
                Task.prompt_hash == new_task.prompt_hash,
                Result.task_id != new_task_id,
                Result.output_text.isnot(None),
            )
            .all()
        )

        sha = git_sha()
        created = 0
        for src in source_rows:
            key = (src.run_id, src.model_id, src.repeat_index)
            if key in already_done:
                continue

            score_result = scorer(src.output_text, new_task.expected)
            session.add(
                Result(
                    run_id=src.run_id,
                    model_id=src.model_id,
                    task_id=new_task_id,
                    repeat_index=src.repeat_index,
                    output_text=src.output_text,
                    latency_ms=src.latency_ms,
                    input_tokens=src.input_tokens,
                    output_tokens=src.output_tokens,
                    cost_usd=0,  # no provider call happened to make this row
                    provider_model_version=src.provider_model_version,
                    error=src.error,
                    score=score_result.score,
                    scorer=new_task.scoring_method,
                    suite_version=suite_version,
                    prompt_hash=new_task.prompt_hash,
                    git_sha=sha,
                    backfilled=True,
                )
            )
            already_done.add(key)
            created += 1

        session.commit()
        print(f"backfilled {created} rows for task {new_task_id} (scoring_method={new_task.scoring_method})")
        session.close()
        return {"task_id": new_task_id, "created": created}

    backfill_task()


backfill()
