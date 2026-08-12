"""Re-scores stored outputs using the current scorer logic — no
provider calls by default, which is what makes fixing a scoring bug or
improving a scorer free to apply across the entire history (the day 4
"done when" gate: re-score everything without spending a shekel).

Deterministic rows are always re-scored, regardless of whether they
already carry a score — that's the point of "re-score," as opposed to
the DAG's own score stage, which only fills in rows that don't have one
yet. Graded rows are skipped by default, since re-grading genuinely
calls a provider and costs real money; pass --include-graded to opt in
explicitly.

Run with: uv run python rescore.py [--run-id N] [--include-graded]
"""

import argparse
import asyncio
import os
from datetime import date
from decimal import Decimal

from dotenv import load_dotenv
from sqlalchemy.orm import joinedload

from watchdog.db.models import Model, ModelPrice, Result, Task
from watchdog.db.session import get_session
from watchdog.providers.client import ProviderClient
from watchdog.providers.pricing import Price
from watchdog.scoring.graded import GRADER_MODEL_NAME, score_graded
from watchdog.scoring.scorers import DETERMINISTIC_SCORERS


def rescore_deterministic(session, run_id: int | None) -> int:
    query = (
        session.query(Result)
        .join(Task, Task.id == Result.task_id)
        .options(joinedload(Result.task))
        .filter(Task.scoring_method != "graded")
    )
    if run_id is not None:
        query = query.filter(Result.run_id == run_id)

    count = 0
    for row in query.all():
        scorer = DETERMINISTIC_SCORERS.get(row.task.scoring_method)
        if scorer is None:
            continue
        score_result = scorer(row.output_text, row.task.expected)
        row.score = score_result.score
        row.scorer = row.task.scoring_method
        count += 1
    session.commit()
    return count


async def rescore_graded(session, run_id: int | None) -> tuple[int, Decimal]:
    query = (
        session.query(Result)
        .join(Task, Task.id == Result.task_id)
        .options(joinedload(Result.task))
        .filter(Task.scoring_method == "graded")
    )
    if run_id is not None:
        query = query.filter(Result.run_id == run_id)
    rows = query.all()
    if not rows:
        return 0, Decimal("0")

    grader_model = session.query(Model).filter_by(name=GRADER_MODEL_NAME).one()
    provider = grader_model.provider
    today = date.today()
    price_row = (
        session.query(ModelPrice)
        .filter(ModelPrice.model_id == grader_model.id, ModelPrice.effective_from <= today)
        .order_by(ModelPrice.effective_from.desc())
        .first()
    )
    price = Price(price_row.input_price_per_million, price_row.output_price_per_million)

    total_cost = Decimal("0")
    api_key = os.environ[provider.credential_env_var]
    async with ProviderClient(
        provider.name, provider.base_url, api_key, concurrency_limit=provider.concurrency_limit
    ) as client:
        for row in rows:
            graded = await score_graded(row.output_text, row.task.rubric, client, price)
            row.score = graded.score_result.score
            row.scorer = "graded"
            row.grader_model_id = grader_model.id
            if graded.grader_cost_usd:
                total_cost += graded.grader_cost_usd
    session.commit()
    return len(rows), total_cost


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=int, default=None, help="limit to one run; default is the entire history")
    parser.add_argument(
        "--include-graded", action="store_true", help="also re-run graded scoring (spends real money)"
    )
    args = parser.parse_args()

    session = get_session()

    det_count = rescore_deterministic(session, args.run_id)
    print(f"re-scored {det_count} deterministic rows — no provider calls, no cost")

    graded_query = session.query(Result).join(Task, Task.id == Result.task_id).filter(Task.scoring_method == "graded")
    if args.run_id is not None:
        graded_query = graded_query.filter(Result.run_id == args.run_id)
    total_graded_rows = graded_query.count()

    if args.include_graded:
        graded_count, cost = asyncio.run(rescore_graded(session, args.run_id))
        print(f"re-graded {graded_count} rows — cost ${cost}")
    else:
        print(f"skipped {total_graded_rows} graded rows (pass --include-graded to re-grade; this spends real money)")

    session.close()


if __name__ == "__main__":
    load_dotenv()
    main()
