"""Day 2 checkpoint: one real call through the real client, written to
the database with full provenance. Uses actual API credit — this is a
one-time manual verification, not part of the pipeline.

The suite/task rows here are a minimal placeholder: day 3 replaces them
with the real YAML-loaded suite. This script exists only to prove a
results row can be written end to end, with every column populated,
before that machinery exists.

Run with: uv run python scripts/first_real_call.py
"""

import asyncio
import hashlib
import os
from datetime import datetime, timezone
from decimal import Decimal

from dotenv import load_dotenv

from watchdog.db.models import Model, ModelPrice, Provider, Result, Run, Suite, Task
from watchdog.db.session import get_session
from watchdog.git_info import git_sha
from watchdog.providers.client import ProviderClient
from watchdog.providers.pricing import Price

load_dotenv()

PROVIDER_NAME = "openai"
MODEL_NAME = "gpt-4o-mini"
PROMPT = "What is 2 + 2? Answer with just the number."


async def main() -> None:
    session = get_session()
    sha = git_sha()

    provider = session.query(Provider).filter_by(name=PROVIDER_NAME).one()
    model = session.query(Model).filter_by(provider_id=provider.id, name=MODEL_NAME).one()
    price_row = (
        session.query(ModelPrice)
        .filter_by(model_id=model.id)
        .order_by(ModelPrice.effective_from.desc())
        .first()
    )
    price = Price(
        input_per_million=price_row.input_price_per_million,
        output_per_million=price_row.output_price_per_million,
    )

    suite = session.query(Suite).filter_by(version=0).one_or_none()
    if suite is None:
        suite = Suite(version=0, git_sha=sha)
        session.add(suite)
        session.flush()

    prompt_hash = hashlib.sha256(PROMPT.encode()).hexdigest()
    task = session.query(Task).filter_by(suite_id=suite.id, external_id="day2_smoke").one_or_none()
    if task is None:
        task = Task(
            suite_id=suite.id,
            external_id="day2_smoke",
            prompt=PROMPT,
            category="smoke",
            scoring_method="exact",
            expected="4",
            prompt_hash=prompt_hash,
        )
        session.add(task)
        session.flush()

    run = Run(
        suite_id=suite.id,
        status="running",
        started_at=datetime.now(timezone.utc),
        pipeline_git_sha=sha,
    )
    session.add(run)
    session.flush()

    async with ProviderClient(
        PROVIDER_NAME,
        provider.base_url,
        os.environ[provider.credential_env_var],
        concurrency_limit=provider.concurrency_limit,
    ) as client:
        result = await client.call(MODEL_NAME, PROMPT, price)

    print("output:", result.output_text)
    print("latency_ms:", result.latency_ms)
    print("input_tokens:", result.input_tokens)
    print("output_tokens:", result.output_tokens)
    print("cost_usd:", result.cost_usd)
    print("provider_model_version:", result.provider_model_version)
    print("error:", result.error)

    row = Result(
        run_id=run.id,
        model_id=model.id,
        task_id=task.id,
        repeat_index=0,
        output_text=result.output_text,
        latency_ms=result.latency_ms,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        cost_usd=result.cost_usd,
        provider_model_version=result.provider_model_version,
        error=result.error,
        suite_version=suite.version,
        prompt_hash=prompt_hash,
        git_sha=sha,
    )
    session.add(row)

    run.status = "completed"
    run.finished_at = datetime.now(timezone.utc)
    run.total_cost_usd = result.cost_usd or Decimal("0")

    session.commit()
    print("result row id:", row.id)
    session.close()


if __name__ == "__main__":
    asyncio.run(main())
