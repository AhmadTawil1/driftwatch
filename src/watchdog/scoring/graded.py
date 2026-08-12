"""Model-graded scoring: a pinned grader model applies a task's rubric
to a stored output. Unlike the four deterministic scorers, this is not
free — it's a real provider call — which is exactly why it runs as its
own separate batch, after deterministic scoring, so its cost stays
visible and separable from the nightly run's per-task cost.

The grader is not a special fifth model bolted on the side — it's one
of the models already registered and evaluated by the suite like any
other. If every graded category moves at once, across every model
under test, that's a grader event, not a model event, and this is how
you can tell (PLAN.md 6.1).
"""

from dataclasses import dataclass
from decimal import Decimal

from watchdog.providers.client import ProviderClient
from watchdog.providers.pricing import Price
from watchdog.scoring.scorers import ScoreResult

GRADER_MODEL_NAME = "gpt-4o"

_GRADING_PROMPT_TEMPLATE = """You are grading whether a response satisfies a rubric. Respond with ONLY the single character "1" if the response satisfies the rubric, or "0" if it does not. No other text.

Rubric: {rubric}

Response to grade:
{output_text}"""


@dataclass
class GradedResult:
    score_result: ScoreResult
    grader_output_text: str | None
    grader_latency_ms: int | None
    grader_cost_usd: Decimal | None


async def score_graded(
    output_text: str | None,
    rubric: str,
    grader_client: ProviderClient,
    grader_price: Price,
) -> GradedResult:
    if output_text is None:
        # Nothing to grade — the original call errored on day 3. Don't
        # spend a grading call on a result that has no content at all.
        return GradedResult(ScoreResult(0.0, "no output to score"), None, None, None)

    prompt = _GRADING_PROMPT_TEMPLATE.format(rubric=rubric, output_text=output_text)
    call_result = await grader_client.call(GRADER_MODEL_NAME, prompt, grader_price)

    if call_result.error:
        return GradedResult(
            ScoreResult(0.0, f"grader call failed: {call_result.error}"),
            None,
            call_result.latency_ms,
            call_result.cost_usd,
        )

    verdict = (call_result.output_text or "").strip()
    if verdict == "1":
        score_result = ScoreResult(1.0)
    elif verdict == "0":
        score_result = ScoreResult(0.0, "grader judged rubric not satisfied")
    else:
        score_result = ScoreResult(0.0, f"grader gave an unparseable verdict: {verdict!r}")

    return GradedResult(score_result, call_result.output_text, call_result.latency_ms, call_result.cost_usd)
