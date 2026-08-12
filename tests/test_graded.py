"""Unit tests for graded scoring, using a stub grader client rather
than the mock HTTP server — nothing here needs real HTTP semantics,
just the logic that turns a grader's verdict into a score."""

from decimal import Decimal

from watchdog.providers.client import CallResult
from watchdog.providers.pricing import Price
from watchdog.scoring.graded import GRADER_MODEL_NAME, score_graded

PRICE = Price(input_per_million=Decimal("2.50"), output_per_million=Decimal("10.00"))


class FakeGraderClient:
    def __init__(self, canned_output: str = "", error: str | None = None):
        self.canned_output = canned_output
        self.error = error
        self.calls = []

    async def call(self, model, prompt, price):
        self.calls.append((model, prompt, price))
        if self.error:
            return CallResult(None, 100, None, None, None, None, self.error)
        return CallResult(self.canned_output, 100, 50, 1, Decimal("0.001"), "gpt-4o-mock", None)


async def test_verdict_1_scores_pass():
    client = FakeGraderClient("1")
    result = await score_graded("some output", "must be polite", client, PRICE)
    assert result.score_result.score == 1.0
    assert result.score_result.error is None


async def test_verdict_0_scores_fail():
    client = FakeGraderClient("0")
    result = await score_graded("rude output", "must be polite", client, PRICE)
    assert result.score_result.score == 0.0
    assert result.score_result.error is not None


async def test_unparseable_verdict_scores_zero_not_raises():
    client = FakeGraderClient("I think this is pretty good actually")
    result = await score_graded("some output", "must be polite", client, PRICE)
    assert result.score_result.score == 0.0
    assert "unparseable" in result.score_result.error


async def test_grader_call_failure_scores_zero_not_raises():
    client = FakeGraderClient(error="HTTP 500: internal error")
    result = await score_graded("some output", "must be polite", client, PRICE)
    assert result.score_result.score == 0.0
    assert "grader call failed" in result.score_result.error


async def test_none_output_scores_zero_without_calling_grader():
    client = FakeGraderClient("1")
    result = await score_graded(None, "must be polite", client, PRICE)
    assert result.score_result.score == 0.0
    assert len(client.calls) == 0  # nothing to grade — never spend a call on it


async def test_prompt_includes_rubric_and_output():
    client = FakeGraderClient("1")
    await score_graded("the actual answer", "checks for X", client, PRICE)
    model, prompt, _price = client.calls[0]
    assert model == GRADER_MODEL_NAME
    assert "checks for X" in prompt
    assert "the actual answer" in prompt


async def test_grader_cost_is_captured_for_separable_tracking():
    client = FakeGraderClient("1")
    result = await score_graded("some output", "must be polite", client, PRICE)
    assert result.grader_cost_usd == Decimal("0.001")
