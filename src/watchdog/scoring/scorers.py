"""The four deterministic scorers, plus the registry mapping a task's
scoring_method to the right one. Every scorer is a pure function: no
network, no database, no provider calls — scoring only ever reads a
stored output, which is the whole reason re-scoring the entire history
never costs anything (day 4's "wire it in" section).

Every scorer returns a ScoreResult and never raises — a malformed input
is a score of 0.0 with the reason recorded in `error`, not an exception
that would take down the whole scoring batch over one bad row.
"""

import json
import re
from dataclasses import dataclass

import jsonschema


@dataclass
class ScoreResult:
    score: float  # 0.0 or 1.0 — all four deterministic methods are pass/fail
    error: str | None = None


def score_exact(output_text: str | None, expected: str) -> ScoreResult:
    if output_text is None:
        return ScoreResult(0.0, "no output to score")
    normalized_output = output_text.strip().casefold()
    normalized_expected = expected.strip().casefold()
    if normalized_output == normalized_expected:
        return ScoreResult(1.0)
    return ScoreResult(0.0, f"expected {expected!r}, got {output_text!r}")


def score_regex(output_text: str | None, expected: str) -> ScoreResult:
    if output_text is None:
        return ScoreResult(0.0, "no output to score")
    try:
        pattern = re.compile(expected)
    except re.error as exc:
        return ScoreResult(0.0, f"invalid pattern in task definition: {exc}")
    if pattern.search(output_text.strip()):
        return ScoreResult(1.0)
    return ScoreResult(0.0, f"output {output_text!r} did not match pattern {expected!r}")


_CODE_FENCE_PATTERN = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```$", re.DOTALL)


def _strip_markdown_code_fence(text: str) -> str:
    """Chat models routinely wrap JSON in ```json ... ``` even when told
    not to — this is such a predictable, common formatting habit that
    any real extraction scorer needs to see through it, or "did it
    extract correctly" gets swamped by "did it follow a formatting
    instruction," which isn't what structured_extraction is meant to
    measure."""
    match = _CODE_FENCE_PATTERN.match(text.strip())
    return match.group(1) if match else text


def score_json_schema(output_text: str | None, expected: str) -> ScoreResult:
    if output_text is None:
        return ScoreResult(0.0, "no output to score")
    try:
        parsed_output = json.loads(_strip_markdown_code_fence(output_text))
    except json.JSONDecodeError as exc:
        return ScoreResult(0.0, f"malformed JSON: {exc}")
    try:
        schema = json.loads(expected)
    except json.JSONDecodeError as exc:
        return ScoreResult(0.0, f"malformed schema in task definition: {exc}")
    try:
        jsonschema.validate(parsed_output, schema)
    except jsonschema.ValidationError as exc:
        return ScoreResult(0.0, f"schema validation failed: {exc.message}")
    except jsonschema.SchemaError as exc:
        return ScoreResult(0.0, f"invalid schema in task definition: {exc}")
    return ScoreResult(1.0)


# Matches the first number in the text, including negatives and decimals
# and thousands-separator commas (e.g. "300,000" or "-4.5" or "the answer
# is 32 apples" -> 32).
_NUMBER_PATTERN = re.compile(r"-?\d[\d,]*\.?\d*")
_DEFAULT_TOLERANCE = 0.01


def score_numeric_tolerance(
    output_text: str | None, expected: str, tolerance: float = _DEFAULT_TOLERANCE
) -> ScoreResult:
    if output_text is None:
        return ScoreResult(0.0, "no output to score")
    match = _NUMBER_PATTERN.search(output_text)
    if match is None:
        return ScoreResult(0.0, f"no number found in output {output_text!r}")
    try:
        actual = float(match.group().replace(",", ""))
        expected_value = float(expected.replace(",", ""))
    except ValueError as exc:
        return ScoreResult(0.0, f"could not parse number: {exc}")
    if abs(actual - expected_value) <= tolerance:
        return ScoreResult(1.0)
    return ScoreResult(0.0, f"expected {expected_value}, got {actual} (tolerance {tolerance})")


DETERMINISTIC_SCORERS = {
    "exact": score_exact,
    "regex": score_regex,
    "json_schema": score_json_schema,
    "numeric_tolerance": score_numeric_tolerance,
}
