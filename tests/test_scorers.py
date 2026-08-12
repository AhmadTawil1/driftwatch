"""Unit tests for the four deterministic scorers. No network, no
database — these run against fixed inputs, including the ugly ones
(malformed JSON, no output at all, non-numeric text)."""

from watchdog.scoring.scorers import (
    DETERMINISTIC_SCORERS,
    score_exact,
    score_json_schema,
    score_numeric_tolerance,
    score_regex,
)


# --- exact ---

def test_exact_match():
    assert score_exact("Au", "Au").score == 1.0


def test_exact_match_ignores_case_and_surrounding_whitespace():
    assert score_exact("  au \n", "Au").score == 1.0


def test_exact_mismatch():
    result = score_exact("Ag", "Au")
    assert result.score == 0.0
    assert result.error is not None


def test_exact_none_output_scores_zero_not_raises():
    result = score_exact(None, "Au")
    assert result.score == 0.0
    assert result.error == "no output to score"


# --- regex ---

def test_regex_match():
    assert score_regex("red, blue, green", r"^[a-z]+, [a-z]+, [a-z]+$").score == 1.0


def test_regex_no_match():
    result = score_regex("Red, Blue, Green", r"^[a-z]+, [a-z]+, [a-z]+$")
    assert result.score == 0.0


def test_regex_none_output_scores_zero_not_raises():
    result = score_regex(None, r"^\d+$")
    assert result.score == 0.0


def test_regex_invalid_pattern_scores_zero_not_raises():
    result = score_regex("anything", r"[unterminated(")
    assert result.score == 0.0
    assert "invalid pattern" in result.error


# --- json_schema ---

PERSON_SCHEMA = '{"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name", "age"]}'


def test_json_schema_valid():
    result = score_json_schema('{"name": "John", "age": 34}', PERSON_SCHEMA)
    assert result.score == 1.0


def test_json_schema_valid_with_extra_whitespace_and_formatting():
    result = score_json_schema('{\n  "name": "John",\n  "age": 34\n}', PERSON_SCHEMA)
    assert result.score == 1.0


def test_json_schema_missing_required_field():
    result = score_json_schema('{"name": "John"}', PERSON_SCHEMA)
    assert result.score == 0.0
    assert "schema validation failed" in result.error


def test_json_schema_wrong_type():
    result = score_json_schema('{"name": "John", "age": "thirty-four"}', PERSON_SCHEMA)
    assert result.score == 0.0


def test_json_schema_malformed_json_scores_zero_never_raises():
    result = score_json_schema("this is not json at all {", PERSON_SCHEMA)
    assert result.score == 0.0
    assert "malformed JSON" in result.error


def test_json_schema_none_output_scores_zero_not_raises():
    result = score_json_schema(None, PERSON_SCHEMA)
    assert result.score == 0.0


def test_json_schema_truncated_json_scores_zero_never_raises():
    result = score_json_schema('{"name": "John", "age": 3', PERSON_SCHEMA)
    assert result.score == 0.0
    assert "malformed JSON" in result.error


# --- numeric_tolerance ---

def test_numeric_tolerance_exact():
    assert score_numeric_tolerance("40", "40").score == 1.0


def test_numeric_tolerance_within_tolerance():
    assert score_numeric_tolerance("39.995", "40").score == 1.0


def test_numeric_tolerance_outside_tolerance():
    result = score_numeric_tolerance("41", "40")
    assert result.score == 0.0


def test_numeric_tolerance_extracts_number_from_prose():
    assert score_numeric_tolerance("The answer is 32 apples.", "32").score == 1.0


def test_numeric_tolerance_handles_thousands_separator():
    assert score_numeric_tolerance("300,000", "300000").score == 1.0


def test_numeric_tolerance_no_number_in_output_scores_zero_not_raises():
    result = score_numeric_tolerance("I don't know.", "40")
    assert result.score == 0.0
    assert "no number found" in result.error


def test_numeric_tolerance_none_output_scores_zero_not_raises():
    result = score_numeric_tolerance(None, "40")
    assert result.score == 0.0


# --- registry ---

def test_registry_has_all_four_deterministic_methods():
    assert set(DETERMINISTIC_SCORERS) == {"exact", "regex", "json_schema", "numeric_tolerance"}


def test_registry_callables_match_module_functions():
    assert DETERMINISTIC_SCORERS["exact"] is score_exact
