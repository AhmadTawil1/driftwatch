"""Statistical validation of the drift detector against synthetic
history — the test that decides whether the alert channel is worth
trusting at all (PLAN.md 5.1). Needs a live Postgres connection
(WATCHDOG_DATABASE_URL); the detector's queries are real SQL, not
something worth faking with a mock session.

All synthetic data lives under a dedicated provider/model/task, named
unmistakably so it can never be confused with real production data,
and is deleted again in the fixture teardown.
"""

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from dotenv import load_dotenv

from watchdog.db.models import Model, Provider, Result, Run, Suite, Task
from watchdog.db.session import get_session
from watchdog.drift.detector import MIN_NIGHTS_FOR_BASELINE, Z_THRESHOLD, compare

load_dotenv()

SYNTHETIC_CATEGORY = "synthetic_test_category"
BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def synthetic_setup():
    session = get_session()

    provider = Provider(
        name="synthetic-test-provider",
        base_url="http://unused.invalid",
        credential_env_var="UNUSED",
        concurrency_limit=1,
    )
    session.add(provider)
    session.flush()

    model = Model(provider_id=provider.id, name="synthetic-test-model")
    session.add(model)
    session.flush()

    suite = Suite(version=999_999, git_sha="synthetic")
    session.add(suite)
    session.flush()

    task = Task(
        suite_id=suite.id,
        external_id="synthetic",
        prompt="synthetic",
        category=SYNTHETIC_CATEGORY,
        scoring_method="exact",
        expected="synthetic",
        prompt_hash="synthetic",
    )
    session.add(task)
    session.commit()

    yield session, model.id, task.id

    run_ids = [r.id for r in session.query(Run).filter(Run.suite_id == suite.id).all()]
    session.query(Result).filter(Result.run_id.in_(run_ids)).delete(synchronize_session=False)
    session.query(Run).filter(Run.suite_id == suite.id).delete(synchronize_session=False)
    session.query(Task).filter(Task.id == task.id).delete(synchronize_session=False)
    session.query(Suite).filter(Suite.id == suite.id).delete(synchronize_session=False)
    session.query(Model).filter(Model.id == model.id).delete(synchronize_session=False)
    session.query(Provider).filter(Provider.id == provider.id).delete(synchronize_session=False)
    session.commit()
    session.close()


def _generate_night(
    session, model_id: int, task_id: int, suite_id: int, night_index: int,
    mean_score: float, stdev_score: float, version: str = "synthetic-v1",
) -> int:
    started_at = BASE_TIME + timedelta(days=night_index)
    run = Run(
        suite_id=suite_id,
        status="completed",
        started_at=started_at,
        finished_at=started_at,
        pipeline_git_sha="synthetic",
    )
    session.add(run)
    session.flush()

    score = max(0.0, min(1.0, random.gauss(mean_score, stdev_score)))
    session.add(
        Result(
            run_id=run.id,
            model_id=model_id,
            task_id=task_id,
            repeat_index=0,
            output_text="synthetic",
            score=Decimal(str(round(score, 4))),
            scorer="synthetic",
            suite_version=999_999,
            prompt_hash="synthetic",
            git_sha="synthetic",
            provider_model_version=version,
        )
    )
    session.commit()
    return run.id


def test_detector_fires_within_two_nights_of_injected_15_percent_drop(synthetic_setup):
    session, model_id, task_id = synthetic_setup
    suite_id = session.get(Task, task_id).suite_id
    random.seed(42)
    baseline_mean = 0.85
    stdev = 0.05

    run_ids = []
    for night in range(20):
        # Step drop of 15% starting at night 15 (index 14).
        mean_for_night = baseline_mean * 0.85 if night >= 14 else baseline_mean
        run_ids.append(_generate_night(session, model_id, task_id, suite_id, night, mean_for_night, stdev))

    fired_within_two = False
    for check_index in (14, 15):
        comparison = compare(session, model_id, SYNTHETIC_CATEGORY, run_ids[check_index])
        if comparison.fired:
            fired_within_two = True
            assert comparison.delta < 0, "fired on an injected drop but reported the wrong direction"
            break

    assert fired_within_two, "detector did not fire within two nights of a 15% step drop"


def test_detector_false_positive_rate_under_5_percent_on_pure_noise(synthetic_setup):
    session, model_id, task_id = synthetic_setup
    suite_id = session.get(Task, task_id).suite_id
    random.seed(123)
    mean_score = 0.85
    stdev = 0.05
    nights = 100

    run_ids = [
        _generate_night(session, model_id, task_id, suite_id, night, mean_score, stdev)
        for night in range(nights)
    ]

    fired_count = 0
    compared_count = 0
    for night in range(nights):
        comparison = compare(session, model_id, SYNTHETIC_CATEGORY, run_ids[night])
        if comparison.reason == "compared":
            compared_count += 1
            if comparison.fired:
                fired_count += 1

    assert compared_count > 0
    false_positive_rate = fired_count / compared_count
    print(f"\nfalse positive rate: {false_positive_rate:.2%} ({fired_count}/{compared_count}) at Z_THRESHOLD={Z_THRESHOLD}")
    assert false_positive_rate < 0.05, (
        f"false positive rate {false_positive_rate:.2%} ({fired_count}/{compared_count}) exceeds 5%"
    )


def test_cold_start_before_minimum_nights_never_fires(synthetic_setup):
    session, model_id, task_id = synthetic_setup
    suite_id = session.get(Task, task_id).suite_id
    random.seed(7)

    run_ids = [
        _generate_night(session, model_id, task_id, suite_id, night, 0.85, 0.05)
        for night in range(MIN_NIGHTS_FOR_BASELINE - 1)
    ]

    comparison = compare(session, model_id, SYNTHETIC_CATEGORY, run_ids[-1])
    assert comparison.reason == "cold_start"
    assert comparison.fired is False
    assert comparison.window_mean is None


def test_version_change_resets_baseline_instead_of_contaminating_it(synthetic_setup):
    session, model_id, task_id = synthetic_setup
    suite_id = session.get(Task, task_id).suite_id
    random.seed(99)

    run_ids = []
    # Ten nights of stable history under the old version.
    for night in range(10):
        run_ids.append(_generate_night(session, model_id, task_id, suite_id, night, 0.85, 0.05, version="model-v1"))
    # Provider swaps the model under the same name — new version string,
    # not enough nights yet to build a new baseline.
    for night in range(10, 13):
        run_ids.append(_generate_night(session, model_id, task_id, suite_id, night, 0.85, 0.05, version="model-v2"))

    comparison = compare(session, model_id, SYNTHETIC_CATEGORY, run_ids[-1])
    assert comparison.reason == "version_change_reset"
    assert comparison.fired is False
    assert comparison.window_mean is None
