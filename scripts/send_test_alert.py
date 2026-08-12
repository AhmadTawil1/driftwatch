"""Day 6 checkpoint: sends one real alert to the configured Slack
webhook, using a throwaway synthetic drift_check row rather than
touching real data — same pattern as day 5's synthetic detector tests.
Cleans up both the synthetic drift_check and the alert row it
generates afterward.

Run with: uv run python scripts/send_test_alert.py
"""

from decimal import Decimal

from dotenv import load_dotenv

from watchdog.db.models import Alert, DriftCheck, Model, Provider
from watchdog.db.session import get_session
from watchdog.notify.notifier import notify_fired_checks

load_dotenv()


def main() -> None:
    session = get_session()

    provider = Provider(
        name="synthetic-alert-test-provider",
        base_url="http://unused.invalid",
        credential_env_var="UNUSED",
        concurrency_limit=1,
    )
    session.add(provider)
    session.flush()

    model = Model(provider_id=provider.id, name="synthetic-alert-test-model")
    session.add(model)
    session.flush()

    check = DriftCheck(
        model_id=model.id,
        category="synthetic_test_category",
        metric="mean_score",
        tonight_mean=Decimal("0.62"),
        window_mean=Decimal("0.88"),
        window_stdev=Decimal("0.04"),
        delta=Decimal("-0.26"),
        z_score=Decimal("-6.50"),
        fired=True,
    )
    session.add(check)
    session.commit()

    sent = notify_fired_checks(session)
    print(f"alerts sent: {sent}")

    # Clean up — this was never a real drift event.
    session.query(Alert).filter(Alert.drift_check_id == check.id).delete()
    session.delete(check)
    session.delete(model)
    session.delete(provider)
    session.commit()
    session.close()


if __name__ == "__main__":
    main()
