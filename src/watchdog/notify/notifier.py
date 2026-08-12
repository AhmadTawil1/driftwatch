"""Formats and posts fired drift_checks to the alert webhook (Slack).

The payload only ever carries metrics, model names, and category
names — never a model's actual output text. That's not just discipline
here: `drift_checks` doesn't have a column for output text at all, so
there's nothing to leak by construction, not by careful omission.

"Unnotified" is determined by absence of a matching `alerts` row, not
by a time window — a fired check gets exactly one alert, whenever this
runs next after it fired, regardless of when that is.
"""

import os
from datetime import datetime, timezone

import httpx
from sqlalchemy.orm import Session

from watchdog.db.models import Alert, DriftCheck, Model


def _format_payload(check: DriftCheck, model_name: str) -> dict:
    direction = "up" if check.delta is not None and check.delta > 0 else "down"
    text = (
        f":rotating_light: *Drift detected* — `{model_name}` / `{check.category}`\n"
        f"metric `{check.metric}` moved *{direction}* by `{float(check.delta):.4f}` "
        f"(z = `{float(check.z_score):.2f}`)\n"
        f"tonight: `{float(check.tonight_mean):.4f}`  |  "
        f"14-night baseline: `{float(check.window_mean):.4f}` "
        f"(±`{float(check.window_stdev):.4f}`)"
    )
    return {"text": text}


def notify_fired_checks(session: Session) -> int:
    """Posts one alert per fired-and-not-yet-notified drift_check.
    Returns the number of alerts sent. A missing/empty webhook URL is
    treated as "alerting not configured" — returns 0 rather than
    raising, so a dev environment without a webhook doesn't break the
    rest of the pipeline."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return 0

    fired_unnotified = (
        session.query(DriftCheck)
        .outerjoin(Alert, Alert.drift_check_id == DriftCheck.id)
        .filter(DriftCheck.fired.is_(True), Alert.id.is_(None))
        .all()
    )

    sent = 0
    with httpx.Client(timeout=10.0) as client:
        for check in fired_unnotified:
            model = session.get(Model, check.model_id)
            payload = _format_payload(check, model.name)

            response = client.post(webhook_url, json=payload)
            response.raise_for_status()

            session.add(
                Alert(
                    drift_check_id=check.id,
                    channel="slack",
                    sent_at=datetime.now(timezone.utc),
                    payload=payload,
                )
            )
            sent += 1

    session.commit()
    return sent
