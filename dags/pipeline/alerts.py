"""Failure alerting: always logged; also posted to a webhook when one is configured.

`ALERT_WEBHOOK_URL` takes a Slack incoming-webhook URL (or anything that accepts
`{"text": ...}`). It is read from the environment, so no secret lives in the repo.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

log = logging.getLogger("pipeline.alerts")


def format_alert(context) -> str:
    ti = context["ti"]
    exception = context.get("exception")
    reason = f"{type(exception).__name__}: {exception}" if exception else "unknown error"
    return (
        f":red_circle: {ti.dag_id}.{ti.task_id} failed "
        f"(run {context['run_id']}, try {ti.try_number}): {reason}"
    )


def notify_failure(context) -> None:
    """`on_failure_callback`: runs once a task has failed for good (after its retries)."""
    text = format_alert(context)
    log.error("ALERT %s", text)
    url = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    if not url:
        return
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            log.info("alert posted to webhook (HTTP %s)", response.status)
    except Exception as exc:  # an alert must never mask the original failure
        log.warning("could not post alert to webhook: %s", exc)
