import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest import mock

import pytest

from dbt_warehouse import failed_files
from pipeline import alerts
from pipeline.settings import target_month
from tlc_ingest import ingest_command, resolve_month


@pytest.mark.parametrize(
    ("run_date", "expected"),
    [
        (datetime(2026, 11, 20, 6), date(2026, 9, 1)),
        (datetime(2026, 2, 20), date(2025, 12, 1)),
        (datetime(2026, 1, 1), date(2025, 11, 1)),
        (date(2026, 3, 31), date(2026, 1, 1)),
    ],
)
def test_target_month_is_two_months_back(run_date, expected):
    assert target_month(run_date) == expected


def test_month_param_overrides_run_date():
    context = {"params": {"month": "2026-06"}, "logical_date": datetime(2026, 11, 20)}
    assert resolve_month(context) == "2026-06"


def test_manual_run_without_logical_date_uses_run_after():
    run = SimpleNamespace(run_after=datetime(2026, 11, 17))
    context = {"params": {"month": None}, "logical_date": None, "dag_run": run}
    assert resolve_month(context) == "2026-09"


def test_ingest_command_sample_and_full():
    sample = ingest_command("yellow", "2026-08", "http://cdn", 50000)
    assert sample[1:] == [
        "--dataset", "yellow", "--from", "2026-08", "--to", "2026-08",
        "--base-url", "http://cdn", "--max-rows", "50000",
    ]  # fmt: skip
    assert "--max-rows" not in ingest_command("yellow", "2026-08", "http://cdn", 0)


def _quality_row(service="yellow", month="2026-08", raw=100, fct=100, valid=97.5):
    return (service, month, 1000, raw, fct, raw == fct, valid, 0, 0)


def test_gate_passes_clean_files():
    assert failed_files([_quality_row(), _quality_row("green")], 90) == []


def test_gate_flags_low_valid_pct_and_unreconciled_files():
    problems = failed_files(
        [_quality_row(valid=85.14), _quality_row("green", fct=90), _quality_row(valid=None)], 90
    )
    assert problems == [
        "yellow 2026-08: valid_pct 85.14 < 90",
        "green 2026-08: raw has 100 rows but fct_trips has 90",
        "yellow 2026-08: valid_pct None < 90",
    ]


def _context():
    ti = SimpleNamespace(dag_id="dbt_warehouse", task_id="quality_gate.check", try_number=1)
    return {"ti": ti, "run_id": "manual__1", "exception": ValueError("bad month")}


def test_alert_text():
    text = alerts.format_alert(_context())
    assert "dbt_warehouse.quality_gate.check failed" in text
    assert "ValueError: bad month" in text


def test_alert_without_webhook_only_logs(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    with mock.patch.object(alerts.urllib.request, "urlopen") as urlopen:
        alerts.notify_failure(_context())
    urlopen.assert_not_called()


def test_alert_posts_to_webhook(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.invalid/x")
    with mock.patch.object(alerts.urllib.request, "urlopen") as urlopen:
        alerts.notify_failure(_context())
    request = urlopen.call_args.args[0]
    assert request.full_url == "https://hooks.example.invalid/x"
    assert "bad month" in json.loads(request.data)["text"]


def test_alert_webhook_failure_does_not_raise(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "https://hooks.example.invalid/x")
    with mock.patch.object(alerts.urllib.request, "urlopen", side_effect=OSError("down")):
        alerts.notify_failure(_context())
