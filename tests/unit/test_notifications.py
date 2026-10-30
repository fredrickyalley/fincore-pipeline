from types import SimpleNamespace

import pytest

from fincore_pipeline import notifications
from fincore_pipeline.notifications import (
    build_pagerduty_event,
    format_source_missing_message,
    format_task_failure_message,
)


def _context() -> dict[str, object]:
    task_instance = SimpleNamespace(
        task_id="spark_etl",
        dag_id="fincore_daily_pipeline",
        log_url="https://airflow.example.com/log?task_id=spark_etl",
        try_number=2,
    )
    return {"task_instance": task_instance, "params": {}, "trade_date": "2026-10-26"}


def test_failure_message_has_task_date_and_log_url() -> None:
    message = format_task_failure_message(_context(), trade_date="2026-10-26")
    assert "spark_etl" in message
    assert "2026-10-26" in message
    assert "https://airflow.example.com/log?task_id=spark_etl" in message


def test_source_missing_message_names_the_source() -> None:
    message = format_source_missing_message("market_data", "2026-10-26")
    assert "market_data" in message
    assert "2026-10-26" in message


def test_pagerduty_event_uses_dedup_key_so_retries_do_not_page_twice() -> None:
    first = build_pagerduty_event("rk", "SLA missed", "critical", dedup_key="fincore-2026-10-26")
    second = build_pagerduty_event("rk", "SLA missed", "critical", dedup_key="fincore-2026-10-26")
    assert first == second
    assert first["event_action"] == "trigger"
    assert first["payload"]["severity"] == "critical"


def test_pagerduty_rejects_unknown_severity() -> None:
    with pytest.raises(ValueError):
        build_pagerduty_event("rk", "x", "panic", dedup_key="k")


def test_callback_swallows_alerting_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken_sender(_text: str) -> None:
        raise ConnectionError("slack is down")

    monkeypatch.setattr(notifications, "send_slack", broken_sender)
    monkeypatch.setattr(notifications, "_trade_date_from_context", lambda _ctx: "2026-10-26")
    # must not raise: a broken alert channel must never mask the original task failure
    notifications.on_task_failure(_context())
