"""Slack and PagerDuty alerting plus the Airflow callbacks built on them.

Callbacks never raise: a broken alert channel must not hide the original failure.
"""

import logging
from collections.abc import Callable, Mapping
from typing import Any

import requests

from fincore_pipeline.settings import HTTP_TIMEOUT_SECONDS, IS_LOCAL, PAGERDUTY_CONN_ID, SLACK_CONN_ID

log = logging.getLogger(__name__)

PAGERDUTY_EVENTS_URL = "https://events.pagerduty.com/v2/enqueue"
PAGERDUTY_SEVERITIES = frozenset({"critical", "error", "warning", "info"})

Context = Mapping[str, Any]


def _secret(conn_id: str) -> str | None:
    """None only in local mode with the connection unset; production must have every channel."""
    from airflow.exceptions import AirflowNotFoundException
    from airflow.hooks.base import BaseHook

    try:
        secret = BaseHook.get_connection(conn_id).password
    except AirflowNotFoundException:
        if IS_LOCAL:
            return None
        raise
    if not secret:
        raise ValueError(f"Airflow connection {conn_id} has no password/secret set")
    return str(secret)


def send_slack(text: str) -> None:
    webhook_url = _secret(SLACK_CONN_ID)
    if webhook_url is None:
        log.warning("[alert, Slack not configured] %s", text)
        return
    response = requests.post(webhook_url, json={"text": text}, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()


def build_pagerduty_event(
    routing_key: str, summary: str, severity: str, dedup_key: str
) -> dict[str, Any]:
    if severity not in PAGERDUTY_SEVERITIES:
        raise ValueError(f"Unknown PagerDuty severity {severity!r}")
    return {
        "routing_key": routing_key,
        "event_action": "trigger",
        "dedup_key": dedup_key,
        "payload": {"summary": summary, "severity": severity, "source": "airflow/fincore"},
    }


def send_pagerduty(summary: str, severity: str, dedup_key: str) -> None:
    routing_key = _secret(PAGERDUTY_CONN_ID)
    if routing_key is None:
        log.warning("[page, PagerDuty not configured] %s: %s", severity, summary)
        return
    event = build_pagerduty_event(routing_key, summary, severity, dedup_key)
    response = requests.post(PAGERDUTY_EVENTS_URL, json=event, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()


def format_task_failure_message(context: Context, trade_date: str) -> str:
    task_instance = context["task_instance"]
    return (
        f":red_circle: FinCore task failed\n"
        f"- task: {task_instance.dag_id}.{task_instance.task_id} (try {task_instance.try_number})\n"
        f"- trade date: {trade_date}\n"
        f"- log: {task_instance.log_url}"
    )


def format_source_missing_message(source: str, trade_date: str) -> str:
    return (
        f":rotating_light: FinCore source *{source}* has not arrived for {trade_date}. "
        f"The run is aborted; the risk team will not have data at 06:30."
    )


def _trade_date_from_context(context: Context) -> str:
    return str(context["task_instance"].xcom_pull(task_ids="resolve_trade_date"))


def safely(action: Callable[[], None], description: str) -> None:
    """Run a best-effort side effect (alert, metric). It must never fail the task or mask an error."""
    try:
        action()
    except Exception:  # noqa: BLE001 - best-effort by design
        log.exception("Best-effort action failed: %s", description)


def on_task_failure(context: Context) -> None:
    trade_date = _trade_date_from_context(context)
    safely(lambda: send_slack(format_task_failure_message(context, trade_date)), "task failure")


def source_missing_callback(source: str) -> Callable[[Context], None]:
    def _callback(context: Context) -> None:
        trade_date = _trade_date_from_context(context)
        message = format_source_missing_message(source, trade_date)
        safely(lambda: send_slack(message), f"{source} missing (slack)")
        safely(
            lambda: send_pagerduty(message, "critical", f"fincore-{trade_date}-{source}-missing"),
            f"{source} missing (pagerduty)",
        )
        on_task_failure(context)

    return _callback


def on_sla_miss(dag: Any, task_list: str, blocking_task_list: str, *_args: Any) -> None:
    summary = f"FinCore SLA missed on {dag.dag_id}: {task_list}. Blocking: {blocking_task_list}"
    safely(lambda: send_pagerduty(summary, "critical", f"{dag.dag_id}-sla"), "sla miss")
