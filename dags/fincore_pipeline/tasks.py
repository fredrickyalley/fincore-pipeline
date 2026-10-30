"""Python callables used by the DAG. Each one is safe to re-run for the same trade date."""

import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fincore_pipeline import notifications
from fincore_pipeline.cloudwatch import publish_metrics
from fincore_pipeline.dates import is_before_sla_deadline, resolve_trade_date
from fincore_pipeline.dbt_results import summarise_run_results
from fincore_pipeline.quality_gate import evaluate_quality_gate, format_gate_summary
from fincore_pipeline.settings import (
    AWS_CONN_ID,
    DBT_BIN,
    DBT_COMMON_ARGS,
    DBT_PROJECT_DIR,
    PROCESSED_BUCKET,
    TIMEZONE,
)

log = logging.getLogger(__name__)

LOAD_START_TASK = "load.delete_partition"
GATE_FAILED_TASK = "quality_gate_failed"
DBT_TEST_TIMEOUT_SECONDS = 1800
DBT_UNHANDLED_ERROR_EXIT = 2
PERCENT = 100.0


def resolve_trade_date_task(dag_run: Any, data_interval_end: datetime) -> str:
    return resolve_trade_date(dag_run.conf if dag_run else None, data_interval_end)


def _publish_safely(metrics: dict[str, float], trade_date: str) -> None:
    # dashboards must not block the risk team's data
    notifications.safely(lambda: publish_metrics(metrics, trade_date), f"CloudWatch {sorted(metrics)}")


def _read_metrics(trade_date: str) -> dict[str, Any]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    key = f"metrics/dt={trade_date}/quality_metrics.json"
    return json.loads(S3Hook(aws_conn_id=AWS_CONN_ID).read_key(key=key, bucket_name=PROCESSED_BUCKET))


def choose_quality_branch(trade_date: str, ti: Any) -> str:
    metrics = _read_metrics(trade_date)
    result = evaluate_quality_gate(metrics)
    ti.xcom_push(key="gate_summary", value=format_gate_summary(metrics, result))
    _publish_safely({
        "TotalTradeCount": float(metrics.get("total_trade_count", 0)),
        "CleanTradeCount": float(metrics.get("clean_trade_count", 0)),
        "RejectedTradeCount": float(metrics.get("rejected_trade_count", 0)),
        "RejectRatePercent": float(metrics.get("reject_rate", 0.0)) * PERCENT,
        "PnlUnresolvableCount": float(metrics.get("pnl_unresolvable_count", 0)),
    }, trade_date)
    return LOAD_START_TASK if result.passed else GATE_FAILED_TASK


def fail_quality_gate(ti: Any) -> None:
    from airflow.exceptions import AirflowFailException

    summary = str(ti.xcom_pull(task_ids="quality_gate", key="gate_summary"))
    try:
        notifications.send_slack(f":no_entry: {summary}\nLoad and dbt were skipped.")
    finally:
        # AirflowFailException: no retries, the data will not get better by retrying
        raise AirflowFailException(summary)


def _invoke_dbt_test(trade_date: str) -> int:
    command = [DBT_BIN, "test", *DBT_COMMON_ARGS, "--vars", json.dumps({"run_date": trade_date})]
    # fixed argv, validated date, no shell
    completed = subprocess.run(
        command,  # noqa: S603
        capture_output=True, text=True, check=False, timeout=DBT_TEST_TIMEOUT_SECONDS,
    )
    log.info("dbt test output:\n%s\n%s", completed.stdout, completed.stderr)
    return completed.returncode


def run_dbt_tests(trade_date: str) -> float:
    """Tests are advisory for this client: failures alert but never fail the task.
    dbt itself crashing (exit 2, or no results) still fails it."""
    results_path = Path(DBT_PROJECT_DIR) / "target" / "run_results.json"
    results_path.unlink(missing_ok=True)  # never report a previous run's results
    if _invoke_dbt_test(trade_date) >= DBT_UNHANDLED_ERROR_EXIT or not results_path.exists():
        raise RuntimeError("dbt test did not complete; see task log")
    summary = summarise_run_results(json.loads(results_path.read_text(encoding="utf-8")))
    _publish_safely({"DbtTestPassRatePercent": summary.pass_rate_percent,
                     "DbtTestsFailed": float(summary.failed + summary.errored)}, trade_date)
    if summary.has_failures:
        message = (f":warning: FinCore dbt tests failed for {trade_date} (advisory, data was loaded): "
                   f"{', '.join(summary.failing_tests)}")
        notifications.safely(lambda: notifications.send_slack(message), "dbt test failure alert")
    return summary.pass_rate_percent


def record_completion(trade_date: str) -> bool:
    on_time = is_before_sla_deadline(trade_date, datetime.now(ZoneInfo(TIMEZONE)))
    _publish_safely({"SlaMet": float(on_time), "SlaMissed": float(not on_time)}, trade_date)
    if not on_time:
        log.warning("FinCore run for %s finished after the 06:30 deadline", trade_date)
    return on_time
