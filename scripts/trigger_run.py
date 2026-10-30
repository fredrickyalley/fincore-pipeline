"""Trigger fincore_daily_pipeline for one trade date and follow it until it finishes.

Runs inside the Airflow container (`bash scripts/run.sh run [YYYY-MM-DD]` calls it).
Without a date it uses FINCORE_DEMO_TRADE_DATE, the day the local stack has sample data for.
It builds the --conf JSON itself, so no shell has to quote JSON.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from fincore_pipeline.dates import validate_iso_date
from fincore_pipeline.settings import DAG_ID

POLL_SECONDS = 15
RUN_TIMEOUT_SECONDS = 3600
TERMINAL_STATES = frozenset({"success", "failed"})

log = logging.getLogger("trigger_run")


@dataclass(frozen=True)
class StateSummary:
    counts: dict[str, int]
    failed_tasks: tuple[str, ...]


def build_run_id(trade_date: str, now: datetime) -> str:
    return f"manual__{trade_date}__{now:%Y%m%dT%H%M%S}"


def build_trigger_command(trade_date: str, run_id: str) -> list[str]:
    conf = json.dumps({"trade_date": validate_iso_date(trade_date)})
    return ["airflow", "dags", "trigger", DAG_ID, "--run-id", run_id, "--conf", conf]


def parse_cli_json(output: str) -> Any:
    """The Airflow CLI can print warnings before the JSON payload; skip to the payload."""
    start = min((index for index in (output.find("["), output.find("{")) if index >= 0), default=-1)
    if start < 0:
        raise ValueError(f"No JSON in Airflow CLI output: {output[:200]!r}")
    return json.loads(output[start:])


def summarise_states(task_states: list[dict[str, Any]]) -> StateSummary:
    counts = Counter(str(task["state"]) for task in task_states)
    failed = tuple(task["task_id"] for task in task_states if task["state"] == "failed")
    return StateSummary(counts=dict(counts), failed_tasks=failed)


def _airflow_json(*args: str) -> Any:
    # fixed argv, no shell; `airflow` is resolved from the container's PATH
    completed = subprocess.run(
        ["airflow", *args, "--output", "json"],  # noqa: S603, S607
        capture_output=True, text=True, check=True,
    )
    return parse_cli_json(completed.stdout)


def trigger(command: list[str]) -> None:
    """One attempt: `start` registers the DAG during init, so any refusal is a real error."""
    completed = subprocess.run(command, capture_output=True, text=True, check=False)  # noqa: S603
    if completed.returncode != 0:
        raise RuntimeError(f"Airflow refused the trigger:\n{completed.stderr[-2000:]}")


def _run_state(run_id: str) -> str:
    runs = _airflow_json("dags", "list-runs", "--dag-id", DAG_ID)
    return next((str(run["state"]) for run in runs if run["run_id"] == run_id), "unknown")


def follow(run_id: str) -> StateSummary:
    deadline, last = time.monotonic() + RUN_TIMEOUT_SECONDS, None
    while time.monotonic() < deadline:
        state = _run_state(run_id)
        summary = summarise_states(_airflow_json("tasks", "states-for-dag-run", DAG_ID, run_id))
        if summary.counts != last:
            log.info("run %s | %s", state, ", ".join(f"{k}={v}" for k, v in sorted(summary.counts.items())))
            last = summary.counts
        if state in TERMINAL_STATES:
            return summary
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"run {run_id} still not finished after {RUN_TIMEOUT_SECONDS // 60} minutes")


def main() -> int:
    parser = argparse.ArgumentParser(description="Trigger and follow one FinCore run")
    parser.add_argument(
        "trade_date", nargs="?", default=os.environ.get("FINCORE_DEMO_TRADE_DATE"),
        help="YYYY-MM-DD (default: FINCORE_DEMO_TRADE_DATE, the day with sample data)",
    )
    args = parser.parse_args()
    if not args.trade_date:
        parser.error("no trade date given and FINCORE_DEMO_TRADE_DATE is not set")
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    run_id = build_run_id(args.trade_date, datetime.now(UTC))
    command = build_trigger_command(args.trade_date, run_id)
    trigger(command)
    log.info("triggered %s", run_id)
    summary = follow(run_id)
    if summary.failed_tasks:
        log.error("FAILED tasks: %s. Open them in the Airflow UI to read their logs.",
                  ", ".join(summary.failed_tasks))
        return 1
    log.info("SUCCESS: run finished. See the results with: bash scripts/run.sh results")
    return 0


if __name__ == "__main__":
    sys.exit(main())
