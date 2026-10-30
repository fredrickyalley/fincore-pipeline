import json
from datetime import datetime

import pytest

from trigger_run import build_run_id, build_trigger_command, parse_cli_json, summarise_states


def test_trigger_command_builds_valid_conf_json_itself() -> None:
    command = build_trigger_command("2026-10-26", "manual__2026-10-26__x")
    conf = command[command.index("--conf") + 1]
    assert json.loads(conf) == {"trade_date": "2026-10-26"}
    assert command[:4] == ["airflow", "dags", "trigger", "fincore_daily_pipeline"]


@pytest.mark.parametrize("bad", ["2026-13-01", "26-10-2026", "2026-10-26; rm -rf /", ""])
def test_trigger_command_rejects_bad_dates(bad: str) -> None:
    with pytest.raises(ValueError):
        build_trigger_command(bad, "run")


def test_run_id_is_unique_per_trigger_and_names_the_date() -> None:
    run_id = build_run_id("2026-10-26", datetime(2026, 10, 28, 9, 5, 1))
    assert run_id == "manual__2026-10-26__20261028T090501"


def test_cli_json_ignores_warning_lines_before_the_payload() -> None:
    output = 'WARNING something deprecated\n[{"run_id": "r1", "state": "success"}]\n'
    assert parse_cli_json(output) == [{"run_id": "r1", "state": "success"}]


def test_summary_counts_states_and_lists_failures() -> None:
    states = [
        {"task_id": "spark_etl", "state": "failed"},
        {"task_id": "quality_gate", "state": "upstream_failed"},
        {"task_id": "resolve_trade_date", "state": "success"},
    ]
    summary = summarise_states(states)
    assert summary.counts == {"failed": 1, "upstream_failed": 1, "success": 1}
    assert summary.failed_tasks == ("spark_etl",)
