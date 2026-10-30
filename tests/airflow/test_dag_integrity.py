"""Loads the real DAG. Skipped when apache-airflow is not installed (pip install -e .[airflow])."""

from pathlib import Path

import pytest

pytest.importorskip("airflow.models")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dag():  # type: ignore[no-untyped-def]
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    return bag.get_dag("fincore_daily_pipeline")


def test_three_parallel_reschedule_sensors(dag) -> None:  # type: ignore[no-untyped-def]
    sensors = [t for t in dag.tasks if t.task_id.startswith("wait_for_")]
    assert len(sensors) == 3
    for sensor in sensors:
        assert sensor.mode == "reschedule"
        assert sensor.poke_interval == 600
        assert sensor.timeout == 4 * 3600
        assert sensor.upstream_task_ids == {"resolve_trade_date"}


def test_every_extract_waits_for_all_sensors(dag) -> None:  # type: ignore[no-untyped-def]
    for source in ("trades", "market_data", "portfolio"):
        extract = dag.get_task(f"extract_{source}")
        assert extract.upstream_task_ids == {
            "wait_for_trades", "wait_for_market_data", "wait_for_portfolio"
        }


def test_glue_retry_policy(dag) -> None:  # type: ignore[no-untyped-def]
    etl = dag.get_task("spark_etl")
    assert etl.retries == 3
    assert etl.retry_delay.total_seconds() == 15 * 60
    assert etl.execution_timeout.total_seconds() == 2 * 3600


def test_gate_branches_to_load_or_abort(dag) -> None:  # type: ignore[no-untyped-def]
    assert dag.get_task("quality_gate").downstream_task_ids == {
        "load.delete_partition", "quality_gate_failed"
    }


def test_final_task_has_six_hour_sla(dag) -> None:  # type: ignore[no-untyped-def]
    final = dag.get_task("pipeline_complete")
    assert final.sla.total_seconds() == 6 * 3600
    assert dag.sla_miss_callback is not None


def test_every_task_alerts_on_failure(dag) -> None:  # type: ignore[no-untyped-def]
    silent = {t.task_id for t in dag.tasks if not t.on_failure_callback}
    assert silent == {"quality_gate_failed"}  # it sends its own richer alert
