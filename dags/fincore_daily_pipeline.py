"""FinCore daily pipeline: S3 arrival -> extract snapshot -> Spark ETL -> quality gate ->
warehouse load -> dbt. Must finish by 06:30 New York time.

FINCORE_ENV=aws   (default) Glue job + Redshift COPY, scheduled Tue-Sat 00:30 New York.
FINCORE_ENV=local spark-submit + Postgres, manual trigger only (docker compose stack).

Target: Airflow 2.10 (MWAA). SLA callbacks were removed in Airflow 3; on 3.x replace
`sla`/`sla_miss_callback` with a DeadlineAlert.
Manual run / re-run of one date: trigger with conf {"trade_date": "YYYY-MM-DD"}.
"""

import shlex

import pendulum
from airflow import DAG
from airflow.models.baseoperator import BaseOperator, cross_downstream
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup

from fincore_pipeline import settings as cfg
from fincore_pipeline import tasks, warehouse
from fincore_pipeline.extract import stage_source
from fincore_pipeline.notifications import on_sla_miss, on_task_failure, source_missing_callback
from fincore_pipeline.redshift_sql import LOAD_TARGETS
from fincore_pipeline.spark_local import build_spark_submit_command

TRADE_DATE = "{{ ti.xcom_pull(task_ids='resolve_trade_date') }}"
DBT_ARGS = shlex.join(cfg.DBT_COMMON_ARGS)
ETL_POLICY = {
    "retries": cfg.ETL_RETRIES,
    "retry_delay": cfg.ETL_RETRY_DELAY,
    "execution_timeout": cfg.ETL_EXECUTION_TIMEOUT,
}


def _sensor_keys(source: str) -> list[str] | str:
    if source == "market_data":
        return [f"market_data/dt={TRADE_DATE}/{name}" for name in cfg.MARKET_DATA_FILES]
    return f"{source}/dt={TRADE_DATE}/*"


def _spark_etl() -> BaseOperator:
    if cfg.IS_LOCAL:
        return BashOperator(
            task_id="spark_etl",
            bash_command=build_spark_submit_command(
                cfg.SPARK_SUBMIT, cfg.ETL_JOB_PATH, cfg.PROCESSED_BUCKET, cfg.SPARK_JARS, cfg.S3_ENDPOINT
            ),
            env={"TRADE_DATE": TRADE_DATE},  # validated YYYY-MM-DD, read as "$TRADE_DATE"
            append_env=True,
            **ETL_POLICY,
        )
    return GlueJobOperator(
        task_id="spark_etl",
        job_name=cfg.GLUE_JOB_NAME,
        script_args={
            "--run_date": TRADE_DATE,
            "--input_root": f"s3://{cfg.PROCESSED_BUCKET}/staging",
            "--output_root": f"s3://{cfg.PROCESSED_BUCKET}",
        },
        aws_conn_id=cfg.AWS_CONN_ID,
        wait_for_completion=True,
        **ETL_POLICY,
    )


with DAG(
    dag_id=cfg.DAG_ID,
    description="FinCore overnight trades, market data and portfolio P&L for the 06:30 risk review",
    schedule=cfg.SCHEDULE,
    start_date=pendulum.datetime(2026, 10, 1, tz=cfg.TIMEZONE),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=pendulum.duration(hours=10),
    sla_miss_callback=on_sla_miss,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "on_failure_callback": on_task_failure,
    },
    tags=["fincore", "risk", "tier-1", cfg.FINCORE_ENV],
) as dag:
    resolve_trade_date = PythonOperator(
        task_id="resolve_trade_date", python_callable=tasks.resolve_trade_date_task
    )

    sensors = [
        S3KeySensor(
            task_id=f"wait_for_{source}",
            bucket_name=cfg.RAW_BUCKET,
            bucket_key=_sensor_keys(source),
            wildcard_match=source != "market_data",
            aws_conn_id=cfg.AWS_CONN_ID,
            mode="reschedule",
            poke_interval=cfg.SENSOR_POKE_INTERVAL.total_seconds(),
            timeout=cfg.SENSOR_TIMEOUT.total_seconds(),
            retries=0,  # a timeout is final: alert naming the source, abort the run
            on_failure_callback=source_missing_callback(source),
        )
        for source in cfg.SOURCES
    ]

    extracts = [
        PythonOperator(
            task_id=f"extract_{source}",
            python_callable=stage_source,
            op_kwargs={"source": source, "trade_date": TRADE_DATE},
        )
        for source in cfg.SOURCES
    ]
    all_sources_extracted = EmptyOperator(task_id="all_sources_extracted")

    spark_etl = _spark_etl()

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=tasks.choose_quality_branch,
        op_kwargs={"trade_date": TRADE_DATE},
    )
    quality_gate_failed = PythonOperator(
        task_id="quality_gate_failed",
        python_callable=tasks.fail_quality_gate,
        on_failure_callback=None,  # it already sent the metrics summary to Slack
    )

    with TaskGroup(group_id="load") as load:
        delete_partition = PythonOperator(
            task_id="delete_partition",
            python_callable=warehouse.delete_partitions,
            op_kwargs={"trade_date": TRADE_DATE},
        )
        copies = [
            PythonOperator(
                task_id=f"copy_{dataset}",
                python_callable=warehouse.load_dataset,
                op_kwargs={"dataset": dataset, "trade_date": TRADE_DATE},
            )
            for dataset in LOAD_TARGETS
        ]
        delete_partition >> copies

    with TaskGroup(group_id="dbt") as dbt:
        dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{cfg.DBT_BIN} seed {DBT_ARGS}")
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                f"{cfg.DBT_BIN} run {DBT_ARGS} --select {' '.join(cfg.DBT_RUN_MODELS)} "
                "--vars \"{run_date: '${TRADE_DATE}'}\""
            ),
            env={"TRADE_DATE": TRADE_DATE},  # validated YYYY-MM-DD, passed via env not inlined
            append_env=True,
        )
        dbt_test = PythonOperator(
            task_id="dbt_test",
            python_callable=tasks.run_dbt_tests,
            op_kwargs={"trade_date": TRADE_DATE},
        )
        dbt_seed >> dbt_run >> dbt_test

    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=tasks.record_completion,
        op_kwargs={"trade_date": TRADE_DATE},
        sla=cfg.PIPELINE_SLA,
    )

    resolve_trade_date >> sensors
    cross_downstream(sensors, extracts)
    extracts >> all_sources_extracted >> spark_etl >> quality_gate >> [delete_partition, quality_gate_failed]
    load >> dbt >> pipeline_complete
