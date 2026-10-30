"""DAG configuration. Everything environment-specific comes from environment variables
(see .env.example); secrets live in Airflow Connections/Variables, never here."""

import os
from datetime import timedelta
from typing import Final

ENVIRONMENTS: Final = ("aws", "local")


def parse_environment(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ENVIRONMENTS:
        raise ValueError(f"FINCORE_ENV must be one of {ENVIRONMENTS}, got {value!r}")
    return normalized


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


# aws: Glue + Redshift (production). local: spark-submit + Postgres (docker compose).
FINCORE_ENV: Final = parse_environment(_env("FINCORE_ENV", "aws"))
IS_LOCAL: Final = FINCORE_ENV == "local"

DAG_ID: Final = "fincore_daily_pipeline"
TIMEZONE: Final = "America/New_York"
# Tue-Sat 00:30 New York: each run processes the previous (trading) day.
# Local stacks are triggered by hand with {"trade_date": ...}.
SCHEDULE: Final = None if IS_LOCAL else "30 0 * * 2-6"

RAW_BUCKET: Final = _env("FINCORE_RAW_BUCKET", "fincore-raw")
PROCESSED_BUCKET: Final = _env("FINCORE_PROCESSED_BUCKET", "fincore-processed")
SOURCES: Final = ("trades", "market_data", "portfolio")
# market data is one file per asset class; wait for all of them, not just the first one
MARKET_DATA_FILES: Final = ("equity.csv", "fixed_income.csv", "fx.csv")

SENSOR_POKE_INTERVAL: Final = timedelta(minutes=10)
SENSOR_TIMEOUT: Final = timedelta(minutes=int(_env("FINCORE_SENSOR_TIMEOUT_MINUTES", "240")))

GLUE_JOB_NAME: Final = _env("FINCORE_GLUE_JOB_NAME", "fincore-etl")
ETL_RETRIES: Final = 3
ETL_RETRY_DELAY: Final = timedelta(minutes=15)
ETL_EXECUTION_TIMEOUT: Final = timedelta(hours=2)
SPARK_SUBMIT: Final = _env("FINCORE_SPARK_SUBMIT", "spark-submit")
ETL_JOB_PATH: Final = _env("FINCORE_ETL_JOB_PATH", "/opt/fincore/jobs/etl_job.py")
SPARK_JARS: Final = _env("FINCORE_SPARK_JARS", "")
S3_ENDPOINT: Final = _env("FINCORE_S3_ENDPOINT", "")

MAX_REJECT_RATE: Final = 0.02
MAX_PNL_UNRESOLVABLE: Final = 100

PIPELINE_SLA: Final = timedelta(hours=6)
SLA_DEADLINE_HOUR: Final = 6
SLA_DEADLINE_MINUTE: Final = 30

AWS_CONN_ID: Final = "aws_default"
WAREHOUSE_CONN_ID: Final = "fincore_warehouse"  # Redshift (aws) or Postgres (local)
SLACK_CONN_ID: Final = "slack_fincore_alerts"
PAGERDUTY_CONN_ID: Final = "pagerduty_fincore"
REDSHIFT_COPY_ROLE_VARIABLE: Final = "fincore_redshift_copy_role_arn"

DBT_PROJECT_DIR: Final = _env("FINCORE_DBT_PROJECT_DIR", "/usr/local/airflow/dags/dbt/fincore")
DBT_BIN: Final = _env("FINCORE_DBT_BIN", "/usr/local/airflow/dbt_venv/bin/dbt")
DBT_TARGET: Final = _env("FINCORE_DBT_TARGET", "prod")
DBT_COMMON_ARGS: Final = (
    "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR, "--target", DBT_TARGET,
)
DBT_RUN_MODELS: Final = (
    "stg_trades",
    "stg_market_data",
    "stg_portfolio_positions",
    "fct_daily_trades",
    "dim_instrument",
    "agg_portfolio_pnl",
)

CLOUDWATCH_NAMESPACE: Final = "FinCore/Pipeline"
HTTP_TIMEOUT_SECONDS: Final = 10
