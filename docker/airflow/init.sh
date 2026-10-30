#!/usr/bin/env bash
# One-time (idempotent) setup for the local stack, run by the airflow-init service.
set -euo pipefail

airflow db migrate
# register the DAGs in the metadata DB now, so a trigger right after `start` finds them
airflow dags reserialize

if ! airflow users list 2>/dev/null | grep -qw "${AIRFLOW_ADMIN_USER}"; then
  airflow users create --role Admin \
    --username "${AIRFLOW_ADMIN_USER}" --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname FinCore --lastname Admin --email admin@fincore.local
fi

# buckets + one day of sample source files in LocalStack S3
python /opt/fincore/scripts/bootstrap_local.py

# dbt packages (dbt_utils); in production this happens at deploy time
cd /opt/fincore/dbt/fincore
/opt/dbt-venv/bin/dbt deps --profiles-dir .
