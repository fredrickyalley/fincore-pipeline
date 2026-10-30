#!/usr/bin/env bash
# FinCore local pipeline helper. Run from the project root as: bash scripts/run.sh <command>
set -euo pipefail
cd "$(dirname "$0")/.."  # project root, wherever this is called from

ENV_FILE="${FINCORE_ENV_FILE:-.env}"
export MSYS_NO_PATHCONV=1  # Git Bash: keep container paths like /sql/... untouched

usage() {
  cat <<'EOF'
FinCore local pipeline helper. Run from the project root as: bash scripts/run.sh <command>

  bash scripts/run.sh setup              create .env (first time), then build the image
  bash scripts/run.sh start              start Airflow, Postgres and LocalStack; load sample data
  bash scripts/run.sh run [YYYY-MM-DD]   trigger the DAG for a trade date and follow it to the end
  bash scripts/run.sh results            show what landed in the warehouse
  bash scripts/run.sh etl                ETL only on local files (no Airflow)
  bash scripts/run.sh test               run the full test suite
  bash scripts/run.sh logs               follow the scheduler log
  bash scripts/run.sh stop | reset       stop the stack | stop and delete all local data
EOF
}

compose() { docker compose --env-file "$ENV_FILE" "$@"; }
need_env() { [ -f "$ENV_FILE" ] || { echo "No $ENV_FILE yet. Run: bash scripts/run.sh setup" >&2; exit 1; }; }
airflow_url() {  # ask compose which host port it actually published, instead of re-reading .env
  local address
  address="$(compose port airflow-webserver 8080 2>/dev/null | head -1 || true)"
  if [ -n "$address" ]; then echo "http://localhost:${address##*:}"; else echo "(webserver not running)"; fi
}

case "${1:-help}" in
  setup)
    if [ ! -f "$ENV_FILE" ]; then
      cp .env.example "$ENV_FILE"
      echo "Created $ENV_FILE. Replace every 'change-me' value in it, then run: bash scripts/run.sh setup"
      exit 0
    fi
    compose build
    echo "Image ready. Next: bash scripts/run.sh start" ;;
  start)
    need_env; compose up -d  # returns only after airflow-init succeeded (fails if it did not)
    echo "Stack is up. Airflow UI: $(airflow_url)   Next: bash scripts/run.sh run" ;;
  run)
    # no date given: trigger_run.py uses FINCORE_DEMO_TRADE_DATE from the container environment
    need_env; echo "Follow along in the Airflow UI: $(airflow_url)"
    compose exec -T airflow-scheduler python scripts/trigger_run.py "${@:2}" ;;
  results)
    need_env; compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d fincore -f /sql/show_results.sql' ;;
  etl)   need_env; compose --profile tools run --rm etl-local ;;
  test)  need_env; compose --profile tools run --rm tests ;;
  logs)  need_env; compose logs -f airflow-scheduler ;;
  stop)  need_env; compose down ;;
  reset) need_env; compose down -v ;;
  *)     usage ;;
esac
