# FinCore overnight pipeline

This pipeline replaces the 5 AM manual scripts. It takes three S3 feeds (FIX trades, market data, portfolio state), produces clean, quality-gated P&L in Redshift and dbt marts, and has to finish before the risk team starts at 06:30.

| Deliverable | Where |
|---|---|
| 1. PySpark ETL | [jobs/etl_job.py](jobs/etl_job.py) (entry point) and the [src/fincore_etl/](src/fincore_etl/) package |
| 2. Airflow DAG | [dags/fincore_daily_pipeline.py](dags/fincore_daily_pipeline.py) and the [dags/fincore_pipeline/](dags/fincore_pipeline/) helpers |
| 3. dbt project | [dbt/fincore/](dbt/fincore/) |
| 4. Monitoring | [monitoring/cloudwatch_dashboard.json](monitoring/cloudwatch_dashboard.json) |
| 5. Runbook | [RUNBOOK.md](RUNBOOK.md) |
| Redshift DDL | [sql/redshift_ddl.sql](sql/redshift_ddl.sql) (local Postgres: [sql/postgres_ddl.sql](sql/postgres_ddl.sql)) |
| Local stack | [docker-compose.yml](docker-compose.yml), [docker/](docker/), [.env.example](.env.example), [scripts/](scripts/) (run helper: [run.sh](scripts/run.sh)) |

## Layout

The code is layered so that the money logic never depends on AWS or Airflow:

```
jobs/etl_job.py              thin entry point: args -> SparkSession -> run_pipeline
src/fincore_etl/
  pipeline.py                the use case: transform -> write -> measure (metrics written last)
  transforms/                pure DataFrame -> DataFrame functions, one module per concern
    trades.py                FIX parse (str_to_map), side/time typing, dedup
    market_data.py           price validation, stale/conflict rejection
    fx.py                    USD-per-unit rates from FX rows (direct beats inverse)
    pnl.py                   realized P&L, unresolvable flagging
    portfolio.py             risk-limit flattening, mark-to-market, limit breach
  rules.py                   RejectReason enum + split_by_rules (first failing rule wins)
  schemas.py                 explicit schemas, decimal money types, output column contract
  quality.py                 metrics document (pure Python)
  io.py / config.py / session.py   adapters: S3/local I/O, paths, Spark settings
dags/fincore_pipeline/       DAG helpers; airflow imports stay inside functions so logic tests run without Airflow
  settings.py                all configuration, read from environment variables
  warehouse.py               load strategy: Redshift COPY (aws) or Parquet bulk insert (local)
  spark_local.py             spark-submit command when Glue is not used
scripts/
  run.sh                     the command you run: bash scripts/run.sh <setup|start|run|results|stop ...>
  trigger_run.py             called by `run`: triggers the DAG inside the container and follows it
  bootstrap_local.py         called by `start`: creates S3 buckets, uploads the sample day
  generate_sample_data.py    writes one realistic trading day of source files
docker/                      local image (Airflow + Java + Spark + dbt) and Postgres init
```

## Design decisions worth knowing

**Money is decimal everywhere.** Prices are `DECIMAL(18,8)`, FX rates `DECIMAL(18,12)` and amounts `DECIMAL(38,6)`. The operand sizes are deliberately small because Spark drops the scale of a decimal product to 6 once the result passes 38 digits, and that would quietly round an FX conversion. Spark runs in ANSI mode, so an overflow raises an error instead of turning into a NULL dollar amount. The metrics file stores total P&L as a string. `test_gbp_pnl_is_exact_to_the_cent` pins down the case that went wrong last quarter.

**FX direction is explicit.** `GBPUSD = 1.27` means multiply GBP by 1.27. `USDJPY = 150` is inverted. The close price and the execution price are each converted using their own currency.

**PERMISSIVE everywhere, never silent.** CSV and JSON use Spark's `PERMISSIVE` mode with `_corrupt_record`. FIX lines are text, so the same contract is applied by hand: a line that isn't a well-formed `tag=value|...` sequence is kept, flagged, and sent to `rejected/` with its raw text so it can be replayed. Every rejected row has exactly one `reject_reason`.

**No stale prices.** A NULL, non-positive, unparseable, stale (`as_of_date` other than the run date) or conflicting close price is rejected. A trade without a usable price stays in `processed/trades` with `pnl_unresolvable=true` and `realized_pnl_usd=NULL`, and is also written to `exceptions/pnl_unresolvable/`. It's never priced at zero.

**Idempotency.** Each layer handles it differently:
- The extract step snapshots raw files into a staging prefix it wipes first.
- Spark overwrites `dt=` partition paths.
- The metrics file is written last, so it doubles as the completion marker.
- Each Redshift COPY runs `DELETE date` + `COPY` in one transaction, so clearing one COPY task can't duplicate rows.
- The dbt marts are `delete+insert` on the date.

**Advisory dbt tests.** `dbt_test` reads `run_results.json`. Test failures send a Slack message and publish the pass rate, but the task succeeds. If dbt itself crashes (exit code 2, or no results), the task fails.

**Untrusted input never reaches SQL or a shell unvalidated.** `trade_date` (which can come from `dag_run.conf`) must match `YYYY-MM-DD` and be a real date. The IAM role ARN and bucket name are checked against strict patterns. dbt gets the date through an environment variable or an argv list, never through shell interpolation.

## Assumptions to confirm with FinCore

These are guesses where the brief was silent:
- **P&L cost basis.** `average_cost` for a trade is its execution price (FIX tag 44). Quantity is tag 38 and the portfolio is tag 1 (Account).
- **Market data CSV columns:** `symbol,asset_class,currency,close_price,as_of_date`. Expected files are `equity.csv`, `fixed_income.csv` and `fx.csv` (the sensor waits for all three; see `MARKET_DATA_FILES`).
- **Portfolio export** is JSON Lines. Set `--portfolio_multiline true` if it's a single JSON array.
- **`processed/portfolio_pnl`** is at position grain (portfolio × symbol). `agg_portfolio_pnl` rolls it up to portfolio × date.
- **Schedule.** Tue–Sat at 00:30 New York time. Exchange holidays aren't modelled. A holiday run fails at the sensors, which is loud but honest. Add a holiday-aware timetable if that becomes noisy.

## What works today

| Area | Status | How it was checked |
|---|---|---|
| Full pipeline locally (sensors → extract → Spark → quality gate → load → dbt) | Working | `run.sh start` + `run.sh run` on a clean stack: 18 tasks succeeded, 1 skipped (the gate's failure branch, as expected) |
| Re-running the same date | Working, no duplicates | Second run left 4,975 trades and 8 portfolio rows, same P&L total |
| P&L reconciliation | Exact | `agg_portfolio_pnl` sums to 146,669.736256, identical to the ETL's `total_realized_pnl_usd` |
| dbt models and tests | Working | `dbt run` built all 6 models; `dbt test` passed 22/22 |
| ETL only (`etl`) | Working | 5,000 trades in, 25 rejected (0.5%), 25 unresolvable, metrics printed |
| Test suite (`test`) | 85 passing | Unit, Spark and DAG-integrity tests inside the image |
| `scripts/run.sh` | Verified | Every command above was run through it |
| AWS production path (Glue, Redshift COPY, MWAA, CloudWatch, Slack, PagerDuty) | Not tested | Needs a real AWS account. The code paths are unit-tested and the DAG loads in Airflow 2.10.4 |

## How to run it

You need **Docker Desktop**, and it must be running ("Engine running") before you start. Airflow, Spark, Java and dbt are all inside the Docker image, so nothing else gets installed on your machine.

Every command is `bash scripts/run.sh <command>`, run in a **bash terminal from the project root** (the folder containing `docker-compose.yml`). On Windows, open **Git Bash** (comes with Git for Windows) or a **WSL** terminal and `cd` into the project folder first.

### Run the whole pipeline locally (first time)

**Step 1: create your settings file.**
```bash
bash scripts/run.sh setup
```
This creates `.env` from `.env.example` and stops. Open `.env` and replace the three `change-me` values (`AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_SECRET_KEY`, `POSTGRES_PASSWORD`). You can pick anything; they are local only. The Airflow UI is served on `AIRFLOW_HOST_PORT` (default 8090); change it if that port is taken.

**Step 2: build the image.** About 10 minutes the first time, seconds afterwards.
```bash
bash scripts/run.sh setup
```
It ends with `Image ready. Next: bash scripts/run.sh start`. On a slow connection pip retries large downloads by itself; if the build still fails on a timeout, run the command again.

**Step 3: start the stack.** About 1–2 minutes.
```bash
bash scripts/run.sh start
```
This starts Postgres, LocalStack (S3) and the Airflow webserver and scheduler. It also creates the S3 buckets, uploads one sample trading day (2026-10-26) and installs the dbt packages. When it prints `Stack is up`, everything is ready.

**Step 4: run the pipeline.** About 3–5 minutes.
```bash
bash scripts/run.sh run
```
This triggers `fincore_daily_pipeline` for 2026-10-26 and prints progress until the run finishes. A good run looks like this:
```
Follow along in the Airflow UI: http://localhost:8090
triggered manual__2026-10-26__20261028T090330
run queued | None=19
run running | None=10, running=1, success=8
run running | None=4, running=1, skipped=1, success=13
run success | skipped=1, success=18
SUCCESS: run finished. See the results with: bash scripts/run.sh results
```
`skipped=1` is correct: it's the quality-gate failure branch, which doesn't run when the data passes. If something fails, the last line names the failed tasks. Open them in the Airflow UI at http://localhost:8090 (or your `AIRFLOW_HOST_PORT`; user `admin`, your `AIRFLOW_ADMIN_PASSWORD`) to read their logs.

**Step 5: look at the results.**
```bash
bash scripts/run.sh results
```
You should see 4,975 trades in `raw_trades` and `fct_daily_trades`, 10 instruments, and 8 rows in `agg_portfolio_pnl` (PF001–PF008) for 2026-10-26.

**When you're done:**
```bash
bash scripts/run.sh stop     # stops the stack, keeps the data
bash scripts/run.sh reset    # stops the stack and deletes all local data
```

### All commands

| Command | What it does |
|---|---|
| `bash scripts/run.sh setup` | First time: creates `.env`. After that: builds the Docker image |
| `bash scripts/run.sh start` | Starts the stack and loads the sample trading day |
| `bash scripts/run.sh run` | Runs the pipeline for `FINCORE_DEMO_TRADE_DATE` and follows it to the end |
| `bash scripts/run.sh run 2026-10-26` | Runs the pipeline for a specific date. Sample data only exists for `FINCORE_DEMO_TRADE_DATE`, so any other date waits at the sensors |
| `bash scripts/run.sh results` | Shows row counts and `agg_portfolio_pnl` from the warehouse |
| `bash scripts/run.sh etl` | ETL only, without Airflow: generates sample files into `data/raw/`, runs Spark, writes `data/out/` and prints the quality metrics (about 1 minute) |
| `bash scripts/run.sh test` | Runs all 85 tests inside the image |
| `bash scripts/run.sh logs` | Follows the scheduler log |
| `bash scripts/run.sh stop` | Stops the stack, keeps the data |
| `bash scripts/run.sh reset` | Stops the stack and deletes all local data |

From your own machine Postgres is on `localhost:5433` (database `fincore`) and LocalStack S3 is on `http://localhost:4566`.

What differs from production: LocalStack replaces S3 and CloudWatch, Postgres replaces Redshift (loaded by bulk insert, since Postgres can't COPY from S3), and `spark-submit` replaces Glue (the brief allows BashOperator for local Spark). The DAG, the ETL code, the quality gate and the dbt models are the same.

### Production on AWS (MWAA + Glue + Redshift)

Not yet exercised against a real AWS account (see "What works today").

1. **Redshift:** apply `sql/redshift_ddl.sql`.
2. **Glue:** create job `fincore-etl` on Glue 5.0 (Spark 3.5.4). Script: `jobs/etl_job.py`. Default arguments: `--extra-py-files s3://<artifacts>/fincore_etl.zip` (a zip of `src/fincore_etl`) and `--enable-metrics true` (the duration widget needs it). Start with G.1X × 10 workers.
3. **MWAA (Airflow 2.10):** copy `dags/` to the DAGs bucket and put `dbt/fincore` at `FINCORE_DBT_PROJECT_DIR`. Install dbt 1.9 (`dbt-core~=1.9.0`, `dbt-redshift~=1.9.0`; pin dbt-core too, or pip may pick a 2.x pre-release) in its own venv at `FINCORE_DBT_BIN` and run `dbt deps` at deploy time. Then set the environment variables, connections and variable listed below.
4. **Dashboard:** `aws cloudwatch put-dashboard --dashboard-name FinCore-Pipeline --dashboard-body file://monitoring/cloudwatch_dashboard.json`

Airflow 3 removed SLA callbacks. On 3.x, replace `sla`/`sla_miss_callback` with a `DeadlineAlert`.

## Configuration reference

### Environment variables read by the DAG (Airflow workers)

| Variable | Default | Purpose |
|---|---|---|
| `FINCORE_ENV` | `aws` | `aws` = Glue + Redshift COPY + Tue–Sat 00:30 schedule. `local` = spark-submit + Postgres, manual trigger only |
| `FINCORE_RAW_BUCKET` | `fincore-raw` | Bucket the source feeds land in |
| `FINCORE_PROCESSED_BUCKET` | `fincore-processed` | Staging snapshot, ETL outputs and metrics |
| `FINCORE_GLUE_JOB_NAME` | `fincore-etl` | Glue job to start (aws) |
| `FINCORE_SENSOR_TIMEOUT_MINUTES` | `240` | How long each S3 sensor waits before aborting the run |
| `FINCORE_DBT_PROJECT_DIR` | `/usr/local/airflow/dags/dbt/fincore` | dbt project (and `profiles.yml`) location |
| `FINCORE_DBT_BIN` | `/usr/local/airflow/dbt_venv/bin/dbt` | dbt executable |
| `FINCORE_DBT_TARGET` | `prod` | dbt output: `prod` (Redshift) or `local` (Postgres) |
| `FINCORE_WAREHOUSE_HOST` / `_PORT` / `_USER` / `_PASSWORD` / `_DB` | none / 5439 / none / none / `fincore` | Used by dbt's `profiles.yml`. Inject the password from Secrets Manager |
| `FINCORE_SPARK_SUBMIT`, `FINCORE_ETL_JOB_PATH`, `FINCORE_SPARK_JARS`, `FINCORE_S3_ENDPOINT` | see `settings.py` | Local Spark only: how to call `spark-submit` and reach S3-compatible storage |

### Airflow connections and variables

| Name | Kind | Contents |
|---|---|---|
| `aws_default` | Connection (aws) | IAM role on MWAA. Locally it points at LocalStack |
| `fincore_warehouse` | Connection (redshift / postgres) | Warehouse used by the load TaskGroup |
| `slack_fincore_alerts` | Connection (http) | Slack incoming-webhook URL in the **password** field. Required on aws, optional locally (alerts go to the task log) |
| `pagerduty_fincore` | Connection (http) | PagerDuty Events v2 routing key in the **password** field. Same rule as Slack |
| `fincore_redshift_copy_role_arn` | Variable | IAM role Redshift assumes for COPY (aws only) |

To enable Slack or PagerDuty locally, add them to a `docker-compose.override.yml` under `x-airflow-common` → `environment`, for example `AIRFLOW_CONN_SLACK_FINCORE_ALERTS: '{"conn_type": "http", "password": "https://hooks.slack.com/services/..."}'`.

### ETL job arguments (`jobs/etl_job.py`)

`--run_date YYYY-MM-DD` (required), `--input_root` (required; the root containing `trades/`, `market_data/` and `portfolio/` with `dt=` folders), `--output_root` (required) and `--portfolio_multiline true|false`. Paths can be `s3://`, `s3a://` or local.

### Sample data

`python scripts/generate_sample_data.py --out data/raw --date 2026-10-26 --trades 5000` writes one deterministic day. It includes about 0.5% bad FIX lines, a handful of unpriced trades, a NULL close price and a truncated JSON record, so the dead-letter path gets exercised and the quality gate still passes.

## Running the tests

```bash
bash scripts/run.sh test     # all 85 tests (unit, Spark, DAG integrity) inside the image
```

Without Docker, use `pip install -e ".[dev]"`, then `pytest tests/unit`. Running `tests/spark` needs a JDK 17 (`JAVA_HOME`) and `tests/airflow` needs `pip install -e ".[airflow]"`. Both skip themselves when their dependency is missing. On native Windows, PySpark workers crash under Python 3.12, so use Docker or WSL there.
