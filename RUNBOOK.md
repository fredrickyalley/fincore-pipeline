# FinCore daily pipeline runbook

The risk team opens their dashboards at **06:30 New York time**, and this pipeline has to finish before then. If you were paged, start with "Check status" and then find your failure below.

## Pipeline overview

DAG `fincore_daily_pipeline` runs Tue–Sat at 00:30 America/New_York. Each run processes the previous trading day (the **trade date**).

```
 s3://fincore-raw/{trades,market_data,portfolio}/dt=D/
        │  (3 S3KeySensors in parallel, reschedule, poke 10m, timeout 4h)
        ▼
 extract_{source} x3 ──► s3://fincore-processed/staging/{source}/dt=D/   (snapshot + _manifest.json)
        │
 all_sources_extracted
        ▼
 spark_etl (Glue job fincore-etl, 3 retries x 15m, 2h timeout)
        ├─► processed/trades/dt=D/          processed/market_data/dt=D/
        ├─► processed/portfolio_pnl/dt=D/   exceptions/pnl_unresolvable/dt=D/
        ├─► rejected/dt=D/                  (dead letter, every bad row + reason)
        └─► metrics/dt=D/quality_metrics.json   (written last = "ETL complete")
        ▼
 quality_gate ── reject_rate >= 2% or unresolvable >= 100 ──► quality_gate_failed (Slack, run FAILS)
        │ pass
        ▼
 load.delete_partition ─► load.copy_{trades,market_data,portfolio_pnl}   (Redshift raw.*)
        ▼
 dbt.dbt_seed ─► dbt.dbt_run ─► dbt.dbt_test (advisory: alerts, never blocks)
        ▼
 pipeline_complete (SLA 6h → PagerDuty on miss; publishes SlaMet/SlaMissed)
```

Every task can be re-run safely for the same date. S3 outputs overwrite their `dt=` partition, Redshift loads do DELETE+COPY in one transaction, and the dbt marts use `delete+insert` on the date.

## Check status

1. Open Airflow (MWAA): `https://<mwaa-env>.airflow.amazonaws.com/dags/fincore_daily_pipeline/grid`. Red is failed, pink is skipped, and light green is a sensor waiting (up_for_reschedule).
2. Open CloudWatch dashboard `FinCore-Pipeline`. The panels worth checking first are reject rate (gate at 2%), unresolvable P&L (gate at 100), Glue duration (timeout at 120 min) and dbt pass rate.
3. Find the trade date in the `resolve_trade_date` task's XCom.
4. Read the quality numbers straight from S3:
   `aws s3 cp s3://fincore-processed/metrics/dt=<D>/quality_metrics.json -`

## Failure scenarios

### Market data sensor timed out (source delayed)

You'll see `wait_for_market_data` failed after 4h, plus a Slack and PagerDuty message naming `market_data`. Everything downstream shows `upstream_failed`.

Investigate:
- `aws s3 ls s3://fincore-raw/market_data/dt=<D>/` should list `equity.csv`, `fixed_income.csv` and `fx.csv`. The sensor waits for all three, so one missing file is enough to time it out.
- Ask the market data vendor desk whether the feed is delayed.

Recover:
- **Do not** point the pipeline at yesterday's prices. Policy says we never use stale prices.
- Once the files land, clear the sensor and everything downstream:
  `airflow tasks clear fincore_daily_pipeline -s <D+1> -e <D+1> -t "wait_for_market_data" --downstream --yes`
- If it will not land before 06:30, tell the Head of Risk on-call (see Escalation). The risk team decides whether to start the day without overnight P&L.

### Glue job failed with OutOfMemory

You'll see `spark_etl` fail repeatedly. The Glue run shows `Container killed by YARN for exceeding memory limits` or `java.lang.OutOfMemoryError`.

Investigate:
- Glue console → `fincore-etl` → the failed run → CloudWatch logs `/aws-glue/jobs/error`.
- Compare input size with normal (normal is about 500K trades): `aws s3 cp s3://fincore-processed/staging/trades/dt=<D>/_manifest.json -`. A 10x jump usually means the upstream system sent a duplicate or a full-history export.
- If one symbol or portfolio dominates, it's skew.

Recover:
- If the input is bad, stop here and get a corrected file from upstream. Raising memory will not fix bad data.
- If the input is genuinely bigger, raise capacity for this run: `aws glue update-job` with more workers or G.2X (keep the other job settings), then clear `spark_etl`. Retrying alone will not help, because the same input runs out of memory again.
- You have 3 retries x 15 min. If the Glue run started after about 04:00, page the escalation contact now rather than waiting for the SLA miss.

### dbt test failure on `assert_agg_portfolio_pnl_matches_fct_daily_trades`

You'll see a Slack warning, "dbt tests failed (advisory)". The DAG still succeeds and the data **is** loaded.

What it means: `agg_portfolio_pnl.total_pnl` does not equal the exact sum of `fct_daily_trades.realized_pnl_usd` for some portfolio and date. That's a reconciliation break, and risk numbers may be wrong.

Investigate:
- `dbt test --select assert_agg_portfolio_pnl_matches_fct_daily_trades --store-failures` then query the stored failures table to see which portfolios and dates disagree.
- Typical causes:
  1. `fct_daily_trades` re-run for a date without rebuilding `agg_portfolio_pnl`, so the two are out of sync.
  2. Somebody edited a mart by hand.
  3. A type change rounded one side.

Recover:
- Rebuild both for the date: `dbt run --select fct_daily_trades agg_portfolio_pnl --vars "{run_date: '<D>'}"`, then re-run the test.
- If it still fails, post the failing rows in #risk-data before 06:30 so the risk team knows which portfolios to distrust.

### Redshift COPY failed with an S3 permissions error

You'll see `load.copy_*` failed with `S3ServiceException: Access Denied` or `not authorized to assume IAM Role`.

Investigate:
- The role ARN lives in Airflow Variable `fincore_redshift_copy_role_arn`. Check it is still attached to the cluster: `aws redshift describe-clusters --query "Clusters[].IamRoles"`.
- Check that the role policy allows `s3:GetObject` and `s3:ListBucket` on `fincore-processed/processed/*`, and that the bucket policy or KMS key policy hasn't changed (`kms:Decrypt` if SSE-KMS).
- `stl_load_errors` in Redshift has the detailed message.

Recover: fix the IAM, bucket or KMS policy, then clear only the load group:
`airflow tasks clear fincore_daily_pipeline -s <D+1> -e <D+1> -t "^load\." --downstream --yes`
The COPY tasks delete their own date first, so partial loads won't double-count.

## Re-run a specific date

Whole pipeline for trade date D, with the date forced so nothing depends on the schedule:

```
airflow dags trigger fincore_daily_pipeline --conf '{"trade_date": "2026-10-26"}'
```

To clear just part of an existing run, use the run's logical date (the night after D) and a task regex:

```
airflow tasks clear fincore_daily_pipeline -s 2026-10-27 -e 2026-10-27 -t "spark_etl" --downstream --yes
```

On MWAA, run these through the CLI token endpoint (`aws mwaa create-cli-token`) or use Grid view → task → Clear → Downstream.

## Escalation and SLA breach

| When | Who | How |
|---|---|---|
| Any page | Data engineering on-call | PagerDuty service `fincore-pipeline` |
| Not done by 05:45, or a source missing | Data engineering lead | PagerDuty escalation level 2 |
| SLA will be missed (06:30) | Head of Risk on-call | Phone, per the risk on-call rota |
| Vendor feed late | Market data desk | #market-data-ops |

When the SLA is breached:
1. Acknowledge the PagerDuty incident.
2. Post in #risk-data what is late, why, and your best ETA.
3. Keep the run going. Don't skip the quality gate to save time.
4. Once data is loaded, confirm in #risk-data and resolve the incident.
5. Write the incident note the same day. The `SlaMissed` metric will show the miss on the dashboard.
