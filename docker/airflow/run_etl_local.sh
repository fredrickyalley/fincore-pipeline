#!/usr/bin/env bash
# ETL only, no Airflow/S3/warehouse: generate a sample day into ./data/raw, run the Spark job,
# write outputs to ./data/out and print the quality metrics.
set -euo pipefail

TRADE_DATE="${FINCORE_DEMO_TRADE_DATE:-2026-10-26}"
cd /opt/fincore

python scripts/generate_sample_data.py --out data/raw --date "${TRADE_DATE}" \
  --trades "${FINCORE_DEMO_TRADE_COUNT:-5000}"

spark-submit --master 'local[*]' jobs/etl_job.py \
  --run_date "${TRADE_DATE}" \
  --input_root /opt/fincore/data/raw \
  --output_root /opt/fincore/data/out

echo "--- data/out/metrics/dt=${TRADE_DATE}/quality_metrics.json"
cat "data/out/metrics/dt=${TRADE_DATE}/quality_metrics.json"
