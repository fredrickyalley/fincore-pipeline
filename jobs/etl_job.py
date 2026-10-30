"""FinCore daily ETL entry point (AWS Glue 5.0 / Spark 3.5, or spark-submit locally).

Glue:   --run_date 2026-10-26 --input_root s3://fincore-processed/staging
        --output_root s3://fincore-processed   (package shipped via --extra-py-files)
Local:  spark-submit jobs/etl_job.py --run_date 2026-10-26 --input_root ./data/staging
        --output_root ./data
"""

import logging
import sys
from collections.abc import Sequence

from fincore_etl.config import parse_job_args
from fincore_etl.pipeline import run_pipeline
from fincore_etl.session import build_spark_session

log = logging.getLogger("fincore_etl")


def main(argv: Sequence[str]) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = parse_job_args(argv)
    spark = build_spark_session(f"fincore-etl-{config.run_date}")
    try:
        run_pipeline(spark, config)
    except Exception:
        log.exception("FinCore ETL failed for %s", config.run_date)
        raise  # non-zero exit so Glue marks the run FAILED and Airflow retries
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
