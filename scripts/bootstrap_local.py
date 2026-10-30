"""Prepare the local stack: create the S3 buckets and upload one day of sample source files.

Run by the airflow-init service. Safe to run again: buckets are reused, files overwritten.
boto3 reads AWS_ENDPOINT_URL, so the same code talks to LocalStack here and S3 elsewhere.
"""

import logging
import os
import sys
import tempfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from fincore_pipeline.settings import PROCESSED_BUCKET, RAW_BUCKET

sys.path.insert(0, str(Path(__file__).resolve().parent))
from generate_sample_data import SampleSpec, generate  # noqa: E402

log = logging.getLogger("bootstrap_local")


def ensure_bucket(s3, name: str) -> None:  # type: ignore[no-untyped-def]
    try:
        s3.head_bucket(Bucket=name)
    except ClientError:
        s3.create_bucket(Bucket=name)
        log.info("created bucket %s", name)


def upload_sample_day(s3, bucket: str, spec: SampleSpec) -> int:  # type: ignore[no-untyped-def]
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        files = generate(spec, root)
        for path in files:
            key = path.relative_to(root).as_posix()
            s3.upload_file(str(path), bucket, key)
            log.info("uploaded s3://%s/%s", bucket, key)
    return len(files)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    trade_date = os.environ.get("FINCORE_DEMO_TRADE_DATE", "2026-10-26")
    trade_count = int(os.environ.get("FINCORE_DEMO_TRADE_COUNT", "5000"))
    s3 = boto3.client("s3")
    for bucket in (RAW_BUCKET, PROCESSED_BUCKET):
        ensure_bucket(s3, bucket)
    count = upload_sample_day(s3, RAW_BUCKET, SampleSpec(trade_date, trade_count))
    log.info("ready: %s source files for trade date %s", count, trade_date)


if __name__ == "__main__":
    main()
