"""Extract step: snapshot each raw source into a run-scoped staging prefix.

Spark reads the snapshot, not the live raw prefix, so a late re-delivery cannot change
what a retry sees halfway through the run. The staging prefix is wiped first, which keeps
a re-run of the same date idempotent.
"""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime

from fincore_pipeline.settings import AWS_CONN_ID, PROCESSED_BUCKET, RAW_BUCKET

log = logging.getLogger(__name__)


def staging_key_for(raw_key: str, source: str, trade_date: str) -> str:
    file_name = raw_key.rsplit("/", 1)[-1]
    return f"staging/{source}/dt={trade_date}/{file_name}"


def build_manifest(source: str, trade_date: str, sizes: Mapping[str, int]) -> dict[str, object]:
    return {
        "source": source,
        "trade_date": trade_date,
        "file_count": len(sizes),
        "total_bytes": sum(sizes.values()),
        "files": sorted(sizes),
        "staged_at": datetime.now(UTC).isoformat(),
    }


def stage_source(source: str, trade_date: str) -> dict[str, object]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    staging_prefix = f"staging/{source}/dt={trade_date}/"
    stale = hook.list_keys(bucket_name=PROCESSED_BUCKET, prefix=staging_prefix) or []
    if stale:
        hook.delete_objects(bucket=PROCESSED_BUCKET, keys=stale)

    raw_keys = hook.list_keys(bucket_name=RAW_BUCKET, prefix=f"{source}/dt={trade_date}/") or []
    data_keys = [key for key in raw_keys if not key.endswith("/")]
    if not data_keys:
        raise FileNotFoundError(f"No {source} files under s3://{RAW_BUCKET}/{source}/dt={trade_date}/")

    sizes: dict[str, int] = {}
    for key in data_keys:
        hook.copy_object(
            source_bucket_key=key,
            dest_bucket_key=staging_key_for(key, source, trade_date),
            source_bucket_name=RAW_BUCKET,
            dest_bucket_name=PROCESSED_BUCKET,
        )
        sizes[key] = int(hook.head_object(key=key, bucket_name=RAW_BUCKET)["ContentLength"])

    manifest = build_manifest(source, trade_date, sizes)
    hook.load_string(
        json.dumps(manifest, indent=2),
        key=f"{staging_prefix}_manifest.json",
        bucket_name=PROCESSED_BUCKET,
        replace=True,
    )
    log.info("Staged %s files (%s bytes) for %s", manifest["file_count"], manifest["total_bytes"], source)
    return manifest
