"""SparkSession factory shared by the Glue job and the test suite, so tests run with
exactly the SQL semantics production uses."""

from typing import Final

from pyspark.sql import SparkSession

SPARK_CONF: Final[dict[str, str]] = {
    # ANSI mode: decimal overflow raises instead of silently becoming NULL money
    "spark.sql.ansi.enabled": "true",
    "spark.sql.session.timeZone": "UTC",  # FIX TransactTime is UTC by spec
    # FIX repeating groups reuse tag numbers; keep the last value instead of failing the job
    "spark.sql.mapKeyDedupPolicy": "LAST_WIN",
    # INT96 is legacy; Redshift COPY reads microsecond timestamps cleanly
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
}


def build_spark_session(app_name: str, master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    for key, value in SPARK_CONF.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
