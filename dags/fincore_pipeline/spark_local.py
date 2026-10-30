"""spark-submit command for running the ETL without Glue (local stack, or any Spark host).

The trade date is read from $TRADE_DATE at run time, so no templated value is pasted into
the shell command itself.
"""

import shlex

S3A_CONF = (
    "spark.hadoop.fs.s3a.path.style.access=true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
    "spark.hadoop.fs.s3a.aws.credentials.provider="
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
)


def build_spark_submit_command(
    spark_submit: str, job_path: str, processed_bucket: str, jars: str, s3_endpoint: str
) -> str:
    parts = [shlex.quote(spark_submit), "--master", "'local[*]'"]
    if jars:
        parts += ["--jars", shlex.quote(jars)]
    if s3_endpoint:
        confs = (f"spark.hadoop.fs.s3a.endpoint={s3_endpoint}", *S3A_CONF)
        for conf in confs:
            parts += ["--conf", shlex.quote(conf)]
    parts += [
        shlex.quote(job_path),
        "--run_date", '"$TRADE_DATE"',
        "--input_root", shlex.quote(f"s3a://{processed_bucket}/staging"),
        "--output_root", shlex.quote(f"s3a://{processed_bucket}"),
    ]
    return " ".join(parts)
