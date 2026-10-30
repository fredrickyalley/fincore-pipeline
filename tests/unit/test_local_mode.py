import pytest

from fincore_pipeline.settings import parse_environment
from fincore_pipeline.spark_local import build_spark_submit_command
from fincore_pipeline.warehouse import build_insert_statement


def test_environment_must_be_aws_or_local() -> None:
    assert parse_environment("local") == "local"
    assert parse_environment("AWS") == "aws"
    with pytest.raises(ValueError):
        parse_environment("staging")


def test_spark_submit_points_s3a_at_the_local_endpoint() -> None:
    command = build_spark_submit_command(
        spark_submit="spark-submit",
        job_path="/opt/fincore/jobs/etl_job.py",
        processed_bucket="fincore-processed",
        jars="/opt/spark-jars/a.jar,/opt/spark-jars/b.jar",
        s3_endpoint="http://localstack:4566",
    )
    assert "--conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566" in command
    assert "spark.hadoop.fs.s3a.path.style.access=true" in command
    assert "--jars /opt/spark-jars/a.jar,/opt/spark-jars/b.jar" in command
    assert '--run_date "$TRADE_DATE"' in command  # date comes from env, never inlined
    assert "--input_root s3a://fincore-processed/staging" in command


def test_spark_submit_without_endpoint_uses_real_aws() -> None:
    command = build_spark_submit_command("spark-submit", "job.py", "fincore-processed", "", "")
    assert "fs.s3a.endpoint" not in command
    assert "--jars" not in command


def test_insert_statement_lists_columns_for_execute_values() -> None:
    sql = build_insert_statement("raw.fincore_trades", ["order_id", "symbol"])
    assert sql == "INSERT INTO raw.fincore_trades (order_id, symbol) VALUES %s"


def test_insert_statement_refuses_unsafe_identifiers() -> None:
    with pytest.raises(ValueError):
        build_insert_statement("raw.fincore_trades", ["order_id", "x); DROP TABLE t; --"])
