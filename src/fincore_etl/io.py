"""Readers and writers. All Parquet writes overwrite their dt= partition path, which is
what makes a re-run for the same date idempotent."""

import json
from collections.abc import Mapping
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fincore_etl.schemas import CORRUPT_RECORD, MARKET_DATA_CSV_SCHEMA, PORTFOLIO_JSON_SCHEMA

S3_SCHEMES = ("s3://", "s3a://")


def read_fix_lines(spark: SparkSession, path: str) -> DataFrame:
    lines = spark.read.text(path).withColumnRenamed("value", "raw_line")
    return lines.filter(F.trim(F.col("raw_line")) != "")


def read_market_data(spark: SparkSession, path: str) -> DataFrame:
    raw = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .schema(MARKET_DATA_CSV_SCHEMA)
        .csv(path)
        .withColumn("source_file", F.input_file_name())
    )
    # Spark refuses queries that only touch the corrupt-record column of an uncached read
    return raw.cache()


def read_portfolio(spark: SparkSession, path: str, multiline: bool) -> DataFrame:
    raw = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD)
        .option("multiLine", str(multiline).lower())
        .schema(PORTFOLIO_JSON_SCHEMA)
        .json(path)
    )
    return raw.cache()


def write_parquet(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def write_json_document(document: Mapping[str, object], uri: str) -> None:
    body = json.dumps(document, indent=2, sort_keys=True)
    if uri.startswith(S3_SCHEMES):
        import boto3

        bucket, _, key = uri.split("://", 1)[1].partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"),
                                      ContentType="application/json")
        return
    target = Path(uri)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
