"""SQL for the Redshift load. Inputs are validated, so string formatting here is safe.

Redshift COPY from Parquet maps columns by position: raw table DDL (sql/redshift_ddl.sql)
must keep the same column order as fincore_etl.schemas.*_OUTPUT_COLUMNS.
"""

import re
from dataclasses import dataclass

from fincore_pipeline.dates import validate_iso_date

_IAM_ROLE_ARN = re.compile(r"^arn:aws:iam::\d{12}:role/[\w+=,.@/-]+$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


@dataclass(frozen=True)
class LoadTarget:
    table: str
    date_column: str
    s3_prefix: str

    def partition_prefix(self, trade_date: str) -> str:
        """Where the ETL writes this dataset for a date (see fincore_etl.config.JobConfig)."""
        return f"{self.s3_prefix}/dt={validate_iso_date(trade_date)}/"


LOAD_TARGETS: dict[str, LoadTarget] = {
    "trades": LoadTarget("raw.fincore_trades", "trade_date", "processed/trades"),
    "market_data": LoadTarget("raw.fincore_market_data", "price_date", "processed/market_data"),
    "portfolio_pnl": LoadTarget(
        "raw.fincore_portfolio_positions", "as_of_date", "processed/portfolio_pnl"
    ),
}


def build_delete_statement(target: LoadTarget, trade_date: str) -> str:
    validate_iso_date(trade_date)
    # table/column are constants and the date is validated, so formatting is safe here
    return f"DELETE FROM {target.table} WHERE {target.date_column} = '{trade_date}'"  # noqa: S608


def build_delete_statements(trade_date: str) -> list[str]:
    return [build_delete_statement(target, trade_date) for target in LOAD_TARGETS.values()]


def build_copy_statements(
    target: LoadTarget, trade_date: str, bucket: str, iam_role_arn: str
) -> list[str]:
    """DELETE + COPY run in one transaction, so re-running only this task never duplicates."""
    validate_iso_date(trade_date)
    if not _IAM_ROLE_ARN.fullmatch(iam_role_arn):
        raise ValueError("COPY IAM role must be a plain IAM role ARN")
    if not _BUCKET.fullmatch(bucket):
        raise ValueError(f"Invalid S3 bucket name {bucket!r}")
    copy = (
        f"COPY {target.table} "
        f"FROM 's3://{bucket}/{target.partition_prefix(trade_date)}' "
        f"IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET"
    )
    return [build_delete_statement(target, trade_date), copy]
