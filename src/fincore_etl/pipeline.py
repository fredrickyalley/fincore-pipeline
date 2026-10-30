"""The ETL use case: read three sources, validate, price, value, write, measure.

The metrics document is written last. The Airflow quality gate treats its presence as the
signal that every output for the date is complete.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fincore_etl import io
from fincore_etl.config import JobConfig
from fincore_etl.quality import build_quality_metrics
from fincore_etl.schemas import (
    MARKET_DATA_OUTPUT_COLUMNS,
    POSITION_OUTPUT_COLUMNS,
    REJECT_OUTPUT_COLUMNS,
    TRADE_OUTPUT_COLUMNS,
)
from fincore_etl.transforms.fx import build_fx_rates
from fincore_etl.transforms.market_data import validate_market_data
from fincore_etl.transforms.pnl import price_trades
from fincore_etl.transforms.portfolio import validate_portfolio, value_positions
from fincore_etl.transforms.trades import parse_fix_lines, validate_trades

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineOutputs:
    total_trade_count: int
    trades: DataFrame  # every validated trade; unresolvable ones carry pnl_unresolvable=True
    market_data: DataFrame
    positions: DataFrame
    rejected: DataFrame


def transform(spark: SparkSession, config: JobConfig) -> PipelineOutputs:
    lines = io.read_fix_lines(spark, config.trades_input).cache()
    trades = validate_trades(parse_fix_lines(lines), config.run_date)
    market = validate_market_data(io.read_market_data(spark, config.market_data_input), config.run_date)
    portfolio = validate_portfolio(
        io.read_portfolio(spark, config.portfolio_input, config.portfolio_multiline), config.run_date
    )
    prices = market.valid.cache()
    fx = build_fx_rates(prices).cache()
    priced = price_trades(trades.valid, prices, fx).cache()
    positions = value_positions(portfolio.valid, prices, fx, priced)
    rejected = (
        trades.rejected.unionByName(market.rejected)
        .unionByName(portfolio.rejected)
        .unionByName(positions.rejected)
    ).cache()
    return PipelineOutputs(lines.count(), priced, prices, positions.valid, rejected)


def _audited(df: DataFrame, loaded_at: datetime, columns: tuple[str, ...]) -> DataFrame:
    return df.withColumn("etl_loaded_at", F.lit(loaded_at)).select(*columns)


def write_outputs(outputs: PipelineOutputs, config: JobConfig, loaded_at: datetime) -> None:
    trades = _audited(outputs.trades, loaded_at, TRADE_OUTPUT_COLUMNS)
    io.write_parquet(trades, config.trades_output)
    io.write_parquet(trades.filter(F.col("pnl_unresolvable")), config.unresolvable_output)
    io.write_parquet(_audited(outputs.market_data, loaded_at, MARKET_DATA_OUTPUT_COLUMNS),
                     config.market_data_output)
    io.write_parquet(_audited(outputs.positions, loaded_at, POSITION_OUTPUT_COLUMNS),
                     config.portfolio_pnl_output)
    rejected = outputs.rejected.withColumn("run_date", F.lit(config.run_date).cast("date"))
    io.write_parquet(_audited(rejected, loaded_at, REJECT_OUTPUT_COLUMNS).coalesce(1),
                     config.rejected_output)


def measure(outputs: PipelineOutputs, run_date: str) -> dict[str, object]:
    rule_counts = outputs.rejected.groupBy("reject_reason").count().collect()
    by_rule = {row.reject_reason: row["count"] for row in rule_counts}
    totals = outputs.trades.agg(
        F.count(F.lit(1)).alias("clean"),
        F.sum(F.col("pnl_unresolvable").cast("int")).alias("unresolvable"),
        F.sum("realized_pnl_usd").alias("pnl"),
    ).first()
    return build_quality_metrics(
        run_date=run_date,
        total_trade_count=outputs.total_trade_count,
        clean_trade_count=int(totals["clean"]),
        rejection_counts=by_rule,
        pnl_unresolvable_count=int(totals["unresolvable"] or 0),
        total_realized_pnl_usd=totals["pnl"] if totals["pnl"] is not None else Decimal(0),
    )


def run_pipeline(spark: SparkSession, config: JobConfig) -> dict[str, object]:
    loaded_at = datetime.now(UTC)  # aware: a naive datetime would be read in the host's local zone
    outputs = transform(spark, config)
    write_outputs(outputs, config, loaded_at)
    metrics = measure(outputs, config.run_date)
    io.write_json_document(metrics, config.metrics_output)
    log.info("FinCore ETL finished for %s: %s", config.run_date, metrics)
    return metrics
