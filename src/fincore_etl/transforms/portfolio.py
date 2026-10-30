"""Portfolio state: validate positions, mark them to market in USD, attach risk limits
and the day's realized P&L from trades."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from fincore_etl.rules import RejectReason, ValidationResult, any_blank, split_by_rules
from fincore_etl.schemas import AMOUNT, CORRUPT_RECORD, PORTFOLIO_JSON_SCHEMA, USD_PRICE
from fincore_etl.transforms.common import normalize_currency, normalize_symbol, to_usd

SOURCE = "portfolio"
_RAW_COLUMNS = [field.name for field in PORTFOLIO_JSON_SCHEMA.fields]


def _flatten(raw: DataFrame, run_date: str) -> DataFrame:
    limits = "metadata.risk_limits"
    return raw.select(
        *_RAW_COLUMNS,
        F.trim(F.col("portfolio_id")).alias("portfolio_id_norm"),
        normalize_symbol(F.col("symbol")).alias("symbol_norm"),
        normalize_currency(F.col("currency")).alias("position_currency"),
        F.lit(run_date).cast("date").alias("as_of"),
        F.col("metadata.portfolio_manager").alias("portfolio_manager"),
        F.col(f"{limits}.max_position_value_usd").alias("max_position_value_usd"),
        F.col(f"{limits}.max_var_usd").alias("max_var_usd"),
        F.col(f"{limits}.max_single_name_pct").alias("max_single_name_pct"),
    )


def validate_portfolio(raw: DataFrame, run_date: str) -> ValidationResult:
    per_position = Window.partitionBy("portfolio_id_norm", "symbol_norm")
    flagged = _flatten(raw, run_date).withColumn("_copies", F.count(F.lit(1)).over(per_position))
    rules = [
        (RejectReason.PF_MALFORMED_RECORD, F.col(CORRUPT_RECORD).isNotNull()),
        (RejectReason.PF_MISSING_KEY, any_blank(["portfolio_id_norm", "symbol_norm"])),
        (RejectReason.PF_MISSING_QUANTITY, F.col("position_quantity").isNull()),
        (RejectReason.PF_DUPLICATE_POSITION, F.col("_copies") > 1),
    ]
    result = split_by_rules(flagged, SOURCE, rules, _RAW_COLUMNS)
    valid = result.valid.select(
        F.col("portfolio_id_norm").alias("portfolio_id"),
        F.col("symbol_norm").alias("symbol"),
        F.col("as_of").alias("as_of_date"),
        "position_quantity", "average_cost", "position_currency", "portfolio_manager",
        "max_position_value_usd", "max_var_usd", "max_single_name_pct",
    )
    return ValidationResult(valid, result.rejected)


def _realized_by_position(priced_trades: DataFrame) -> DataFrame:
    return priced_trades.groupBy("portfolio_id", "symbol").agg(
        F.sum("realized_pnl_usd").cast(AMOUNT).alias("realized_pnl_usd")
    )


def value_positions(
    positions: DataFrame, prices: DataFrame, fx: DataFrame, priced_trades: DataFrame
) -> ValidationResult:
    closes = prices.select("symbol", "close_price", F.col("currency").alias("price_currency"))
    rates = fx.select(
        F.col("currency").alias("price_currency"), F.col("usd_per_unit").alias("fx_rate_to_usd")
    )
    joined = (
        positions.join(F.broadcast(closes), "symbol", "left")
        .join(F.broadcast(rates), "price_currency", "left")
    )
    rules = [
        (RejectReason.PF_NO_MARKET_PRICE, F.col("close_price").isNull()),
        (RejectReason.PF_NO_FX_RATE, F.col("fx_rate_to_usd").isNull()),
    ]
    result = split_by_rules(joined, SOURCE, rules, list(positions.columns))
    return ValidationResult(_with_valuation(result.valid, priced_trades), result.rejected)


def _with_valuation(priced: DataFrame, priced_trades: DataFrame) -> DataFrame:
    close_usd = to_usd(F.col("close_price"), F.col("fx_rate_to_usd"), USD_PRICE)
    cost_usd = to_usd(F.col("average_cost"), F.col("fx_rate_to_usd"), USD_PRICE)
    market_value = (F.col("position_quantity") * close_usd).cast(AMOUNT)
    return (
        priced.join(_realized_by_position(priced_trades), ["portfolio_id", "symbol"], "left")
        .withColumn("market_value_usd", market_value)
        .withColumn("unrealized_pnl_usd", ((close_usd - cost_usd) * F.col("position_quantity")).cast(AMOUNT))
        .withColumn("realized_pnl_usd", F.coalesce(F.col("realized_pnl_usd"), F.lit(0).cast(AMOUNT)))
        .withColumn(
            "position_limit_breached",
            F.coalesce(F.abs(F.col("market_value_usd")) > F.col("max_position_value_usd"), F.lit(False)),
        )
    )
