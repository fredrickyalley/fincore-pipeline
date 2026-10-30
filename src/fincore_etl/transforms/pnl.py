"""Trade P&L against the same-day close.

realized_pnl_usd = (close_usd - cost_usd) * quantity * side_multiplier

Close and execution price are converted to USD separately, using each one's own currency,
so a GBP-priced instrument traded in GBP and a USD ADR both come out right.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fincore_etl.schemas import AMOUNT, USD_PRICE
from fincore_etl.transforms.common import to_usd

NO_MARKET_PRICE = "NO_MARKET_PRICE"
NO_FX_RATE = "NO_FX_RATE"


def _rates(fx: DataFrame, currency_column: str, rate_column: str) -> DataFrame:
    return fx.select(F.col("currency").alias(currency_column), F.col("usd_per_unit").alias(rate_column))


def price_trades(trades: DataFrame, prices: DataFrame, fx: DataFrame) -> DataFrame:
    closes = prices.select("symbol", "close_price", F.col("currency").alias("close_currency"))
    joined = (
        trades.join(F.broadcast(closes), "symbol", "left")
        .join(F.broadcast(_rates(fx, "currency", "cost_fx_rate")), "currency", "left")
        .join(F.broadcast(_rates(fx, "close_currency", "close_fx_rate")), "close_currency", "left")
    )
    reason = (
        F.when(F.col("close_price").isNull(), F.lit(NO_MARKET_PRICE))
        .when(F.col("cost_fx_rate").isNull() | F.col("close_fx_rate").isNull(), F.lit(NO_FX_RATE))
    )
    close_usd = to_usd(F.col("close_price"), F.col("close_fx_rate"), USD_PRICE)
    cost_usd = to_usd(F.col("price"), F.col("cost_fx_rate"), USD_PRICE)
    long_pnl = (close_usd - cost_usd) * F.col("quantity")
    # negate rather than multiply by an int: an int operand would widen the decimal and cost scale
    signed_pnl = F.when(F.col("side") == "SELL", -long_pnl).otherwise(long_pnl)
    return joined.select(
        "*",
        F.when(reason.isNull(), signed_pnl.cast(AMOUNT)).alias("realized_pnl_usd"),
        reason.isNotNull().alias("pnl_unresolvable"),
        reason.alias("unresolvable_reason"),
    ).drop("cost_fx_rate", "close_fx_rate")
