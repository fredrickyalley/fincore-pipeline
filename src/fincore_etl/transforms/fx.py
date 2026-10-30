"""FX rates derived from the FX asset-class rows of the market data snapshot.

GBPUSD = 1.27 means 1 GBP = 1.27 USD, so GBP amounts are *multiplied* by 1.27.
USDJPY = 150 is inverted to 1/150. Direct quotes win over inverted ones.
"""

from decimal import Decimal

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from fincore_etl.schemas import FX_RATE

USD = "USD"
_PAIR = r"^[A-Z]{6}$"
_PRIORITY_USD, _PRIORITY_DIRECT, _PRIORITY_INVERSE = 0, 1, 2


def build_fx_rates(market_data: DataFrame) -> DataFrame:
    """Returns (currency, usd_per_unit) with one row per currency, always including USD."""
    pairs = market_data.filter((F.col("asset_class") == "FX") & F.col("symbol").rlike(_PAIR))
    base, quote = F.substring("symbol", 1, 3), F.substring("symbol", 4, 3)
    direct = pairs.filter((quote == USD) & (base != USD)).select(
        base.alias("currency"),
        F.col("close_price").cast(FX_RATE).alias("usd_per_unit"),
        F.lit(_PRIORITY_DIRECT).alias("_priority"),
    )
    inverse = pairs.filter((base == USD) & (quote != USD)).select(
        quote.alias("currency"),
        (F.lit(Decimal(1)) / F.col("close_price")).cast(FX_RATE).alias("usd_per_unit"),
        F.lit(_PRIORITY_INVERSE).alias("_priority"),
    )
    schema = f"currency string, usd_per_unit {FX_RATE.simpleString()}, _priority int"
    usd = market_data.sparkSession.createDataFrame([(USD, Decimal(1), _PRIORITY_USD)], schema)
    best_first = Window.partitionBy("currency").orderBy("_priority")
    return (
        usd.unionByName(direct).unionByName(inverse)
        .withColumn("_rank", F.row_number().over(best_first))
        .filter(F.col("_rank") == 1)
        .select("currency", "usd_per_unit")
    )
