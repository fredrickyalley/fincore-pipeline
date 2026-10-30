"""Market data snapshot: one CSV per asset class -> one validated close price per symbol.

Financial policy: a missing, non-positive, stale or ambiguous price is rejected. We never
fall back to a previous day's price.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from fincore_etl.rules import RejectReason, Rule, ValidationResult, any_blank, split_by_rules
from fincore_etl.schemas import CORRUPT_RECORD, MARKET_DATA_CSV_SCHEMA, PRICE
from fincore_etl.transforms.common import normalize_currency, normalize_symbol, try_cast

SOURCE = "market_data"
_RAW_COLUMNS = [field.name for field in MARKET_DATA_CSV_SCHEMA.fields] + ["source_file"]


def _typed(raw: DataFrame, run_date: str) -> DataFrame:
    return raw.select(
        *_RAW_COLUMNS,
        normalize_symbol(F.col("symbol")).alias("symbol_norm"),
        F.upper(F.trim(F.col("asset_class"))).alias("asset_class_norm"),
        normalize_currency(F.col("currency")).alias("currency_norm"),
        try_cast("close_price", PRICE).alias("close_price_typed"),
        F.lit(run_date).cast("date").alias("price_date"),
        F.lit(run_date).alias("_run_date"),
    )


def _field_rules() -> list[Rule]:
    close = F.col("close_price_typed")
    return [
        (RejectReason.MD_MALFORMED_ROW, F.col(CORRUPT_RECORD).isNotNull()),
        (RejectReason.MD_MISSING_SYMBOL, F.col("symbol_norm").isNull()),
        (RejectReason.MD_NULL_CLOSE_PRICE, any_blank(["close_price"])),
        (RejectReason.MD_INVALID_CLOSE_PRICE, close.isNull()),
        (RejectReason.MD_NON_POSITIVE_CLOSE_PRICE, close <= 0),
        (RejectReason.MD_STALE_PRICE,
         F.coalesce(F.trim(F.col("as_of_date")) != F.col("_run_date"), F.lit(True))),
    ]


def validate_market_data(raw: DataFrame, run_date: str) -> ValidationResult:
    fields = split_by_rules(_typed(raw, run_date), SOURCE, _field_rules(), _RAW_COLUMNS)
    per_symbol = Window.partitionBy("symbol_norm")
    with_conflicts = fields.valid.withColumn(
        "_distinct_prices", F.size(F.collect_set("close_price_typed").over(per_symbol))
    )
    conflicts = split_by_rules(
        with_conflicts, SOURCE,
        [(RejectReason.MD_CONFLICTING_PRICE, F.col("_distinct_prices") > 1)], _RAW_COLUMNS,
    )
    clean = conflicts.valid.dropDuplicates(["symbol_norm"]).select(
        F.col("symbol_norm").alias("symbol"),
        F.col("asset_class_norm").alias("asset_class"),
        F.col("currency_norm").alias("currency"),
        F.col("close_price_typed").alias("close_price"),
        "price_date",
    )
    return ValidationResult(clean, fields.rejected.unionByName(conflicts.rejected))
