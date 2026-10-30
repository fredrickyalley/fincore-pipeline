"""FIX 4.4 trade log: pipe-delimited tag=value lines -> typed, de-duplicated trades.

PERMISSIVE semantics: a bad line never fails the job. Lines that are not well-formed
tag=value sequences get their text in _corrupt_record and go to the dead-letter output.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from fincore_etl.rules import RejectReason, Rule, ValidationResult, any_blank, split_by_rules
from fincore_etl.schemas import (
    CORRUPT_RECORD,
    FIX_REQUIRED_COLUMNS,
    FIX_SIDES,
    FIX_TAGS,
    PRICE,
    QUANTITY,
)
from fincore_etl.transforms.common import normalize_currency, normalize_symbol, try_cast

SOURCE = "trades"
_WELL_FORMED_FIX = r"^\d+=[^|=]*(\|\d+=[^|=]*)*\|?$"
_TRANSACT_TIME_FORMAT = "yyyyMMdd-HH:mm:ss[.SSS]"


def parse_fix_lines(lines: DataFrame) -> DataFrame:
    """lines has one string column, raw_line. Each known FIX tag becomes a column."""
    trimmed = F.trim(F.col("raw_line"))
    well_formed = trimmed.rlike(_WELL_FORMED_FIX)
    without_trailing_pipe = F.regexp_replace(trimmed, r"\|$", "")
    tags = F.when(well_formed, F.str_to_map(without_trailing_pipe, F.lit(r"\|"), F.lit("=")))
    with_map = lines.select(
        "raw_line",
        tags.alias("_tags"),
        F.when(~well_formed, F.col("raw_line")).alias(CORRUPT_RECORD),
    )
    tag_columns = [F.col("_tags").getItem(tag).alias(name) for tag, name in FIX_TAGS.items()]
    return with_map.select("raw_line", CORRUPT_RECORD, *tag_columns)


def _side(side_code: Column) -> Column:
    side = F.lit(None).cast("string")
    for code, name in FIX_SIDES.items():
        side = F.when(side_code == code, F.lit(name)).otherwise(side)
    return side


def _typed(parsed: DataFrame, run_date: str) -> DataFrame:
    return parsed.select(
        "*",
        normalize_symbol(F.col("symbol_raw")).alias("symbol"),
        _side(F.col("side_code")).alias("side"),
        try_cast("quantity_raw", QUANTITY).alias("quantity"),
        try_cast("price_raw", PRICE).alias("price"),
        normalize_currency(F.col("currency_raw")).alias("currency"),
        F.expr(f"try_to_timestamp(transact_time_raw, '{_TRANSACT_TIME_FORMAT}')").alias("transact_time"),
        F.lit(run_date).cast("date").alias("trade_date"),
    )


def _field_rules() -> list[Rule]:
    return [
        (RejectReason.FIX_MALFORMED_LINE, F.col(CORRUPT_RECORD).isNotNull()),
        (RejectReason.FIX_MISSING_REQUIRED_TAG, any_blank(FIX_REQUIRED_COLUMNS)),
        (RejectReason.FIX_INVALID_SIDE, F.col("side").isNull()),
        (RejectReason.FIX_INVALID_QUANTITY, F.coalesce(F.col("quantity") <= 0, F.lit(True))),
        (RejectReason.FIX_INVALID_PRICE, F.coalesce(F.col("price") <= 0, F.lit(True))),
        (RejectReason.FIX_INVALID_TRANSACT_TIME, F.col("transact_time").isNull()),
    ]


def validate_trades(parsed: DataFrame, run_date: str) -> ValidationResult:
    fields = split_by_rules(_typed(parsed, run_date), SOURCE, _field_rules(), ["raw_line"])
    earliest_first = Window.partitionBy("order_id").orderBy("transact_time", "raw_line")
    ranked = fields.valid.withColumn("_order_rank", F.row_number().over(earliest_first))
    deduped = split_by_rules(
        ranked, SOURCE, [(RejectReason.FIX_DUPLICATE_ORDER_ID, F.col("_order_rank") > 1)], ["raw_line"]
    )
    raw_only = [name for name in FIX_TAGS.values() if name.endswith("_raw")]
    valid = deduped.valid.drop("_order_rank", CORRUPT_RECORD, "msg_type", "side_code", *raw_only)
    return ValidationResult(valid, fields.rejected.unionByName(deduped.rejected))
