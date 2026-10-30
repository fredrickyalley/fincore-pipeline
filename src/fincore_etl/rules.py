"""Rule-based validation: every row is either valid or rejected with exactly one reason."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

REASON_COLUMN = "_reject_reason"


class RejectReason(StrEnum):
    FIX_MALFORMED_LINE = "FIX_MALFORMED_LINE"
    FIX_MISSING_REQUIRED_TAG = "FIX_MISSING_REQUIRED_TAG"
    FIX_INVALID_SIDE = "FIX_INVALID_SIDE"
    FIX_INVALID_QUANTITY = "FIX_INVALID_QUANTITY"
    FIX_INVALID_PRICE = "FIX_INVALID_PRICE"
    FIX_INVALID_TRANSACT_TIME = "FIX_INVALID_TRANSACT_TIME"
    FIX_DUPLICATE_ORDER_ID = "FIX_DUPLICATE_ORDER_ID"
    MD_MALFORMED_ROW = "MD_MALFORMED_ROW"
    MD_MISSING_SYMBOL = "MD_MISSING_SYMBOL"
    MD_NULL_CLOSE_PRICE = "MD_NULL_CLOSE_PRICE"
    MD_INVALID_CLOSE_PRICE = "MD_INVALID_CLOSE_PRICE"
    MD_NON_POSITIVE_CLOSE_PRICE = "MD_NON_POSITIVE_CLOSE_PRICE"
    MD_STALE_PRICE = "MD_STALE_PRICE"
    MD_CONFLICTING_PRICE = "MD_CONFLICTING_PRICE"
    PF_MALFORMED_RECORD = "PF_MALFORMED_RECORD"
    PF_MISSING_KEY = "PF_MISSING_KEY"
    PF_MISSING_QUANTITY = "PF_MISSING_QUANTITY"
    PF_DUPLICATE_POSITION = "PF_DUPLICATE_POSITION"
    PF_NO_MARKET_PRICE = "PF_NO_MARKET_PRICE"
    PF_NO_FX_RATE = "PF_NO_FX_RATE"


Rule = tuple[RejectReason, Column]


@dataclass(frozen=True)
class ValidationResult:
    valid: DataFrame
    rejected: DataFrame  # columns: source, reject_reason, record

    def plus_rejects(self, other: DataFrame) -> "ValidationResult":
        return ValidationResult(self.valid, self.rejected.unionByName(other))


def any_blank(columns: Sequence[str]) -> Column:
    checks = [F.col(name).isNull() | (F.trim(F.col(name)) == "") for name in columns]
    return reduce(lambda left, right: left | right, checks)


def split_by_rules(
    df: DataFrame, source: str, rules: Sequence[Rule], record_columns: Sequence[str]
) -> ValidationResult:
    """First matching rule wins, so order rules from structural to semantic."""
    reason = F.coalesce(*[F.when(condition, F.lit(rule.value)) for rule, condition in rules])
    flagged = df.withColumn(REASON_COLUMN, reason)
    valid = flagged.filter(F.col(REASON_COLUMN).isNull()).drop(REASON_COLUMN)
    rejected = flagged.filter(F.col(REASON_COLUMN).isNotNull()).select(
        F.lit(source).alias("source"),
        F.col(REASON_COLUMN).alias("reject_reason"),
        F.to_json(F.struct(*record_columns)).alias("record"),
    )
    return ValidationResult(valid, rejected)
