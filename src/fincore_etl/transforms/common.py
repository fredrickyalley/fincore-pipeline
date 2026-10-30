"""Column helpers shared by the source transforms."""

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DataType

DEFAULT_CURRENCY = "USD"


def normalize_symbol(column: Column) -> Column:
    """' brk/b ' -> 'BRK.B'. The same rule is applied to every source before any join."""
    cleaned = F.upper(F.regexp_replace(F.regexp_replace(column, r"\s+", ""), "/", "."))
    return F.when(F.length(cleaned) > 0, cleaned)


def normalize_currency(column: Column) -> Column:
    cleaned = F.upper(F.trim(column))
    return F.when(F.length(cleaned) > 0, cleaned).otherwise(F.lit(DEFAULT_CURRENCY))


def try_cast(column_name: str, data_type: DataType) -> Column:
    """NULL instead of an exception for unparseable input (we run with ANSI mode on)."""
    return F.expr(f"try_cast(trim(`{column_name}`) AS {data_type.simpleString()})")


def to_usd(amount: Column, usd_per_unit: Column, result_type: DataType) -> Column:
    return (amount * usd_per_unit).cast(result_type)
