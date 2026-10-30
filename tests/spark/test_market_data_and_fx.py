from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from fincore_etl.rules import RejectReason
from fincore_etl.schemas import MARKET_DATA_CSV_SCHEMA
from fincore_etl.transforms.fx import build_fx_rates
from fincore_etl.transforms.market_data import validate_market_data

RUN_DATE = "2026-10-26"


def _raw(spark: SparkSession, rows: list[tuple[str | None, ...]]) -> DataFrame:
    padded = [(*row, None, "equity.csv") for row in rows]  # _corrupt_record, source_file
    schema = StructType([*MARKET_DATA_CSV_SCHEMA.fields, StructField("source_file", StringType())])
    return spark.createDataFrame(padded, schema)


def _row(symbol: str | None, price: str | None, asset: str = "EQUITY", ccy: str = "USD",
         as_of: str = RUN_DATE) -> tuple[str | None, ...]:
    return (symbol, asset, ccy, price, as_of)


def test_valid_prices_are_typed_and_symbols_normalised(spark: SparkSession) -> None:
    result = validate_market_data(_raw(spark, [_row(" brk/b ", "412.5")]), RUN_DATE)
    row = result.valid.collect()[0]
    assert (row.symbol, row.close_price, str(row.price_date)) == ("BRK.B", Decimal("412.5"), RUN_DATE)


def test_bad_prices_go_to_dead_letter_never_to_output(spark: SparkSession) -> None:
    raw = _raw(spark, [
        _row("A", None), _row("B", "0"), _row("C", "-1"), _row("D", "n/a"),
        _row(None, "10"), _row("E", "10", as_of="2026-10-23"),
    ])
    result = validate_market_data(raw, RUN_DATE)
    assert result.valid.count() == 0
    assert sorted(r.reject_reason for r in result.rejected.collect()) == sorted([
        RejectReason.MD_NULL_CLOSE_PRICE, RejectReason.MD_NON_POSITIVE_CLOSE_PRICE,
        RejectReason.MD_NON_POSITIVE_CLOSE_PRICE, RejectReason.MD_INVALID_CLOSE_PRICE,
        RejectReason.MD_MISSING_SYMBOL, RejectReason.MD_STALE_PRICE,
    ])


def test_conflicting_prices_for_one_symbol_reject_both(spark: SparkSession) -> None:
    raw = _raw(spark, [_row("X", "10"), _row("X", "11"), _row("Y", "5"), _row("Y", "5")])
    result = validate_market_data(raw, RUN_DATE)
    assert [r.symbol for r in result.valid.collect()] == ["Y"]
    assert result.rejected.count() == 2


def test_fx_rates_direct_inverse_and_usd(spark: SparkSession) -> None:
    raw = _raw(spark, [_row("GBPUSD", "1.27", "FX"), _row("USDJPY", "150", "FX")])
    fx = build_fx_rates(validate_market_data(raw, RUN_DATE).valid)
    rates = {r.currency: r.usd_per_unit for r in fx.collect()}
    assert rates["USD"] == Decimal("1")
    assert rates["GBP"] == Decimal("1.27")
    assert rates["JPY"] == (Decimal(1) / Decimal(150)).quantize(Decimal("1e-12"))
