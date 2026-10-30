from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

from fincore_etl.rules import RejectReason
from fincore_etl.transforms.trades import parse_fix_lines, validate_trades

RUN_DATE = "2026-10-26"
GOOD = (
    "8=FIX.4.4|35=8|37=ORD1|11=CL1|1=PF001|55= aapl |54=1|38=100|44=150.25|15=usd"
    "|60=20261026-14:30:05|10=128|"
)


def _line(**tags: str) -> str:
    base = {"35": "8", "37": "ORD9", "1": "PF001", "55": "MSFT", "54": "2", "38": "10",
            "44": "300", "60": "20261026-15:00:00"}
    base.update(tags)
    return "|".join(f"{tag}={value}" for tag, value in base.items() if value is not None) + "|"


def _validate(spark: SparkSession, *lines: str) -> tuple[DataFrame, DataFrame]:
    raw = spark.createDataFrame([(line,) for line in lines], "raw_line string")
    result = validate_trades(parse_fix_lines(raw), RUN_DATE)
    return result.valid, result.rejected


def _reasons(rejected: DataFrame) -> list[str]:
    return sorted(row.reject_reason for row in rejected.collect())


def test_parses_tags_into_typed_columns(spark: SparkSession) -> None:
    valid, rejected = _validate(spark, GOOD)
    row = valid.collect()[0]
    assert rejected.count() == 0
    assert (row.order_id, row.portfolio_id, row.symbol, row.side) == ("ORD1", "PF001", "AAPL", "BUY")
    assert row.quantity == Decimal("100")
    assert row.price == Decimal("150.25")
    assert row.currency == "USD"
    assert row.transact_time == datetime(2026, 10, 26, 14, 30, 5)
    assert str(row.trade_date) == RUN_DATE


def test_sell_side_and_millisecond_timestamps(spark: SparkSession) -> None:
    valid, _ = _validate(spark, _line(**{"60": "20261026-15:00:00.250"}))
    row = valid.collect()[0]
    assert row.side == "SELL"
    assert row.transact_time == datetime(2026, 10, 26, 15, 0, 0, 250000)


def test_missing_currency_defaults_to_usd(spark: SparkSession) -> None:
    valid, _ = _validate(spark, _line())
    assert valid.collect()[0].currency == "USD"


def test_each_rule_routes_to_dead_letter_with_reason(spark: SparkSession) -> None:
    _, rejected = _validate(
        spark,
        "this is not a fix message",
        _line(**{"55": None}),
        _line(**{"37": "A", "54": "7"}),
        _line(**{"37": "B", "38": "-5"}),
        _line(**{"37": "C", "44": "abc"}),
        _line(**{"37": "D", "60": "2026-10-26 15:00"}),
    )
    assert _reasons(rejected) == sorted([
        RejectReason.FIX_MALFORMED_LINE,
        RejectReason.FIX_MISSING_REQUIRED_TAG,
        RejectReason.FIX_INVALID_SIDE,
        RejectReason.FIX_INVALID_QUANTITY,
        RejectReason.FIX_INVALID_PRICE,
        RejectReason.FIX_INVALID_TRANSACT_TIME,
    ])
    assert {row.source for row in rejected.collect()} == {"trades"}


def test_rejected_record_keeps_the_raw_line_for_replay(spark: SparkSession) -> None:
    _, rejected = _validate(spark, "garbage")
    assert "garbage" in rejected.collect()[0].record


def test_duplicate_order_id_keeps_earliest_and_rejects_the_rest(spark: SparkSession) -> None:
    valid, rejected = _validate(
        spark,
        _line(**{"37": "DUP", "60": "20261026-15:00:01"}),
        _line(**{"37": "DUP", "60": "20261026-15:00:00"}),
    )
    assert [row.transact_time.second for row in valid.collect()] == [0]
    assert _reasons(rejected) == [RejectReason.FIX_DUPLICATE_ORDER_ID]


def test_repeated_tag_in_one_message_does_not_crash_the_job(spark: SparkSession) -> None:
    valid, _ = _validate(spark, _line() + "448=A|448=B|")
    assert valid.count() == 1
