from datetime import date
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession

from fincore_etl.rules import RejectReason
from fincore_etl.schemas import PORTFOLIO_JSON_SCHEMA
from fincore_etl.transforms.pnl import price_trades
from fincore_etl.transforms.portfolio import validate_portfolio, value_positions

RUN_DATE = "2026-10-26"


def _prices(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("AAPL", "USD", Decimal("155")), ("VOD.L", "GBP", Decimal("10.50"))],
        "symbol string, currency string, close_price decimal(18,8)",
    )


def _fx(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("USD", Decimal("1")), ("GBP", Decimal("1.27"))], "currency string, usd_per_unit decimal(18,12)"
    )


def _trades(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataFrame:
    schema = ("order_id string, portfolio_id string, symbol string, side string, quantity decimal(18,6), "
              "price decimal(18,8), currency string, trade_date date")
    return spark.createDataFrame([(*row, date(2026, 10, 26)) for row in rows], schema)


def _pnl(spark: SparkSession, *rows: tuple[object, ...]) -> dict[str, object]:
    priced = price_trades(_trades(spark, list(rows)), _prices(spark), _fx(spark))
    return {row.order_id: row for row in priced.collect()}


def test_buy_and_sell_pnl_signs(spark: SparkSession) -> None:
    result = _pnl(spark, ("B", "PF1", "AAPL", "BUY", Decimal(100), Decimal(150), "USD"),
                  ("S", "PF1", "AAPL", "SELL", Decimal(100), Decimal(150), "USD"))
    assert result["B"].realized_pnl_usd == Decimal("500")
    assert result["S"].realized_pnl_usd == Decimal("-500")


def test_gbp_pnl_is_exact_to_the_cent(spark: SparkSession) -> None:
    # (10.50 - 10.00) GBP * 1000 * 1.27 = 635.00 USD exactly; a float path gives 634.9999...
    result = _pnl(spark, ("G", "PF1", "VOD.L", "BUY", Decimal(1000), Decimal("10.00"), "GBP"))
    assert result["G"].realized_pnl_usd == Decimal("635.000000")


def test_missing_price_or_fx_is_unresolvable_not_zero(spark: SparkSession) -> None:
    result = _pnl(spark, ("N", "PF1", "TSLA", "BUY", Decimal(1), Decimal(1), "USD"),
                  ("F", "PF1", "AAPL", "BUY", Decimal(1), Decimal(1), "EUR"))
    assert result["N"].pnl_unresolvable and result["N"].realized_pnl_usd is None
    assert result["N"].unresolvable_reason == "NO_MARKET_PRICE"
    assert result["F"].unresolvable_reason == "NO_FX_RATE"


def _portfolio(spark: SparkSession, rows: list[dict[str, object]]) -> DataFrame:
    return spark.createDataFrame(rows, PORTFOLIO_JSON_SCHEMA)


def _position(portfolio_id: str, symbol: str, qty: str, cost: str) -> dict[str, object]:
    limits = {"max_position_value_usd": Decimal("50000"), "max_var_usd": Decimal("1000"),
              "max_single_name_pct": Decimal("10")}
    return {"portfolio_id": portfolio_id, "symbol": symbol, "position_quantity": Decimal(qty),
            "average_cost": Decimal(cost), "currency": "GBP", "as_of_date": RUN_DATE,
            "metadata": {"portfolio_manager": "J. Doe", "strategy": "core", "risk_limits": limits},
            "_corrupt_record": None}


def test_positions_mark_to_market_in_usd_with_risk_limits(spark: SparkSession) -> None:
    positions = validate_portfolio(_portfolio(spark, [_position("PF1", " vod.l", "5000", "10")]), RUN_DATE)
    trades = price_trades(_trades(spark, [("G", "PF1", "VOD.L", "BUY", Decimal(1000), Decimal("10"), "GBP")]),
                          _prices(spark), _fx(spark))
    result = value_positions(positions.valid, _prices(spark), _fx(spark), trades)
    row = result.valid.collect()[0]
    assert row.market_value_usd == Decimal("66675.000000")  # 5000 * 10.50 * 1.27
    assert row.unrealized_pnl_usd == Decimal("3175.000000")
    assert row.realized_pnl_usd == Decimal("635.000000")
    assert row.max_position_value_usd == Decimal("50000")
    assert row.position_limit_breached is True


def test_position_without_price_is_rejected(spark: SparkSession) -> None:
    positions = validate_portfolio(_portfolio(spark, [_position("PF1", "ZZZ", "1", "1")]), RUN_DATE)
    empty_trades = price_trades(_trades(spark, []), _prices(spark), _fx(spark))
    result = value_positions(positions.valid, _prices(spark), _fx(spark), empty_trades)
    assert [r.reject_reason for r in result.rejected.collect()] == [RejectReason.PF_NO_MARKET_PRICE]
