"""Explicit input schemas, money types and output column order.

Money is DecimalType end to end. The operand precisions are kept small on purpose:
Spark widens decimal products and, past 38 digits, drops scale to 6, which would round
an intermediate FX product. With these sizes every product fits without precision loss.

Output column order is a contract with the Redshift DDL (COPY from Parquet maps by position).
"""

from typing import Final

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

QUANTITY: Final = DecimalType(18, 6)
PRICE: Final = DecimalType(18, 8)
FX_RATE: Final = DecimalType(18, 12)  # USD per one unit of currency
USD_PRICE: Final = DecimalType(24, 12)
AMOUNT: Final = DecimalType(38, 6)
PERCENTAGE: Final = DecimalType(9, 4)

CORRUPT_RECORD: Final = "_corrupt_record"

# FIX 4.4 tag -> column name. Anything else in the message is ignored.
FIX_TAGS: Final[dict[str, str]] = {
    "35": "msg_type",
    "37": "order_id",
    "11": "client_order_id",
    "1": "portfolio_id",  # Account
    "55": "symbol_raw",
    "54": "side_code",
    "38": "quantity_raw",  # OrderQty
    "44": "price_raw",  # Price
    "15": "currency_raw",
    "60": "transact_time_raw",
}
FIX_REQUIRED_COLUMNS: Final = (
    "order_id", "portfolio_id", "symbol_raw", "side_code", "quantity_raw", "price_raw",
    "transact_time_raw",
)
FIX_SIDES: Final[dict[str, str]] = {"1": "BUY", "2": "SELL"}

MARKET_DATA_CSV_SCHEMA: Final = StructType([
    StructField("symbol", StringType()),
    StructField("asset_class", StringType()),
    StructField("currency", StringType()),
    StructField("close_price", StringType()),  # typed later so bad values get a reason
    StructField("as_of_date", StringType()),
    StructField(CORRUPT_RECORD, StringType()),
])

_RISK_LIMITS = StructType([
    StructField("max_position_value_usd", AMOUNT),
    StructField("max_var_usd", AMOUNT),
    StructField("max_single_name_pct", PERCENTAGE),
])
PORTFOLIO_JSON_SCHEMA: Final = StructType([
    StructField("portfolio_id", StringType()),
    StructField("symbol", StringType()),
    StructField("position_quantity", QUANTITY),
    StructField("average_cost", PRICE),
    StructField("currency", StringType()),
    StructField("as_of_date", StringType()),
    StructField("metadata", StructType([
        StructField("portfolio_manager", StringType()),
        StructField("strategy", StringType()),
        StructField("risk_limits", _RISK_LIMITS),
    ])),
    StructField(CORRUPT_RECORD, StringType()),
])

TRADE_OUTPUT_COLUMNS: Final = (
    "order_id", "client_order_id", "portfolio_id", "symbol", "side", "quantity", "price",
    "currency", "transact_time", "trade_date", "close_price", "close_currency",
    "realized_pnl_usd", "pnl_unresolvable", "unresolvable_reason", "etl_loaded_at",
)
MARKET_DATA_OUTPUT_COLUMNS: Final = (
    "symbol", "asset_class", "currency", "close_price", "price_date", "etl_loaded_at",
)
POSITION_OUTPUT_COLUMNS: Final = (
    "portfolio_id", "symbol", "as_of_date", "position_quantity", "average_cost", "close_price",
    "price_currency", "fx_rate_to_usd", "market_value_usd", "unrealized_pnl_usd",
    "realized_pnl_usd", "portfolio_manager", "max_position_value_usd", "max_var_usd",
    "max_single_name_pct", "position_limit_breached", "etl_loaded_at",
)
REJECT_OUTPUT_COLUMNS: Final = ("source", "reject_reason", "record", "run_date", "etl_loaded_at")
