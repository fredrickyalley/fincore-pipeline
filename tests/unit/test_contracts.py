"""Cross-artifact contracts that would otherwise only break in production."""

import json
import re
from pathlib import Path

import pytest

from fincore_etl.schemas import (
    MARKET_DATA_OUTPUT_COLUMNS,
    POSITION_OUTPUT_COLUMNS,
    TRADE_OUTPUT_COLUMNS,
)

ROOT = Path(__file__).resolve().parents[2]
PUBLISHED_METRICS = {
    "TotalTradeCount", "CleanTradeCount", "RejectedTradeCount", "RejectRatePercent",
    "PnlUnresolvableCount", "SlaMet", "SlaMissed", "DbtTestPassRatePercent", "DbtTestsFailed",
}


def _ddl_columns(ddl_file: str, table: str) -> tuple[str, ...]:
    ddl = (ROOT / "sql" / ddl_file).read_text(encoding="utf-8")
    body = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)} \((.*?)\n\)", ddl, re.S)
    assert body, f"{table} not found in {ddl_file}"
    return tuple(line.split()[0] for line in body.group(1).strip().splitlines())


@pytest.mark.parametrize("ddl_file", ["redshift_ddl.sql", "postgres_ddl.sql"])
@pytest.mark.parametrize(
    ("table", "columns"),
    [
        ("raw.fincore_trades", TRADE_OUTPUT_COLUMNS),
        ("raw.fincore_market_data", MARKET_DATA_OUTPUT_COLUMNS),
        ("raw.fincore_portfolio_positions", POSITION_OUTPUT_COLUMNS),
    ],
)
def test_warehouse_ddl_column_order_matches_parquet_output(
    ddl_file: str, table: str, columns: tuple[str, ...]
) -> None:
    # COPY FORMAT AS PARQUET maps by position, so a reorder silently shifts data between columns
    assert _ddl_columns(ddl_file, table) == columns


def test_dashboard_charts_every_metric_the_dag_publishes() -> None:
    dashboard = json.loads((ROOT / "monitoring" / "cloudwatch_dashboard.json").read_text(encoding="utf-8"))
    charted = {
        entry[1]
        for widget in dashboard["widgets"] if widget["type"] == "metric"
        for entry in widget["properties"]["metrics"] if entry and entry[0] == "FinCore/Pipeline"
    }
    assert charted == PUBLISHED_METRICS
