import json
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from fincore_etl.config import JobConfig
from fincore_etl.pipeline import run_pipeline

RUN_DATE = "2026-10-26"
FIX_LINES = [
    "8=FIX.4.4|35=8|37=O1|1=PF1|55=AAPL|54=1|38=100|44=150|15=USD|60=20261026-14:00:00|",
    "8=FIX.4.4|35=8|37=O2|1=PF1|55=VOD.L|54=2|38=1000|44=11|15=GBP|60=20261026-14:00:01|",
    "8=FIX.4.4|35=8|37=O3|1=PF2|55=NOPRICE|54=1|38=1|44=1|60=20261026-14:00:02|",
    "corrupted line",
]
MARKET_FILES = {
    "equity.csv": "symbol,asset_class,currency,close_price,as_of_date\n"
                  f"AAPL,EQUITY,USD,155,{RUN_DATE}\nVOD.L,EQUITY,GBP,10.50,{RUN_DATE}\nBAD,EQUITY,USD,,{RUN_DATE}\n",
    "fx.csv": f"symbol,asset_class,currency,close_price,as_of_date\nGBPUSD,FX,USD,1.27,{RUN_DATE}\n",
}
PORTFOLIO = [
    {"portfolio_id": "PF1", "symbol": "aapl", "position_quantity": 200, "average_cost": 140,
     "currency": "USD", "metadata": {"risk_limits": {"max_position_value_usd": 1000000}}},
    "{not json",
]


def _write_inputs(root: Path) -> None:
    trades = root / "staging" / "trades" / f"dt={RUN_DATE}"
    market = root / "staging" / "market_data" / f"dt={RUN_DATE}"
    portfolio = root / "staging" / "portfolio" / f"dt={RUN_DATE}"
    for folder in (trades, market, portfolio):
        folder.mkdir(parents=True)
    (trades / "trades.fix").write_text("\n".join(FIX_LINES) + "\n")
    for name, body in MARKET_FILES.items():
        (market / name).write_text(body)
    lines = [json.dumps(item) if isinstance(item, dict) else item for item in PORTFOLIO]
    (portfolio / "portfolio.json").write_text("\n".join(lines) + "\n")


def test_full_run_writes_outputs_and_consistent_metrics(spark: SparkSession, tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    root = tmp_path.as_posix()
    config = JobConfig(run_date=RUN_DATE, input_root=f"{root}/staging", output_root=root)

    metrics = run_pipeline(spark, config)

    assert metrics["total_trade_count"] == 4
    assert metrics["clean_trade_count"] == 3
    assert metrics["pnl_unresolvable_count"] == 1
    assert metrics["rejection_count_by_rule"]["FIX_MALFORMED_LINE"] == 1
    assert metrics["rejection_count_by_rule"]["MD_NULL_CLOSE_PRICE"] == 1
    assert metrics["rejection_count_by_rule"]["PF_MALFORMED_RECORD"] == 1
    # O1: (155-150)*100 = 500 ; O2: SELL (10.50-11)*1000*1.27 = +635
    assert Decimal(metrics["total_realized_pnl_usd"]) == Decimal("1135")

    trades = spark.read.parquet(config.trades_output)
    assert trades.count() == 3
    assert spark.read.parquet(config.unresolvable_output).count() == 1
    assert spark.read.parquet(config.portfolio_pnl_output).count() == 1
    assert spark.read.parquet(config.rejected_output).count() == 3
    assert json.loads(Path(config.metrics_output).read_text())["clean_trade_count"] == 3


def test_rerun_for_same_date_overwrites_instead_of_duplicating(spark: SparkSession, tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    root = tmp_path.as_posix()
    config = JobConfig(run_date=RUN_DATE, input_root=f"{root}/staging", output_root=root)
    run_pipeline(spark, config)
    run_pipeline(spark, config)
    assert spark.read.parquet(config.trades_output).count() == 3
