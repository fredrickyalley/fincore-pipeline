from fincore_pipeline.cloudwatch import build_metric_data
from fincore_pipeline.extract import build_manifest, staging_key_for


def test_staging_key_keeps_file_name_under_run_scoped_prefix() -> None:
    key = staging_key_for("market_data/dt=2026-10-26/fx.csv", "market_data", "2026-10-26")
    assert key == "staging/market_data/dt=2026-10-26/fx.csv"


def test_manifest_lists_files_and_total_bytes() -> None:
    manifest = build_manifest("trades", "2026-10-26", {"trades/dt=2026-10-26/a.fix": 10, "b": 5})
    assert manifest["file_count"] == 2
    assert manifest["total_bytes"] == 15
    assert manifest["source"] == "trades"


def test_metric_data_carries_pipeline_dimension_and_units() -> None:
    data = build_metric_data({"RejectRatePercent": 1.5, "TotalTradeCount": 10}, "2026-10-26")
    names = {item["MetricName"]: item for item in data}
    assert names["RejectRatePercent"]["Unit"] == "Percent"
    assert names["TotalTradeCount"]["Unit"] == "Count"
    assert {"Name": "Pipeline", "Value": "fincore_daily"} in names["TotalTradeCount"]["Dimensions"]
