"""Custom CloudWatch metrics behind monitoring/cloudwatch_dashboard.json."""

from collections.abc import Mapping
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from fincore_pipeline.settings import AWS_CONN_ID, CLOUDWATCH_NAMESPACE, TIMEZONE

PIPELINE_DIMENSION = {"Name": "Pipeline", "Value": "fincore_daily"}


def _unit_for(metric_name: str) -> str:
    if metric_name.endswith("Percent"):
        return "Percent"
    if metric_name.endswith("Seconds"):
        return "Seconds"
    return "Count"


def build_metric_data(metrics: Mapping[str, float], trade_date: str) -> list[dict[str, Any]]:
    # Stamp the datapoint at the trade date so a re-run lands on the same day in the chart
    timestamp = datetime.combine(date.fromisoformat(trade_date), time(12), tzinfo=ZoneInfo(TIMEZONE))
    return [
        {
            "MetricName": name,
            "Dimensions": [PIPELINE_DIMENSION],
            "Timestamp": timestamp,
            "Value": float(value),
            "Unit": _unit_for(name),
        }
        for name, value in metrics.items()
    ]


def publish_metrics(metrics: Mapping[str, float], trade_date: str) -> None:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

    client = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="cloudwatch").get_conn()
    client.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE, MetricData=build_metric_data(metrics, trade_date)
    )
