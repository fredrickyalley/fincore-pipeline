"""Job configuration: one run date, one input root, one output root. Every path is derived
from these, so a re-run for the same date always writes to the same partitions."""

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class JobConfig:
    run_date: str
    input_root: str  # e.g. s3://fincore-processed/staging (the extract snapshot)
    output_root: str  # e.g. s3://fincore-processed
    portfolio_multiline: bool = False

    def __post_init__(self) -> None:
        date.fromisoformat(self.run_date)  # fail fast on a bad --run_date

    def _input(self, source: str) -> str:
        return f"{self.input_root}/{source}/dt={self.run_date}/"

    def _output(self, dataset: str) -> str:
        return f"{self.output_root}/{dataset}/dt={self.run_date}/"

    @property
    def trades_input(self) -> str:
        return self._input("trades")

    @property
    def market_data_input(self) -> str:
        return self._input("market_data")

    @property
    def portfolio_input(self) -> str:
        return self._input("portfolio")

    @property
    def trades_output(self) -> str:
        return self._output("processed/trades")

    @property
    def market_data_output(self) -> str:
        return self._output("processed/market_data")

    @property
    def portfolio_pnl_output(self) -> str:
        return self._output("processed/portfolio_pnl")

    @property
    def unresolvable_output(self) -> str:
        return self._output("exceptions/pnl_unresolvable")

    @property
    def rejected_output(self) -> str:
        return self._output("rejected")

    @property
    def metrics_output(self) -> str:
        return f"{self._output('metrics')}quality_metrics.json"


def parse_job_args(argv: Sequence[str]) -> JobConfig:
    parser = argparse.ArgumentParser(description="FinCore daily ETL")
    parser.add_argument("--run_date", required=True)
    parser.add_argument("--input_root", required=True)
    parser.add_argument("--output_root", required=True)
    parser.add_argument("--portfolio_multiline", default="false")
    # Glue appends its own arguments (--JOB_NAME, --job-bookmark-option ...); ignore them
    args, _unknown = parser.parse_known_args(argv)
    return JobConfig(
        run_date=args.run_date,
        input_root=args.input_root.rstrip("/"),
        output_root=args.output_root.rstrip("/"),
        portfolio_multiline=args.portfolio_multiline.lower() == "true",
    )
