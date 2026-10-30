"""Parse dbt's target/run_results.json into a summary the DAG can alert and report on."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

PERCENT = 100.0


@dataclass(frozen=True)
class DbtTestSummary:
    passed: int
    failed: int
    warned: int
    errored: int
    failing_tests: tuple[str, ...]

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.warned + self.errored

    @property
    def pass_rate_percent(self) -> float:
        return (self.passed + self.warned) / self.total * PERCENT

    @property
    def has_failures(self) -> bool:
        return bool(self.failing_tests)


def summarise_run_results(run_results: Mapping[str, Any]) -> DbtTestSummary:
    results = run_results.get("results") or []
    if not results:
        raise ValueError("dbt produced no test results; treat as a pipeline error")
    statuses = [str(result["status"]) for result in results]
    failing = tuple(
        str(result["unique_id"]) for result in results if result["status"] in ("fail", "error")
    )
    return DbtTestSummary(
        passed=statuses.count("pass"),
        failed=statuses.count("fail"),
        warned=statuses.count("warn"),
        errored=statuses.count("error"),
        failing_tests=failing,
    )
