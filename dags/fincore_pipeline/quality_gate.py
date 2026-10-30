"""Pass/fail decision on the ETL quality metrics. Fails closed on missing or malformed input."""

from collections.abc import Mapping
from dataclasses import dataclass

from fincore_pipeline.settings import MAX_PNL_UNRESOLVABLE, MAX_REJECT_RATE


@dataclass(frozen=True)
class GateResult:
    failures: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.failures


def _check(metrics: Mapping[str, object]) -> list[str]:
    total = int(metrics["total_trade_count"])  # type: ignore[call-overload]
    reject_rate = float(metrics["reject_rate"])  # type: ignore[arg-type]
    unresolvable = int(metrics["pnl_unresolvable_count"])  # type: ignore[call-overload]
    failures = []
    if total <= 0:
        failures.append("total_trade_count is 0: trade file was empty")
    if reject_rate >= MAX_REJECT_RATE:
        failures.append(f"reject_rate {reject_rate:.2%} is not below {MAX_REJECT_RATE:.0%}")
    if unresolvable >= MAX_PNL_UNRESOLVABLE:
        failures.append(
            f"pnl_unresolvable_count {unresolvable} is not below {MAX_PNL_UNRESOLVABLE}"
        )
    return failures


def evaluate_quality_gate(metrics: Mapping[str, object]) -> GateResult:
    try:
        return GateResult(failures=tuple(_check(metrics)))
    except (KeyError, TypeError, ValueError) as error:
        return GateResult(failures=(f"metrics file unreadable: {error!r}",))


def format_gate_summary(metrics: Mapping[str, object], result: GateResult) -> str:
    status = "PASSED" if result.passed else "FAILED"
    lines = [
        f"FinCore quality gate {status} for {metrics.get('run_date', '?')}",
        f"- trades: total={metrics.get('total_trade_count')} "
        f"clean={metrics.get('clean_trade_count')} rejected={metrics.get('rejected_trade_count')}",
        f"- reject_rate: {float(metrics.get('reject_rate', 0.0)):.2%}",  # type: ignore[arg-type]
        f"- pnl_unresolvable_count: {metrics.get('pnl_unresolvable_count')}",
        f"- total_realized_pnl_usd: {metrics.get('total_realized_pnl_usd')}",
        f"- rejections by rule: {metrics.get('rejection_count_by_rule')}",
    ]
    lines.extend(f"* {failure}" for failure in result.failures)
    return "\n".join(lines)
