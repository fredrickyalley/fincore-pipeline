"""Quality metrics document written next to the outputs and read by the Airflow quality gate.

Pure Python on purpose: the gate's inputs must be easy to reason about and unit-test.
"""

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal

TRADE_RULE_PREFIX = "FIX_"


def build_quality_metrics(
    run_date: str,
    total_trade_count: int,
    clean_trade_count: int,
    rejection_counts: Mapping[str, int],
    pnl_unresolvable_count: int,
    total_realized_pnl_usd: Decimal,
) -> dict[str, object]:
    rejected_trade_count = sum(
        count for rule, count in rejection_counts.items() if rule.startswith(TRADE_RULE_PREFIX)
    )
    if clean_trade_count + rejected_trade_count != total_trade_count:
        raise ValueError(
            f"Trade counts do not add up: clean={clean_trade_count} + "
            f"rejected={rejected_trade_count} != total={total_trade_count}"
        )
    reject_rate = rejected_trade_count / total_trade_count if total_trade_count else 0.0
    return {
        "run_date": run_date,
        "generated_at": datetime.now(UTC).isoformat(),
        "total_trade_count": total_trade_count,
        "clean_trade_count": clean_trade_count,
        "rejected_trade_count": rejected_trade_count,
        "reject_rate": reject_rate,
        "rejection_count_by_rule": dict(sorted(rejection_counts.items())),
        "pnl_unresolvable_count": pnl_unresolvable_count,
        # string, not float: a float here is exactly how last quarter's GBP error crept in
        "total_realized_pnl_usd": str(total_realized_pnl_usd),
    }
