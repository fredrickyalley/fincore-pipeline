from decimal import Decimal

import pytest

from fincore_etl.quality import build_quality_metrics


def _build(**overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "run_date": "2026-10-26",
        "total_trade_count": 1000,
        "clean_trade_count": 990,
        "rejection_counts": {"FIX_MALFORMED_LINE": 6, "FIX_INVALID_SIDE": 4, "MD_NULL_CLOSE_PRICE": 2},
        "pnl_unresolvable_count": 3,
        "total_realized_pnl_usd": Decimal("12345.678901"),
    }
    kwargs.update(overrides)
    return build_quality_metrics(**kwargs)  # type: ignore[arg-type]


def test_reject_rate_counts_only_trade_rejections() -> None:
    metrics = _build()
    assert metrics["rejected_trade_count"] == 10
    assert metrics["reject_rate"] == pytest.approx(0.01)


def test_rejection_counts_cover_every_rule_sorted() -> None:
    metrics = _build()
    assert metrics["rejection_count_by_rule"] == {
        "FIX_INVALID_SIDE": 4,
        "FIX_MALFORMED_LINE": 6,
        "MD_NULL_CLOSE_PRICE": 2,
    }


def test_total_pnl_is_serialised_as_exact_decimal_string() -> None:
    metrics = _build()
    assert metrics["total_realized_pnl_usd"] == "12345.678901"


def test_zero_trades_gives_zero_reject_rate_not_division_error() -> None:
    metrics = _build(total_trade_count=0, clean_trade_count=0, rejection_counts={})
    assert metrics["reject_rate"] == 0.0


def test_inconsistent_counts_are_refused() -> None:
    with pytest.raises(ValueError, match="do not add up"):
        _build(clean_trade_count=500)
