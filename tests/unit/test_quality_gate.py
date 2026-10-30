from fincore_pipeline.quality_gate import evaluate_quality_gate, format_gate_summary


def _metrics(**overrides: object) -> dict[str, object]:
    metrics: dict[str, object] = {
        "run_date": "2026-10-26",
        "total_trade_count": 500_000,
        "clean_trade_count": 495_000,
        "rejected_trade_count": 5_000,
        "reject_rate": 0.01,
        "pnl_unresolvable_count": 12,
        "total_realized_pnl_usd": "1500.25",
        "rejection_count_by_rule": {"FIX_INVALID_SIDE": 5_000},
    }
    metrics.update(overrides)
    return metrics


def test_passes_when_both_thresholds_are_met() -> None:
    result = evaluate_quality_gate(_metrics())
    assert result.passed
    assert result.failures == ()


def test_reject_rate_of_exactly_two_percent_fails() -> None:
    result = evaluate_quality_gate(_metrics(reject_rate=0.02))
    assert not result.passed
    assert "reject_rate" in result.failures[0]


def test_hundred_unresolvable_records_fails() -> None:
    result = evaluate_quality_gate(_metrics(pnl_unresolvable_count=100))
    assert not result.passed
    assert "pnl_unresolvable_count" in result.failures[0]


def test_empty_trade_file_fails_even_with_zero_reject_rate() -> None:
    result = evaluate_quality_gate(_metrics(total_trade_count=0, reject_rate=0.0))
    assert not result.passed


def test_reports_every_failed_criterion() -> None:
    result = evaluate_quality_gate(_metrics(reject_rate=0.05, pnl_unresolvable_count=250))
    assert len(result.failures) == 2


def test_missing_metric_fails_closed() -> None:
    metrics = _metrics()
    del metrics["reject_rate"]
    result = evaluate_quality_gate(metrics)
    assert not result.passed


def test_summary_mentions_date_counts_and_failures() -> None:
    metrics = _metrics(reject_rate=0.05)
    summary = format_gate_summary(metrics, evaluate_quality_gate(metrics))
    assert "2026-10-26" in summary
    assert "5.00%" in summary
    assert "FAILED" in summary
