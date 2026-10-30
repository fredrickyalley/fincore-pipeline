import pytest

from fincore_pipeline.dbt_results import DbtTestSummary, summarise_run_results


def _result(unique_id: str, status: str) -> dict[str, object]:
    return {"unique_id": unique_id, "status": status}


def test_counts_pass_fail_warn_and_error() -> None:
    run_results = {
        "results": [
            _result("test.fincore.not_null_a", "pass"),
            _result("test.fincore.unique_b", "pass"),
            _result("test.fincore.assert_pnl_matches", "fail"),
            _result("test.fincore.freshness", "warn"),
            _result("test.fincore.broken_sql", "error"),
        ]
    }
    summary = summarise_run_results(run_results)
    assert summary == DbtTestSummary(
        passed=2,
        failed=1,
        warned=1,
        errored=1,
        failing_tests=("test.fincore.assert_pnl_matches", "test.fincore.broken_sql"),
    )


def test_pass_rate_counts_warnings_as_passing() -> None:
    summary = DbtTestSummary(passed=8, failed=1, warned=1, errored=0, failing_tests=("x",))
    assert summary.pass_rate_percent == pytest.approx(90.0)
    assert summary.has_failures


def test_no_tests_is_an_error_not_a_hundred_percent_pass() -> None:
    with pytest.raises(ValueError, match="no test results"):
        summarise_run_results({"results": []})
