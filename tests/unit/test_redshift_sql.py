import pytest

from fincore_pipeline.redshift_sql import (
    LOAD_TARGETS,
    build_copy_statements,
    build_delete_statements,
)


def test_delete_touches_every_target_table_for_the_date_only() -> None:
    statements = build_delete_statements("2026-10-26")
    assert len(statements) == len(LOAD_TARGETS)
    for statement in statements:
        assert statement.startswith("DELETE FROM raw.")
        assert "= '2026-10-26'" in statement


def test_copy_is_delete_then_copy_so_a_single_task_rerun_is_idempotent() -> None:
    target = LOAD_TARGETS["trades"]
    statements = build_copy_statements(
        target, "2026-10-26", "fincore-processed", "arn:aws:iam::123456789012:role/redshift-copy"
    )
    assert statements[0].startswith("DELETE FROM raw.fincore_trades")
    assert "COPY raw.fincore_trades" in statements[1]
    assert "s3://fincore-processed/processed/trades/dt=2026-10-26/" in statements[1]
    assert "FORMAT AS PARQUET" in statements[1]


@pytest.mark.parametrize("bad", ["2026-10-26' OR '1'='1", "yesterday"])
def test_sql_builders_refuse_non_date_input(bad: str) -> None:
    with pytest.raises(ValueError):
        build_delete_statements(bad)


def test_copy_refuses_suspicious_role_arn() -> None:
    with pytest.raises(ValueError):
        build_copy_statements(LOAD_TARGETS["trades"], "2026-10-26", "bucket", "x' ; DROP TABLE t; --")
