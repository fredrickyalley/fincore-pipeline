from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fincore_pipeline.dates import is_before_sla_deadline, resolve_trade_date, validate_iso_date

NEW_YORK = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def test_tuesday_run_processes_monday() -> None:
    interval_end = datetime(2026, 10, 27, 0, 30, tzinfo=NEW_YORK)
    assert resolve_trade_date(None, interval_end) == "2026-10-26"


def test_saturday_run_processes_friday() -> None:
    interval_end = datetime(2026, 10, 31, 0, 30, tzinfo=NEW_YORK)
    assert resolve_trade_date({}, interval_end) == "2026-10-30"


def test_utc_interval_end_is_converted_to_new_york_first() -> None:
    # 03:30 UTC on the 27th is still 23:30 on the 26th in New York
    interval_end = datetime(2026, 10, 27, 3, 30, tzinfo=UTC)
    assert resolve_trade_date(None, interval_end) == "2026-10-25"


def test_manual_conf_override_wins() -> None:
    interval_end = datetime(2026, 10, 27, 0, 30, tzinfo=NEW_YORK)
    assert resolve_trade_date({"trade_date": "2026-10-20"}, interval_end) == "2026-10-20"


@pytest.mark.parametrize("bad", ["2026-13-01", "20261026", "2026-10-26; rm -rf /", "", "'; DROP"])
def test_invalid_dates_are_rejected_before_reaching_sql_or_shell(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_iso_date(bad)


def test_sla_deadline_is_0630_new_york_the_day_after_trade_date() -> None:
    assert is_before_sla_deadline("2026-10-26", datetime(2026, 10, 27, 6, 29, tzinfo=NEW_YORK))
    assert not is_before_sla_deadline("2026-10-26", datetime(2026, 10, 27, 6, 31, tzinfo=NEW_YORK))
