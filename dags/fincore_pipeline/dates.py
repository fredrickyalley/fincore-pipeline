"""Trade-date resolution. Every date that reaches SQL or a shell goes through validate_iso_date."""

import re
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from fincore_pipeline.settings import SLA_DEADLINE_HOUR, SLA_DEADLINE_MINUTE, TIMEZONE

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_iso_date(value: str) -> str:
    if not _ISO_DATE.fullmatch(value or ""):
        raise ValueError(f"Expected a YYYY-MM-DD date, got {value!r}")
    date.fromisoformat(value)  # rejects 2026-13-01 and friends
    return value


def resolve_trade_date(conf: Mapping[str, object] | None, interval_end: datetime) -> str:
    """Manual runs pass {"trade_date": "YYYY-MM-DD"}; scheduled runs process the day
    before the run's scheduled time in New York."""
    override = (conf or {}).get("trade_date")
    if override:
        return validate_iso_date(str(override))
    local_end = interval_end.astimezone(ZoneInfo(TIMEZONE))
    return (local_end.date() - timedelta(days=1)).isoformat()


def is_before_sla_deadline(trade_date: str, completed_at: datetime) -> bool:
    deadline_day = date.fromisoformat(validate_iso_date(trade_date)) + timedelta(days=1)
    deadline = datetime.combine(
        deadline_day, time(SLA_DEADLINE_HOUR, SLA_DEADLINE_MINUTE), tzinfo=ZoneInfo(TIMEZONE)
    )
    return completed_at <= deadline
