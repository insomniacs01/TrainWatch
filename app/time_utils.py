from datetime import datetime, timedelta, timezone
from typing import Optional


UTC = timezone.utc
UTC_ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> datetime:
    return datetime.now(UTC)


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).strftime(UTC_ISO_FORMAT)


def utc_now_iso() -> str:
    return format_utc(utc_now())


def parse_utc_timestamp(value: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("timestamp is required")
    if raw.isdigit():
        return datetime.fromtimestamp(float(raw), tz=UTC)
    return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)


def coerce_utc_timestamp(value: Optional[str], default_delta_hours: Optional[int] = None) -> str:
    if not value:
        if default_delta_hours is None:
            return utc_now_iso()
        return format_utc(utc_now() - timedelta(hours=default_delta_hours))
    return format_utc(parse_utc_timestamp(value))
