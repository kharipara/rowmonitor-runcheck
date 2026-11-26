"""Durations and Airflow timestamps. Python 3.9 has no datetime.UTC."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from rowmonitor_runcheck.errors import ConfigError

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_MAX_AGE = re.compile(r"(\d+)([smhd])")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_max_age(value: str) -> tuple[timedelta, str]:
    text = value.strip().lower()
    match = _MAX_AGE.fullmatch(text)
    if match is None:
        raise ConfigError("--max-age must look like 30s, 45m, 26h, or 2d")
    amount = int(match.group(1))
    if amount <= 0:
        raise ConfigError("--max-age must be greater than zero")
    unit = match.group(2)
    label = f"{amount}{unit}"
    return timedelta(seconds=amount * _UNITS[unit]), label


def parse_datetime(value: object) -> datetime | None:
    """Parse an Airflow timestamp. Null and empty become None. Naive means UTC."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("timestamp is not a string")
    text = value.strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("invalid timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def format_age(delta: timedelta) -> str:
    seconds = max(0, int(delta.total_seconds()))
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)
