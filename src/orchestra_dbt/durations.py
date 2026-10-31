"""dbt State `lag_tolerance` duration parsing (no package imports, to avoid cycles)."""

import math
import re

_UNIT_SECONDS: dict[str, float] = {
    **dict.fromkeys(("ms", "msec", "millis", "millisecond", "milliseconds"), 0.001),
    **dict.fromkeys(("s", "sec", "secs", "second", "seconds"), 1),
    **dict.fromkeys(("m", "min", "mins", "minute", "minutes"), 60),
    **dict.fromkeys(("h", "hr", "hrs", "hour", "hours"), 3600),
    **dict.fromkeys(("d", "day", "days"), 86400),
    **dict.fromkeys(("w", "week", "weeks"), 604800),
}
# dbt-oss (v2) writes lag_tolerance to the manifest as {count, period}.
_PERIOD_SECONDS = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 604800,
}
_DURATION_PART = re.compile(r"(\d+)\s*([a-z]+)")
_DURATION = re.compile(r"^(?:\d+\s*[a-z]+\s*)+$")


def _parse_lag_tolerance_object(value: dict) -> int:
    count, period = value.get("count"), value.get("period")
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or period not in _PERIOD_SECONDS
    ):
        raise ValueError(f"Invalid lag_tolerance: {value!r}")
    return count * _PERIOD_SECONDS[str(period)]


def parse_lag_tolerance_seconds(value: str | int | dict) -> int:
    """Parse a dbt State `lag_tolerance` into seconds.

    Accepts the duration strings dbt takes (`0s`, `45m`, `1h30m`, `4 hours`,
    `120sec`, `2w`), the `{count, period}` object dbt-oss writes to the manifest,
    and a bare integer as seconds. Sub-second totals round up to whole seconds, so
    a non-zero duration never becomes a zero window.
    """
    if isinstance(value, dict):
        return _parse_lag_tolerance_object(value)
    if isinstance(value, int) and not isinstance(value, bool):
        if value < 0:
            raise ValueError(f"Invalid lag_tolerance: {value!r}")
        return value
    text = str(value).strip().lower()
    if not _DURATION.match(text):
        raise ValueError(
            f"Invalid lag_tolerance: {value!r}. Expected a duration such as '45m' or '1h30m'."
        )
    total = 0.0
    for count, unit in _DURATION_PART.findall(text):
        if unit not in _UNIT_SECONDS:
            raise ValueError(
                f"Invalid lag_tolerance: {value!r}. Unknown unit {unit!r}."
            )
        total += int(count) * _UNIT_SECONDS[unit]
    return math.ceil(total)
