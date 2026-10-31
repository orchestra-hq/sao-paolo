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
_DURATION = re.compile(r"^(\s*\d+\s*[a-z]+)+\s*$")
_PART = re.compile(r"(\d+)\s*([a-z]+)")


def parse_lag_tolerance_seconds(value: str | int | dict) -> int:
    """Seconds from a dbt `lag_tolerance`: "45m"/"1h30m" (dbt-core), {count, period}
    (dbt-oss manifest) or int seconds. Sub-second values round up."""
    if isinstance(value, dict):
        value = f"{value.get('count')}{value.get('period')}"
    elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    text = str(value).strip().lower()
    parts = _PART.findall(text) if _DURATION.match(text) else []
    if not parts or any(unit not in _UNIT_SECONDS for _, unit in parts):
        raise ValueError(f"Invalid lag_tolerance: {value!r}")
    return math.ceil(sum(int(n) * _UNIT_SECONDS[unit] for n, unit in parts))
