import re

# Units dbt accepts for lag_tolerance, in nanoseconds (case-sensitive: M is months).
_SECOND = 10**9
_UNIT_NS: dict[str, int] = {
    **dict.fromkeys(("nanos", "nsec", "ns"), 1),
    **dict.fromkeys(("usec", "us", "µs"), 10**3),
    **dict.fromkeys(("millis", "msec", "ms"), 10**6),
    **dict.fromkeys(("seconds", "second", "secs", "sec", "s"), _SECOND),
    **dict.fromkeys(("minutes", "minute", "mins", "min", "m"), 60 * _SECOND),
    **dict.fromkeys(("hours", "hour", "hrs", "hr", "h"), 3600 * _SECOND),
    **dict.fromkeys(("days", "day", "d"), 86400 * _SECOND),
    **dict.fromkeys(("weeks", "week", "wks", "wk", "w"), 604800 * _SECOND),
    **dict.fromkeys(("months", "month", "M"), 2630016 * _SECOND),
    **dict.fromkeys(("years", "year", "yrs", "yr", "y"), 31557600 * _SECOND),
}
_DURATION = re.compile(r"^(\s*\d+\s*[a-zA-Zµ]+)+\s*$")
_PART = re.compile(r"(\d+)\s*([a-zA-Zµ]+)")


def parse_lag_tolerance_seconds(value: str | int | dict) -> int:
    """Seconds from a dbt `lag_tolerance`: "45m"/"1h30m" (dbt-core), {count, period}
    (dbt-oss manifest) or int seconds. Like dbt, the total must be whole seconds."""
    if isinstance(value, dict):
        value = f"{value.get('count')}{value.get('period')}"
    elif isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    text = str(value).strip()
    parts = _PART.findall(text) if _DURATION.match(text) else []
    if not parts or any(unit not in _UNIT_NS for _, unit in parts):
        raise ValueError(f"Invalid lag_tolerance: {value!r}")
    total_ns = sum(int(n) * _UNIT_NS[unit] for n, unit in parts)
    if total_ns % _SECOND:
        raise ValueError(f"Invalid lag_tolerance: {value!r} is not whole seconds")
    return total_ns // _SECOND
