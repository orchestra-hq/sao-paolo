from .models import FreshnessConfig


def parse_build_after_duration_minutes(build_after: dict[str, str | int]) -> int:
    period = build_after["period"]  # minute | hour | day
    match period:
        case "minute":
            mins_multiplier = 1
        case "hour":
            mins_multiplier = 60
        case "day":
            mins_multiplier = 1440
        case _:
            raise ValueError(f"Invalid period: {period}")

    count = build_after["count"]
    if not isinstance(count, int):
        raise ValueError(f"Invalid count: {count}")

    return count * mins_multiplier


def parse_freshness_config(config_on_node: dict | None) -> FreshnessConfig:
    freshness_config = FreshnessConfig()
    if config_on_node and "build_after" in config_on_node:
        build_after_config = config_on_node["build_after"]
        if "updates_on" in build_after_config:
            updates_on = str(build_after_config["updates_on"]).lower()
            match updates_on:
                case "any" | "all":
                    freshness_config.updates_on = updates_on
                case _:
                    freshness_config.updates_on = "any"
        freshness_config.minutes_sla = parse_build_after_duration_minutes(
            build_after_config
        )
    return freshness_config
