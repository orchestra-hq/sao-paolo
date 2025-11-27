def get_pipelines() -> list[tuple[str, str]]:
    return [("A", "1"), ("B", "2")]


def get_caches() -> list[str]:
    return ["taskA:default", "taskB:default"]
