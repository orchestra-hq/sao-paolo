import subprocess


def run_dbt_command(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(["dbt"] + args)
