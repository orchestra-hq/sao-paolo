import subprocess

from .utils import log_info


def run_dbt_command(args: list[str]) -> subprocess.CompletedProcess:
    cmd = ["dbt"] + args
    log_info(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd)
