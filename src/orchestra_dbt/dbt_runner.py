import subprocess
from .utils import log_info


def run_dbt_command(args: list[str]):
    cmd = ["dbt"] + args
    log_info(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError:
        pass
