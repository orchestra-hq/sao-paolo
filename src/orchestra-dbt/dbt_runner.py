import subprocess
from .utils import log_info, log_error


def run_dbt_command(args: list[str], passthrough: bool = False) -> list[str]:
    cmd = ["dbt"] + args

    if passthrough:
        log_info(f"Running: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError:
            pass
        return []

    try:
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            log_error(result.stderr or "dbt command failed - see dbt output.")
            raise RuntimeError("dbt command failed")
    except subprocess.CalledProcessError as e:
        log_error(e)
        raise e

    return result.stdout.strip().splitlines()
