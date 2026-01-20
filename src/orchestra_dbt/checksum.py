from hashlib import sha256
from os.path import getsize

from .constants import MAX_SEED_SIZE_BYTES
from .logger import log_error, log_warn
from .utils import load_seed_bytes


def calculate_checksum(
    resource_type: str, node_checksum: str, file_path: str
) -> str | None:
    if resource_type != "seed":
        return node_checksum

    try:
        file_size = getsize(file_path)
        if file_size > MAX_SEED_SIZE_BYTES:
            log_warn(
                f"Seed file {file_path} ({file_size / 1024 / 1024:.2f}MB) is over "
                f"{MAX_SEED_SIZE_BYTES / 1024 / 1024:.2f}MB. Skipping checksum."
            )
            return None
        return sha256(load_seed_bytes(file_path)).hexdigest()
    except FileNotFoundError:
        log_error(f"Seed file {file_path} not found. Cannot check state for this node.")
        return None
