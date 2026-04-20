"""Supported Python and dbt-core versions (see README)."""

import sys
from importlib.metadata import PackageNotFoundError, version

from packaging.specifiers import SpecifierSet
from packaging.version import Version

# Keep in sync with README and optional-dependencies in pyproject.toml.
SUPPORTED_PYTHON = (3, 11)
MAX_SUPPORTED_PYTHON_EXCLUSIVE = (
    3,
    15,
)  # i.e. 3.11 <= x < 3.15 (3.11 through 3.14 inclusive)

SUPPORTED_DBT_CORE_SPEC = ">=1.10,<1.12"


def supported_dbt_core_specifier() -> SpecifierSet:
    return SpecifierSet(SUPPORTED_DBT_CORE_SPEC)


def check_python_version() -> None:
    vi = sys.version_info
    current = (vi[0], vi[1])
    if current < SUPPORTED_PYTHON:
        sys.stderr.write(
            f"orchestra-dbt requires Python >= {SUPPORTED_PYTHON[0]}.{SUPPORTED_PYTHON[1]}, "
            f"< {MAX_SUPPORTED_PYTHON_EXCLUSIVE[0]}.{MAX_SUPPORTED_PYTHON_EXCLUSIVE[1]} "
            f"(this interpreter is {current[0]}.{current[1]}).\n"
        )
        raise SystemExit(1)
    if current >= MAX_SUPPORTED_PYTHON_EXCLUSIVE:
        sys.stderr.write(
            f"orchestra-dbt requires Python >= {SUPPORTED_PYTHON[0]}.{SUPPORTED_PYTHON[1]}, "
            f"< {MAX_SUPPORTED_PYTHON_EXCLUSIVE[0]}.{MAX_SUPPORTED_PYTHON_EXCLUSIVE[1]} "
            f"(this interpreter is {current[0]}.{current[1]}).\n"
        )
        raise SystemExit(1)


def check_dbt_core_version() -> None:
    try:
        installed = version("dbt-core")
    except PackageNotFoundError:
        sys.stderr.write(
            "dbt-core is not installed. Install a supported version "
            f"({SUPPORTED_DBT_CORE_SPEC}); see README.\n"
        )
        raise SystemExit(1) from None

    parsed = Version(installed)
    spec = supported_dbt_core_specifier()
    if parsed not in spec:
        sys.stderr.write(
            f"Unsupported dbt-core version {installed}. "
            f"Supported range is {SUPPORTED_DBT_CORE_SPEC}. See README.\n"
        )
        raise SystemExit(1)
