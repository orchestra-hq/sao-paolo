import sys
from importlib.metadata import PackageNotFoundError

import pytest

from src.orchestra_dbt import compatibility


def test_supported_dbt_core_specifier_accepts_1_11():
    assert (
        compatibility.Version("1.11.6") in compatibility.supported_dbt_core_specifier()
    )


def test_supported_dbt_core_specifier_rejects_1_9():
    assert (
        compatibility.Version("1.9.0")
        not in compatibility.supported_dbt_core_specifier()
    )


def test_supported_dbt_core_specifier_rejects_1_12():
    assert (
        compatibility.Version("1.12.0")
        not in compatibility.supported_dbt_core_specifier()
    )


def test_check_python_version_exits_on_too_old(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 10, 0, "final", 0))
    with pytest.raises(SystemExit) as exc:
        compatibility.check_python_version()
    assert exc.value.code == 1


def test_check_python_version_exits_on_too_new(monkeypatch):
    monkeypatch.setattr(sys, "version_info", (3, 15, 0, "final", 0))
    with pytest.raises(SystemExit) as exc:
        compatibility.check_python_version()
    assert exc.value.code == 1


def test_check_dbt_core_version_exits_when_missing(monkeypatch):
    def raise_not_found(_name: str):
        raise PackageNotFoundError

    monkeypatch.setattr(compatibility, "version", raise_not_found)
    with pytest.raises(SystemExit) as exc:
        compatibility.check_dbt_core_version()
    assert exc.value.code == 1


def test_check_dbt_core_version_exits_when_out_of_range(monkeypatch):
    monkeypatch.setattr(compatibility, "version", lambda _name: "1.12.0")
    with pytest.raises(SystemExit) as exc:
        compatibility.check_dbt_core_version()
    assert exc.value.code == 1


def test_check_dbt_core_version_ok(monkeypatch):
    monkeypatch.setattr(compatibility, "version", lambda _name: "1.11.6")
    compatibility.check_dbt_core_version()
