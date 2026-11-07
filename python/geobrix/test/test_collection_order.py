"""The order the root conftest gives a process: the files that use netCDF4 run after the others."""

from test import conftest as root
from types import SimpleNamespace

import pytest


def _item(path, *keywords, fixturenames=()):
    return SimpleNamespace(
        path=path, keywords=set(keywords), fixturenames=list(fixturenames)
    )


@pytest.mark.parametrize(
    "source,expected",
    [
        ("import netCDF4\n", True),
        ("from netCDF4 import Dataset\n", True),
        ("def test_x():\n    from netCDF4 import Dataset\n", True),
        ("def test_x():\n    import netCDF4, rasterio\n", True),
        ('"""Reads what netCDF4 wrote."""\nimport numpy\n', False),
        ("import netCDF4_extra_thing\n", False),
    ],
)
def test_a_file_that_imports_netcdf4_is_found(tmp_path, source, expected):
    path = tmp_path / f"t{abs(hash(source))}.py"
    path.write_text(source)
    assert root._uses_netcdf4(path) is expected


def test_the_netcdf4_files_run_after_the_rest_and_before_the_policy_meta_row(tmp_path):
    plain = tmp_path / "test_plain.py"
    plain.write_text("def test_a():\n    pass\n")
    nc = tmp_path / "test_nc.py"
    nc.write_text("def test_b():\n    from netCDF4 import Dataset\n")
    items = [
        _item(nc, "a"),
        _item(plain, "policy_meta"),
        _item(plain, "b", fixturenames=["spark_connect_local"]),
        _item(plain, "c"),
    ]
    items.sort(key=root._collection_rank)
    assert [i.path.name for i in items] == [
        "test_plain.py",  # c
        "test_nc.py",
        "test_plain.py",  # policy_meta
        "test_plain.py",  # connect
    ]
    assert [sorted(i.keywords) for i in items] == [
        ["c"],
        ["a"],
        ["policy_meta"],
        ["b"],
    ]
