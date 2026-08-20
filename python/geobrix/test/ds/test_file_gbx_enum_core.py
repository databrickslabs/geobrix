"""Task 1: session-free enumeration core — _accept_basename + list_local_files."""

import os

import pytest

from databricks.labs.gbx.ds import file_gbx


def test_accept_basename_skips_hidden_by_default():
    assert (
        file_gbx._accept_basename("_SUCCESS", include_hidden=False, glob_patterns=None)
        is False
    )
    assert (
        file_gbx._accept_basename(".crc", include_hidden=False, glob_patterns=None)
        is False
    )
    assert (
        file_gbx._accept_basename("a.tif", include_hidden=False, glob_patterns=None)
        is True
    )


def test_accept_basename_include_hidden_admits_underscore():
    assert (
        file_gbx._accept_basename("_data.tif", include_hidden=True, glob_patterns=None)
        is True
    )


def test_accept_basename_glob_is_anded_case_insensitive():
    # include_hidden True but glob still excludes non-matching names
    assert (
        file_gbx._accept_basename("a.TIF", include_hidden=True, glob_patterns=["*.tif"])
        is True
    )
    assert (
        file_gbx._accept_basename("a.nc", include_hidden=True, glob_patterns=["*.tif"])
        is False
    )


def test_list_local_files_sorted_paths_and_extension_filter(tmp_path):
    (tmp_path / "b.tif").write_bytes(b"x")
    (tmp_path / "a.tif").write_bytes(b"x")
    (tmp_path / "c.nc").write_bytes(b"x")
    (tmp_path / "_SUCCESS").write_bytes(b"")
    out = file_gbx.list_local_files(str(tmp_path), extensions=(".tif",))
    assert out == sorted(out)
    assert [os.path.basename(p) for p in out] == ["a.tif", "b.tif"]


def test_list_local_files_raises_when_no_match(tmp_path):
    (tmp_path / "a.nc").write_bytes(b"x")
    with pytest.raises(FileNotFoundError):
        file_gbx.list_local_files(str(tmp_path), extensions=(".tif",))
