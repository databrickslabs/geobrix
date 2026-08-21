"""Executes the GBX Common Functions doc examples against real sample data (Docker)."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import common_functions_examples as ex  # noqa: E402


def test_gbx_file_read_then_decode(spark):
    """gbx_file_read -> rst_fromfile returns at least one tile."""
    ex.gbx_file_read_then_decode(spark)


def test_list_local_files_example(tmp_path):
    """list_local_files returns sorted paths filtered by extension."""
    (tmp_path / "a.tif").write_bytes(b"x")
    (tmp_path / "b.nc").write_bytes(b"x")
    out = ex.list_local_files_example(str(tmp_path))
    assert [os.path.basename(p) for p in out] == ["a.tif"]
