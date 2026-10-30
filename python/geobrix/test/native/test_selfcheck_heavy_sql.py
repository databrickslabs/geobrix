"""``_selfcheck.heavy_sql`` hands the heavy reader a complete Shapefile.

A Shapefile is written non-sequentially: its ``.shx`` and ``.dbf`` headers are rewritten when the
writer closes. A ``/Volumes`` FUSE mount cannot do that, so a Shapefile written straight onto it holds
no rows. The oracle writes the five files on local disk and copies them over.
"""

import os
from pathlib import Path

import pyogrio

from databricks.labs.gbx._native import _selfcheck


class _Frame:
    """What ``spark.read.format(..).load(..)`` returns, counting the features the files hold."""

    def __init__(self, path):
        self.path = path

    def count(self):
        return pyogrio.read_info(os.path.join(self.path, "points.shp"))["features"]


class _Reader:
    def format(self, _name):
        return self

    def load(self, path):
        return _Frame(path)


class _Row:
    t = object()


class _Spark:
    read = _Reader()

    def sql(self, _query):
        return self

    def first(self):
        return _Row()


def _offline(monkeypatch):
    monkeypatch.setattr(_selfcheck, "_heavy_register", lambda spark: None)
    monkeypatch.setattr(
        _selfcheck,
        "_size_sql",
        lambda spark, df, view: {"ok": True, "width": 388, "height": 385},
    )
    monkeypatch.setattr(_selfcheck, "_jvm_gdal", lambda spark: {"ok": True})


def test_heavy_sql_never_writes_the_shapefile_onto_the_scratch_dir(
    tmp_path, monkeypatch
):
    """The writer sees local disk only; the scratch dir (a Volume on Databricks) gets finished copies."""
    _offline(monkeypatch)
    scratch = tmp_path / "volume"
    written = []
    real = _selfcheck._write_points

    def spy(path, driver):
        written.append(Path(path))
        return real(path, driver)

    monkeypatch.setattr(_selfcheck, "_write_points", spy)

    report = _selfcheck.heavy_sql(_Spark(), str(scratch))

    assert written, "the spy saw no Shapefile write"
    assert all(scratch not in p.parents for p in written), written
    # the copies are complete: every file the writer made is there, and the rows are readable
    shp_dir = scratch / "gbx-selfcheck-points"
    assert {p.suffix for p in shp_dir.iterdir()} >= {".shp", ".shx", ".dbf", ".prj"}
    assert report["shapefile_count"] == len(_selfcheck.POINTS_27700)
    assert report["ok"] is True
    # nothing of the local scratch is left behind
    assert not any(p.exists() and scratch not in p.parents for p in written)


def test_heavy_sql_replaces_a_torn_shapefile_left_by_an_earlier_run(
    tmp_path, monkeypatch
):
    """A previous run on a Volume left a header-only Shapefile; the next run must not trust it."""
    _offline(monkeypatch)
    shp_dir = tmp_path / "gbx-selfcheck-points"
    shp_dir.mkdir()
    (shp_dir / "points.shp").write_bytes(b"\x00" * 100)
    (shp_dir / "points.shx").write_bytes(b"\x00" * 100)

    report = _selfcheck.heavy_sql(_Spark(), str(tmp_path))

    assert report["shapefile_count"] == len(_selfcheck.POINTS_27700)
