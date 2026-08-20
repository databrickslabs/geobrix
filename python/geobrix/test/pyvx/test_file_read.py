"""Task 6: vector_file_read — function-layer FILE read (parity with raster)."""

import json
from unittest.mock import patch

import pytest


def _write_geojson(p, n):
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": i},
                "geometry": {"type": "Point", "coordinates": [float(i), float(i)]},
            }
            for i in range(n)
        ],
    }
    p.write_text(json.dumps(fc))


def test_vector_file_read_fuse_reads_members(spark, tmp_path):
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_geojson(tmp_path / "a.geojson", 2)
    _write_geojson(tmp_path / "b.geojson", 3)
    with patch("databricks.labs.gbx.pyvx.file_read.file_supported", return_value=False):
        df = vector_file_read(spark, str(tmp_path), driver="GeoJSON")
        rows = df.collect()
    assert len(rows) == 5
    assert set(r["source"] for r in rows) == {
        str(tmp_path / "a.geojson"),
        str(tmp_path / "b.geojson"),
    }
    assert all(r["geometry"] is not None for r in rows)


def test_vector_file_read_injects_file_ref_when_supported(spark, tmp_path, monkeypatch):
    from databricks.labs.gbx.pyvx import file_read

    _write_geojson(tmp_path / "a.geojson", 1)
    captured = {}

    real_inject = file_read._inject_file_ref

    def _spy_inject(df, col):
        captured["injected"] = True
        return real_inject(df, col)

    monkeypatch.setattr(file_read, "file_supported", lambda s=None: True)
    monkeypatch.setattr(
        file_read,
        "file_ref_arg",
        lambda c, spark=None: __import__("pyspark.sql.functions", fromlist=["lit"]).lit(
            None
        ),
    )
    monkeypatch.setattr(file_read, "_inject_file_ref", _spy_inject)
    df = file_read.vector_file_read(spark, str(tmp_path), driver="GeoJSON")
    df.collect()
    assert captured.get("injected") is True


def test_vector_file_read_managed_raises_on_fuse(spark, tmp_path):
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_geojson(tmp_path / "a.geojson", 1)
    with patch(
        "databricks.labs.gbx.pyvx.file_read.file_access_tier", return_value="fuse"
    ):
        with pytest.raises(ValueError, match="FILE is not available"):
            vector_file_read(spark, str(tmp_path), driver="GeoJSON", access="managed")


def test_vector_file_read_as_wkb_false_returns_wkt(spark, tmp_path):
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_geojson(tmp_path / "c.geojson", 2)
    with patch("databricks.labs.gbx.pyvx.file_read.file_supported", return_value=False):
        df = vector_file_read(spark, str(tmp_path), driver="GeoJSON", as_wkb=False)
        rows = df.collect()
    assert len(rows) == 2
    assert all(
        isinstance(r["geometry"], str) and r["geometry"].startswith("POINT")
        for r in rows
    )
