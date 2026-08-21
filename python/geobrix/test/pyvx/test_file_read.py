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


def test_vector_file_read_managed_on_path_raises(spark, tmp_path):
    """access='managed' on a location (path/dir) source raises the table-only rule
    error — aligned with gbx_file_read. vector_file_read is location-only, so
    managed can never resolve here."""
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_geojson(tmp_path / "a.geojson", 1)
    with pytest.raises(
        ValueError, match="only valid for a MANAGED FILE-column table source"
    ):
        vector_file_read(spark, str(tmp_path), driver="GeoJSON", access="managed")


def test_vector_file_read_managed_raises_regardless_of_tier(spark, tmp_path):
    """managed raises the table-only error even on a FILE-capable tier — it is a
    source-kind rule, not a tier gate (vector_file_read reads a location)."""
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_geojson(tmp_path / "a.geojson", 1)
    with patch(
        "databricks.labs.gbx.pyvx.file_read.file_access_tier", return_value="list_files"
    ):
        with pytest.raises(
            ValueError, match="only valid for a MANAGED FILE-column table source"
        ):
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


# ---------------------------------------------------------------------------
# BUG B: pyogrio.read_dataframe (geopandas-required) must not be used
# ---------------------------------------------------------------------------


def _write_gpkg(p, n):
    """Write a .gpkg with n Point features using pyogrio.write_arrow (no geopandas)."""
    import pyarrow as pa
    import pyogrio
    import shapely

    wkbs = [bytes(shapely.to_wkb(shapely.Point(float(i), float(i)))) for i in range(n)]
    tbl = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
    pyogrio.write_arrow(
        tbl,
        str(p),
        driver="GPKG",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )


def test_vector_file_read_source_uses_read_arrow_not_read_dataframe():
    """Static guard: file_read.py must not call read_dataframe (geopandas-required).
    Uses read_arrow instead, which works without geopandas.
    This test FAILS until the BUG-B fix is applied.
    """
    import inspect

    from databricks.labs.gbx.pyvx import file_read

    src = inspect.getsource(file_read)
    assert "read_dataframe" not in src, (
        "file_read.py still calls pyogrio.read_dataframe which requires geopandas. "
        "Refactor to pyogrio.read_arrow."
    )
    assert "import geopandas" not in src and "geopandas." not in src, (
        "file_read.py must not import or call geopandas (not a light-tier dep). "
        "Comments mentioning the name are fine; actual imports/calls are not."
    )


def test_vector_file_read_gpkg_reads_wkb_without_geopandas(spark, tmp_path):
    """Behavioral test: vector_file_read reads a .gpkg and returns WKB bytes.
    The fixture is written via pyogrio.write_arrow (no geopandas), so this
    exercises the geopandas-free read path end-to-end.
    """
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_gpkg(tmp_path / "t.gpkg", 3)
    with patch("databricks.labs.gbx.pyvx.file_read.file_supported", return_value=False):
        df = vector_file_read(spark, str(tmp_path), driver="GPKG", as_wkb=True)
        rows = df.collect()
    assert len(rows) == 3
    assert all(r["source"].endswith("t.gpkg") for r in rows)
    assert all(r["geometry"] is not None for r in rows)
    # Each value must be valid WKB — round-trip through shapely
    import shapely

    for r in rows:
        geom = shapely.from_wkb(bytes(r["geometry"]))
        assert geom.geom_type == "Point"


def test_vector_file_read_gpkg_wkt_mode(spark, tmp_path):
    """as_wkb=False on a .gpkg returns WKT strings (no geopandas needed)."""
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    _write_gpkg(tmp_path / "t.gpkg", 2)
    with patch("databricks.labs.gbx.pyvx.file_read.file_supported", return_value=False):
        df = vector_file_read(spark, str(tmp_path), driver="GPKG", as_wkb=False)
        rows = df.collect()
    assert len(rows) == 2
    assert all(
        isinstance(r["geometry"], str) and r["geometry"].startswith("POINT")
        for r in rows
    )
