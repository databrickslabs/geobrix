"""Task 6/Task-2: vector_file_read — function-layer FILE read (parity with raster)
and table mode (FILE-column-table read gap closure).
"""

import json
import shutil
from pathlib import Path
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


# ---------------------------------------------------------------------------
# Task 2: table mode for vector_file_read
# (FILE-column-table read gap, plain-path branch, no FILE support needed locally)
# ---------------------------------------------------------------------------


def _make_path_table(spark, name, paths):
    """Create a plain Hive table with a single ``path`` column from *paths*.

    Drops and recreates so tests are idempotent.  Uses the same warehouse-dir
    cleanup pattern as the resolve_file_table tests in test_file_gbx.py.
    """
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    wh = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse").replace(
        "file:", ""
    )
    stale = Path(wh) / name
    if stale.exists():
        shutil.rmtree(str(stale))
    df = spark.createDataFrame([(p,) for p in paths], "path string")
    df.write.saveAsTable(name)


def test_vector_file_read_table_roundtrip_external(spark, tmp_path):
    """Table mode round-trip: write a gpkg to a local dir acting as the Volume
    staging area, register its path in a plain Hive table, then read back via
    ``vector_file_read`` table mode and assert feature count + geometry type.

    Uses the plain (no FILE column) branch of ``resolve_file_table`` so the test
    runs on local[2] without FILE support.  Feature-level semantics (one .gpkg = one
    row = one FILE ref → decode yields all its features) is verified end-to-end.
    """
    import shapely

    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    gpkg_path = tmp_path / "roundtrip.gpkg"
    _write_gpkg(gpkg_path, 4)

    tbl = "_vfr_table_roundtrip_ext"
    _make_path_table(spark, tbl, [str(gpkg_path)])

    # Auto-detect: dotted-looking table name → table mode
    df = vector_file_read(spark, tbl, driver="GPKG")
    rows = df.collect()

    assert len(rows) == 4, f"expected 4 features from table mode, got {len(rows)}"
    assert all(r["geometry"] is not None for r in rows)
    for r in rows:
        geom = shapely.from_wkb(bytes(r["geometry"]))
        assert geom.geom_type == "Point", f"unexpected geom type: {geom.geom_type}"


def test_vector_file_read_source_type_detection(spark, tmp_path):
    """_classify_vector_source routes correctly for all source_type values.

    - ``source_type='auto'`` + dotted, extension-less, non-existent → ``'table'``
    - ``source_type='auto'`` + ``/``-prefixed path → ``'path'``
    - ``source_type='auto'`` + path with file extension → ``'path'``
    - ``source_type='auto'`` + existing filesystem directory → ``'path'``
    - Explicit ``source_type='table'`` overrides heuristic → ``'table'``
    - Explicit ``source_type='path'`` overrides heuristic → ``'path'``
    """
    from databricks.labs.gbx.pyvx.file_read import _classify_vector_source

    # Qualified table names → table
    assert _classify_vector_source("catalog.schema.table", "auto") == "table"
    assert _classify_vector_source("schema.table", "auto") == "table"
    assert (
        _classify_vector_source("my_table", "auto") == "table"
    )  # no dot, no ext, non-existent

    # Absolute path → path
    assert _classify_vector_source("/Volumes/cat/sch/vol/file.gpkg", "auto") == "path"
    assert _classify_vector_source("/tmp/something", "auto") == "path"

    # String with file extension → path
    assert _classify_vector_source("relative_file.gpkg", "auto") == "path"
    assert _classify_vector_source("data.geojson", "auto") == "path"

    # Existing directory on the filesystem → path
    assert _classify_vector_source(str(tmp_path), "auto") == "path"

    # Explicit overrides
    assert _classify_vector_source("catalog.schema.tbl", "path") == "path"
    assert _classify_vector_source("/Volumes/cat/sch/vol/data", "table") == "table"


def test_vector_file_read_table_skip_ordering(spark, tmp_path):
    """skip_ordering kwarg is forwarded to resolve_file_table.

    Patches ``resolve_file_table`` in the ``pyvx.file_read`` module and asserts
    it is called with the ``skip_ordering`` value the caller specified.
    """
    import databricks.labs.gbx.pyvx.file_read as vfr_mod
    from databricks.labs.gbx.pyvx.file_read import vector_file_read

    gpkg_path = tmp_path / "f.gpkg"
    _write_gpkg(gpkg_path, 1)

    tbl = "_vfr_skip_ordering_fwd_test"
    _make_path_table(spark, tbl, [str(gpkg_path)])

    original_rft = vfr_mod.resolve_file_table
    calls = []

    def _spy(sp, t, *, skip_ordering=False):
        calls.append(skip_ordering)
        return original_rft(sp, t, skip_ordering=skip_ordering)

    with patch.object(vfr_mod, "resolve_file_table", _spy):
        vector_file_read(spark, tbl, driver="GPKG", skip_ordering=False).collect()
        vector_file_read(spark, tbl, driver="GPKG", skip_ordering=True).collect()

    assert calls == [
        False,
        True,
    ], f"resolve_file_table must be called with skip_ordering forwarded; got {calls}"
