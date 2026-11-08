"""Tests for the FILE-write layout sweep orchestrator.

Verifies ``run_file_write_layout_sweep`` emits exactly one ResultRow per layout
with the ``layout`` field set correctly, and that ``OPTIMIZE`` is attempted only
for the ``cluster`` layout (not the others).

All tests use ``file_mode="fuse"`` and ``where="venv"`` so no FILE tier or
cluster is required — they run on local[2] in the Docker dev container.
"""


def _write_one_gpkg(tmp_path, rows=5):
    """Write a single .gpkg file and return its path (mirrors test_gpkg_file_legs)."""
    import pyarrow as pa
    import pyogrio
    import shapely

    p = str(tmp_path / "sweep_src.gpkg")
    wkbs = [bytes(shapely.to_wkb(shapely.Point(float(j), 0.0))) for j in range(rows)]
    tbl = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
    pyogrio.write_arrow(
        tbl,
        p,
        driver="GPKG",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )
    return p


def test_layout_sweep_gpkg_one_row_per_layout_in_its_own_target(spark, tmp_path):
    """Three layouts give three rows, each tagged with its layout and written to its own target path; OPTIMIZE is
    attempted (and noted) on the cluster layout only."""
    import os

    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    prefix = str(tmp_path / "lw")
    rows = run_file_write_layout_sweep(
        spark,
        fmt="gpkg",
        source=_write_one_gpkg(tmp_path, rows=5),
        target_prefix=prefix,
        run_id="t",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "cluster", "plain"),
        where="venv",
    )
    assert sorted(r.layout for r in rows) == ["cluster", "order", "plain"]
    by_layout = {r.layout: r for r in rows}
    assert "optimize" in (by_layout["cluster"].note or "").lower()
    for lyt in ("order", "plain"):
        assert "OPTIMIZE" not in (by_layout[lyt].note or ""), lyt
    for lyt in by_layout:
        assert os.path.exists(f"{prefix}_{lyt}"), lyt


def _one_tile_df(spark):
    """A one-row DataFrame holding a materialized 4 x 3 GeoTIFF tile."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.ds.raster import reader_schema_v2

    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(np.arange(12, dtype="float32").reshape(3, 4), 1)
        gtiff_bytes = bytearray(mf.read())
    tile = {
        "cellid": 0,
        "raster": gtiff_bytes,
        "path": None,
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }
    return spark.createDataFrame([("test_src", tile)], schema=reader_schema_v2())


def test_layout_sweep_gtiff_rows_and_the_not_available_modes(spark, tmp_path):
    """The gtiff path gives a row per layout. A managed write with no filespace is na_by_design, and an external one
    on a session with no catalog is na_by_design or an error row: neither raises out of the sweep.
    """
    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    tile_df = _one_tile_df(spark)

    def sweep(file_mode, target, layouts, **kw):
        return run_file_write_layout_sweep(
            spark,
            fmt="gtiff",
            source=tile_df,
            target_prefix=target,
            run_id="t",
            warmup=0,
            measured=1,
            file_mode=file_mode,
            layouts=layouts,
            where="venv",
            **kw,
        )

    rows = sweep("fuse", str(tmp_path / "gt"), ("order", "plain"))
    assert sorted(r.layout for r in rows) == ["order", "plain"]
    (managed,) = sweep("managed", str(tmp_path / "mgd"), ("order",), filespace=None)
    assert managed.status == "na_by_design"
    (external,) = sweep(
        "external", "no_catalog.no_schema.bench_layout_ext", ("order",), filespace=None
    )
    assert external.status in ("na_by_design", "error")


def test_backtick_qualified_quotes_each_part():
    """The cluster-layout OPTIMIZE must quote each identifier part, NOT the whole
    dotted name — backticking `cat.sch.tbl` as one identifier resolves to a table
    of that literal name in the current schema (TABLE_OR_VIEW_NOT_FOUND), which
    silently skipped OPTIMIZE and left the cluster layout unmaterialized."""
    from databricks.labs.gbx.bench.readers import _backtick_qualified

    assert _backtick_qualified("cat.sch.tbl") == "`cat`.`sch`.`tbl`"
    assert _backtick_qualified("tbl") == "`tbl`"
    # Regression guard: must NOT backtick the whole dotted name.
    assert _backtick_qualified("a.b.c") != "`a.b.c`"
