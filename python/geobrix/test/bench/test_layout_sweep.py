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


def test_layout_sweep_emits_row_per_layout(spark, tmp_path):
    """Three layouts → three ResultRows, each with the correct layout tag."""
    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    out = _write_one_gpkg(tmp_path, rows=5)
    rows = run_file_write_layout_sweep(
        spark,
        fmt="gpkg",
        source=out,
        target_prefix=str(tmp_path / "lw"),
        run_id="t",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "cluster", "plain"),
        where="venv",
    )
    assert len(rows) == 3
    assert {r.layout for r in rows} == {"order", "cluster", "plain"}


def test_layout_sweep_layouts_set_on_each_row(spark, tmp_path):
    """Each row's ``layout`` field matches the layout it was produced for."""
    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    out = _write_one_gpkg(tmp_path, rows=5)
    rows = run_file_write_layout_sweep(
        spark,
        fmt="gpkg",
        source=out,
        target_prefix=str(tmp_path / "ls2"),
        run_id="t2",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "plain"),
        where="venv",
    )
    layout_map = {r.layout: r for r in rows}
    assert "order" in layout_map
    assert "plain" in layout_map
    assert layout_map["order"].layout == "order"
    assert layout_map["plain"].layout == "plain"


def test_layout_sweep_optimize_only_for_cluster(spark, tmp_path):
    """OPTIMIZE note appears only on the cluster row (fuse mode → skip, but not on others)."""
    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    out = _write_one_gpkg(tmp_path, rows=5)
    rows = run_file_write_layout_sweep(
        spark,
        fmt="gpkg",
        source=out,
        target_prefix=str(tmp_path / "ls3"),
        run_id="t3",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "cluster", "plain"),
        where="venv",
    )
    by_layout = {r.layout: r for r in rows}
    # cluster row's note should mention OPTIMIZE (either "+OPTIMIZE" success or skip reason)
    cluster_note = by_layout["cluster"].note or ""
    assert (
        "OPTIMIZE" in cluster_note or "optimize" in cluster_note.lower()
    ), f"cluster note should reference OPTIMIZE, got: {cluster_note!r}"
    # non-cluster rows should NOT mention OPTIMIZE
    for lyt in ("order", "plain"):
        note = by_layout[lyt].note or ""
        assert (
            "OPTIMIZE" not in note
        ), f"layout={lyt!r} should not mention OPTIMIZE, got: {note!r}"


def test_layout_sweep_uses_separate_targets(spark, tmp_path):
    """Each layout writes to its own target path (leg isolation)."""
    import os

    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep

    out = _write_one_gpkg(tmp_path, rows=5)
    prefix = str(tmp_path / "ls4")
    run_file_write_layout_sweep(
        spark,
        fmt="gpkg",
        source=out,
        target_prefix=prefix,
        run_id="t4",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "plain"),
        where="venv",
    )
    # Each layout target directory should exist (written separately)
    for lyt in ("order", "plain"):
        expected = f"{prefix}_{lyt}"
        assert os.path.exists(
            expected
        ), f"Expected per-layout target dir {expected!r} to exist"


def test_layout_sweep_gtiff_fmt(spark, tmp_path):
    """Smoke-test the gtiff format path of the sweep."""
    import numpy as np

    # Build a minimal one-tile DataFrame (materialized GeoTIFF)
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.bench.readers import run_file_write_layout_sweep
    from databricks.labs.gbx.ds.raster import reader_schema_v2

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    )
    data = np.arange(12, dtype="float32").reshape(3, 4)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
        gtiff_bytes = bytearray(mf.read())

    schema = reader_schema_v2()
    row = (
        "test_src",
        {
            "cellid": 0,
            "raster": gtiff_bytes,
            "path": None,
            "path_mode": None,
            "window": None,
            "clip_polygon": None,
            "clip_crs": None,
            "crs": None,
            "metadata": {},
        },
    )
    tile_df = spark.createDataFrame([row], schema=schema)

    rows = run_file_write_layout_sweep(
        spark,
        fmt="gtiff",
        source=tile_df,
        target_prefix=str(tmp_path / "gt"),
        run_id="tg",
        warmup=0,
        measured=1,
        file_mode="fuse",
        layouts=("order", "plain"),
        where="venv",
    )
    assert len(rows) == 2
    assert {r.layout for r in rows} == {"order", "plain"}
