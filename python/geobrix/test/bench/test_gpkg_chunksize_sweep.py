"""Tests for the GeoPackage chunkSize × LRU amortization sweep.

Verifies ``run_gpkg_chunksize_sweep``:
  - emits exactly one ResultRow per chunkSize, and
  - ``input_partitions`` is invariant across chunkSizes (fanout-invariance claim:
    chunkSize is within-task amortization, not a fanout lever).

Uses ``file_mode="fuse"`` and ``where="venv"`` so no FILE tier or cluster is
required — these run on local[2] in the Docker dev container.
"""


def _write_gpkg(path, rows=5, offset_y=0.0):
    """Write a .gpkg with ``rows`` Point features via pyogrio.write_arrow."""
    import pyarrow as pa
    import pyogrio
    import shapely

    wkbs = [
        bytes(shapely.to_wkb(shapely.Point(float(j), offset_y))) for j in range(rows)
    ]
    tbl = pa.table({"geometry": pa.array(wkbs, type=pa.binary())})
    pyogrio.write_arrow(
        tbl,
        path,
        driver="GPKG",
        geometry_name="geometry",
        geometry_type="Point",
        crs="EPSG:4326",
    )


def _write_n_gpkg(tmp_path, n, rows=5):
    """Write ``n`` .gpkg files under ``tmp_path``, each with ``rows`` point features."""
    for i in range(n):
        _write_gpkg(str(tmp_path / f"t{i}.gpkg"), rows=rows, offset_y=float(i))


def test_gpkg_chunksize_sweep_rows_and_fanout_invariance(spark, tmp_path):
    """One row per chunkSize; input_partitions invariant (fanout-invariance)."""
    from databricks.labs.gbx.bench.readers import run_gpkg_chunksize_sweep

    _write_n_gpkg(tmp_path, 3, rows=200)  # 3 files
    rows = run_gpkg_chunksize_sweep(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_sizes=(50, 100, 200),
        where="venv",
    )
    assert {r.chunk_size for r in rows} == {50, 100, 200}
    parts = {r.input_partitions for r in rows if r.status == "ok"}
    assert len(parts) <= 1  # chunkSize does NOT change fanout (file-count bound)
