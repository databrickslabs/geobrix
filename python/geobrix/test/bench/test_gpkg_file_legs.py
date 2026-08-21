"""Tests for GeoPackage FILE read+write bench legs.

FUSE legs (fuse tier, local[2]):
  - run_gpkg_file_read with file_mode="fuse" → status="ok", chunk_size recorded
  - run_gpkg_file_write with file_mode="fuse" → status="ok" (or "na_by_design"
    if the gpkg_gbx writer is unavailable in the local env)

External leg on FUSE tier:
  - run_gpkg_file_read with file_mode="external" → status="na_by_design"
    (vector_file_read raises ValueError when FILE is unavailable)

Fixtures use pyogrio.write_arrow (no geopandas required).
"""


def _write_gpkg(path, rows=5, offset_y=0.0):
    """Write a .gpkg with `rows` Point features via pyogrio.write_arrow (no geopandas)."""
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
    """Write n .gpkg files under tmp_path, each with `rows` point features."""
    for i in range(n):
        _write_gpkg(str(tmp_path / f"t{i}.gpkg"), rows=rows, offset_y=float(i))


def _write_one_gpkg(tmp_path, rows=5):
    """Write a single .gpkg file under tmp_path and return its path."""
    p = str(tmp_path / "out.gpkg")
    _write_gpkg(p, rows=rows)
    return p


def test_gpkg_file_read_external_skips_on_fuse_tier(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 2, rows=5)
    r = run_gpkg_file_read(
        spark, str(tmp_path), "t", 0, 1, file_mode="external", where="venv"
    )
    assert r.status == "na_by_design"
    assert r.file_mode == "external"


def test_gpkg_file_read_fuse_ok_records_chunksize(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_read

    _write_n_gpkg(tmp_path, 2, rows=5)
    r = run_gpkg_file_read(
        spark,
        str(tmp_path),
        "t",
        0,
        1,
        file_mode="fuse",
        chunk_size=1000,
        where="venv",
    )
    assert r.status == "ok"
    assert r.rows == 10  # 2 files x 5 features
    assert r.chunk_size == 1000


def test_gpkg_file_write_fuse_ok(spark, tmp_path):
    from databricks.labs.gbx.bench.readers import run_gpkg_file_write

    out = _write_one_gpkg(tmp_path, rows=5)
    r = run_gpkg_file_write(
        spark,
        out,
        str(tmp_path / "wt"),
        "t",
        0,
        1,
        file_mode="fuse",
        where="venv",
    )
    assert r.status in ("ok", "na_by_design")
