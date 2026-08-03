"""Integration tests for the raster_gbx DataSource (uses local Spark)."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource

EXPECTED_METADATA_KEYS = {
    "path",
    "sourcePath",
    "driver",
    "format",
    "last_command",
    "last_error",
    "all_parents",
    "size",
    "compression",
    "isZipped",
    "isSubset",
}


def _write_sample(path, width=4, height=3):
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_schema_is_v2_tile():
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    ds = RasterGbxDataSource(options={"path": "/tmp/none"})
    schema = ds.schema()
    assert [f.name for f in schema.fields] == ["source", "tile"]
    assert schema["tile"].dataType == V2_TILE_SCHEMA


def test_materialized_row_is_v2_with_populated_raster(spark, tmp_path):
    f = tmp_path / "sample.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    row = spark.read.format("raster_gbx").load(str(f)).collect()[0]
    tile = row["tile"]
    assert tile["cellid"] == -1
    assert tile["raster"] is not None  # materialized: bytes present
    assert tile["path"] is not None  # provenance populated
    # whole-file GTiff passthrough -> window is the whole file
    assert tile["window"]["width"] > 0 and tile["window"]["height"] > 0
    # new v2 clip/crs fields are null for a plain materialized tile
    assert tile["clip_polygon"] is None and tile["crs"] is None
    # pixels still decode
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as out:
        assert out.width > 0


def test_read_single_file_yields_one_row(spark, tmp_path):
    f = tmp_path / "sample.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(f))
    rows = df.collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["source"] == str(f)
    assert row["tile"]["cellid"] == -1
    assert EXPECTED_METADATA_KEYS <= set(row["tile"]["metadata"].keys())
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_read_directory_one_partition_per_file(spark, tmp_path):
    for i in range(3):
        _write_sample(str(tmp_path / f"s{i}.tif"))
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("filterRegex", r".*\.tif$")
        .load(str(tmp_path))
    )
    assert df.rdd.getNumPartitions() == 3
    assert df.count() == 3


def test_corrupt_file_fails_fast(spark, tmp_path):
    import pytest

    bad = tmp_path / "bad.tif"
    bad.write_bytes(b"not a raster")
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(str(bad))
    with pytest.raises(Exception):
        df.collect()


def test_multi_tile_split_matches_plan_layout(spark, tmp_path):
    import rasterio

    from databricks.labs.gbx.ds.raster import _numpy_itemsize
    from databricks.labs.gbx.pyrx.core import budget as budget_mod

    # Incompressible noise so the on-disk file is genuinely large -> forces a split.
    f = tmp_path / "big.tif"
    rng = np.random.default_rng(0)
    data = rng.integers(0, 255, size=(3, 2048, 2048), dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    )
    with rasterio.open(str(f), "w", **profile) as ds:
        ds.write(data)

    # Expected count via plan_layout (decoded-size budget, the new semantics for sizeInMB).
    budget_bytes = 1 * 1024 * 1024  # 1 MiB
    with rasterio.open(str(f)) as ds:
        width, height = ds.width, ds.height
        bands = ds.count
        itemsize = _numpy_itemsize(ds.dtypes[0])
        tiled = bool(ds.profile.get("tiled", False))
        bx = ds.profile.get("blockxsize")
        by = ds.profile.get("blockysize")
    plan = budget_mod.plan_layout(
        width, height, bands, itemsize, tiled, bx, by, budget_bytes
    )
    expected = len(plan.tiles)
    assert expected > 1, "test setup failed to force a multi-tile split"

    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("sizeInMB", "1").load(str(f))
    assert df.count() == expected
    # every emitted tile is a fresh, un-tessellated tile
    assert all(r["tile"]["cellid"] == -1 for r in df.select("tile").collect())


def test_whole_file_gtiff_is_passthrough(spark, tmp_path):
    # A single whole-raster GTiff tile must emit the ORIGINAL file bytes verbatim
    # (no decode/re-encode) — the fast path. Still cellid=-1 + 11 metadata keys,
    # and the decoded pixels equal the source.
    f = tmp_path / "whole.tif"
    _write_sample(str(f))
    raw = f.read_bytes()

    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(str(f)).collect()
    assert len(rows) == 1
    tile = rows[0]["tile"]
    assert (
        bytes(tile["raster"]) == raw
    ), "whole-file GTiff should pass through unchanged"
    assert tile["cellid"] == -1
    assert EXPECTED_METADATA_KEYS <= set(tile["metadata"].keys())
    with MemoryFile(bytes(tile["raster"])) as mf, mf.open() as out:
        arr = out.read(1)
    np.testing.assert_allclose(
        arr, np.arange(12, dtype="float32").reshape(3, 4), rtol=1e-6
    )


def test_source_column_is_spark_uri_qualified(spark, tmp_path):
    # The emitted source column is to_spark_uri()-qualified so a light-produced
    # DataFrame joins cleanly against binaryFile / heavy gdal (dbfs:/Volumes/...).
    # A tmp-dir read is a bare local path, so to_spark_uri leaves it UNCHANGED
    # (proves no mangling of local dev/test paths); the /Volumes mapping is
    # asserted directly since local tests cannot read from a real Volume.
    from databricks.labs.gbx.ds._listing import to_spark_uri

    f = tmp_path / "sample.tif"
    _write_sample(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    rows = spark.read.format("raster_gbx").load(str(f)).collect()
    assert len(rows) == 1
    # (a) bare local tmp path passes through unchanged.
    assert rows[0]["source"] == to_spark_uri(str(f)) == str(f)
    # (b) the Volume case the reader exists to fix maps to the Hadoop form.
    assert to_spark_uri("/Volumes/x/y.tif") == "dbfs:/Volumes/x/y.tif"


def test_multi_tile_subwindows_are_reencoded(spark, tmp_path):
    # When the source splits into sub-tiles, those CANNOT pass through (each is a
    # window of the source), so they must be re-encoded -> bytes differ from source.
    f = tmp_path / "big.tif"
    rng = np.random.default_rng(1)
    data = rng.integers(0, 255, size=(3, 2048, 2048), dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=2048,
        height=2048,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    )
    with rasterio.open(str(f), "w", **profile) as ds:
        ds.write(data)
    raw = f.read_bytes()

    spark.dataSource.register(RasterGbxDataSource)
    rows = (
        spark.read.format("raster_gbx").option("sizeInMB", "1").load(str(f)).collect()
    )
    assert len(rows) > 1  # split happened
    assert all(
        bytes(r["tile"]["raster"]) != raw for r in rows
    ), "sub-tiles must be re-encoded"


def _write_big_incompressible(path, side=2048):
    rng = np.random.default_rng(2)
    data = rng.integers(0, 255, size=(3, side, side), dtype="uint8")
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0.0, 0.0, 1.0, 1.0),
    )
    with rasterio.open(str(path), "w", **profile) as ds:
        ds.write(data)


def test_none_strategy_yields_one_row(spark, tmp_path):
    # splitStrategy=none explicitly disables auto-split. One file -> one row.
    f = tmp_path / "big_default.tif"
    _write_big_incompressible(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("splitStrategy", "none").load(str(f))
    rows = df.collect()
    assert len(rows) == 1, "splitStrategy=none must emit exactly one tile per file"
    assert rows[0]["tile"]["cellid"] == -1


def test_default_strategy_is_no_split(tmp_path):
    # Default splitStrategy=none (halo mode): large rasters are NOT split by default.
    # One partition per file, regardless of size.
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    f = tmp_path / "big_default.tif"
    _write_big_incompressible(str(f))

    # Read with DEFAULT options — no splitStrategy, no sizeInMB.
    reader = RasterGbxReader({"path": str(f)})
    assert reader.strategy == "none"
    parts = reader.partitions()

    # Default=none: exactly one partition per file (no split).
    assert (
        len(parts) == 1
    ), "default strategy=none must emit exactly one partition per file"

    # Each partition yields exactly one row.
    part_rows = list(reader.read(parts[0]))
    assert len(part_rows) == 1, "read() must yield exactly one row per partition"


def test_optin_split_splits_large_raster(tmp_path, monkeypatch):
    # Opt-in splitStrategy=serverless must split large rasters on a decoded-memory budget.
    #
    # With the one-tile-per-partition architecture, partitions() returns one
    # _TilePartition per tile window.  Each read() call yields exactly ONE row.
    # We monkeypatch the budget so a 2048×2048×3 uint8 raster (12 MiB decoded)
    # forces a split under a 1 MiB budget.
    import databricks.labs.gbx.pyrx.core.budget as budget_mod
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    monkeypatch.setattr(
        budget_mod, "decoded_budget_bytes", lambda _strategy: 1024 * 1024
    )

    f = tmp_path / "big_split.tif"
    _write_big_incompressible(str(f))

    # Opt-in split with explicit splitStrategy=serverless.
    reader = RasterGbxReader({"path": str(f), "splitStrategy": "serverless"})
    parts = reader.partitions()

    # One-tile-per-partition: the split must produce more than one partition.
    assert (
        len(parts) > 1
    ), "partitions() must produce >1 _TilePartition for a raster exceeding the budget"

    # Each partition yields exactly one row (structural OOM fix).
    for p in parts:
        part_rows = list(reader.read(p))
        assert (
            len(part_rows) == 1
        ), "read() must yield exactly one row per _TilePartition"

    # Total emitted rows across all partitions equals total planned tiles.
    total_rows = sum(len(list(reader.read(p))) for p in parts)
    assert total_rows == len(parts)

    # Opt-in split tiles are plain GTiff (not COG — COG is a writer concern).
    from databricks.labs.gbx.pyrx.core import cog as cog_mod

    for p in parts:
        for _, tile in reader.read(p):
            # read() emits a raw v2 tuple in V2_TILE_SCHEMA order; metadata is last.
            md = tile[-1]
            assert (
                md.get(cog_mod.GBX_FORMAT) == "gtiff"
            ), f"split tiles must be plain gtiff; got {md.get(cog_mod.GBX_FORMAT)!r}"


def test_explicit_small_sizeinmb_still_splits(spark, tmp_path):
    # Tiling is explicit opt-in: a small positive sizeInMB still splits >1.
    f = tmp_path / "big_split.tif"
    _write_big_incompressible(str(f))
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").option("sizeInMB", "8").load(str(f))
    assert df.count() > 1


def test_no_split_oversized_tile_raises_clear_error(tmp_path, monkeypatch):
    # A single whole-image tile that would exceed the ~2GB cell limit must fail
    # with an actionable "set sizeInMB" message instead of a giant cell. Driving
    # the reader's read() directly (not through Spark workers) lets a tiny
    # monkeypatched threshold stand in for 2GB without any large allocation.
    import pytest

    import databricks.labs.gbx.ds.raster as raster_mod
    from databricks.labs.gbx.ds.raster import RasterGbxReader, _FilePartition

    f = tmp_path / "oversized.tif"
    _write_sample(str(f))  # tiny raster; threshold made tiny instead
    monkeypatch.setattr(raster_mod, "_MAX_TILE_BYTES", 1)

    reader = RasterGbxReader({"path": str(f)})
    assert reader.size_mib == -1  # default = no split
    with pytest.raises(ValueError) as ei:
        list(reader.read(_FilePartition(str(f), reader.size_mib)))
    msg = str(ei.value)
    assert "sizeInMB" in msg and "single tile" in msg


def test_estimate_tile_bytes_uses_max_of_raw_and_file():
    import databricks.labs.gbx.ds.raster as raster_mod

    # raw = 4*3*1*4 (float32) = 48 bytes; file_size larger -> picks file_size.
    assert raster_mod._estimate_tile_bytes(4, 3, 1, "float32", 1000) == 1000
    # raw larger than file_size -> picks raw.
    assert (
        raster_mod._estimate_tile_bytes(100, 100, 2, "float32", 10) == 100 * 100 * 2 * 4
    )


def test_reader_default_is_no_split():
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    r = RasterGbxReader({"path": "/x"})
    assert r.strategy == "none"  # default reversed from 0.4.4 'auto'


def test_reader_optin_split_still_works():
    from databricks.labs.gbx.ds.raster import RasterGbxReader

    r = RasterGbxReader({"path": "/x", "splitStrategy": "serverless"})
    assert r.strategy == "serverless"


def test_gtiff_writer_has_no_cog_option(tmp_path):
    # The gtiff lane writes plain GTiff; cog options belong to cog_gbx.
    # Even if a caller passes cog=true as an option, gtiff_gbx must IGNORE it
    # (COG kwargs were removed from GTiffGbxDataSource.writer — they don't reach
    # RasterGbxWriter so it stays at its default cog=False).
    from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
    from databricks.labs.gbx.ds.raster import reader_schema

    src = GTiffGbxDataSource(options={"path": str(tmp_path), "cog": "true"})
    writer = src.writer(reader_schema(), overwrite=True)
    # RasterGbxWriter.cog must be False — gtiff lane never COGs.
    assert getattr(writer, "cog", False) is False


def test_file_and_cog_registered():
    from databricks.labs.gbx.ds.register import _SOURCES

    names = {s.name() for s in _SOURCES}
    assert "file_gbx" in names and "cog_gbx" in names
