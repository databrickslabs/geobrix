"""cog_gbx default file names for the retile family, end to end.

``gbx_rst_retile`` windows and ``gbx_rst_separatebands`` bands share their parent's
cellid and source. Written with ``cog_gbx`` and no ``nameCol`` they must each land in
their own file, not overwrite one another.
"""

import glob
import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx import functions as prx


def _write_source(path, *, width, height, count):
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        for band in range(1, count + 1):
            ds.write(np.full((height, width), band * 40, dtype="uint8"), band)


def _write_chain(spark, src, out, fns, read_opts=None):
    """Read ``src`` (source, tile), cut it with each LATERAL function in ``fns`` in
    turn (the source column stays), and write the (source, tile) parts with cog_gbx
    and no nameCol. Returns ``(files, tiles)``."""
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(CogGbxDataSource)
    prx.register(spark)
    reader = spark.read.format("raster_gbx")
    for key, value in (read_opts or {}).items():
        reader = reader.option(key, value)
    reader.load(str(src)).createOrReplaceTempView("_cog_stage_0")
    for i, fn in enumerate(fns):
        spark.sql(
            f"SELECT source, struct(t.*) AS tile FROM _cog_stage_{i}, LATERAL {fn} t"
        ).createOrReplaceTempView(f"_cog_stage_{i + 1}")
    parts = spark.table(f"_cog_stage_{len(fns)}")
    tiles = parts.count()
    parts.write.format("cog_gbx").mode("overwrite").option("cogBlockSize", "128").save(
        str(out)
    )
    return sorted(glob.glob(os.path.join(str(out), "*.tif"))), tiles


def _write_parts(spark, src, out, fn):
    """One cut of ``src`` with the LATERAL function ``fn``; returns the files."""
    return _write_chain(spark, src, out, [fn])[0]


def _write_random_source(path, *, width, height):
    """Incompressible pixels, so the encoded size is about the raw size."""
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    data = np.random.default_rng(7).integers(0, 256, (height, width), dtype="uint8")
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data, 1)


def test_retile_windows_write_to_distinct_files(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=512, height=512, count=1)

    files = _write_parts(spark, src, tmp_path / "out", "gbx_rst_retile(tile, 256, 256)")

    assert len(files) == 4, f"expected one file per window, got {files}"
    for f in files:
        with rasterio.open(f) as ds:
            assert (ds.width, ds.height) == (256, 256)


def test_separatebands_write_to_distinct_files(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=256, height=256, count=3)

    files = _write_parts(spark, src, tmp_path / "out", "gbx_rst_separatebands(tile)")

    assert len(files) == 3, f"expected one file per band, got {files}"
    values = []
    for f in files:
        with rasterio.open(f) as ds:
            assert ds.count == 1
            values.append(int(ds.read(1)[0, 0]))
    assert sorted(values) == [40, 80, 120]


def test_retiled_bands_write_to_distinct_files(spark, tmp_path):
    """Two bands with identical pixels, each retiled: the pieces differ only by their band
    marker, which the retile keeps, so every piece still gets its own file."""
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    with rasterio.open(
        str(src),
        "w",
        driver="GTiff",
        width=256,
        height=256,
        count=2,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    ) as ds:
        ds.write(np.full((2, 256, 256), 40, dtype="uint8"))

    files, tiles = _write_chain(
        spark,
        src,
        tmp_path / "out",
        ["gbx_rst_separatebands(tile)", "gbx_rst_retile(tile, 128, 128)"],
    )

    assert tiles == 8 and len(files) == 8, f"{tiles} tiles wrote {len(files)} files"


def test_materialized_read_keeps_source_name(spark, tmp_path):
    """A whole-file read with virtualTiles=false is one materialized tile with
    window provenance (0,0,W,H): written with cog_gbx and no nameCol it keeps the
    plain source name."""
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=256, height=256, count=1)
    out = tmp_path / "out"
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(CogGbxDataSource)

    df = spark.read.format("raster_gbx").option("virtualTiles", "false").load(str(src))
    df.write.format("cog_gbx").mode("overwrite").option("cogBlockSize", "128").save(
        str(out)
    )

    assert [os.path.basename(p) for p in glob.glob(str(out / "*.tif"))] == ["scene.tif"]


def test_single_maketiles_tile_keeps_source_name(spark, tmp_path):
    """rst_maketiles on a raster that fits the budget returns one tile covering the
    whole parent: the window is its full extent, so it keeps the plain source name."""
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=256, height=256, count=1)

    files = _write_parts(spark, src, tmp_path / "out", "gbx_rst_maketiles(tile, 100)")

    assert [os.path.basename(p) for p in files] == ["scene.tif"]


# -- every cut of a cut keeps one file per tile ------------------------------
#
# Parts of parts share the source column and the parent's cellid, and each
# parent's first child sits at window (0,0,tw,th). None of them may take the plain
# source name or overwrite a sibling from another parent.


def _assert_one_file_per_tile(files, tiles, expected):
    assert tiles == expected, f"unexpected tile count {tiles}"
    assert len(files) == tiles, f"{tiles} tiles wrote {len(files)} files: {files}"


def test_nested_retile_writes_one_file_per_tile(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=512, height=512, count=1)

    files, tiles = _write_chain(
        spark,
        src,
        tmp_path / "out",
        ["gbx_rst_retile(tile, 256, 256)", "gbx_rst_retile(tile, 128, 128)"],
        {"virtualTiles": "false"},
    )

    _assert_one_file_per_tile(files, tiles, 16)


def test_maketiles_then_retile_writes_one_file_per_tile(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    # 1536x1536 incompressible bytes: rst_maketiles(tile, 1) splits it into 4 tiles.
    _write_random_source(str(src), width=1536, height=1536)

    files, tiles = _write_chain(
        spark,
        src,
        tmp_path / "out",
        ["gbx_rst_maketiles(tile, 1)", "gbx_rst_retile(tile, 384, 384)"],
    )

    _assert_one_file_per_tile(files, tiles, 16)


def test_reader_split_then_retile_writes_one_file_per_tile(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=512, height=512, count=1)

    files, tiles = _write_chain(
        spark,
        src,
        tmp_path / "out",
        ["gbx_rst_retile(tile, 128, 128)"],
        {"virtualTiles": "false", "tileSize": "256"},
    )

    _assert_one_file_per_tile(files, tiles, 16)


def test_noop_retile_of_parts_writes_one_file_per_tile(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=512, height=512, count=1)

    files, tiles = _write_chain(
        spark,
        src,
        tmp_path / "out",
        ["gbx_rst_retile(tile, 256, 256)", "gbx_rst_retile(tile, 256, 256)"],
        {"virtualTiles": "false"},
    )

    _assert_one_file_per_tile(files, tiles, 4)
