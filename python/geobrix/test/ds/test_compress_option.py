"""Writer compress/compressLevel/predictor option surface + cogCompression alias.

Writes a real tile DataFrame to a temp dir and reads the output back, asserting
rasterio.open(out).compression.  No JAR required — uses local PySpark with the
DataSource V2 registration path.

Run:
    export PYSPARK_PYTHON="$(pwd)/.venv-pyrx/bin/python"
    export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv-pyrx/bin/python"
    .venv-pyrx/bin/python -m pytest python/geobrix/test/ds/test_compress_option.py -v
"""

from __future__ import annotations

import os

import numpy as np
import pytest
import rasterio
from rasterio.enums import Compression
from rasterio.transform import from_origin

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_session():
    import logging

    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-compress-option-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    from databricks.labs.gbx.ds.cog import CogGbxDataSource
    from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    session.dataSource.register(RasterGbxDataSource)
    session.dataSource.register(GTiffGbxDataSource)
    session.dataSource.register(CogGbxDataSource)
    yield session


@pytest.fixture(scope="module")
def src_tif(tmp_path_factory):
    """Write a small int16 GeoTIFF as the read source."""
    p = tmp_path_factory.mktemp("src") / "tile.tif"
    profile = dict(
        driver="GTiff",
        width=32,
        height=32,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=from_origin(0.0, 10.0, 0.1, 0.1),
    )
    data = np.arange(32 * 32, dtype="int16").reshape(1, 32, 32)
    with rasterio.open(str(p), "w", **profile) as ds:
        ds.write(data)
    return str(p)


def _read_df(spark, src, virtual=False):
    """Read src as a raster_gbx DataFrame (materialized by default)."""
    vt = "true" if virtual else "false"
    return spark.read.format("raster_gbx").option("virtualTiles", vt).load(src)


def _write_and_open(df, out_dir, format="gtiff_gbx", **opts):
    """Write *df* to *out_dir* with the given options and return an open rasterio ds."""
    w = df.write.format(format).mode("overwrite")
    for k, v in opts.items():
        w = w.option(k, v)
    w.save(str(out_dir))
    tifs = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
    assert tifs, f"writer produced no .tif files in {out_dir}"
    return rasterio.open(os.path.join(out_dir, tifs[0]))


# ---------------------------------------------------------------------------
# The option table: (writer format, options, source codec) -> the codec of the file written
# ---------------------------------------------------------------------------

NONE = (None, Compression.none)
Z, D = Compression.zstd, Compression.deflate

# What the file on disk must say. "zstd source" rows prove the codec comes from the options: a plain
# source cannot tell the driver default, the source's codec and the requested one apart.
CODEC_ROWS = {
    "gtiff_default": ("gtiff_gbx", {}, "plain", (Z,)),
    "gtiff_auto": ("gtiff_gbx", {"compress": "auto"}, "plain", (Z,)),
    "gtiff_auto_with_a_level": (
        "gtiff_gbx",
        {"compress": "auto", "compressLevel": "22"},
        "plain",
        (Z,),
    ),
    "gtiff_deflate_level": (
        "gtiff_gbx",
        {"compress": "deflate", "compressLevel": "9"},
        "plain",
        (D,),
    ),
    "gtiff_lzw": ("gtiff_gbx", {"compress": "lzw"}, "plain", (Compression.lzw,)),
    "gtiff_none": ("gtiff_gbx", {"compress": "none"}, "plain", NONE),
    "gtiff_cog_compression_alias": (
        "gtiff_gbx",
        {"cogCompression": "DEFLATE"},
        "plain",
        (D,),
    ),
    "gtiff_compress_beats_the_alias": (
        "gtiff_gbx",
        {"compress": "none", "cogCompression": "deflate"},
        "plain",
        NONE,
    ),
    "gtiff_deflate_reencodes_a_zstd_source": (
        "gtiff_gbx",
        {"compress": "deflate"},
        "zstd",
        (D,),
    ),
    "cog_zstd": ("cog_gbx", {"compress": "zstd"}, "plain", (Z,)),
    "cog_auto_is_zstd_not_deflate": ("cog_gbx", {"compress": "auto"}, "plain", (Z,)),
    "cog_cog_compression_alias": (
        "cog_gbx",
        {"cogCompression": "DEFLATE"},
        "plain",
        (D,),
    ),
    "raster_cog_auto_is_zstd": (
        "raster_gbx",
        {"cog": "true", "compress": "auto"},
        "plain",
        (Z,),
    ),
    # The COG driver compresses with LZW when COMPRESS is absent: 'none' and 'raw' must say NONE.
    "raster_cog_none": (
        "raster_gbx",
        {"cog": "true", "compress": "none"},
        "zstd",
        NONE,
    ),
    "raster_cog_raw": ("raster_gbx", {"cog": "true", "compress": "raw"}, "zstd", NONE),
    "raster_cog_raw_by_alias": (
        "raster_gbx",
        {"cog": "true", "cogCompression": "raw"},
        "zstd",
        NONE,
    ),
    "cog_none": ("cog_gbx", {"compress": "none"}, "zstd", NONE),
    "cog_raw": ("cog_gbx", {"compress": "raw"}, "zstd", NONE),
}


@pytest.fixture(scope="module")
def src_zstd_tif(tmp_path_factory):
    """A ZSTD-compressed GTiff: an already-compressed source."""
    p = tmp_path_factory.mktemp("src_zstd") / "tile_zstd.tif"
    profile = dict(
        driver="GTiff",
        width=32,
        height=32,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=from_origin(0.0, 10.0, 0.1, 0.1),
        compress="zstd",
        zstd_level="9",
        predictor="2",
    )
    data = np.arange(32 * 32, dtype="int16").reshape(1, 32, 32)
    with rasterio.open(str(p), "w", **profile) as ds:
        ds.write(data)
    return str(p)


@pytest.mark.parametrize(
    "format,opts,source,want", CODEC_ROWS.values(), ids=CODEC_ROWS.keys()
)
def test_the_codec_written_follows_the_options(
    spark_session, src_tif, src_zstd_tif, tmp_path, format, opts, source, want
):
    df = _read_df(spark_session, src_zstd_tif if source == "zstd" else src_tif)
    with _write_and_open(df, tmp_path / "out", format=format, **opts) as ds:
        assert ds.compression in want, ds.compression
        if want == NONE:
            assert "COMPRESSION" not in ds.tags(ns="IMAGE_STRUCTURE")


def test_gtiff_auto_on_a_zstd_source_keeps_it_zstd_and_the_pixels(
    spark_session, src_zstd_tif, tmp_path
):
    """The fast path leaves a tile that is already ZSTD alone: same codec, same pixels."""
    df = _read_df(spark_session, src_zstd_tif)
    with (
        _write_and_open(df, tmp_path / "out", compress="auto") as out,
        rasterio.open(src_zstd_tif) as src,
    ):
        assert out.compression == Compression.zstd
        np.testing.assert_array_equal(src.read(), out.read())


# ---------------------------------------------------------------------------
# Issue 2 unit test: _apply_compression fast-path (no Spark)
# ---------------------------------------------------------------------------


def _make_zstd_bytes(dtype="int16", width=32, height=32):
    """Return raw GTiff bytes compressed with ZSTD."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin as _fo

    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        crs="EPSG:4326",
        transform=_fo(0.0, 10.0, 0.1, 0.1),
        compress="zstd",
        zstd_level="9",
        predictor="2",
    )
    data = np.arange(width * height, dtype=dtype).reshape(1, height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


def test_apply_compression_fast_path_returns_same_bytes():
    """_apply_compression compress='auto' on ZSTD input returns EXACT same bytes object.

    ISSUE 2 unit-level regression: before the fast-path fix, _apply_compression
    re-encoded even already-ZSTD tiles, returning a NEW bytes object.  After the
    fix, it detects that the tile is already ZSTD-compressed and returns
    ``raster_bytes`` unchanged (same identity).
    """
    from databricks.labs.gbx.ds.writer import _apply_compression

    zstd_bytes = _make_zstd_bytes()
    result = _apply_compression(zstd_bytes, "auto", None, None)
    assert result is zstd_bytes, (
        "fast-path should return the SAME bytes object for auto on ZSTD input; "
        "got a new bytes object (re-encoding happened)"
    )


def test_apply_compression_explicit_codec_reencodes():
    """_apply_compression with explicit codec always re-encodes (returns new bytes)."""
    from databricks.labs.gbx.ds.writer import _apply_compression

    zstd_bytes = _make_zstd_bytes()
    result = _apply_compression(zstd_bytes, "deflate", None, None)
    # Must be a NEW bytes object (re-encoded) AND be DEFLATE
    assert result is not zstd_bytes, "explicit deflate on zstd should re-encode"
    with rasterio.open(rasterio.io.MemoryFile(result)) as ds:
        assert ds.compression == Compression.deflate
