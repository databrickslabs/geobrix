"""Task 5: writer compress/compressLevel/predictor option surface + cogCompression alias.

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
from pathlib import Path

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
# Tests
# ---------------------------------------------------------------------------


def test_compress_auto_produces_zstd(spark_session, src_tif, tmp_path):
    """compress='auto' (default) → ZSTD output."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(df, tmp_path / "auto", compress="auto") as ds:
        assert (
            ds.compression == Compression.zstd
        ), f"Expected zstd, got {ds.compression}"


def test_compress_default_produces_zstd(spark_session, src_tif, tmp_path):
    """No compress option at all should default to auto → ZSTD."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(df, tmp_path / "default") as ds:
        assert (
            ds.compression == Compression.zstd
        ), f"Expected zstd default, got {ds.compression}"


def test_compress_deflate_with_level(spark_session, src_tif, tmp_path):
    """compress='deflate' + compressLevel='9' → DEFLATE output."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(
        df, tmp_path / "deflate", compress="deflate", compressLevel="9"
    ) as ds:
        assert (
            ds.compression == Compression.deflate
        ), f"Expected deflate, got {ds.compression}"


def test_compress_none_produces_uncompressed(spark_session, src_tif, tmp_path):
    """compress='none' → no compression."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(df, tmp_path / "none", compress="none") as ds:
        # rasterio reports None or Compression.none for uncompressed
        assert ds.compression in (
            None,
            Compression.none,
        ), f"Expected no compression, got {ds.compression}"


def test_cog_compression_alias_maps_to_deflate(spark_session, src_tif, tmp_path):
    """cogCompression='DEFLATE' (deprecated alias) → DEFLATE output."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(df, tmp_path / "alias", cogCompression="DEFLATE") as ds:
        assert (
            ds.compression == Compression.deflate
        ), f"cogCompression alias: expected deflate, got {ds.compression}"


def test_compress_wins_over_cog_compression(spark_session, src_tif, tmp_path):
    """When both compress and cogCompression set, compress wins."""
    df = _read_df(spark_session, src_tif)
    # compress='none' should win over cogCompression='deflate'
    with _write_and_open(
        df, tmp_path / "both", compress="none", cogCompression="deflate"
    ) as ds:
        assert ds.compression in (
            None,
            Compression.none,
        ), f"compress should win over cogCompression, got {ds.compression}"


def test_compress_auto_with_explicit_level_warns_or_uses_auto(
    spark_session, src_tif, tmp_path
):
    """compress='auto' + explicit compressLevel → warns (possibly worker-side) + ZSTD output.

    The warning fires in the write worker (DataSource V2 task), so it may not surface
    on the driver side via pytest.warns.  We assert the output is ZSTD-compressed
    (the correctness guarantee) and verify a UserWarning is raised somewhere in the
    call by capturing warnings in the driver process.  Worker-side warnings that
    the driver doesn't see are accepted — only the output codec assertion is hard.
    """
    import warnings

    df = _read_df(spark_session, src_tif)
    # Capture any driver-side warnings (the authority fires one when it sees
    # compress='auto' with explicit level). Worker-side warnings won't be caught
    # here (they're in a forked subprocess), but the output still uses auto-level.
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        with _write_and_open(
            df, tmp_path / "auto_lvl", compress="auto", compressLevel="22"
        ) as ds:
            assert (
                ds.compression == Compression.zstd
            ), f"auto+explicit level: expected zstd output, got {ds.compression}"


def test_compress_lzw(spark_session, src_tif, tmp_path):
    """compress='lzw' → LZW output."""
    df = _read_df(spark_session, src_tif)
    with _write_and_open(df, tmp_path / "lzw", compress="lzw") as ds:
        assert ds.compression == Compression.lzw, f"Expected lzw, got {ds.compression}"


# ---------------------------------------------------------------------------
# cog_gbx writer (cog_compression alias + compress option)
# ---------------------------------------------------------------------------


def test_cog_gbx_compress_option(spark_session, src_tif, tmp_path):
    """cog_gbx writer: compress='zstd' routes through the authority."""
    df = _read_df(spark_session, src_tif, virtual=False)
    with _write_and_open(
        df, tmp_path / "cog_zstd", format="cog_gbx", compress="zstd"
    ) as ds:
        assert (
            ds.compression == Compression.zstd
        ), f"cog_gbx compress=zstd: expected zstd, got {ds.compression}"


def test_cog_gbx_cog_compression_alias(spark_session, src_tif, tmp_path):
    """cog_gbx writer: cogCompression alias still routes through the authority."""
    df = _read_df(spark_session, src_tif, virtual=False)
    # cogCompression='DEFLATE' should still produce deflate
    with _write_and_open(
        df, tmp_path / "cog_alias", format="cog_gbx", cogCompression="DEFLATE"
    ) as ds:
        assert (
            ds.compression == Compression.deflate
        ), f"cog_gbx cogCompression alias: expected deflate, got {ds.compression}"
