import glob

import numpy as np
import rasterio

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _a_tif():
    # any real multi-band tif; bench-corpus rows are 4-band EPSG:4326
    for c in sorted(
        glob.glob("sample-data/Volumes/main/default/bench-corpus/rows/*.tif")
    ):
        return c
    return sorted(glob.glob("target/test-classes/modis/*_B02.TIF"))[0]


def _virtual(tif, **meta):
    with rasterio.open(tif) as ds:
        w, h = ds.width, ds.height
    return VirtualTile(
        cellid=-1, raster=None, path=tif, window=(0, 0, w, h), metadata=dict(meta)
    )


def test_pending_nodata_applied_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.nodata == -9999.0


def test_pending_srid_relabels_crs_at_open():
    tif = _a_tif()
    vt = _virtual(tif, pending_srid="3857")
    with ot.open_tile(vt) as ds:
        assert ds.crs.to_epsg() == 3857


def test_pending_bands_selects_bands_at_open():
    tif = _a_tif()
    with rasterio.open(tif) as ds:
        assert ds.count >= 2, "test needs a multi-band source"
    vt = _virtual(tif, pending_bands="1")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1


def test_pending_apply_order_band_then_nodata():
    # band-select THEN nodata: result is single-band with nodata set
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_nodata="-9999")
    with ot.open_tile(vt) as ds:
        assert ds.count == 1
        assert ds.nodata == -9999.0


def test_no_pending_keys_is_noop():
    tif = _a_tif()
    vt = _virtual(tif)
    with rasterio.open(tif) as src:
        want_bands = src.count
    with ot.open_tile(vt) as ds:
        assert ds.count == want_bands


def test_open_header_reflects_pending_bands_and_srid():
    tif = _a_tif()
    vt = _virtual(tif, pending_bands="1", pending_srid="3857")
    with ot.open_header(vt) as ds:
        assert ds.count == 1
        assert ds.crs.to_epsg() == 3857


def test_materialize_strips_pending_keys():
    from databricks.labs.gbx.pyrx.core.open_tile import (
        PENDING_NODATA,
        materialize_to_bytes,
    )

    tif = _a_tif()
    vt = _virtual(tif, pending_nodata="-9999", pending_srid="3857")
    mat = materialize_to_bytes(vt)
    assert mat.raster is not None
    assert PENDING_NODATA not in (mat.metadata or {})
    assert "pending_srid" not in (mat.metadata or {})
    # and the bytes actually honor the instructions
    import io

    from rasterio.io import MemoryFile

    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 3857


# ---------------------------------------------------------------------------
# Task 2: rst_initnodata records pending nodata on virtual tiles, emits v2
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession

from databricks.labs.gbx.ds.register import register
from databricks.labs.gbx.pyrx import functions as rx


def _spark():
    return (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _read_virtual_df(spark, tif):
    register(spark)
    return (
        spark.read.format("gtiff_gbx")
        .option("driver", "GTiff")
        .option("filterRegex", r".*\.(tif|TIF)$")
        .load(tif)
    )


def test_initnodata_virtual_stays_virtual_records_key():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile"))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None, "virtual tile must stay virtual (no bytes)"
    assert row["path"] is not None, "path reference preserved"
    assert (
        row["metadata"]["pending_nodata"] == "-9999.0"
        or row["metadata"]["pending_nodata"] == "-9999"
    )


def test_initnodata_virtual_emits_v2_struct():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile"))
    fields = [f.name for f in out.schema["tile"].dataType.fields]
    assert "path" in fields and "window" in fields  # v2, not v1 3-field


def test_initnodata_materialize_true_bakes_bytes():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_initnodata("tile", materialize=True))
    row = out.select("tile.raster").first()
    assert row["raster"] is not None and len(row["raster"]) > 0


# ---------------------------------------------------------------------------
# Task 3: rst_setsrid / rst_band record on virtual, v2 everywhere
# ---------------------------------------------------------------------------


def test_setsrid_virtual_records_key_stays_virtual():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_setsrid("tile", 3857))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None and row["path"] is not None
    assert row["metadata"]["pending_srid"] == "3857"
    # reading it applies the relabel
    row2 = out.select(rx.rst_srid("tile").alias("s")).first()
    assert row2["s"] == 3857


def test_band_virtual_records_key_stays_virtual():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = df.withColumn("tile", rx.rst_band("tile", 1))
    row = out.select("tile.raster", "tile.path", "tile.metadata").first()
    assert row["raster"] is None and row["path"] is not None
    assert row["metadata"]["pending_bands"] == "1"
    row2 = out.select(rx.rst_numbands("tile").alias("n")).first()
    assert row2["n"] == 1


def test_setsrid_band_v2_struct():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    for col in (rx.rst_setsrid("tile", 3857), rx.rst_band("tile", 1)):
        fields = [
            f.name for f in df.withColumn("tile", col).schema["tile"].dataType.fields
        ]
        assert "path" in fields and "window" in fields
