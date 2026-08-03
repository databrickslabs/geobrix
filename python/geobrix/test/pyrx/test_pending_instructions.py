import glob
import tempfile

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

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


# ---------------------------------------------------------------------------
# Task 4: rst_memsize virtual-aware
# ---------------------------------------------------------------------------


def test_memsize_virtual_not_null():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    v = df.select(rx.rst_memsize("tile").alias("m")).first()["m"]
    assert v is not None and v > 0


def test_memsize_materialized_is_byte_length():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    # materialize first, then check memsize equals raster byte length
    mat = df.withColumn("tile", rx.rst_initnodata("tile", materialize=True))
    row = mat.select(rx.rst_memsize("tile").alias("m"), "tile.raster").first()
    assert row["m"] is not None and row["m"] > 0
    assert row["m"] == len(bytes(row["raster"]))


# ---------------------------------------------------------------------------
# Task 5: accumulated instructions compose + consume-on-materialize invariant
# ---------------------------------------------------------------------------


def test_accumulated_instructions_apply_in_order():
    spark = _spark()
    df = _read_virtual_df(spark, _a_tif())
    out = (
        df.withColumn("tile", rx.rst_band("tile", 1))
        .withColumn("tile", rx.rst_initnodata("tile"))
        .withColumn("tile", rx.rst_setsrid("tile", 3857))
    )
    row = out.select("tile.raster", "tile.metadata").first()
    assert row["raster"] is None  # still virtual after 3 ops
    md = row["metadata"]
    assert md["pending_bands"] == "1"
    assert md["pending_nodata"].startswith("-9999")
    assert md["pending_srid"] == "3857"
    # materialize once: all three apply
    m = (
        df.withColumn("tile", rx.rst_band("tile", 1))
        .withColumn("tile", rx.rst_setsrid("tile", 3857))
        .withColumn("tile", rx.rst_initnodata("tile", materialize=True))
    )
    r = m.select(
        rx.rst_numbands("tile").alias("n"),
        rx.rst_srid("tile").alias("s"),
        "tile.raster",
        "tile.metadata",
    ).first()
    assert r["n"] == 1 and r["s"] == 3857
    assert r["raster"] is not None  # materialized
    # pending keys consumed on materialization to bytes
    md = r["metadata"] or {}
    assert (
        "pending_bands" not in md
        and "pending_srid" not in md
        and "pending_nodata" not in md
    )


# ---------------------------------------------------------------------------
# Dtype / preserve tests: nodata "ensure/preserve" semantics
# ---------------------------------------------------------------------------


def _write_tif(path, dtype, nodata=None, width=4, height=4):
    """Write a minimal GTiff to *path* with given dtype and nodata."""
    transform = from_bounds(0, 0, 1, 1, width, height)
    profile = dict(
        driver="GTiff",
        dtype=dtype,
        width=width,
        height=height,
        count=1,
        crs="EPSG:4326",
        transform=transform,
    )
    if nodata is not None:
        profile["nodata"] = nodata
    arr = np.zeros((1, height, width), dtype=dtype)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(arr)


def _virtual_from_path(path):
    with rasterio.open(path) as ds:
        w, h = ds.width, ds.height
    return VirtualTile(
        cellid=-1, raster=None, path=path, window=(0, 0, w, h), metadata={}
    )


def test_pending_nodata_preserves_existing_uint16():
    """uint16 tile that already has nodata=0: open_tile must PRESERVE 0, not set -9999."""
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        path = tmp.name
    _write_tif(path, "uint16", nodata=0)
    vt = _virtual_from_path(path)
    vt.metadata = {"pending_nodata": "-9999.0"}
    with ot.open_tile(vt) as ds:
        assert (
            ds.nodata == 0.0
        ), f"Expected nodata=0.0 (preserved from source), got {ds.nodata}"


def test_pending_nodata_float32_no_nodata_applies_default():
    """float32 tile with no nodata: pending_nodata=-9999 must be applied (fits float)."""
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        path = tmp.name
    _write_tif(path, "float32", nodata=None)
    vt = _virtual_from_path(path)
    vt.metadata = {"pending_nodata": "-9999.0"}
    with ot.open_tile(vt) as ds:
        assert (
            ds.nodata == -9999.0
        ), f"Expected nodata=-9999.0 (default applied to float32), got {ds.nodata}"


def test_pending_nodata_uint16_no_nodata_leaves_unset():
    """uint16 tile with no nodata: -9999 doesn't fit uint16; nodata must remain None, no exception."""
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        path = tmp.name
    _write_tif(path, "uint16", nodata=None)
    vt = _virtual_from_path(path)
    vt.metadata = {"pending_nodata": "-9999.0"}
    # Must NOT raise ValueError about out-of-range nodata
    with ot.open_tile(vt) as ds:
        assert (
            ds.nodata is None
        ), f"Expected nodata=None (default -9999 doesn't fit uint16), got {ds.nodata}"
