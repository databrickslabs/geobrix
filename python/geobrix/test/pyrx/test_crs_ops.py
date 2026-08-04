"""Task 3 tests: rst_setcrs / rst_transformcrs + tile.crs population in readers.

Tests:
- rst_setcrs(tile, "ESRI:54008") -> CRS relabeled; pixels unchanged; rst_crs reads back.
- rst_setcrs(tile, "4326") == rst_setsrid(tile, 4326) via int-cast rule.
- rst_setcrs on a VIRTUAL tile stays virtual + records pending_crs; open applies it.
- rst_transformcrs(tile, "EPSG:3857") reprojects (bounds change).
- rst_transformcrs with a non-EPSG target works where rst_transform(int) would fail.
- Reader: gtiff_gbx yields tile.crs == "ESRI:54008" for a MODIS TIF (was None).
- Reader: gtiff_gbx yields tile.crs == "EPSG:4326" for an EPSG:4326 raster.

Uses REAL rasters: make_geotiff_bytes for EPSG rasters; MODIS TIF for ESRI:54008.
"""

import os
import tempfile

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from .conftest import make_geotiff_bytes

# Path to a real ESRI:54008 raster (built into target/ by Maven).
_MODIS_TIF = os.path.join(
    os.path.dirname(__file__),
    "../../../../target/test-classes/modis",
    "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF",
)

_HAS_MODIS = os.path.exists(_MODIS_TIF)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tile_from_bytes(b):
    """Open bytes as a DatasetReader context manager."""
    return _serde.open_tile(b)


def _epsg4326_tif() -> bytes:
    return make_geotiff_bytes(epsg=4326)


def _epsg32633_tif() -> bytes:
    return make_geotiff_bytes(epsg=32633)


def _make_esri54008_bytes() -> bytes:
    """Create a small in-memory GTiff stamped with ESRI:54008."""
    from rasterio.crs import CRS

    crs = CRS.from_user_input("ESRI:54008")
    transform = from_origin(0.0, 1000000.0, 10000.0, 10000.0)
    profile = dict(
        driver="GTiff",
        width=4,
        height=3,
        count=1,
        dtype="float32",
        crs=crs,
        transform=transform,
        nodata=-9999.0,
    )
    data = np.ones((1, 3, 4), dtype="float32")
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


# ---------------------------------------------------------------------------
# rst_setcrs — unit tests (no Spark)
# ---------------------------------------------------------------------------


def test_set_srid_bounds_and_esri_and_zero():
    """set_srid: >=0 storage bound; positive resolves (ESRI works); 0 clears; -1 raises."""
    from databricks.labs.gbx.pyrx.core import edit

    b = _epsg4326_tif()
    # negative -> reject (the one set-time bound)
    with _tile_from_bytes(b) as ds:
        with pytest.raises(ValueError, match=">= 0"):
            edit.set_srid(ds, -1)
    # ESRI code (54008) now stamps ESRI:54008 (resolves via authoritative sets)
    with _tile_from_bytes(b) as ds:
        esri_bytes = edit.set_srid(ds, 54008)
    with _tile_from_bytes(esri_bytes) as ds:
        assert crs_to_canonical(ds.crs) == "ESRI:54008"
    # 0 -> "no CRS": clears the CRS (no raise)
    with _tile_from_bytes(b) as ds:
        zero_bytes = edit.set_srid(ds, 0)
    with _tile_from_bytes(zero_bytes) as ds:
        assert ds.crs is None
    # unresolvable positive code -> raise (materialized stamp is an apply moment)
    with _tile_from_bytes(b) as ds:
        with pytest.raises(ValueError):
            edit.set_srid(ds, 99999999)


def test_setcrs_esri54008_relabels_no_reproject():
    """rst_setcrs('ESRI:54008') stamps CRS but keeps pixels identical."""
    from databricks.labs.gbx.pyrx.core import edit

    b = _epsg4326_tif()
    with _tile_from_bytes(b) as src:
        orig_data = src.read()
        orig_transform = src.transform
        new_bytes = edit.set_crs(src, "ESRI:54008")
    with _tile_from_bytes(new_bytes) as ds:
        assert crs_to_canonical(ds.crs) == "ESRI:54008"
        # Pixels unchanged.
        np.testing.assert_array_equal(ds.read(), orig_data)
        # Transform unchanged.
        assert ds.transform == orig_transform


def test_setcrs_intlike_string_same_as_setsrid():
    """rst_setcrs('4326') == rst_setsrid(4326): int-cast rule."""
    from databricks.labs.gbx.pyrx.core import edit

    b = _epsg32633_tif()
    # setcrs with "4326" (int-castable string)
    with _tile_from_bytes(b) as ds:
        setcrs_bytes = edit.set_crs(ds, "4326")
    # setsrid with 4326 (int)
    with _tile_from_bytes(b) as ds:
        setsrid_bytes = edit.set_srid(ds, 4326)

    with _tile_from_bytes(setcrs_bytes) as ds_sc:
        with _tile_from_bytes(setsrid_bytes) as ds_ss:
            assert crs_to_canonical(ds_sc.crs) == crs_to_canonical(ds_ss.crs)
            assert crs_to_canonical(ds_sc.crs) == "EPSG:4326"
            np.testing.assert_array_equal(ds_sc.read(), ds_ss.read())


def test_setcrs_on_real_modis():
    """MODIS ESRI:54008 TIF round-trip: set CRS reads back correctly."""
    if not _HAS_MODIS:
        pytest.skip("MODIS TIF not built yet")
    from databricks.labs.gbx.pyrx.core import edit

    with open(_MODIS_TIF, "rb") as fh:
        src_bytes = fh.read()
    # Verify source CRS
    with _tile_from_bytes(src_bytes) as src:
        assert crs_to_canonical(src.crs) == "ESRI:54008"
        orig_data = src.read()
        # Round-trip: setcrs to EPSG:4326
        relabeled = edit.set_crs(src, "EPSG:4326")
    with _tile_from_bytes(relabeled) as ds:
        assert crs_to_canonical(ds.crs) == "EPSG:4326"
        restored = edit.set_crs(ds, "ESRI:54008")
    with _tile_from_bytes(restored) as ds:
        assert crs_to_canonical(ds.crs) == "ESRI:54008"
        np.testing.assert_array_equal(ds.read(), orig_data)


# ---------------------------------------------------------------------------
# rst_setcrs on virtual tiles (pending_crs)
# ---------------------------------------------------------------------------


def test_setcrs_virtual_records_pending_crs_stays_virtual():
    """rst_setcrs on a VIRTUAL tile records pending_crs and stays virtual."""
    b = _epsg4326_tif()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        import rasterio

        with rasterio.open(tif_path) as ds:
            w, h = ds.width, ds.height
        vt = VirtualTile(
            cellid=-1, raster=None, path=tif_path, window=(0, 0, w, h), metadata={}
        )
        # Simulate what _setcrs_udf does on a virtual tile.
        canonical = crs_to_canonical(resolve_crs("ESRI:54008"))
        md = dict(vt.metadata or {})
        md[ot.PENDING_CRS] = canonical
        md.pop(ot.PENDING_SRID, None)
        vt.metadata = md
        # Still virtual (raster is None).
        assert vt.is_virtual()
        # pending_crs is recorded.
        assert vt.metadata.get(ot.PENDING_CRS) == "ESRI:54008"
        # open_tile applies it.
        with ot.open_tile(vt) as ds:
            assert crs_to_canonical(ds.crs) == "ESRI:54008"
    finally:
        os.unlink(tif_path)


def test_pending_srid_esri_code_materializes_as_esri():
    """A virtual tile whose pending_srid is an ESRI code (54008) opens as ESRI:54008
    — the apply-site routes through resolve_crs, not the EPSG-only from_epsg."""
    b = _epsg4326_tif()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        import rasterio

        with rasterio.open(tif_path) as ds:
            w, h = ds.width, ds.height
        vt = VirtualTile(
            cellid=-1,
            raster=None,
            path=tif_path,
            window=(0, 0, w, h),
            metadata={ot.PENDING_SRID: "54008"},
        )
        with ot.open_tile(vt) as ds:
            assert crs_to_canonical(ds.crs) == "ESRI:54008"
    finally:
        os.unlink(tif_path)


def test_setcrs_virtual_pending_crs_supersedes_pending_srid():
    """pending_crs takes precedence over pending_srid when both are present."""
    b = _epsg4326_tif()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        import rasterio

        with rasterio.open(tif_path) as ds:
            w, h = ds.width, ds.height
        vt = VirtualTile(
            cellid=-1,
            raster=None,
            path=tif_path,
            window=(0, 0, w, h),
            metadata={ot.PENDING_SRID: "3857", ot.PENDING_CRS: "ESRI:54008"},
        )
        with ot.open_tile(vt) as ds:
            # pending_crs should win.
            assert crs_to_canonical(ds.crs) == "ESRI:54008"
    finally:
        os.unlink(tif_path)


def test_setsrid_after_setcrs_clears_pending_crs():
    """rst_setsrid on a virtual tile must clear a stale pending_crs so the later
    int SRID wins (reverse of the supersede case). Regression: without the pop,
    a prior rst_setcrs's pending_crs supersedes and the setsrid relabel is lost.
    """
    b = _epsg4326_tif()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        import rasterio

        with rasterio.open(tif_path) as ds:
            w, h = ds.width, ds.height
        # A virtual tile that already carries a pending_crs (from a prior setcrs);
        # apply rst_setsrid(3857) through the public UDF and confirm it wins.
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as f
        from pyspark.sql.types import StructField, StructType

        from databricks.labs.gbx.pyrx import functions as prx
        from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

        spark = SparkSession.builder.master("local[2]").getOrCreate()
        vt = VirtualTile(
            cellid=-1,
            raster=None,
            path=tif_path,
            window=(0, 0, w, h),
            metadata={ot.PENDING_CRS: "ESRI:54008"},
        )
        schema = StructType([StructField("tile", V2_TILE_SCHEMA, False)])
        df = spark.createDataFrame([(vt.to_row(),)], schema)
        row = df.select(
            prx.rst_crs(prx.rst_setsrid("tile", f.lit(3857))).alias("c")
        ).first()
        assert (
            row["c"] == "EPSG:3857"
        ), f"rst_setsrid must clear stale pending_crs and win; got {row['c']!r}"
    finally:
        os.unlink(tif_path)


def test_setcrs_virtual_open_header_reflects_pending_crs():
    """open_header on a virtual tile with pending_crs reflects it without pixel read."""
    b = _epsg4326_tif()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        import rasterio

        with rasterio.open(tif_path) as ds:
            w, h = ds.width, ds.height
        vt = VirtualTile(
            cellid=-1,
            raster=None,
            path=tif_path,
            window=(0, 0, w, h),
            metadata={ot.PENDING_CRS: "ESRI:54008"},
        )
        with ot.open_header(vt) as ds:
            assert crs_to_canonical(ds.crs) == "ESRI:54008"
    finally:
        os.unlink(tif_path)


# ---------------------------------------------------------------------------
# rst_transformcrs — unit tests (no Spark)
# ---------------------------------------------------------------------------


def test_transformcrs_epsg_changes_bounds():
    """Reprojecting to EPSG:3857 changes the bounds from 4326 (degrees to metres)."""
    from databricks.labs.gbx.pyrx.core import warp

    b = _epsg4326_tif()
    with _tile_from_bytes(b) as src:
        src_bounds = src.bounds
    with _tile_from_bytes(b) as src:
        new_bytes = warp.reproject_to_crs(src, "EPSG:3857")
    with _tile_from_bytes(new_bytes) as ds:
        assert crs_to_canonical(ds.crs) == "EPSG:3857"
        dst_bounds = ds.bounds
    # Bounds should differ (degrees vs metres).
    assert abs(dst_bounds.left) > abs(src_bounds.left) or abs(dst_bounds.bottom) > abs(
        src_bounds.bottom
    )


def test_transformcrs_non_epsg_target_works():
    """Reprojecting to ESRI:54008 succeeds (non-EPSG target)."""
    from databricks.labs.gbx.pyrx.core import warp

    b = _epsg4326_tif()
    with _tile_from_bytes(b) as src:
        new_bytes = warp.reproject_to_crs(src, "ESRI:54008")
    with _tile_from_bytes(new_bytes) as ds:
        assert crs_to_canonical(ds.crs) == "ESRI:54008"


def test_transformcrs_intlike_string_works():
    """reproject_to_crs('3857') same as reproject_to_srid(3857) via int-cast."""
    from databricks.labs.gbx.pyrx.core import warp

    b = _epsg4326_tif()
    with _tile_from_bytes(b) as src:
        bytes_crs = warp.reproject_to_crs(src, "3857")
    with _tile_from_bytes(b) as src:
        bytes_srid = warp.reproject_to_srid(src, 3857)
    with _tile_from_bytes(bytes_crs) as dc, _tile_from_bytes(bytes_srid) as ds:
        assert dc.crs == ds.crs
        np.testing.assert_array_almost_equal(
            dc.read().astype(float), ds.read().astype(float)
        )


def test_transformcrs_identity_short_circuit():
    """reproject_to_crs with same CRS is an identity (no pixel change)."""
    from databricks.labs.gbx.pyrx.core import warp

    b = _epsg4326_tif()
    with _tile_from_bytes(b) as src:
        orig_data = src.read()
        new_bytes = warp.reproject_to_crs(src, "EPSG:4326")
    with _tile_from_bytes(new_bytes) as ds:
        assert crs_to_canonical(ds.crs) == "EPSG:4326"
        np.testing.assert_array_equal(ds.read(), orig_data)


# ---------------------------------------------------------------------------
# Spark Column UDF tests
# ---------------------------------------------------------------------------


def test_rst_setcrs_spark_esri(spark):
    """rst_setcrs('ESRI:54008') Spark UDF relabels CRS; rst_crs reads back."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    b = _epsg4326_tif()
    df = spark.createDataFrame([(b,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(
        prx.rst_crs(prx.rst_setcrs("tile", f.lit("ESRI:54008"))).alias("c")
    ).first()
    crs_str = row["c"]
    assert crs_str is not None
    assert "ESRI" in crs_str and "54008" in crs_str, f"Got: {crs_str!r}"


def test_rst_setcrs_intcast_spark(spark):
    """rst_setcrs('4326') same result as rst_setsrid(4326) via int-cast rule."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    b = make_geotiff_bytes(epsg=32633)
    df = spark.createDataFrame([(b,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(
        prx.rst_crs(prx.rst_setcrs("tile", f.lit("4326"))).alias("setcrs_crs"),
        prx.rst_crs(prx.rst_setsrid("tile", f.lit(4326))).alias("setsrid_crs"),
    ).first()
    assert row["setcrs_crs"] == row["setsrid_crs"] == "EPSG:4326"


def test_rst_transformcrs_spark_epsg(spark):
    """rst_transformcrs('EPSG:3857') reprojects; crs reflects new target."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    b = _epsg4326_tif()
    df = spark.createDataFrame([(b,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(
        prx.rst_crs(prx.rst_transformcrs("tile", f.lit("EPSG:3857"))).alias("c")
    ).first()
    assert row["c"] == "EPSG:3857"


def test_rst_transformcrs_non_epsg_spark(spark):
    """rst_transformcrs('ESRI:54008') works for non-EPSG target via Spark UDF."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    b = _epsg4326_tif()
    df = spark.createDataFrame([(b,)], ["raster"])
    df = df.select(prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))
    row = df.select(
        prx.rst_crs(prx.rst_transformcrs("tile", f.lit("ESRI:54008"))).alias("c")
    ).first()
    crs_str = row["c"]
    assert crs_str is not None
    assert "ESRI" in crs_str and "54008" in crs_str, f"Got: {crs_str!r}"


# ---------------------------------------------------------------------------
# Reader tile.crs population tests
# (uses raster_gbx — pure-Python DataSource V2; requires registration)
# ---------------------------------------------------------------------------


def _register_raster_gbx(spark):
    from databricks.labs.gbx.ds.raster import RasterGbxDataSource

    spark.dataSource.register(RasterGbxDataSource)


def test_reader_raster_gbx_epsg4326_populates_crs(spark):
    """raster_gbx materialized reader yields tile.crs == 'EPSG:4326'."""
    _register_raster_gbx(spark)
    b = make_geotiff_bytes(epsg=4326)
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        df = (
            spark.read.format("raster_gbx")
            .option("virtualTiles", "false")
            .load(tif_path)
        )
        row = df.select("tile.crs").first()
        assert row["crs"] == "EPSG:4326", f"Expected 'EPSG:4326', got: {row['crs']!r}"
    finally:
        os.unlink(tif_path)


@pytest.mark.skipif(not _HAS_MODIS, reason="MODIS TIF not built yet")
def test_reader_raster_gbx_esri54008_populates_crs(spark):
    """raster_gbx materialized reader yields tile.crs == 'ESRI:54008' for MODIS TIF."""
    _register_raster_gbx(spark)
    df = (
        spark.read.format("raster_gbx").option("virtualTiles", "false").load(_MODIS_TIF)
    )
    row = df.select("tile.crs").first()
    crs_str = row["crs"]
    assert crs_str is not None, "tile.crs must not be None for ESRI:54008 raster"
    assert "ESRI" in crs_str and "54008" in crs_str, f"Got: {crs_str!r}"


def test_reader_virtual_tile_crs_is_none(spark):
    """raster_gbx virtual reader: tile.crs is None for virtual tiles.

    tile.crs doubles as the open_tile warp-target; setting it to the source
    CRS for virtual tiles would conflict with rst_setsrid (pending_srid !=
    tile.crs would trigger a spurious warp).  The source CRS is implicit in
    the path and readable via rst_crs (which uses open_header -> source CRS).
    """
    _register_raster_gbx(spark)
    b = make_geotiff_bytes(epsg=4326)
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(b)
        tif_path = f.name
    try:
        df = (
            spark.read.format("raster_gbx")
            .option("virtualTiles", "true")
            .load(tif_path)
        )
        row = df.select("tile.crs").first()
        # Virtual tile: tile.crs intentionally None (warp-target semantics).
        assert (
            row["crs"] is None
        ), f"Expected None for virtual tile.crs, got: {row['crs']!r}"
        # But rst_crs (which reads the source header) should still return the CRS.
        from databricks.labs.gbx.pyrx import functions as prx

        row2 = df.select(prx.rst_crs("tile").alias("c")).first()
        assert (
            row2["c"] == "EPSG:4326"
        ), f"rst_crs via header expected 'EPSG:4326', got: {row2['c']!r}"
    finally:
        os.unlink(tif_path)
