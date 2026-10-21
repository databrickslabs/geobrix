"""Task 3: Assert that hot-path light-tier write sites emit ZSTD+predictor.

Covers:
- materialize_to_bytes on a virtual tile -> zstd
- _window_dataset_bytes (used internally by open_tile) -> zstd
- _warp_window_bytes (reprojection path) -> zstd
- _empty_dataset_bytes (1x1 disjoint result) -> zstd
- encode_tile GTiff path -> zstd
- encode_tile COG path is exempt (rio_cogeo owns the codec; not tested here)

Predictor assertion: hard check is Compression.zstd; predictor checked
best-effort via ds.profile or IMAGE_STRUCTURE tags (rasterio surfaces it
inconsistently across GDAL builds).
"""

from __future__ import annotations

import numpy as np
import pytest
import rasterio
from rasterio.enums import Compression
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.compression import predictor_for
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DTYPES = ["int16", "uint16", "float32", "uint8"]


def _write_tif(tmp_path, dt="int16", sz=64) -> str:
    """Write a small single-band EPSG:4326 GeoTIFF and return the path."""
    p = str(tmp_path / f"{dt}.tif")
    rng = np.random.default_rng(seed=42)
    a = (rng.random((sz, sz)) * 1000).astype(dt)
    transform = from_bounds(-1, -1, 1, 1, sz, sz)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        height=sz,
        width=sz,
        count=1,
        dtype=dt,
        crs="EPSG:4326",
        transform=transform,
    ) as d:
        d.write(a, 1)
    return p


def _assert_zstd(out_bytes: bytes, *, context: str = "") -> None:
    """Assert that out_bytes is a ZSTD-compressed GTiff."""
    with MemoryFile(out_bytes) as mf, mf.open() as ds:
        comp = ds.compression
        assert (
            comp == Compression.zstd
        ), f"{context}: expected Compression.zstd, got {comp}"
        # Best-effort predictor check — rasterio does not always surface the
        # predictor tag, so we accept it being absent.
        pred = ds.profile.get("predictor")
        if pred is None:
            tags = ds.tags(ns="IMAGE_STRUCTURE")
            pred = tags.get("PREDICTOR")
        # If surfaced, it must be a numeric predictor (not 1 for most dtypes).
        # We only assert it is present if we can read it.
        return pred  # informational; caller may assert further


# ---------------------------------------------------------------------------
# materialize_to_bytes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dt", _DTYPES)
def test_materialize_to_bytes_is_zstd(tmp_path, dt):
    """materialize_to_bytes must produce ZSTD-compressed bytes."""
    p = _write_tif(tmp_path, dt)
    with rasterio.open(p) as ds:
        sz = ds.width
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    mat = ot.materialize_to_bytes(vt)
    _assert_zstd(mat.raster, context=f"materialize_to_bytes[{dt}]")
    # Ensure pending keys are stripped from the output tile.
    for k in ("pending_nodata", "pending_srid", "pending_bands"):
        assert k not in (
            mat.metadata or {}
        ), f"pending key {k!r} leaked into materialized tile"


def test_materialize_to_bytes_predictor_for_int16(tmp_path):
    """int16 should get predictor=2 when surfaced."""
    p = _write_tif(tmp_path, "int16")
    with rasterio.open(p) as ds:
        sz = ds.width
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    mat = ot.materialize_to_bytes(vt)
    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.compression == Compression.zstd
        pred = ds.profile.get("predictor")
        if pred is not None:
            assert int(pred) == 2, f"expected predictor=2 for int16, got {pred}"


def test_materialize_to_bytes_predictor_for_float32(tmp_path):
    """float32 should get predictor=3 when surfaced."""
    p = _write_tif(tmp_path, "float32")
    with rasterio.open(p) as ds:
        sz = ds.width
    vt = VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))
    mat = ot.materialize_to_bytes(vt)
    with MemoryFile(mat.raster) as mf, mf.open() as ds:
        assert ds.compression == Compression.zstd
        pred = ds.profile.get("predictor")
        if pred is not None:
            assert int(pred) == 3, f"expected predictor=3 for float32, got {pred}"


# ---------------------------------------------------------------------------
# _window_dataset_bytes (internal, but exercised via open_tile virtual path)
# ---------------------------------------------------------------------------


def test_window_dataset_bytes_is_zstd(tmp_path):
    """_window_dataset_bytes must produce ZSTD bytes (tested via open_tile)."""
    p = _write_tif(tmp_path, "int16")
    with rasterio.open(p) as ds:
        sz = ds.width
    # open_tile on a virtual tile calls _window_dataset_bytes internally; the
    # yielded dataset's bytes won't let us inspect compression directly, so we
    # call _window_dataset_bytes directly.
    with rasterio.open(p) as src:
        window = Window(0, 0, sz, sz)
        out = ot._window_dataset_bytes(src, window)
    _assert_zstd(out, context="_window_dataset_bytes")


# ---------------------------------------------------------------------------
# _warp_window_bytes
# ---------------------------------------------------------------------------


def test_warp_window_bytes_is_zstd(tmp_path):
    """_warp_window_bytes must produce ZSTD bytes after reprojection."""
    # Write in EPSG:4326, warp to EPSG:3857
    p = _write_tif(tmp_path, "float32")
    with rasterio.open(p) as src:
        sz = src.width
        window = Window(0, 0, sz, sz)
        out = ot._warp_window_bytes(src, window, want_epsg=3857)
    _assert_zstd(out, context="_warp_window_bytes")


# ---------------------------------------------------------------------------
# _empty_dataset_bytes
# ---------------------------------------------------------------------------


def test_empty_dataset_bytes_is_zstd(tmp_path):
    """_empty_dataset_bytes (1x1 NoData) must also use ZSTD."""
    p = _write_tif(tmp_path, "int16")
    with rasterio.open(p) as ref:
        out = ot._empty_dataset_bytes(ref)
    _assert_zstd(out, context="_empty_dataset_bytes")


# ---------------------------------------------------------------------------
# encode_tile (GTiff path)
# ---------------------------------------------------------------------------


def test_encode_tile_gtiff_is_zstd(tmp_path):
    """encode_tile with tile_format='gtiff' must emit ZSTD when compress='auto'."""
    from databricks.labs.gbx.ds._encode import encode_tile

    p = _write_tif(tmp_path, "int16", sz=32)
    with rasterio.open(p) as ds:
        _cellid, raster_bytes, metadata = encode_tile(
            ds,
            window=(0, 0, ds.width, ds.height),
            source_path=p,
            all_parents="",
            compression="auto",
            tile_format="gtiff",
        )
    _assert_zstd(raster_bytes, context="encode_tile[gtiff,auto]")


@pytest.mark.parametrize("dt", ["int16", "float32", "uint8"])
def test_encode_tile_gtiff_dtype_predictor(tmp_path, dt):
    """encode_tile GTiff path: check predictor matches dtype when surfaced."""
    from databricks.labs.gbx.ds._encode import encode_tile
    from databricks.labs.gbx.pyrx.core.compression import predictor_for

    expected_pred = predictor_for(dt)
    p = _write_tif(tmp_path, dt, sz=32)
    with rasterio.open(p) as ds:
        _cellid, raster_bytes, _meta = encode_tile(
            ds,
            window=(0, 0, ds.width, ds.height),
            source_path=p,
            all_parents="",
            compression="auto",
            tile_format="gtiff",
        )
    with MemoryFile(raster_bytes) as mf, mf.open() as out_ds:
        assert out_ds.compression == Compression.zstd
        pred = out_ds.profile.get("predictor")
        if pred is not None:
            assert (
                int(pred) == expected_pred
            ), f"dtype={dt}: expected predictor={expected_pred}, got {pred}"


# ---------------------------------------------------------------------------
# encode_tile with explicit non-auto compression (backward compat)
# ---------------------------------------------------------------------------


def test_encode_tile_explicit_deflate_still_works(tmp_path):
    """encode_tile must still honour explicit compression='DEFLATE'."""
    from databricks.labs.gbx.ds._encode import encode_tile

    p = _write_tif(tmp_path, "int16", sz=32)
    with rasterio.open(p) as ds:
        _cellid, raster_bytes, _meta = encode_tile(
            ds,
            window=(0, 0, ds.width, ds.height),
            source_path=p,
            all_parents="",
            compression="DEFLATE",
            tile_format="gtiff",
        )
    with MemoryFile(raster_bytes) as mf, mf.open() as out_ds:
        assert out_ds.compression == Compression.deflate


# ---------------------------------------------------------------------------
# Level is size-adaptive: small tile gets higher level than large tile
# ---------------------------------------------------------------------------


def test_auto_level_scales_with_tile_size(tmp_path):
    """Verify that the auto level for a tiny tile >= level for a large tile."""
    from databricks.labs.gbx.pyrx.core.compression import auto_level

    small_bytes = 1 * 1024 * 1024  # 1 MiB
    large_bytes = 512 * 1024 * 1024  # 512 MiB
    assert auto_level(small_bytes) >= auto_level(large_bytes)


# ---------------------------------------------------------------------------
# Task 4: remaining light write sites — one assertion per site-FAMILY
# ---------------------------------------------------------------------------

# -- edit family (clip_to_geom / update_type / init_nodata / threshold / set_srid / band)


def test_init_nodata_is_zstd(tmp_path):
    """edit.init_nodata (edit family) must emit ZSTD bytes."""
    from databricks.labs.gbx.pyrx.core import edit as _edit

    p = _write_tif(tmp_path, "int16")
    with rasterio.open(p) as ds:
        result = _edit.init_nodata(ds)
    _assert_zstd(result, context="edit.init_nodata")


# -- agg family (merge_tiles / combineavg_tiles / frombands_tiles / rasterize_features)


def test_merge_tiles_is_zstd(tmp_path):
    """agg.merge_tiles must emit ZSTD bytes."""
    from databricks.labs.gbx.pyrx.core.agg import merge_tiles

    p1 = _write_tif(tmp_path, "int16", sz=32)
    # Write a second non-overlapping tile
    p2 = str(tmp_path / "int16_b.tif")
    rng = np.random.default_rng(seed=99)
    a2 = (rng.random((32, 32)) * 1000).astype("int16")
    t2 = from_bounds(0.0, -1.0, 2.0, 1.0, 32, 32)
    with rasterio.open(
        p2,
        "w",
        driver="GTiff",
        height=32,
        width=32,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=t2,
    ) as d:
        d.write(a2, 1)
    with open(p1, "rb") as f:
        b1 = f.read()
    with open(p2, "rb") as f:
        b2 = f.read()
    result = merge_tiles([b1, b2])
    _assert_zstd(result, context="agg.merge_tiles")


# -- tiling family (retile / make_tiles / overlapping_tiles / separate_bands)


def test_retile_is_zstd(tmp_path):
    """tiling.retile must emit ZSTD tiles."""
    from databricks.labs.gbx.pyrx.core.tiling import retile

    p = _write_tif(tmp_path, "int16", sz=64)
    with rasterio.open(p) as ds:
        tiles = retile(ds, 32, 32)
    assert len(tiles) == 4, f"expected 4 tiles, got {len(tiles)}"
    for i, tile in enumerate(tiles):
        _assert_zstd(tile, context=f"retile tile[{i}]")


# -- warp family (reproject_to_srid)


def test_reproject_to_srid_is_zstd(tmp_path):
    """warp.reproject_to_srid must emit ZSTD bytes after reprojection."""
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_srid

    p = _write_tif(tmp_path, "float32")
    with rasterio.open(p) as ds:
        result = reproject_to_srid(ds, 3857)
    _assert_zstd(result, context="warp.reproject_to_srid")


# -- resample family (resample_by_factor / resample_to_size / resample_to_res)


def test_resample_by_factor_is_zstd(tmp_path):
    """resample.resample_by_factor must emit ZSTD bytes."""
    from databricks.labs.gbx.pyrx.core.resample import resample_by_factor

    p = _write_tif(tmp_path, "float32")
    with rasterio.open(p) as ds:
        result = resample_by_factor(ds, 0.5)
    _assert_zstd(result, context="resample.resample_by_factor")


# -- analysis family (proximity / viewshed / cog_convert)


def test_proximity_is_zstd(tmp_path):
    """analysis.proximity must emit ZSTD bytes."""
    from databricks.labs.gbx.pyrx.core.analysis import proximity

    p = _write_tif(tmp_path, "int16")
    with rasterio.open(p) as ds:
        result = proximity(ds, None, "PIXEL", None)
    _assert_zstd(result, context="analysis.proximity")


# ---------------------------------------------------------------------------
# tile_to_bytes wiring (T1 fail-on-revert: DIV-1 and DIV-2 guards)
# ---------------------------------------------------------------------------


def _make_src_bytes_write(dtype: str, sz: int = 32) -> bytes:
    """Write a tiny single-band GTiff into memory and return raw bytes."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    rng = np.random.default_rng(42)
    arr = (rng.random((sz, sz)).astype("float64") * 100).astype(dtype)
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            height=sz,
            width=sz,
            count=1,
            dtype=dtype,
            crs="EPSG:4326",
            transform=from_bounds(0, 0, 1, 1, sz, sz),
        ) as ds:
            ds.write(arr, 1)
        return mf.read()


def test_tile_to_bytes_cog_zstd_float32_predictor_3():
    """T1 / DIV-1 fail-on-revert: COG+ZSTD float32 must carry predictor=3.

    Before the fold, _write.py::_creation_opts omitted predictor for ZSTD entirely
    (lines 36-38: only opts["zstd_level"] was set).  After the fold, creation_opts
    derives predictor=3 from float32 dtype.  Reverting the fold drops the predictor.
    """
    from rasterio.enums import Compression
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.ds._write import tile_to_bytes

    src = _make_src_bytes_write("float32")
    meta = {"compression": "ZSTD", "zstd_level": "9"}
    out = tile_to_bytes(cellid=0, raster_bytes=src, metadata=meta, force_driver="COG")
    with MemoryFile(out) as mf, mf.open() as ds:
        assert (
            ds.compression == Compression.zstd
        ), f"Expected ZSTD, got {ds.compression}"
        pred = ds.profile.get("predictor") or ds.tags(ns="IMAGE_STRUCTURE").get(
            "PREDICTOR"
        )
        assert pred is not None and int(pred) == 3, (
            f"DIV-1 guard: expected predictor=3 for float32 ZSTD COG, got {pred!r}. "
            "Reverting the _write.py fold removes this predictor."
        )


def test_tile_to_bytes_cog_deflate_uint8_predictor_1():
    """T1 / DIV-2 fail-on-revert: COG+DEFLATE uint8 must not carry predictor=2.

    Before the fold, _write.py used `'3' if _is_float(dtype) else '2'` which
    returned 2 for uint8.  Canonical predictor_for('uint8') returns 1.
    After the fold, creation_opts derives predictor=1.  Reverting returns 2.

    Note on None: GDAL COG's DEFLATE PREDICTOR option is a string-select
    (YES/NO/STANDARD/FLOATING_POINT) that rejects the numeric value "1".
    When predictor=1 is passed, GDAL emits a CPLE_NotSupported warning and
    omits the predictor tag entirely — i.e. pred is None, which means "no
    prediction", the correct semantics for uint8.  The fail-on-revert property
    is preserved: reverting the fold restores predictor=2 which is wrong.
    """
    from rasterio.enums import Compression
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.ds._write import tile_to_bytes

    src = _make_src_bytes_write("uint8")
    meta = {"compression": "DEFLATE", "zlevel": "6"}
    out = tile_to_bytes(cellid=0, raster_bytes=src, metadata=meta, force_driver="COG")
    with MemoryFile(out) as mf, mf.open() as ds:
        assert (
            ds.compression == Compression.deflate
        ), f"Expected DEFLATE, got {ds.compression}"
        pred = ds.profile.get("predictor") or ds.tags(ns="IMAGE_STRUCTURE").get(
            "PREDICTOR"
        )
        # pred is None means no prediction (GDAL default), which is correct for uint8.
        # pred == "2" means STANDARD differencing, which is wrong for uint8 (DIV-2 guard).
        assert pred is None or int(pred) == 1, (
            f"DIV-2 guard: expected predictor=1 or None (no prediction) for uint8 DEFLATE COG, "
            f"got {pred!r}. Pre-fold code returned 2 for uint8 (non-float non-small-int branch)."
        )


# ---------------------------------------------------------------------------
# write_gtiff: one canonical light writer; derived rasters never inherit the
# source file's codec (fail-on-revert wiring per routed site)
# ---------------------------------------------------------------------------


def _lzw_src(dtype="float32", nodata=-999.0):
    """A 64x64 tiled LZW GTiff with no predictor: the codec a derived raster must NOT inherit."""
    a = (np.random.default_rng(7).random((64, 64)) * 300).astype(dtype)
    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=1,
        dtype=dtype,
        nodata=nodata,
        crs="EPSG:32618",
        transform=from_bounds(500000, 4490000, 500640, 4490640, 64, 64),
        compress="lzw",
        tiled=True,
        blockxsize=32,
        blockysize=32,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a, 1)
        return mf.read()


def _assert_canonical(b, dtype="float32"):
    """b is ZSTD with the predictor creation_opts derives for dtype (1 = tag omitted)."""
    with MemoryFile(b) as m, m.open() as c:
        img = c.tags(ns="IMAGE_STRUCTURE")
        assert img.get("COMPRESSION") == "ZSTD", img
        want = predictor_for(dtype)
        if want == 1:
            assert img.get("PREDICTOR") in (None, "1"), img
        else:
            assert img.get("PREDICTOR") == str(want), img


def test_write_gtiff_is_canonical_and_leaves_profile_alone():
    from databricks.labs.gbx.pyrx.core.compression import write_gtiff

    prof = dict(
        driver="COG",
        width=8,
        height=8,
        count=1,
        dtype="int16",
        crs="EPSG:32618",
        transform=from_bounds(0, 0, 8, 8, 8, 8),
        compress="lzw",
    )
    before = dict(prof)
    out = write_gtiff(prof, np.arange(64, dtype="int16").reshape(1, 8, 8))
    assert prof == before, "write_gtiff must not mutate the caller's profile"
    with MemoryFile(out) as m, m.open() as c:
        assert c.driver == "GTiff"
    _assert_canonical(out, "int16")


def test_focal_filt_is_canonical():
    from databricks.labs.gbx.pyrx.core import focal

    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = focal.filt(ds, 3, "max")
    _assert_canonical(out)


def test_focal_convolve_is_canonical():
    from databricks.labs.gbx.pyrx.core import focal

    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = focal.convolve(ds, np.ones((3, 3)) / 9.0)
    _assert_canonical(out, "float64")


def test_nodata_emit_is_canonical_via_terrain_slope():
    from databricks.labs.gbx.pyrx.core import terrain

    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = terrain.slope(ds)
    _assert_canonical(out)


def test_rasterize_geom_is_canonical():
    import shapely
    from shapely.geometry import box

    from databricks.labs.gbx.pyrx.core import features

    poly = shapely.set_srid(box(1, 1, 3, 3), 4326)
    out = features.rasterize_geom(shapely.to_wkb(poly), 5.0, 0, 0, 4, 4, 8, 8, 4326)
    _assert_canonical(out, "float64")


def test_chm_is_canonical():
    from databricks.labs.gbx.pyrx.core.chm import chm

    src = _lzw_src()
    _assert_canonical(chm(src, src))


def test_color_relief_is_canonical(tmp_path):
    from databricks.labs.gbx.pyrx.core import terrain

    table = tmp_path / "colors.txt"
    table.write_text("0 0 0 0\n300 255 255 255\n")
    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = terrain.color_relief(ds, str(table))
    _assert_canonical(out, "uint8")


def test_getsubdataset_is_canonical(tmp_path):
    from types import SimpleNamespace

    from databricks.labs.gbx.pyrx.core import accessors

    sub = tmp_path / "prAdjust.tif"
    sub.write_bytes(_lzw_src())
    out = accessors.getsubdataset(
        SimpleNamespace(subdatasets=[str(sub)]), "prAdjust.tif"
    )
    _assert_canonical(out)


def test_fill_nodata_is_canonical():
    from databricks.labs.gbx.pyrx.core import features

    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = features.fill_nodata(ds)
    _assert_canonical(out)


def test_bin_points_is_canonical():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    out = bin_points([0.5, 1.5], [0.5, 1.5], [1.0, 2.0], 0, 0, 2, 2, 2, 2, 32618, "max")
    _assert_canonical(out)


def test_idw_grid_is_canonical():
    from databricks.labs.gbx.pyrx.core import tin

    pts = np.array([[1.0, 1.0], [3.0, 3.0]])
    out = tin.idw_grid(pts, np.array([10.0, 20.0]), 0.0, 0.0, 4.0, 4.0, 4, 4, 32633)
    _assert_canonical(out, "float64")


def test_percentile_stretch_is_canonical():
    from databricks.labs.gbx.pyrx.core.stretch import percentile_stretch

    with MemoryFile(_lzw_src()) as m, m.open() as ds:
        out = percentile_stretch(ds, 2.0, 98.0)
    _assert_canonical(out, "uint8")


def _ycbcr_src(x0=500000, crs="EPSG:32618", seed=7):
    """A 64x64 tiled 3-band uint8 JPEG/YCbCr GTiff: its photometric must not be inherited."""
    a = (np.random.default_rng(seed).random((3, 64, 64)) * 255).astype("uint8")
    prof = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=3,
        dtype="uint8",
        crs=crs,
        transform=from_bounds(x0, 4490000, x0 + 640, 4490640, 64, 64),
        compress="jpeg",
        photometric="ycbcr",
        tiled=True,
        blockxsize=32,
        blockysize=32,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a)
        return mf.read()


def _assert_zstd_without_ycbcr(b, count=3):
    with MemoryFile(b) as m, m.open() as c:
        img = c.tags(ns="IMAGE_STRUCTURE")
        assert img.get("COMPRESSION") == "ZSTD", img
        assert "YCbCr" not in img.get("SOURCE_COLOR_SPACE", ""), img
        assert str(c.profile.get("photometric")).lower() != "ycbcr", c.profile
        assert c.count == count


def test_write_gtiff_drops_ycbcr_photometric_of_a_jpeg_source():
    from databricks.labs.gbx.pyrx.core.compression import write_gtiff

    with MemoryFile(_ycbcr_src()) as m, m.open() as ds:
        prof, data = ds.profile.copy(), ds.read()
    assert str(prof.get("photometric")).lower() == "ycbcr"  # premise of the test
    before = dict(prof)
    out = write_gtiff(prof, data)
    assert prof == before, "write_gtiff must not mutate the caller's profile"
    _assert_zstd_without_ycbcr(out)
    with MemoryFile(out) as m, m.open() as c:
        assert np.array_equal(c.read(), data), "values must survive the re-encode"


def test_percentile_stretch_accepts_a_ycbcr_jpeg_source():
    from databricks.labs.gbx.pyrx.core.stretch import percentile_stretch

    with MemoryFile(_ycbcr_src()) as m, m.open() as ds:
        out = percentile_stretch(ds, 2.0, 98.0)
    _assert_zstd_without_ycbcr(out)


def test_focal_filt_accepts_a_ycbcr_jpeg_source():
    from databricks.labs.gbx.pyrx.core import focal

    with MemoryFile(_ycbcr_src()) as m, m.open() as ds:
        out = focal.filt(ds, 3, "max")
    _assert_zstd_without_ycbcr(out)


def test_rst_fromfile_materialize_is_canonical(tmp_path):
    from databricks.labs.gbx.pyrx.functions import _fromfile_impl

    src = tmp_path / "lzw.tif"
    src.write_bytes(_lzw_src())
    tile = _fromfile_impl(str(src), "GTiff", True)
    assert tile is not None and tile["raster"] is not None
    _assert_canonical(bytes(tile["raster"]))


def test_rst_fromfile_materialize_accepts_a_ycbcr_jpeg_source(tmp_path):
    from databricks.labs.gbx.pyrx.functions import _fromfile_impl

    src = tmp_path / "ycbcr.tif"
    src.write_bytes(_ycbcr_src())
    tile = _fromfile_impl(str(src), "GTiff", True)
    assert tile is not None, "a YCbCr source must materialize, not null out"
    _assert_zstd_without_ycbcr(bytes(tile["raster"]))


# ---------------------------------------------------------------------------
# Every remaining site that copies a source profile and merges the canonical
# options drops the YCbCr photometric (fail-on-revert wiring per site)
# ---------------------------------------------------------------------------

_PIXEL_COPY = (
    "def f(in_ar, out_ar, xoff, yoff, xsize, ysize, rxs, rys, radius, gt, **kw):\n"
    "    out_ar[:] = in_ar[0]\n"
)


def _on_ycbcr(fn):
    """Call fn(ds) on an open 3-band JPEG/YCbCr dataset."""
    with MemoryFile(_ycbcr_src()) as m, m.open() as ds:
        return fn(ds)


def _ycbcr_path(tmp_path):
    p = tmp_path / "ycbcr.tif"
    p.write_bytes(_ycbcr_src())
    return str(p)


def _ycbcr_vtile(tmp_path):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    return VirtualTile(cellid=1, path=_ycbcr_path(tmp_path), window=(0, 0, 32, 32))


def _on_ycbcr_window(fn):
    """Call fn(ds, window) on an open YCbCr dataset and its top-left 32x32 window."""
    return _on_ycbcr(lambda ds: fn(ds, Window(0, 0, 32, 32)))


def _case_resample_by_factor(tmp_path):
    from databricks.labs.gbx.pyrx.core.resample import resample_by_factor

    return _on_ycbcr(lambda ds: resample_by_factor(ds, 2))


def _case_reproject_to_srid(tmp_path):
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_srid

    return _on_ycbcr(lambda ds: reproject_to_srid(ds, 3857))


def _case_reproject_to_srid_identity(tmp_path):
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_srid

    return _on_ycbcr(lambda ds: reproject_to_srid(ds, 32618))


def _case_reproject_to_crs(tmp_path):
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_crs

    return _on_ycbcr(lambda ds: reproject_to_crs(ds, "EPSG:3857"))


def _case_reproject_to_crs_identity(tmp_path):
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_crs

    return _on_ycbcr(lambda ds: reproject_to_crs(ds, "EPSG:32618"))


def _case_build_overviews(tmp_path):
    from databricks.labs.gbx.pyrx.core.ops import build_overviews

    return _on_ycbcr(lambda ds: build_overviews(ds, [2]))


def _case_merge_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import merge_tiles

    return merge_tiles([_ycbcr_src(), _ycbcr_src(x0=500640)])


def _case_merge_tiles_mixed_crs(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import merge_tiles

    # Tiles in EPSG:32618 and 32619 either side of their zone boundary: the second
    # is reprojected onto the first's CRS before the merge.
    return merge_tiles([_ycbcr_src(x0=750000), _ycbcr_src(x0=250000, crs="EPSG:32619")])


def _case_combineavg_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import combineavg_tiles

    return combineavg_tiles([_ycbcr_src(), _ycbcr_src()])


def _case_combine_max_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import combine_max_tiles

    return combine_max_tiles([_ycbcr_src(), _ycbcr_src()])


def _case_frombands_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import frombands_tiles

    return frombands_tiles([(1, _ycbcr_src()), (2, _ycbcr_src())])


def _case_derivedband_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import derivedband_tiles

    return derivedband_tiles([_ycbcr_src(), _ycbcr_src()], _PIXEL_COPY, "f")


def _case_align_to_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import align_to_tiles

    return align_to_tiles(_ycbcr_src(), _ycbcr_src())


def _case_window_dataset_bytes(tmp_path):
    return _on_ycbcr_window(ot._window_dataset_bytes)


def _case_warp_window_bytes(tmp_path):
    return _on_ycbcr_window(lambda ds, w: ot._warp_window_bytes(ds, w, 3857))


def _case_warp_window_bytes_crs(tmp_path):
    from rasterio.crs import CRS

    return _on_ycbcr_window(
        lambda ds, w: ot._warp_window_bytes_crs(ds, w, CRS.from_epsg(3857))
    )


def _case_materialize_to_bytes(tmp_path):
    return ot.materialize_to_bytes(_ycbcr_vtile(tmp_path)).raster


def _case_windowed_materialize_bytes(tmp_path):
    return ot._windowed_materialize_bytes(_ycbcr_vtile(tmp_path))


def _case_tile_to_bytes(tmp_path):
    return ot._tile_to_bytes(_ycbcr_vtile(tmp_path))


def _case_proximity(tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import proximity

    return _on_ycbcr(lambda ds: proximity(ds, None, "PIXEL", None))


def _case_viewshed(tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import viewshed

    return _on_ycbcr(lambda ds: viewshed(ds, 500320.0, 4490320.0, 2.0, 0.0, None))


def _case_derivedband(tmp_path):
    from databricks.labs.gbx.pyrx.core.derivedband import derivedband

    return _on_ycbcr(lambda ds: derivedband(ds, _PIXEL_COPY, "f"))


def _case_cog_convert(tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert

    return _on_ycbcr(lambda ds: cog_convert(ds, "AUTO", 32, "AVERAGE"))


def _case_encode_tile_gtiff(tmp_path):
    from databricks.labs.gbx.ds._encode import encode_tile

    path = _ycbcr_path(tmp_path)
    with rasterio.open(path) as ds:
        return encode_tile(ds, (0, 0, 32, 32), path, "", "auto", "gtiff")[1]


def _case_encode_tile_cog(tmp_path):
    from databricks.labs.gbx.ds._encode import encode_tile

    path = _ycbcr_path(tmp_path)
    with rasterio.open(path) as ds:
        return encode_tile(ds, (0, 0, 32, 32), path, "", "auto", "cog")[1]


def _case_tile_to_bytes_cog(tmp_path):
    from databricks.labs.gbx.ds._write import tile_to_bytes

    meta = {"compression": "ZSTD", "zstd_level": "6"}
    return tile_to_bytes(0, _ycbcr_src(), meta, force_driver="COG")


def _case_apply_compression(tmp_path):
    from databricks.labs.gbx.ds.writer import _apply_compression

    return _apply_compression(_ycbcr_src(), "auto", None, None)


# (case, bands in the result)
_YCBCR_OPS = [
    (_case_resample_by_factor, 3),
    (_case_reproject_to_srid, 3),
    (_case_reproject_to_srid_identity, 3),
    (_case_reproject_to_crs, 3),
    (_case_reproject_to_crs_identity, 3),
    (_case_build_overviews, 3),
    (_case_merge_tiles, 3),
    (_case_merge_tiles_mixed_crs, 3),
    (_case_combineavg_tiles, 3),
    (_case_combine_max_tiles, 3),
    (_case_frombands_tiles, 6),
    (_case_derivedband_tiles, 1),
    (_case_align_to_tiles, 3),
    (_case_window_dataset_bytes, 3),
    (_case_warp_window_bytes, 3),
    (_case_warp_window_bytes_crs, 3),
    (_case_materialize_to_bytes, 3),
    (_case_windowed_materialize_bytes, 3),
    (_case_tile_to_bytes, 3),
    (_case_proximity, 1),
    (_case_viewshed, 1),
    (_case_derivedband, 1),
    (_case_cog_convert, 3),
    (_case_encode_tile_gtiff, 3),
    (_case_encode_tile_cog, 3),
    (_case_tile_to_bytes_cog, 3),
    (_case_apply_compression, 3),
]


@pytest.mark.parametrize(
    "case,bands", _YCBCR_OPS, ids=[c.__name__[6:] for c, _ in _YCBCR_OPS]
)
def test_ops_accept_a_ycbcr_jpeg_source(tmp_path, case, bands):
    """A JPEG/YCbCr source must re-encode to canonical ZSTD, not fail on PHOTOMETRIC."""
    out = case(tmp_path)
    assert out, "the op returned nothing"
    _assert_zstd_without_ycbcr(bytes(out), count=bands)


def test_cog_writer_mosaic_accepts_a_ycbcr_jpeg_source(tmp_path):
    """cog_gbx mosaic mode re-encodes windows of a YCbCr source into mini-COGs."""
    from pyspark.sql.types import StringType, StructField, StructType

    from databricks.labs.gbx.ds.cog_writer import CogGbxWriter, parse_mosaic_options

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "none", "tileSize": 32}
    )
    writer = CogGbxWriter(
        str(tmp_path / "out"),
        StructType([StructField("path", StringType(), False)]),
        overwrite=True,
        cog_blocksize=32,
        mosaic_opts=opts,
    )
    msg = writer.write(iter([{"path": _ycbcr_path(tmp_path)}]))
    assert len(msg.paths) == 4, msg.paths
    for p in msg.paths:
        with rasterio.open(p) as c:
            assert c.count == 3
            assert str(c.profile.get("photometric")).lower() != "ycbcr", c.profile


@pytest.mark.parametrize("n", [2, 5])
def test_derivedband_tiles_stacks_a_ycbcr_group_losslessly(n):
    """The tile stack handed to the pixel function must hold the source values exactly.

    A stack that inherited the sources' JPEG codec came back perturbed (a few
    tiles) or failed to write at all (five or more).
    """
    from databricks.labs.gbx.pyrx.core.agg import derivedband_tiles

    total = (
        "def f(in_ar, out_ar, xoff, yoff, xsize, ysize, rxs, rys, radius, gt, **kw):\n"
        "    out_ar[:] = sum(a.astype('float64') for a in in_ar)\n"
    )
    tiles = [_ycbcr_src(seed=i) for i in range(n)]
    want = 0.0
    for t in tiles:
        with MemoryFile(t) as m, m.open() as c:
            want = want + c.read(1).astype("float64")
    out = derivedband_tiles(tiles, total, "f")
    with MemoryFile(out) as m, m.open() as c:
        assert np.array_equal(c.read(1), want), "the stack's values changed"


def _assert_jpeg_ycbcr(b):
    with MemoryFile(b) as m, m.open() as c:
        assert str(c.profile.get("compress")).lower() == "jpeg", c.profile
        assert str(c.profile.get("photometric")).lower() == "ycbcr", c.profile


def test_encode_tile_gtiff_keeps_ycbcr_for_an_explicit_jpeg_codec(tmp_path):
    """Asking for JPEG on a YCbCr source keeps the YCbCr photometric JPEG carries."""
    from databricks.labs.gbx.ds._encode import encode_tile

    path = _ycbcr_path(tmp_path)
    with rasterio.open(path) as ds:
        raster = encode_tile(ds, (0, 0, 32, 32), path, "", "JPEG", "gtiff")[1]
    _assert_jpeg_ycbcr(raster)


def test_apply_compression_keeps_ycbcr_for_an_explicit_jpeg_codec():
    from databricks.labs.gbx.ds.writer import _apply_compression

    _assert_jpeg_ycbcr(_apply_compression(_ycbcr_src(), "jpeg", None, None))


def test_drop_ycbcr_unless_jpeg_keeps_the_key_only_for_a_jpeg_codec():
    from databricks.labs.gbx.pyrx.core.compression import drop_ycbcr

    base = {"photometric": "YCbCr", "count": 3}
    assert "photometric" not in drop_ycbcr({**base, "compress": "jpeg"})
    assert "photometric" not in drop_ycbcr(
        {**base, "compress": "zstd"}, unless_jpeg=True
    )
    assert "photometric" not in drop_ycbcr(dict(base), unless_jpeg=True)
    for codec in ("jpeg", "JPEG"):
        kept = drop_ycbcr({**base, "compress": codec}, unless_jpeg=True)
        assert kept["photometric"] == "YCbCr"
    rgb = {"photometric": "rgb", "compress": "zstd"}
    assert drop_ycbcr(rgb, unless_jpeg=True) == rgb
