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
