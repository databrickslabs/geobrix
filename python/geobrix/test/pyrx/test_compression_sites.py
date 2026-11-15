"""Every site that writes a raster writes the canonical encoding.

``test_compression`` pins the canonical writer once (codec, level, predictor, alpha labels, the
YCbCr rule). Here each site gets one row: it must come out ZSTD with the predictor its dtype
takes, whatever codec its source had, and a JPEG/YCbCr source must not fail it. Each regression
a site once had (a copied source profile, BLOCKXSIZE noise, a no-codec name) keeps one table.
"""

from __future__ import annotations

import pathlib
from test._volume_safe import open_raster

import numpy as np
import pytest
import rasterio
from rasterio.enums import ColorInterp, Compression
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rasterio.windows import Window

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from ._compression_sources import (
    assert_canonical,
    assert_zstd_without_ycbcr,
    lzw_src,
    ycbcr_src,
)

# ---------------------------------------------------------------------------
# The canonical encoding at every site: ZSTD + the predictor of the output dtype
# ---------------------------------------------------------------------------


def _write_tif(tmp_path, dt="int16", sz=64) -> str:
    """Write a small single-band EPSG:4326 GeoTIFF and return the path."""
    p = str(tmp_path / f"{dt}.tif")
    rng = np.random.default_rng(seed=42)
    a = (rng.random((sz, sz)) * 1000).astype(dt)
    with open_raster(
        p,
        "w",
        driver="GTiff",
        height=sz,
        width=sz,
        count=1,
        dtype=dt,
        crs="EPSG:4326",
        transform=from_bounds(-1, -1, 1, 1, sz, sz),
    ) as d:
        d.write(a, 1)
    return p


def _second_tif(tmp_path, sz=32) -> bytes:
    """A non-overlapping int16 neighbour of ``_write_tif(tmp_path, "int16", sz=32)``."""
    p = str(tmp_path / "int16_b.tif")
    a = (np.random.default_rng(seed=99).random((sz, sz)) * 1000).astype("int16")
    with open_raster(
        p,
        "w",
        driver="GTiff",
        height=sz,
        width=sz,
        count=1,
        dtype="int16",
        crs="EPSG:4326",
        transform=from_bounds(0.0, -1.0, 2.0, 1.0, sz, sz),
    ) as d:
        d.write(a, 1)
    return pathlib.Path(p).read_bytes()


def _virtual_tile(tmp_path, dt):
    p = _write_tif(tmp_path, dt)
    with rasterio.open(p) as ds:
        sz = ds.width
    return VirtualTile(cellid=-1, raster=None, path=p, window=(0, 0, sz, sz))


def _on_src(tmp_path, dt, fn):
    with rasterio.open(_write_tif(tmp_path, dt)) as ds:
        return fn(ds)


def _on_lzw(fn):
    with MemoryFile(lzw_src()) as m, m.open() as ds:
        return fn(ds)


def _encode_gtiff(tmp_path, dt):
    from databricks.labs.gbx.ds._encode import encode_tile

    p = _write_tif(tmp_path, dt, sz=32)
    with rasterio.open(p) as ds:
        return encode_tile(ds, (0, 0, 32, 32), p, "", "auto", "gtiff")[1]


def _site_materialize(dt):
    return lambda tp: ot.materialize_to_bytes(_virtual_tile(tp, dt)).raster


def _site_window_dataset_bytes(tp):
    return _on_src(
        tp, "int16", lambda ds: ot._window_dataset_bytes(ds, Window(0, 0, 64, 64))
    )


def _site_warp_window_bytes(tp):
    return _on_src(
        tp,
        "float32",
        lambda ds: ot._warp_window_bytes(ds, Window(0, 0, 64, 64), want_epsg=3857),
    )


def _site_empty_dataset_bytes(tp):
    return _on_src(tp, "int16", ot._empty_dataset_bytes)


def _site_init_nodata(tp):
    from databricks.labs.gbx.pyrx.core import edit

    return _on_src(tp, "int16", edit.init_nodata)


def _site_merge_tiles(tp):
    from databricks.labs.gbx.pyrx.core.agg import merge_tiles

    first = pathlib.Path(_write_tif(tp, "int16", sz=32)).read_bytes()
    return merge_tiles([first, _second_tif(tp)])


def _site_retile(tp):
    from databricks.labs.gbx.pyrx.core.tiling import retile

    tiles = _on_src(tp, "int16", lambda ds: retile(ds, 32, 32))
    assert len(tiles) == 4
    return tiles


def _site_reproject(tp):
    from databricks.labs.gbx.pyrx.core.warp import reproject_to_srid

    return _on_src(tp, "float32", lambda ds: reproject_to_srid(ds, 3857))


def _site_proximity(tp):
    from databricks.labs.gbx.pyrx.core.analysis import proximity

    return _on_src(tp, "int16", lambda ds: proximity(ds, None, "PIXEL", None))


def _site_focal_filt(tp):
    from databricks.labs.gbx.pyrx.core import focal

    return _on_lzw(lambda ds: focal.filt(ds, 3, "max"))


def _site_focal_convolve(tp):
    from databricks.labs.gbx.pyrx.core import focal

    return _on_lzw(lambda ds: focal.convolve(ds, np.ones((3, 3)) / 9.0))


def _site_rasterize_geom(tp):
    import shapely
    from shapely.geometry import box

    from databricks.labs.gbx.pyrx.core import features

    poly = shapely.set_srid(box(1, 1, 3, 3), 4326)
    return features.rasterize_geom(shapely.to_wkb(poly), 5.0, 0, 0, 4, 4, 8, 8, 4326)


def _site_chm(tp):
    from databricks.labs.gbx.pyrx.core.chm import chm

    return chm(lzw_src(), lzw_src())


def _site_getsubdataset(tp):
    from types import SimpleNamespace

    from databricks.labs.gbx.pyrx.core import accessors

    sub = tp / "prAdjust.tif"
    sub.write_bytes(lzw_src())
    return accessors.getsubdataset(
        SimpleNamespace(subdatasets=[str(sub)]), "prAdjust.tif"
    )


def _site_bin_points(tp):
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    return bin_points(
        [0.5, 1.5], [0.5, 1.5], [1.0, 2.0], 0, 0, 2, 2, 2, 2, 32618, "max"
    )


def _site_idw_grid(tp):
    from databricks.labs.gbx.pyrx.core import tin

    pts = np.array([[1.0, 1.0], [3.0, 3.0]])
    return tin.idw_grid(pts, np.array([10.0, 20.0]), 0.0, 0.0, 4.0, 4.0, 4, 4, 32633)


def _site_percentile_stretch(tp):
    from databricks.labs.gbx.pyrx.core.stretch import percentile_stretch

    return _on_lzw(lambda ds: percentile_stretch(ds, 2.0, 98.0))


def _site_fromfile_materialize(tp):
    from databricks.labs.gbx.pyrx.functions import _fromfile_impl

    src = tp / "lzw.tif"
    src.write_bytes(lzw_src())
    tile = _fromfile_impl(str(src), "GTiff", True)
    assert tile is not None and tile["raster"] is not None
    return bytes(tile["raster"])


# (site, how it is called, the dtype of the raster it writes)
_CANONICAL_SITES = [
    ("materialize_to_bytes[int16]", _site_materialize("int16"), "int16"),
    ("materialize_to_bytes[float32]", _site_materialize("float32"), "float32"),
    ("materialize_to_bytes[uint8]", _site_materialize("uint8"), "uint8"),
    ("encode_tile[int16]", lambda tp: _encode_gtiff(tp, "int16"), "int16"),
    ("encode_tile[float32]", lambda tp: _encode_gtiff(tp, "float32"), "float32"),
    ("window_dataset_bytes", _site_window_dataset_bytes, "int16"),
    ("warp_window_bytes", _site_warp_window_bytes, "float32"),
    ("empty_dataset_bytes", _site_empty_dataset_bytes, "int16"),
    ("edit.init_nodata", _site_init_nodata, "int16"),
    ("agg.merge_tiles", _site_merge_tiles, "int16"),
    ("tiling.retile", _site_retile, "int16"),
    ("warp.reproject_to_srid", _site_reproject, "float32"),
    ("analysis.proximity", _site_proximity, "float32"),
    ("focal.filt", _site_focal_filt, "float32"),
    ("focal.convolve", _site_focal_convolve, "float64"),
    ("features.rasterize_geom", _site_rasterize_geom, "float64"),
    ("chm.chm", _site_chm, "float32"),
    ("accessors.getsubdataset", _site_getsubdataset, "float32"),
    ("binning.bin_points", _site_bin_points, "float32"),
    ("tin.idw_grid", _site_idw_grid, "float64"),
    ("stretch.percentile_stretch", _site_percentile_stretch, "uint8"),
    ("functions._fromfile_impl", _site_fromfile_materialize, "float32"),
]


@pytest.mark.parametrize(
    "call,dtype",
    [(c, d) for _, c, d in _CANONICAL_SITES],
    ids=[n for n, _, _ in _CANONICAL_SITES],
)
def test_a_site_writes_the_canonical_encoding(tmp_path, call, dtype):
    """ZSTD and the predictor of the dtype, whatever codec the source had (the LZW sources have
    none); a site that copied its source's profile, or kept a hand-written one, fails.
    """
    out = call(tmp_path)
    for tile in out if isinstance(out, list) else [out]:
        assert_canonical(tile, dtype)


def test_encode_tile_explicit_deflate_still_works(tmp_path):
    from databricks.labs.gbx.ds._encode import encode_tile

    p = _write_tif(tmp_path, "int16", sz=32)
    with rasterio.open(p) as ds:
        raster = encode_tile(ds, (0, 0, 32, 32), p, "", "DEFLATE", "gtiff")[1]
    with MemoryFile(raster) as mf, mf.open() as out_ds:
        assert out_ds.compression == Compression.deflate


def test_materialize_to_bytes_strips_the_pending_keys(tmp_path):
    mat = ot.materialize_to_bytes(_virtual_tile(tmp_path, "int16"))
    for k in ("pending_nodata", "pending_srid", "pending_bands"):
        assert k not in (mat.metadata or {}), f"pending key {k!r} leaked"


# ---------------------------------------------------------------------------
# tile_to_bytes wiring (fail-on-revert guards: ZSTD float and DEFLATE uint8 predictors)
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
    """Fail-on-revert: COG+ZSTD float32 must carry predictor=3.

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
            f"expected predictor=3 for float32 ZSTD COG, got {pred!r}. "
            "Reverting the _write.py fold removes this predictor."
        )


def test_tile_to_bytes_cog_deflate_uint8_predictor_1():
    """Fail-on-revert: COG+DEFLATE uint8 must not carry predictor=2.

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
        # pred == "2" means STANDARD differencing, which is wrong for uint8.
        assert pred is None or int(pred) == 1, (
            f"expected predictor=1 or None (no prediction) for uint8 DEFLATE COG, "
            f"got {pred!r}. Pre-fold code returned 2 for uint8 (non-float non-small-int branch)."
        )


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
    with MemoryFile(ycbcr_src()) as m, m.open() as ds:
        return fn(ds)


def _ycbcr_path(tmp_path):
    p = tmp_path / "ycbcr.tif"
    p.write_bytes(ycbcr_src())
    return str(p)


def _ycbcr_vtile(tmp_path):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    return VirtualTile(cellid=1, path=_ycbcr_path(tmp_path), window=(0, 0, 32, 32))


def _on_ycbcr_window(fn):
    """Call fn(ds, window) on an open YCbCr dataset and its top-left 32x32 window."""
    return _on_ycbcr(lambda ds: fn(ds, Window(0, 0, 32, 32)))


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

    return merge_tiles([ycbcr_src(), ycbcr_src(x0=500640)])


def _case_merge_tiles_mixed_crs(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import merge_tiles

    # Tiles in EPSG:32618 and 32619 either side of their zone boundary: the second
    # is reprojected onto the first's CRS before the merge.
    return merge_tiles([ycbcr_src(x0=750000), ycbcr_src(x0=250000, crs="EPSG:32619")])


def _case_combineavg_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import combineavg_tiles

    return combineavg_tiles([ycbcr_src(), ycbcr_src()])


def _case_combine_max_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import combine_max_tiles

    return combine_max_tiles([ycbcr_src(), ycbcr_src()])


def _case_frombands_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import frombands_tiles

    return frombands_tiles([(1, ycbcr_src()), (2, ycbcr_src())])


def _case_derivedband_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import derivedband_tiles

    return derivedband_tiles([ycbcr_src(), ycbcr_src()], _PIXEL_COPY, "f")


def _case_align_to_tiles(tmp_path):
    from databricks.labs.gbx.pyrx.core.agg import align_to_tiles

    return align_to_tiles(ycbcr_src(), ycbcr_src())


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
    return tile_to_bytes(0, ycbcr_src(), meta, force_driver="COG")


def _case_windowed_download(tmp_path):
    from databricks.labs.gbx.stac._download import windowed_download

    path = _ycbcr_path(tmp_path)
    out = windowed_download(
        path,
        str(tmp_path / "out.tif"),
        (500000.0, 4490000.0, 500640.0, 4490640.0),
    )
    return pathlib.Path(out).read_bytes()


def _case_percentile_stretch(tmp_path):
    from databricks.labs.gbx.pyrx.core.stretch import percentile_stretch

    return _on_ycbcr(lambda ds: percentile_stretch(ds, 2.0, 98.0))


def _case_focal_filt(tmp_path):
    from databricks.labs.gbx.pyrx.core import focal

    return _on_ycbcr(lambda ds: focal.filt(ds, 3, "max"))


def _case_fromfile_materialize(tmp_path):
    from databricks.labs.gbx.pyrx.functions import _fromfile_impl

    tile = _fromfile_impl(_ycbcr_path(tmp_path), "GTiff", True)
    assert tile is not None, "a YCbCr source must materialize, not null out"
    return bytes(tile["raster"])


def _case_simplify_raster(tmp_path):
    from databricks.labs.gbx.vizx._simplify import _simplify_raster

    return _simplify_raster(_ycbcr_path(tmp_path), {"raster_max_px": 64}, None)


def _case_apply_compression(tmp_path):
    from databricks.labs.gbx.ds.writer import _apply_compression

    return _apply_compression(ycbcr_src(), "auto", None, None)


# (case, bands in the result)
_YCBCR_OPS = [
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
    (_case_derivedband, 1),
    (_case_cog_convert, 3),
    (_case_encode_tile_gtiff, 3),
    (_case_encode_tile_cog, 3),
    (_case_tile_to_bytes_cog, 3),
    (_case_apply_compression, 3),
    (_case_windowed_download, 3),
    (_case_simplify_raster, 3),
    (_case_percentile_stretch, 3),
    (_case_focal_filt, 3),
    (_case_fromfile_materialize, 3),
]


@pytest.mark.parametrize(
    "case,bands", _YCBCR_OPS, ids=[c.__name__[6:] for c, _ in _YCBCR_OPS]
)
def test_ops_accept_a_ycbcr_jpeg_source(tmp_path, case, bands):
    """A JPEG/YCbCr source must re-encode to canonical ZSTD, not fail on PHOTOMETRIC."""
    out = case(tmp_path)
    assert out, "the op returned nothing"
    assert_zstd_without_ycbcr(bytes(out), count=bands)


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
    tiles = [ycbcr_src(seed=i) for i in range(n)]
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

    _assert_jpeg_ycbcr(_apply_compression(ycbcr_src(), "jpeg", None, None))


# ---------------------------------------------------------------------------
# Centroid tessellate chips (cell chips and synthetic empties) use the canonical
# encoding whatever the source's codec is
# ---------------------------------------------------------------------------


def _dem(compress="lzw", nodata=-999999.0):
    rng = np.random.default_rng(7)
    a = (rng.random((72, 96)) * 300).astype("float32")
    a[:, :20] = nodata if nodata is not None else a[:, :20]
    px = 0.17 / 1836
    prof = dict(
        driver="GTiff",
        width=96,
        height=72,
        count=1,
        dtype="float32",
        crs="EPSG:4269",
        transform=from_bounds(
            -122.52, 37.83 - 72 * px, -122.52 + 96 * px, 37.83, 96, 72
        ),
        tiled=True,
        blockxsize=32,
        blockysize=32,
        nodata=nodata,
        **({"compress": compress} if compress else {}),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a, 1)
        return mf.read()


@pytest.mark.parametrize("src_compress", ["lzw", None])
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
def test_centroid_tessellate_chips_use_creation_opts(src_compress, coverage):
    from databricks.labs.gbx.pyrx.core import tessellate as T

    # Res 10: the 20-column NoData strip is wider than a cell there, so "complete"
    # also emits synthetic empty chips (at res 9 every covered cell gets a pixel).
    with MemoryFile(_dem(src_compress)) as mf, mf.open() as ds:
        chips = list(
            T.iter_tessellate(ds, 10, "h3", mode="centroid", coverage=coverage)
        )
    assert chips
    empties = 0
    for _, b in chips:
        assert_canonical(b, "float32")
        with MemoryFile(b) as m, m.open() as c:
            empties += int(not (c.read_masks() != 0).any())
    if coverage == "complete":
        assert empties, "the NoData strip must yield synthetic empty chips"


# ---------------------------------------------------------------------------
# A striped source's profile copied into a new GTiff must not log BLOCKXSIZE warnings
# ---------------------------------------------------------------------------


def _striped_source(count=1, alpha=False) -> bytes:
    """A 64x64 uint8 GTiff written striped: no ``tiled=True``, so its profile carries ``blockxsize``."""
    profile = dict(
        driver="GTiff",
        width=64,
        height=64,
        count=count,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(0, 0, 1, 1, 64, 64),
    )
    data = np.arange(count * 64 * 64, dtype="uint8").reshape(count, 64, 64) % 251
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
            if alpha:
                dst.colorinterp = [
                    ColorInterp.red,
                    ColorInterp.green,
                    ColorInterp.blue,
                    ColorInterp.alpha,
                ][:count]
        return mf.read()


def _striped_path(tmp_path, **kw) -> str:
    p = tmp_path / "striped.tif"
    p.write_bytes(_striped_source(**kw))
    return str(p)


# site -> (how it takes its source, call). "ds" is an open dataset, "bytes" the file's bytes and
# "path" a file on disk; ``tmp_path`` is for sites that write beside their input.
def _striped_sites():
    from databricks.labs.gbx.bench import grouped_file, spec, synth
    from databricks.labs.gbx.ds import _write, _xyz_mosaic
    from databricks.labs.gbx.pyrx import functions as F
    from databricks.labs.gbx.pyrx.core import gridagg, ops, tiling, warp
    from databricks.labs.gbx.stac import _download
    from databricks.labs.gbx.vizx import _simplify

    return {
        "reproject_to_srid": ("ds", lambda ds, tp: warp.reproject_to_srid(ds, 3857)),
        "build_overviews": ("ds", lambda ds, tp: ops.build_overviews(ds, [2])),
        "dataset_to_gtiff_bytes": ("ds", lambda ds, tp: F._dataset_to_gtiff_bytes(ds)),
        "stamp_crs_bytes": (
            "ds",
            lambda ds, tp: gridagg._stamp_crs_bytes(ds, "EPSG:3857"),
        ),
        "encoded_size_bytes": ("ds", lambda ds, tp: tiling._encoded_size_bytes(ds)),
        "as_format_gtiff": ("ds", lambda ds, tp: ops.as_format(ds, "GTiff")),
        "tile_to_bytes_cog": (
            "bytes",
            lambda b, tp: _write.tile_to_bytes(0, b, {"compression": "ZSTD"}, "COG"),
        ),
        "windowed_download": (
            "path",
            lambda p, tp: _download.windowed_download(
                p, str(tp / "out.tif"), (0.0, 0.0, 0.5, 0.5)
            ),
        ),
        "simplify_raster": (
            "path",
            lambda p, tp: _simplify._simplify_raster(p, {"raster_max_px": 32}, None),
        ),
        "bench_synth_combineavg": (
            "path",
            lambda p, tp: synth.synthesize(p, "combineavg", str(tp / "synth")),
        ),
        "bench_window_gtiff_bytes": (
            "path",
            lambda p, tp: grouped_file._window_gtiff_bytes(p, (0, 0, 32, 32)),
        ),
        "bench_synthetic_rgb_tile": ("ds", lambda ds, tp: spec._synthetic_rgb_tile(ds)),
        "bench_ds_to_gtiff_bytes": ("ds", lambda ds, tp: spec._ds_to_gtiff_bytes(ds)),
        "to_render_rgb": (
            "rgba-path",
            lambda p, tp: _xyz_mosaic.to_render_rgb(p),
        ),
    }


@pytest.mark.parametrize(
    "op",
    [
        "reproject_to_srid",
        "build_overviews",
        "dataset_to_gtiff_bytes",
        "stamp_crs_bytes",
        "encoded_size_bytes",
        "as_format_gtiff",
        "tile_to_bytes_cog",
        "windowed_download",
        "simplify_raster",
        "bench_synth_combineavg",
        "bench_window_gtiff_bytes",
        "bench_synthetic_rgb_tile",
        "bench_ds_to_gtiff_bytes",
        "to_render_rgb",
    ],
)
def test_a_striped_source_logs_no_blockxsize_warning(op, caplog, tmp_path):
    """A striped GeoTIFF's profile carries ``blockxsize`` with ``tiled`` False; written to a new
    GTiff GDAL logs "BLOCKXSIZE can only be used with TILED=YES" for each dataset."""
    import logging

    kind, call = _striped_sites()[op]
    if kind == "rgba-path":
        arg = _striped_path(tmp_path, count=4, alpha=True)
    else:
        arg = _striped_path(tmp_path) if kind == "path" else _striped_source()
    with MemoryFile(_striped_source()) as mf, mf.open() as ds:
        assert not ds.profile["tiled"] and ds.profile.get("blockxsize")
        if kind == "ds":
            arg = ds
        with caplog.at_level(logging.WARNING):
            assert call(arg, tmp_path)
    noisy = [r.getMessage() for r in caplog.records if "BLOCKXSIZE" in r.getMessage()]
    assert noisy == [], noisy


# ---------------------------------------------------------------------------
# The codec names that mean "no compression" write none at every site that accepts them
# ---------------------------------------------------------------------------


def _deflate_tiled_src(dtype="uint16", sz=64) -> bytes:
    """A tiled DEFLATE GTiff: a codec to inherit by mistake, and not the COG driver's default (LZW)."""
    a = (np.arange(sz * sz, dtype="int64").reshape(1, sz, sz) % 251).astype(dtype)
    prof = dict(
        driver="GTiff",
        width=sz,
        height=sz,
        count=1,
        dtype=dtype,
        crs="EPSG:32630",
        transform=from_bounds(500000, 5500000, 500640, 5500640, sz, sz),
        compress="deflate",
        tiled=True,
        blockxsize=16,
        blockysize=16,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as d:
            d.write(a)
        return mf.read()


def _assert_no_codec(b: bytes, *, context: str) -> None:
    with MemoryFile(bytes(b)) as m, m.open() as ds:
        img = ds.tags(ns="IMAGE_STRUCTURE")
        assert "COMPRESSION" not in img, f"{context}: {img}"
        assert ds.compression in (
            None,
            Compression.none,
        ), f"{context}: {ds.compression}"


def _no_codec_cog_convert(name, tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert

    with MemoryFile(_deflate_tiled_src()) as m, m.open() as ds:
        return cog_convert(ds, name, 16, "AVERAGE")


def _no_codec_cog_convert_file(name, tmp_path):
    from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

    src, dst = tmp_path / "src.tif", tmp_path / "dst.tif"
    src.write_bytes(_deflate_tiled_src())
    cog_convert_file(str(src), str(dst), name, 16, "AVERAGE")
    return dst.read_bytes()


def _no_codec_encode_tile(fmt):
    def run(name, tmp_path):
        from databricks.labs.gbx.ds._encode import encode_tile

        src = tmp_path / "src.tif"
        src.write_bytes(_deflate_tiled_src())
        with rasterio.open(src) as ds:
            return encode_tile(ds, (0, 0, 64, 64), str(src), "", name, fmt, 16)[1]

    return run


def _no_codec_tile_to_bytes_cog(name, tmp_path):
    from databricks.labs.gbx.ds._write import tile_to_bytes

    return tile_to_bytes(
        0, _deflate_tiled_src(), {"compression": name}, force_driver="COG"
    )


def _no_codec_apply_compression(name, tmp_path):
    from databricks.labs.gbx.ds.writer import _apply_compression

    return _apply_compression(_deflate_tiled_src(), name, None, None)


# the spellings of "no compression" (the canonical module pins every one, in either case) at every site
_NO_CODEC_NAMES = ["raw", "NONE"]
_NO_CODEC_SITES = {
    "cog_convert": _no_codec_cog_convert,
    "cog_convert_file": _no_codec_cog_convert_file,
    "encode_tile_cog": _no_codec_encode_tile("cog"),
    "encode_tile_gtiff": _no_codec_encode_tile("gtiff"),
    "tile_to_bytes_cog": _no_codec_tile_to_bytes_cog,
    "apply_compression": _no_codec_apply_compression,
}


@pytest.mark.parametrize(
    "site,name",
    [(s, n) for s in _NO_CODEC_SITES for n in _NO_CODEC_NAMES],
)
def test_a_no_codec_name_writes_no_compression(site, name, tmp_path):
    """Read back ``COMPRESSION``: absent. The COG driver gave LZW for an omitted COMPRESS, and a
    site that starts from the source's profile kept the source's DEFLATE."""
    out = _NO_CODEC_SITES[site](name, tmp_path)
    _assert_no_codec(out, context=f"{site}({name})")
