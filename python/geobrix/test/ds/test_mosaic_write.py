"""Mosaic-mode write tests: Phase A (native tiling) + quadbin/h3 tiling.

Tests the mosaic-mode write path in CogGbxWriter.  Pure Python (no Spark, no JAR).

Coverage:
  Phase A (native pixel tiling):
    1. Non-overlapping tile grid covers the full source extent.
    2. Each tile's pixels are pixel-equal to the corresponding source region.
    3. Each mini-COG is a valid COG (internally-tiled).
    4. pruneEmpty=True drops all-nodata edge tiles.
    5. overlapPercent>0 produces halo-expanded tile extents.
    6. MosaicOptions pickle round-trip (dataclass survives serialization to worker).
    7. Single-COG mode regression (mosaic_opts=None, existing path unchanged).
    8. Multiple source rows produce correctly-named tiles per source.

  Quadbin (per-cell reproject):
    14. gridSystem=quadbin → N≥2 mini-COGs, each in EPSG:3857, each tagged
        GBX_CELLID and GBX_GRIDSYSTEM=quadbin.
    15. Cell pixel values match a reference rasterio.warp.reproject (allclose).

  H3 (per-cell reproject + hex-clip):
    16. gridSystem=h3 → ≥1 mini-COGs, each in EPSG:4326, each tagged
        GBX_CELLID (valid h3index at the requested resolution) and GBX_GRIDSYSTEM=h3.
    17. Pixels outside the hexagon boundary are set to nodata; interior pixels
        are non-nodata.  Nodata is always present even when the source had none
        (derived sentinel — Ruling A).
    18. Interior pixels match a reference rasterio.warp.reproject (allclose, not
        byte-equal since nearest resampling and windowed source may vary slightly).

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/ds/test_mosaic_write.py \
        --log mosaic-write.log
"""

from __future__ import annotations

import math
import os
import pickle
import re

import numpy as np
import pytest
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin
from rasterio.warp import Resampling, reproject

from databricks.labs.gbx.ds import _listing
from databricks.labs.gbx.ds.cog_writer import (
    CogCommitMessage,
    CogGbxWriter,
    MosaicOptions,
    _is_all_nodata,
    _source_discriminator,
    _tile_grid_windows,
    parse_mosaic_options,
)
from databricks.labs.gbx.pyrx.core import cog as gbxcog

_TILE_NAME_RE = re.compile(r"^tile_([0-9A-Za-z]+)_(\d+)_(\d+)\.tif$")


def _tname(src_path: str, r: int, c: int) -> str:
    """Expected mini-COG filename for source *src_path* at grid cell (r, c).

    Mirrors CogGbxWriter._write_mosaic naming: the writer applies
    ``_listing.to_local_path`` to the row path before hashing, so tests must do
    the same to reproduce the discriminator.
    """
    disc = _source_discriminator(_listing.to_local_path(str(src_path)))
    return f"tile_{disc}_{r}_{c}.tif"


def _row_col(tile_path: str) -> tuple:
    """Parse (row, col) from a tile filename — the final two ``_`` tokens."""
    name = os.path.basename(tile_path)
    m = _TILE_NAME_RE.match(name)
    assert m, f"unexpected tile name: {name}"
    return int(m.group(2)), int(m.group(3))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _path_schema() -> StructType:
    return StructType([StructField("path", StringType(), False)])


def _write_src(
    path: str,
    w: int = 200,
    h: int = 120,
    count: int = 1,
    dtype: str = "uint16",
    nodata=None,
) -> None:
    """Write a small striped GTiff to *path* with deterministic pixel values.

    Pixel value = (row * w + col) % 65535 for dtype=uint16, so every pixel is
    unique and easily verified against a re-read of the source region.
    """
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=count,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
    )
    if nodata is not None:
        profile["nodata"] = nodata
    data = np.arange(w * h, dtype=dtype).reshape(1, h, w) % np.iinfo(dtype).max
    if count > 1:
        data = np.stack([data[0] + b * 1000 for b in range(count)])[np.newaxis]
        data = data.squeeze(0)  # (count, h, w)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _make_writer(out_dir: str, mosaic_opts: MosaicOptions = None) -> CogGbxWriter:
    """Construct a CogGbxWriter with the given mosaic_opts (no Spark needed)."""
    return CogGbxWriter(
        str(out_dir),
        _path_schema(),
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=mosaic_opts,
    )


def _default_mosaic_opts(**kwargs) -> MosaicOptions:
    """Return MosaicOptions(gridSystem='none', ...) from keyword overrides."""
    base = {
        "vrtMosaic": "true",
        "gridSystem": "none",
    }
    base.update(kwargs)
    return parse_mosaic_options(base)


# ---------------------------------------------------------------------------
# 1. Non-overlapping grid covers the full extent
# ---------------------------------------------------------------------------


def test_tile_grid_covers_full_extent():
    """A 200x120 source with tileSize=100 produces a 2x2 grid covering the full extent."""
    windows = list(_tile_grid_windows(200, 120, 100, 0.0))
    assert len(windows) == 4, f"expected 4 tiles for 200x120 / 100, got {len(windows)}"
    # Row/col indices
    row_cols = [(r, c) for r, c, _ in windows]
    assert (0, 0) in row_cols
    assert (0, 1) in row_cols
    assert (1, 0) in row_cols
    assert (1, 1) in row_cols
    # Union of windows == full extent [0..200) x [0..120)
    all_x = set()
    all_y = set()
    for _, _, w in windows:
        for x in range(int(w.col_off), int(w.col_off) + int(w.width)):
            all_x.add(x)
        for y in range(int(w.row_off), int(w.row_off) + int(w.height)):
            all_y.add(y)
    assert all_x == set(range(200)), "tiles do not cover all columns"
    assert all_y == set(range(120)), "tiles do not cover all rows"


def test_tile_grid_non_overlapping_windows_do_not_share_pixels():
    """Non-overlapping tiles (overlapPercent=0) have disjoint pixel regions."""
    windows = list(_tile_grid_windows(100, 80, 50, 0.0))
    assert len(windows) == 4
    seen = set()
    for _, _, w in windows:
        for x in range(int(w.col_off), int(w.col_off) + int(w.width)):
            for y in range(int(w.row_off), int(w.row_off) + int(w.height)):
                assert (x, y) not in seen, f"pixel ({x},{y}) covered by multiple tiles"
                seen.add((x, y))
    assert seen == {(x, y) for x in range(100) for y in range(80)}


def test_write_mosaic_produces_correct_tile_count(tmp_path):
    """200x120 source with tileSize=100 → 4 mini-COGs (2 rows × 2 cols)."""
    src = tmp_path / "src" / "input.tif"
    _write_src(str(src), w=200, h=120)
    opts = _default_mosaic_opts(tileSize=100)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": str(src)}]))

    assert isinstance(msg, CogCommitMessage)
    tifs = [p for p in msg.paths if p.endswith(".tif")]
    assert len(tifs) == 4, f"expected 4 mini-COGs, got {len(tifs)}: {tifs}"
    for p in tifs:
        assert os.path.exists(p), f"mini-COG not on disk: {p}"


def test_write_mosaic_tile_naming(tmp_path):
    """Mini-COGs follow tile_<row>_<col>.tif naming."""
    src = tmp_path / "src" / "input.tif"
    _write_src(str(src), w=200, h=120)
    opts = _default_mosaic_opts(tileSize=100)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": str(src)}]))

    names = {os.path.basename(p) for p in msg.paths}
    # Tile names carry a per-source discriminator: tile_<disc>_<row>_<col>.tif.
    expected = {_tname(str(src), r, c) for r in (0, 1) for c in (0, 1)}
    assert names == expected, f"unexpected tile names: {names}"
    # A single source yields exactly one discriminator across all its tiles.
    discs = {_TILE_NAME_RE.match(n).group(1) for n in names}
    assert len(discs) == 1, f"single source must use one discriminator: {discs}"


# ---------------------------------------------------------------------------
# 2. Pixel fidelity — tile pixels == source region pixels
# ---------------------------------------------------------------------------


def test_write_mosaic_pixel_equality(tmp_path):
    """Each mini-COG's pixels match the corresponding source region exactly."""
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=200, h=120, dtype="uint16")
    opts = _default_mosaic_opts(tileSize=100)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    with rasterio.open(src_path) as src:
        for tile_path in msg.paths:
            # Parse row/col from tile_<disc>_<row>_<col>.tif (final two tokens).
            tile_row, tile_col = _row_col(tile_path)

            # Reconstruct the window used for this tile.
            windows = {
                (r, c): win
                for r, c, win in _tile_grid_windows(src.width, src.height, 100, 0.0)
            }
            win = windows[(tile_row, tile_col)]
            expected_data = src.read(window=win)

            with rasterio.open(tile_path) as tile_ds:
                actual_data = tile_ds.read()

            np.testing.assert_array_equal(
                actual_data,
                expected_data,
                err_msg=f"pixel mismatch in {os.path.basename(tile_path)}",
            )


# ---------------------------------------------------------------------------
# 3. Valid COG output
# ---------------------------------------------------------------------------


def test_write_mosaic_produces_valid_cogs(tmp_path):
    """Every mini-COG produced by mosaic write must be internally tiled (COG format).

    Uses a 600×400 source with tileSize=200 so each 200×200 tile is large
    enough for GDAL's COG driver to build at least one overview level, making
    sniff_header.is_cog=True.  For smaller tiles GDAL may skip overviews; in
    that case we fall back to asserting tiled=True (the COG driver was used).
    """
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=600, h=400)
    # cog_blocksize=128 ensures the 200×200 tiles trigger overview generation.
    writer = CogGbxWriter(
        str(tmp_path / "out"),
        _path_schema(),
        overwrite=True,
        cog_blocksize=128,
        mosaic_opts=_default_mosaic_opts(tileSize=200),
    )
    msg = writer.write(iter([{"path": src_path}]))

    for tile_path in msg.paths:
        with open(tile_path, "rb") as fh:
            info = gbxcog.sniff_header(fh.read())
        # At minimum the output must be internally tiled (COG driver was used).
        assert info.tiled, f"{os.path.basename(tile_path)} is not internally tiled"


# ---------------------------------------------------------------------------
# 4. pruneEmpty=True drops all-nodata tiles
# ---------------------------------------------------------------------------


def _write_src_with_nodata_patch(
    path: str, w: int = 200, h: int = 120, nodata: float = 0.0
) -> None:
    """Write a raster where the bottom-right 100x20 block is all nodata."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
        nodata=nodata,
    )
    data = np.ones((1, h, w), dtype="float32") * 99.0
    # Fill bottom-right tile (rows 100..120, cols 100..200) with nodata.
    data[0, 100:, 100:] = nodata
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def test_prune_empty_drops_all_nodata_tile(tmp_path):
    """pruneEmpty=True (default): a fully-nodata tile is not written."""
    src_path = str(tmp_path / "src" / "nodata.tif")
    _write_src_with_nodata_patch(src_path, w=200, h=120, nodata=0.0)
    opts = _default_mosaic_opts(tileSize=100, pruneEmpty="true")
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    names = {os.path.basename(p) for p in msg.paths}
    # tile_1_1 covers rows 100..120, cols 100..200 → all nodata → pruned.
    assert (
        _tname(src_path, 1, 1) not in names
    ), "all-nodata tile_1_1 should have been pruned by pruneEmpty=True"
    # The other 3 tiles should be present.
    for r, c in ((0, 0), (0, 1), (1, 0)):
        assert (
            _tname(src_path, r, c) in names
        ), f"tile_{r}_{c} should be present (has non-nodata pixels)"


def test_prune_empty_false_writes_nodata_tile(tmp_path):
    """pruneEmpty=False: all-nodata tile is written (not pruned)."""
    src_path = str(tmp_path / "src" / "nodata.tif")
    _write_src_with_nodata_patch(src_path, w=200, h=120, nodata=0.0)
    opts = _default_mosaic_opts(tileSize=100, pruneEmpty="false")
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    names = {os.path.basename(p) for p in msg.paths}
    assert (
        _tname(src_path, 1, 1) in names
    ), "all-nodata tile_1_1 should be written when pruneEmpty=False"


# ---------------------------------------------------------------------------
# 5. overlapPercent > 0 produces halo-expanded tile extents
# ---------------------------------------------------------------------------


def test_tile_grid_overlap_expands_extents():
    """overlapPercent=10 expands each tile's extent beyond the non-overlap version."""
    no_halo = list(_tile_grid_windows(200, 120, 100, 0.0))
    with_halo = list(_tile_grid_windows(200, 120, 100, 10.0))

    assert len(no_halo) == len(with_halo), "halo must not change tile count"

    # At least one interior tile should be wider/taller with halo.
    expanded = any(
        (wh.width > wn.width or wh.height > wn.height)
        for (_, _, wn), (_, _, wh) in zip(no_halo, with_halo)
    )
    assert expanded, "overlapPercent=10 must expand at least one tile's window"


def test_write_mosaic_with_overlap_produces_larger_tiles(tmp_path):
    """Mini-COGs with overlapPercent=10 are larger (in pixels) than without overlap."""
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=200, h=120)

    # No overlap
    out_no = tmp_path / "no_overlap"
    opts_no = _default_mosaic_opts(tileSize=100, overlapPercent="0")
    w_no = _make_writer(str(out_no), mosaic_opts=opts_no)
    msg_no = w_no.write(iter([{"path": src_path}]))

    # With 10 % overlap
    out_halo = tmp_path / "with_overlap"
    opts_halo = _default_mosaic_opts(tileSize=100, overlapPercent="10")
    w_halo = _make_writer(str(out_halo), mosaic_opts=opts_halo)
    msg_halo = w_halo.write(iter([{"path": src_path}]))

    # Pick an interior tile — tile_0_0 is a corner, tile_0_1 has halo on left and right.
    def _get_dims(paths, name):
        for p in paths:
            if os.path.basename(p) == name:
                with rasterio.open(p) as ds:
                    return ds.width, ds.height
        return None

    # tile_1_0 is corner-bottom-left; tile_0_1 has a left halo (interior edge).
    # For a 200x120 src with tileSize=100: tile_0_1 covers cols 100..200 (right edge).
    # With halo, it expands leftward into cols ~90..200 → wider.
    tile_0_1 = _tname(src_path, 0, 1)
    dims_no = _get_dims(msg_no.paths, tile_0_1)
    dims_halo = _get_dims(msg_halo.paths, tile_0_1)
    assert dims_no is not None and dims_halo is not None
    assert (
        dims_halo[0] > dims_no[0] or dims_halo[1] > dims_no[1]
    ), f"halo tile ({dims_halo}) should be wider/taller than no-halo tile ({dims_no})"


# ---------------------------------------------------------------------------
# 6. MosaicOptions pickle round-trip
# ---------------------------------------------------------------------------


def test_mosaic_options_pickle_roundtrip():
    """MosaicOptions survives pickle serialization (rides on writer to worker)."""
    opts = parse_mosaic_options(
        {
            "vrtMosaic": "true",
            "gridSystem": "none",
            "tileSize": "256",
            "overlapPercent": "5.0",
            "pruneEmpty": "false",
            "writeVrt": "false",
            "vrtPaths": "absolute",
        }
    )
    assert opts is not None
    data = pickle.dumps(opts)
    restored = pickle.loads(data)
    assert restored == opts, f"pickle round-trip changed opts: {restored!r} != {opts!r}"
    assert restored.tile_size == 256
    assert restored.overlap_percent == 5.0
    assert restored.prune_empty is False
    assert restored.write_vrt is False
    assert restored.vrt_paths == "absolute"


def test_cog_gbx_writer_with_mosaic_opts_pickles(tmp_path):
    """CogGbxWriter with mosaic_opts set is pickle-serializable (worker-safe)."""
    opts = _default_mosaic_opts(tileSize=512)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    data = pickle.dumps(w)
    restored = pickle.loads(data)
    assert restored.mosaic_opts is not None
    assert restored.mosaic_opts.tile_size == 512


# ---------------------------------------------------------------------------
# 7. Single-COG mode regression
# ---------------------------------------------------------------------------


def test_single_cog_mode_unchanged(tmp_path):
    """mosaic_opts=None (default): single-COG path produces one internally-tiled file."""
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=100, h=80)
    # No mosaic_opts → single-COG mode.
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=None)
    msg = w.write(iter([{"path": src_path}]))

    assert isinstance(msg, CogCommitMessage)
    assert (
        len(msg.paths) == 1
    ), f"single-COG mode must produce exactly 1 output, got {msg.paths}"
    name = os.path.basename(msg.paths[0])
    assert not name.startswith(
        "tile_"
    ), f"single-COG must not use tile_* naming: {name}"
    assert os.path.exists(msg.paths[0])
    with open(msg.paths[0], "rb") as fh:
        info = gbxcog.sniff_header(fh.read())
    # At minimum the output must be internally tiled.  For small rasters
    # (< blocksize) GDAL may skip overview generation (is_cog requires >= 1
    # overview), but tiled=True confirms the COG driver was used.
    assert info.tiled, "single-COG output must be internally tiled (COG driver)"


# ---------------------------------------------------------------------------
# 8. CogCommitMessage carries mini-COG paths for Task 3
# ---------------------------------------------------------------------------


def test_commit_message_paths_are_mini_cog_paths(tmp_path):
    """CogCommitMessage.paths holds all written mini-COG paths for Task 3 VRT."""
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=200, h=120)
    opts = _default_mosaic_opts(tileSize=100)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    assert isinstance(msg, CogCommitMessage)
    assert len(msg.paths) == 4
    # pending_paths must be empty (mosaic write does not use the driver-side COG path).
    assert msg.pending_paths == [], "mosaic mode must not populate pending_paths"
    # Every path in msg.paths must be an existing .tif file.
    for p in msg.paths:
        assert p.endswith(".tif") and os.path.exists(p), f"stale path in message: {p}"


# ---------------------------------------------------------------------------
# 9. Partial-coverage edge tiles (non-divisible source dimensions)
# ---------------------------------------------------------------------------


def test_tile_grid_non_divisible_dimensions():
    """Non-divisible source dims produce correct edge tiles without gaps."""
    # 210 x 130 with tileSize=100 → 3 cols (100, 100, 10) × 2 rows (100, 30)
    windows = list(_tile_grid_windows(210, 130, 100, 0.0))
    assert len(windows) == 6, f"expected 6 tiles, got {len(windows)}"
    # No pixel gaps
    covered_cols = set()
    covered_rows = set()
    for _, _, win in windows:
        covered_cols.update(range(int(win.col_off), int(win.col_off) + int(win.width)))
        covered_rows.update(range(int(win.row_off), int(win.row_off) + int(win.height)))
    assert covered_cols == set(range(210)), "non-divisible source has column gap"
    assert covered_rows == set(range(130)), "non-divisible source has row gap"


def test_write_mosaic_non_divisible_source(tmp_path):
    """210 x 130 source with tileSize=100 → 6 tiles, all on disk."""
    src_path = str(tmp_path / "src" / "nondiv.tif")
    _write_src(src_path, w=210, h=130)
    opts = _default_mosaic_opts(tileSize=100)
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    assert len(msg.paths) == 6, f"expected 6 mini-COGs, got {len(msg.paths)}"


# ---------------------------------------------------------------------------
# 10. _is_all_nodata helper unit tests
# ---------------------------------------------------------------------------


def test_is_all_nodata_true():
    data = np.full((1, 4, 4), -9999.0, dtype="float32")
    assert _is_all_nodata(data, -9999.0)


def test_is_all_nodata_false_mixed():
    data = np.ones((1, 4, 4), dtype="float32")
    data[0, 0, 0] = 42.0
    assert not _is_all_nodata(data, -9999.0)


def test_is_all_nodata_none_nodata():
    data = np.zeros((1, 4, 4), dtype="float32")
    assert not _is_all_nodata(data, None), "None nodata must never prune"


def test_is_all_nodata_nan():
    data = np.full((1, 2, 2), float("nan"), dtype="float32")
    assert _is_all_nodata(data, float("nan"))


def test_is_all_nodata_nan_mixed():
    data = np.array([[[1.0, float("nan")]]], dtype="float32")
    assert not _is_all_nodata(data, float("nan"))


# ---------------------------------------------------------------------------
# 11. FIX 1 — two sources in one write must NOT collide (silent data loss)
# ---------------------------------------------------------------------------


def _write_src_at(path: str, origin_x: float, w: int = 200, h: int = 120) -> None:
    """Write a striped uint16 GTiff positioned at *origin_x* (distinct extent)."""
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint16",
        crs="EPSG:32632",
        transform=from_origin(origin_x, 5000000.0, 10.0, 10.0),
    )
    data = np.arange(w * h, dtype="uint16").reshape(1, h, w) % np.iinfo("uint16").max
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def test_write_mosaic_two_sources_no_collision(tmp_path):
    """Two distinct sources in ONE partition/write must not clobber each other.

    Both sources are 200x120 with tileSize=100 → 4 tiles each.  Without a
    per-source discriminator the two would write identical tile_<r>_<c>.tif names
    into the shared out_dir — cog_skip_if_exists would skip the second source's
    tiles, silently losing its data and leaving a VRT that presents source A as
    the whole mosaic.  With the discriminator all 8 tiles are distinct.
    """
    src_a = str(tmp_path / "a" / "input.tif")
    src_b = str(tmp_path / "b" / "input.tif")  # same basename, different dir
    _write_src_at(src_a, origin_x=400000.0)
    _write_src_at(src_b, origin_x=402000.0)  # spatially to the right of A

    out_dir = str(tmp_path / "out")
    w = _make_writer(out_dir, mosaic_opts=_default_mosaic_opts(tileSize=100))
    # One partition, two source rows (coalesce(1) equivalent).
    msg = w.write(iter([{"path": src_a}, {"path": src_b}]))

    # 8 distinct tiles (4 per source), all on disk — no clobber.
    assert len(msg.paths) == 8, f"expected 8 tiles (4 per source), got {msg.paths}"
    assert len(set(msg.paths)) == 8, "tile paths must be unique across sources"
    for p in msg.paths:
        assert os.path.exists(p), f"tile missing on disk (clobbered?): {p}"

    # Two distinct discriminators — one per source.
    discs = {_TILE_NAME_RE.match(os.path.basename(p)).group(1) for p in msg.paths}
    assert len(discs) == 2, f"expected 2 source discriminators, got {discs}"
    assert _source_discriminator(_listing.to_local_path(src_a)) in discs
    assert _source_discriminator(_listing.to_local_path(src_b)) in discs

    # commit() builds a VRT referencing BOTH sources' members.
    w.commit([msg])
    vrt = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt), "commit() must write mosaic.vrt"
    import xml.etree.ElementTree as ET

    members = {
        os.path.basename((sf.text or "").strip())
        for sf in ET.parse(vrt).getroot().iter("SourceFilename")
    }
    assert members == {os.path.basename(p) for p in msg.paths}, (
        "VRT must reference every tile from both sources; "
        f"vrt={sorted(members)} written={sorted(os.path.basename(p) for p in msg.paths)}"
    )

    # Round-trip: every referenced member is a valid, readable raster.
    for name in members:
        with rasterio.open(os.path.join(out_dir, name)) as ds:
            assert ds.read().size > 0


# ---------------------------------------------------------------------------
# 12. FIX 3 — tiled/COG source with partial-edge tiles < internal block size
# ---------------------------------------------------------------------------


def _write_tiled_src(path: str, w: int, h: int, block: int = 512) -> None:
    """Write an internally-tiled GTiff whose profile carries block dims == *block*.

    A COG / tiled source's profile advertises tiled=True + blockxsize/blockysize.
    When _write_mosaic copies that profile to write an intermediate GTiff for a
    partial-edge tile smaller than the block, the carried-over block dims are the
    defect FIX 3 normalizes away.
    """
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint16",
        crs="EPSG:32632",
        transform=from_origin(400000.0, 5000000.0, 10.0, 10.0),
        tiled=True,
        blockxsize=block,
        blockysize=block,
    )
    data = np.arange(w * h, dtype="uint16").reshape(1, h, w) % np.iinfo("uint16").max
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def test_write_mosaic_tiled_source_partial_edge_tiles(tmp_path):
    """A 700x600 tiled(512) source with tileSize=512 → partial-edge tiles < 512.

    Grid: cols 512+188, rows 512+88 → 4 tiles, three with an edge dim < 512.
    The source profile carries blockxsize/blockysize=512; those must be dropped
    before writing each partial-edge tile's intermediate GTiff.  All 4 tiles must
    be written, valid, and pixel-equal to their source windows.
    """
    src_path = str(tmp_path / "src" / "tiled.tif")
    _write_tiled_src(src_path, w=700, h=600, block=512)

    # Precondition: the fixture really carries the conflicting block dims.
    with rasterio.open(src_path) as ds:
        assert ds.profile.get("blockxsize") == 512, "fixture must be block-512 tiled"

    out_dir = str(tmp_path / "out")
    w = _make_writer(out_dir, mosaic_opts=_default_mosaic_opts(tileSize=512))
    msg = w.write(iter([{"path": src_path}]))

    # 2x2 grid → 4 tiles, all written (no crash on partial-edge blocks).
    assert len(msg.paths) == 4, f"expected 4 tiles, got {len(msg.paths)}: {msg.paths}"

    windows = {(r, c): win for r, c, win in _tile_grid_windows(700, 600, 512, 0.0)}
    with rasterio.open(src_path) as src:
        for tile_path in msg.paths:
            assert os.path.exists(tile_path)
            r, c = _row_col(tile_path)
            expected = src.read(window=windows[(r, c)])
            with rasterio.open(tile_path) as tds:
                actual = tds.read()
            np.testing.assert_array_equal(
                actual, expected, err_msg=f"pixel mismatch in tile {r},{c}"
            )


# ---------------------------------------------------------------------------
# 13. FIX 4 — overwrite cleanup must remove a stale mosaic.vrt
# ---------------------------------------------------------------------------


def test_overwrite_removes_stale_vrt(tmp_path):
    """mode('overwrite') must sweep a stale mosaic.vrt, not just *.tif.

    First write+commit produces tiles + mosaic.vrt.  A second writer over the
    same out_dir with overwrite=True and writeVrt=False must leave NO stale
    mosaic.vrt behind (otherwise readers pointed at the dir/vrt see stale members).
    """
    src_path = str(tmp_path / "src" / "input.tif")
    _write_src(src_path, w=200, h=120)
    out_dir = str(tmp_path / "out")

    # First write: creates tiles + mosaic.vrt.
    w1 = _make_writer(out_dir, mosaic_opts=_default_mosaic_opts(tileSize=100))
    w1.commit([w1.write(iter([{"path": src_path}]))])
    vrt = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt), "first write must create mosaic.vrt"

    # Second writer with overwrite=True + writeVrt=False: __init__ cleanup must
    # remove the stale mosaic.vrt.
    CogGbxWriter(
        out_dir,
        _path_schema(),
        overwrite=True,
        cog_blocksize=256,
        mosaic_opts=_default_mosaic_opts(tileSize=100, writeVrt="false"),
    )
    assert not os.path.exists(vrt), "overwrite cleanup must remove the stale mosaic.vrt"


# ---------------------------------------------------------------------------
# Fixtures shared by quadbin tests
# ---------------------------------------------------------------------------


@pytest.fixture
def small_source_raster(tmp_path):
    """200×200 raster, 100 m pixels, EPSG:32630 (UTM 30N, London area).

    Covers 20 km × 20 km centred near the EPSG:32630 central meridian.
    At quadbin resolution 12, cells are ~6–10 km wide at this latitude, so
    the raster spans several cells and produces at least 2 non-empty mini-COGs.
    """
    path = str(tmp_path / "src_qb" / "source_qb.tif")
    w, h = 200, 200
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="uint16",
        crs="EPSG:32630",
        transform=from_origin(500000.0, 5700000.0, 100.0, 100.0),
    )
    data = np.arange(w * h, dtype="uint16").reshape(1, h, w) % np.iinfo("uint16").max
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


# ---------------------------------------------------------------------------
# 14. Quadbin write: tagged, EPSG:3857 mini-COGs
# ---------------------------------------------------------------------------


def test_quadbin_write_produces_tagged_3857_minicogs(tmp_path, small_source_raster):
    """gridSystem=quadbin → ≥2 mini-COGs, each in EPSG:3857, with GBX_CELLID and GBX_GRIDSYSTEM tags."""
    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "quadbin", "gridResolution": "12"}
    )
    out_dir = str(tmp_path / "qb")
    w = _make_writer(out_dir, mosaic_opts=opts)
    msg = w.write(iter([{"path": small_source_raster}]))

    tifs = [p for p in msg.paths if p.endswith(".tif")]
    assert len(tifs) >= 2, f"expected ≥2 quadbin mini-COGs, got {len(tifs)}"

    for t in tifs:
        assert os.path.exists(t), f"mini-COG not on disk: {t}"
        assert os.path.basename(t).startswith(
            "cell_"
        ), f"quadbin tiles must use cell_* naming: {t}"
        with rasterio.open(t) as ds:
            # CRS must be EPSG:3857
            assert (
                ds.crs.to_epsg() == 3857
            ), f"{os.path.basename(t)}: expected EPSG:3857, got {ds.crs}"
            tags = ds.tags()
            # GBX_CELLID must be present and parseable as a positive int
            assert (
                "GBX_CELLID" in tags
            ), f"{os.path.basename(t)}: missing GBX_CELLID tag; got {tags}"
            cellid = int(tags["GBX_CELLID"])
            assert cellid > 0, f"GBX_CELLID must be a positive int; got {cellid}"
            # GBX_GRIDSYSTEM must be "quadbin"
            assert (
                tags.get("GBX_GRIDSYSTEM") == "quadbin"
            ), f"{os.path.basename(t)}: expected GBX_GRIDSYSTEM=quadbin; got {tags}"

    # The tagged cellid from each tile's filename must match its GBX_CELLID tag.
    _CELL_NAME_RE = re.compile(r"^cell_[0-9A-Za-z]+_(\d+)\.tif$")
    for t in tifs:
        m = _CELL_NAME_RE.match(os.path.basename(t))
        assert m, f"unexpected cell tile name: {os.path.basename(t)}"
        name_cellid = int(m.group(1))
        with rasterio.open(t) as ds:
            tag_cellid = int(ds.tags()["GBX_CELLID"])
        assert (
            name_cellid == tag_cellid
        ), f"filename cellid {name_cellid} != GBX_CELLID tag {tag_cellid} in {t}"


# ---------------------------------------------------------------------------
# 15. Quadbin correctness: pixel values match reference rasterio.warp.reproject
# ---------------------------------------------------------------------------


def test_quadbin_cell_reproject_correctness(tmp_path, small_source_raster):
    """A single quadbin cell's pixels match a reference rasterio.warp.reproject.

    The reference uses the same source, transform, and destination grid as the
    implementation.  Because Resampling.average re-samples the source pixels,
    values are NOT byte-identical to the source — hence allclose, not equal.
    """
    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "quadbin", "gridResolution": "12"}
    )
    out_dir = str(tmp_path / "qb_corr")
    w = _make_writer(out_dir, mosaic_opts=opts)
    msg = w.write(iter([{"path": small_source_raster}]))

    tifs = [p for p in msg.paths if p.endswith(".tif")]
    assert tifs, "no quadbin tiles produced"

    # Pick the first tile for the correctness check.
    tile_path = tifs[0]
    with rasterio.open(tile_path) as tile_ds:
        actual_data = tile_ds.read()
        dst_transform = tile_ds.transform
        dst_crs = tile_ds.crs
        dst_h, dst_w = tile_ds.height, tile_ds.width
        count = tile_ds.count

    # Build a reference reproject directly from the source (full-source input).
    with rasterio.open(small_source_raster) as src:
        ref_data = np.zeros((count, dst_h, dst_w), dtype=src.dtypes[0])
        reproject(
            source=rasterio.band(src, list(range(1, count + 1))),
            destination=ref_data,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=dst_transform,
            dst_crs=dst_crs,
            resampling=Resampling.average,
            src_nodata=src.nodata,
            dst_nodata=src.nodata,
        )

    # Values must be close (resampled, not byte-equal to the source).
    np.testing.assert_allclose(
        actual_data.astype(float),
        ref_data.astype(float),
        rtol=1e-4,
        atol=1.0,
        err_msg=(
            f"Quadbin cell pixel values diverge from reference reproject: "
            f"{tile_path}"
        ),
    )


# ---------------------------------------------------------------------------
# Fixtures / helpers shared by h3 tests
# ---------------------------------------------------------------------------


@pytest.fixture
def _write_src_fn():
    """Return a callable that writes a small EPSG:32632 source raster.

    The default source has NO nodata value — exercises Ruling A (derived nodata).
    Signature matches _write_src(path) with default args: 200×120 uint16, nodata=None.
    """
    return _write_src


def _write_mosaic_for_test(src_path: str, out_dir: str, opts: MosaicOptions) -> None:
    """Test helper: drive _write_mosaic_h3 directly, bypassing Spark.

    Wraps _make_writer + write() in the same pattern as the quadbin tests.
    Lives in the test module per Ruling D — NOT in production cog_writer.py.
    """
    os.makedirs(out_dir, exist_ok=True)
    w = _make_writer(out_dir, mosaic_opts=opts)
    w.write(iter([{"path": src_path}]))


# ---------------------------------------------------------------------------
# 16. h3 write: tagged, EPSG:4326 mini-COGs
# ---------------------------------------------------------------------------


def test_h3_write_produces_tagged_4326_minicogs(tmp_path, _write_src_fn):
    """h3 mosaic write must produce >=1 mini-COGs in EPSG:4326 with GBX_CELLID tags."""
    import glob

    src_path = str(tmp_path / "src_h3" / "input.tif")
    out_dir = str(tmp_path / "mosaic_h3")
    _write_src_fn(src_path)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    _write_mosaic_for_test(src_path, out_dir, opts)

    tifs = sorted(glob.glob(os.path.join(out_dir, "cell_*.tif")))
    assert len(tifs) >= 1, "expected at least one h3 cell mini-COG"

    for t in tifs:
        with rasterio.open(t) as ds:
            assert ds.crs.to_epsg() == 4326, f"expected EPSG:4326, got {ds.crs}"
            tags = ds.tags()
            assert "GBX_CELLID" in tags, f"GBX_CELLID missing in {t}"
            assert (
                tags.get("GBX_GRIDSYSTEM") == "h3"
            ), f"GBX_GRIDSYSTEM expected 'h3', got {tags.get('GBX_GRIDSYSTEM')!r}"
            # cellid is a valid h3 string at the requested resolution
            import h3 as _h3

            assert _h3.get_resolution(tags["GBX_CELLID"]) == 5


# ---------------------------------------------------------------------------
# 17. h3 hex-clip: outside-hex pixels are nodata; interior pixels are not
#     (exercises Ruling A: source has NO declared nodata → derived sentinel)
# ---------------------------------------------------------------------------


def test_h3_hex_clip_sets_outside_nodata(tmp_path, _write_src_fn):
    """Pixels outside the hexagon must be nodata; interior pixels non-nodata.

    The source is created WITHOUT a nodata value (Ruling A test case): the writer
    must still hex-clip by deriving a sentinel nodata (dtype max for uint16 = 65535).
    The output file must carry nodata even though the source did not.
    """
    import glob

    import h3 as _h3
    from rasterio.features import geometry_mask as _geometry_mask

    src_path = str(tmp_path / "src_h3_clip" / "input.tif")
    out_dir = str(tmp_path / "mosaic_h3_clip")
    _write_src_fn(src_path)  # nodata=None source (Ruling A case)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    _write_mosaic_for_test(src_path, out_dir, opts)

    tifs = sorted(glob.glob(os.path.join(out_dir, "cell_*.tif")))
    assert tifs, "no mini-COGs produced"

    # Take the first cell and verify hex-clip correctness.
    with rasterio.open(tifs[0]) as ds:
        tags = ds.tags()
        cellid_str = tags["GBX_CELLID"]
        data = ds.read(1)
        nodata = ds.nodata

    # Ruling A: output must always carry a nodata value even for a nodata-less source.
    assert (
        nodata is not None
    ), "h3 output must carry a nodata value even when the source had none (Ruling A)"

    # Build the hex polygon from cell_to_boundary [(lat, lon) -> (lon, lat)].
    boundary = _h3.cell_to_boundary(cellid_str)
    hex_coords = [(lon, lat) for lat, lon in boundary]
    hex_geojson = {
        "type": "Polygon",
        "coordinates": [hex_coords + [hex_coords[0]]],
    }

    with rasterio.open(tifs[0]) as ds:
        outside_mask = _geometry_mask(
            [hex_geojson],
            transform=ds.transform,
            out_shape=data.shape,
            invert=False,
        )

    # Every pixel outside the hex must equal nodata.
    outside_vals = data[outside_mask]
    if outside_vals.size > 0:
        if isinstance(nodata, float) and math.isnan(nodata):
            assert np.all(
                np.isnan(outside_vals)
            ), f"outside-hex pixels must be NaN nodata, got: {outside_vals[:5]}"
        else:
            np.testing.assert_array_equal(
                outside_vals,
                nodata,
                err_msg=f"outside-hex pixels must equal nodata={nodata}",
            )

    # At least some interior pixels must be non-nodata.
    interior_vals = data[~outside_mask]
    assert interior_vals.size > 0, "no interior pixels found"
    if isinstance(nodata, float) and math.isnan(nodata):
        assert np.any(~np.isnan(interior_vals)), "all interior pixels are NaN nodata"
    else:
        assert np.any(
            interior_vals != nodata
        ), f"all interior pixels equal nodata={nodata}"


# ---------------------------------------------------------------------------
# 18. h3 reproject correctness: interior pixels match a reference warp
# ---------------------------------------------------------------------------


def test_h3_cell_reproject_matches_reference(tmp_path, _write_src_fn):
    """h3 cell interior pixels match a reference rasterio.warp.reproject.

    Uses the same source, source window, and destination transform as the
    implementation.  Resampling.nearest means values are exact copies of the
    nearest source pixel — assert_allclose with rtol=1e-5 confirms the
    reprojection itself is correct, not just pixel-equal bytes.
    """
    import glob

    import h3 as _h3
    from rasterio.crs import CRS
    from rasterio.features import geometry_mask as _geometry_mask
    from rasterio.warp import reproject as _reproject
    from rasterio.warp import transform_bounds

    src_path = str(tmp_path / "src_ref" / "input.tif")
    out_dir = str(tmp_path / "mosaic_ref")
    _write_src_fn(src_path)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    _write_mosaic_for_test(src_path, out_dir, opts)

    tifs = sorted(glob.glob(os.path.join(out_dir, "cell_*.tif")))
    assert tifs, "no h3 mini-COGs produced"

    # Check the first cell only (deterministic).
    with rasterio.open(tifs[0]) as cell_ds:
        written_data = cell_ds.read(1).astype(np.float64)
        cell_transform = cell_ds.transform
        cell_w, cell_h = cell_ds.width, cell_ds.height
        dst_crs = cell_ds.crs
        cell_nodata = cell_ds.nodata
        cellid_str = cell_ds.tags()["GBX_CELLID"]
        cell_bounds = cell_ds.bounds

    with rasterio.open(src_path) as src:
        src_crs = src.crs
        src_nodata = src.nodata
        # Replicate the implementation's windowed approach.
        src_bounds_in_src_crs = transform_bounds(
            CRS.from_epsg(4326),
            src.crs,
            cell_bounds.left,
            cell_bounds.bottom,
            cell_bounds.right,
            cell_bounds.top,
        )
        src_win = src.window(*src_bounds_in_src_crs)
        src_win = src_win.intersection(
            rasterio.windows.Window(0, 0, src.width, src.height)
        )
        src_data = src.read(window=src_win)
        src_win_transform = src.window_transform(src_win)

    # Build reference with the same nodata the implementation uses.
    ref_nodata = cell_nodata  # already derived by the impl
    ref_fill = (
        float("nan")
        if (isinstance(ref_nodata, float) and math.isnan(ref_nodata))
        else float(ref_nodata or 0)
    )
    ref_data = np.full((cell_h, cell_w), ref_fill, dtype=np.float64)
    _reproject(
        source=src_data,
        destination=ref_data[np.newaxis],
        src_transform=src_win_transform,
        src_crs=src_crs,
        dst_transform=cell_transform,
        dst_crs=dst_crs,
        resampling=Resampling.nearest,
        src_nodata=src_nodata,
        dst_nodata=ref_nodata,
    )

    # Interior mask (inside hex polygon).
    boundary = _h3.cell_to_boundary(cellid_str)
    hex_coords = [(lon, lat) for lat, lon in boundary]
    hex_geojson = {
        "type": "Polygon",
        "coordinates": [hex_coords + [hex_coords[0]]],
    }
    interior = ~_geometry_mask(
        [hex_geojson],
        transform=cell_transform,
        out_shape=(cell_h, cell_w),
        invert=False,
    )

    # Interior pixels must match reference within tolerance.
    np.testing.assert_allclose(
        written_data[interior],
        ref_data[interior],
        rtol=1e-5,
        atol=1.0,
        err_msg="h3 cell interior pixels must match reference rasterio.warp.reproject",
    )


# ---------------------------------------------------------------------------
# 19. Coarse-resolution hard-error guard (h3 + quadbin retrofit)
# ---------------------------------------------------------------------------


def test_h3_coarse_res_cap_raises(tmp_path, _write_src_fn, monkeypatch):
    """A cell whose decoded size exceeds the Serverless cap must raise with gridResolution in message."""
    import databricks.labs.gbx.ds.cog_writer as _cw

    # Patch the cap in cog_writer's namespace to 1 byte so every cell exceeds it.
    # Must patch cog_writer._connect_aware_lru_sizing (the locally-imported name),
    # not file_gbx._connect_aware_lru_sizing — cog_writer uses a direct binding.
    monkeypatch.setattr(
        _cw, "_connect_aware_lru_sizing", lambda *a, **kw: (1, "patched")
    )

    src_path = str(tmp_path / "src_cap_h3" / "input.tif")
    out_dir = str(tmp_path / "mosaic_cap_h3")
    _write_src_fn(src_path)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    with pytest.raises(Exception, match="gridResolution"):
        _write_mosaic_for_test(src_path, out_dir, opts)


def test_quadbin_coarse_res_cap_raises(tmp_path, _write_src_fn, monkeypatch):
    """Quadbin cap guard must also raise with gridResolution in the message (retrofit)."""
    import databricks.labs.gbx.ds.cog_writer as _cw

    monkeypatch.setattr(
        _cw, "_connect_aware_lru_sizing", lambda *a, **kw: (1, "patched")
    )

    src_path = str(tmp_path / "src_cap_qb" / "input.tif")
    out_dir = str(tmp_path / "mosaic_cap_qb")
    _write_src_fn(src_path)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "quadbin", "gridResolution": "12"}
    )
    with pytest.raises(Exception, match="gridResolution"):
        _write_mosaic_for_test(src_path, out_dir, opts)


def test_h3_normal_resolution_proceeds(tmp_path, _write_src_fn):
    """A cell within cap must produce mini-COGs without raising."""
    import glob

    src_path = str(tmp_path / "src_ok_h3" / "input.tif")
    out_dir = str(tmp_path / "mosaic_ok_h3")
    _write_src_fn(src_path)

    opts = parse_mosaic_options(
        {"vrtMosaic": "true", "gridSystem": "h3", "gridResolution": "5"}
    )
    _write_mosaic_for_test(src_path, out_dir, opts)
    tifs = glob.glob(os.path.join(out_dir, "cell_*.tif"))
    assert len(tifs) >= 1
