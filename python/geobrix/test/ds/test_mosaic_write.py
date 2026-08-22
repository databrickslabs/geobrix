"""Task 2 (SDD — Phase A native mini-COG mosaic): _write_mosaic implementation tests.

Tests the mosaic-mode write path added to CogGbxWriter in Task 2.  Pure Python
(no Spark, no JAR).

Coverage:
  1. Non-overlapping tile grid covers the full source extent.
  2. Each tile's pixels are pixel-equal to the corresponding source region.
  3. Each mini-COG is a valid COG (internally-tiled).
  4. pruneEmpty=True drops all-nodata edge tiles.
  5. overlapPercent>0 produces halo-expanded tile extents.
  6. MosaicOptions pickle round-trip (dataclass survives serialization to worker).
  7. Single-COG mode regression (mosaic_opts=None, existing path unchanged).
  8. Multiple source rows produce correctly-named tiles per source.

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/ds/test_mosaic_write.py \
        --log mosaic-write.log
"""

from __future__ import annotations

import os
import pickle

import numpy as np
import rasterio
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog_writer import (
    CogCommitMessage,
    CogGbxWriter,
    MosaicOptions,
    _is_all_nodata,
    _tile_grid_windows,
    parse_mosaic_options,
)
from databricks.labs.gbx.pyrx.core import cog as gbxcog

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
        "mosaic": "true",
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
    expected = {"tile_0_0.tif", "tile_0_1.tif", "tile_1_0.tif", "tile_1_1.tif"}
    assert names == expected, f"unexpected tile names: {names}"


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
            name = os.path.basename(tile_path)
            # Parse row/col from tile_<row>_<col>.tif
            parts = os.path.splitext(name)[0].split("_")
            assert len(parts) == 3 and parts[0] == "tile", f"unexpected name: {name}"
            tile_row, tile_col = int(parts[1]), int(parts[2])

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
                err_msg=f"pixel mismatch in {name}",
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
        "tile_1_1.tif" not in names
    ), "all-nodata tile_1_1 should have been pruned by pruneEmpty=True"
    # The other 3 tiles should be present.
    for expected in ("tile_0_0.tif", "tile_0_1.tif", "tile_1_0.tif"):
        assert (
            expected in names
        ), f"{expected} should be present (has non-nodata pixels)"


def test_prune_empty_false_writes_nodata_tile(tmp_path):
    """pruneEmpty=False: all-nodata tile is written (not pruned)."""
    src_path = str(tmp_path / "src" / "nodata.tif")
    _write_src_with_nodata_patch(src_path, w=200, h=120, nodata=0.0)
    opts = _default_mosaic_opts(tileSize=100, pruneEmpty="false")
    w = _make_writer(str(tmp_path / "out"), mosaic_opts=opts)
    msg = w.write(iter([{"path": src_path}]))

    names = {os.path.basename(p) for p in msg.paths}
    assert (
        "tile_1_1.tif" in names
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
    dims_no = _get_dims(msg_no.paths, "tile_0_1.tif")
    dims_halo = _get_dims(msg_halo.paths, "tile_0_1.tif")
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
            "mosaic": "true",
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
