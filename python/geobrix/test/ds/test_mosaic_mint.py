"""Task 4 (SDD — Phase A native mini-COG mosaic): on-demand transient VRT via mint_vrt.

Tests the mint_vrt() public function that builds a TRANSIENT VRT over a
dynamic tile collection (filtered query result, ad-hoc list, Volume dir).
Pure Python (no Spark, no JAR, no osgeo).

Coverage:
  1. mint_vrt([paths]) returns a path; rasterio.open() opens the mosaic.
  2. A windowed read from the minted VRT equals the source pixels.
  3. A dynamic subset (only some tiles) assembles a smaller mosaic with fewer
     members than the full-collection VRT.
  4. Transient: the returned path is NOT inside the tiles directory.
  5. mint_vrt(out=<path>) writes the VRT to the caller-supplied destination.
  6. No osgeo import in _mosaic.py (light-tier compliance).

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/ds/test_mosaic_mint.py \\
        --log mosaic-mint.log
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import xml.etree.ElementTree as ET

import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.windows import Window

from databricks.labs.gbx.ds._mosaic import mint_vrt

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TILE_W = 100  # pixels per tile column
_TILE_H = 80  # pixels per tile row
_ORIGIN_X = 400000.0  # EPSG:32632 easting (metres)
_ORIGIN_Y = 5000000.0  # EPSG:32632 northing (metres)
_PIXEL_SIZE = 10.0  # metres/pixel


def _write_tile(
    path: str,
    col_offset: int = 0,
    row_offset: int = 0,
    w: int = _TILE_W,
    h: int = _TILE_H,
    dtype: str = "uint16",
    count: int = 1,
) -> str:
    """Write one GTiff tile spatially positioned at (col_offset, row_offset) in the grid."""
    origin_x = _ORIGIN_X + col_offset * w * _PIXEL_SIZE
    origin_y = _ORIGIN_Y - row_offset * h * _PIXEL_SIZE  # north-up: y decreases south
    seed = col_offset * 10000 + row_offset * 1000
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=count,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(origin_x, origin_y, _PIXEL_SIZE, _PIXEL_SIZE),
    )
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    data = (
        np.arange(w * h * count, dtype=np.uint32).reshape(count, h, w) + seed
    ).astype(dtype) % (
        np.iinfo(dtype).max if np.issubdtype(np.dtype(dtype), np.integer) else 1
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


def _write_tile_grid(
    tmp_path: pathlib.Path,
    n_cols: int = 2,
    n_rows: int = 1,
    *,
    subdir: str = "tiles",
    dtype: str = "uint16",
) -> list:
    """Write a (n_rows × n_cols) grid of spatially adjacent tiles.  Returns sorted paths."""
    tiles_dir = tmp_path / subdir
    tiles_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for r in range(n_rows):
        for c in range(n_cols):
            path = str(tiles_dir / f"tile_{r}_{c}.tif")
            _write_tile(path, col_offset=c, row_offset=r)
            paths.append(path)
    return sorted(paths)


def _unique_source_filenames(vrt_path: str) -> set:
    """Unique SourceFilename text values in a VRT (normalised to basenames)."""
    tree = ET.parse(vrt_path)
    return {
        os.path.basename(sf.text or "") for sf in tree.getroot().iter("SourceFilename")
    }


# ---------------------------------------------------------------------------
# 1. mint_vrt opens as a valid rasterio dataset
# ---------------------------------------------------------------------------


def test_mint_vrt_opens_as_mosaic(tmp_path):
    """mint_vrt([paths]) returns a path that rasterio can open."""
    tiles = _write_tile_grid(tmp_path, n_cols=2, n_rows=1)
    vrt_path = mint_vrt(tiles)

    assert os.path.exists(vrt_path), f"VRT not created: {vrt_path}"
    with rasterio.open(vrt_path) as ds:
        assert ds.count == 1
        assert ds.width == _TILE_W * 2  # two tiles side by side
        assert ds.height == _TILE_H


# ---------------------------------------------------------------------------
# 2. Windowed read matches source pixels
# ---------------------------------------------------------------------------


def test_mint_vrt_windowed_read_matches_source(tmp_path):
    """A windowed read from the minted VRT equals the same window in the left tile."""
    tiles = _write_tile_grid(tmp_path, n_cols=2, n_rows=1)
    tile_left = tiles[0]

    vrt_path = mint_vrt(tiles)

    # The left tile occupies columns 0.._TILE_W-1 in the mosaic.
    win = Window(0, 0, _TILE_W, _TILE_H)
    with rasterio.open(tile_left) as ref_ds:
        ref_data = ref_ds.read()

    with rasterio.open(vrt_path) as vrt_ds:
        vrt_data = vrt_ds.read(window=win)

    np.testing.assert_array_equal(
        vrt_data,
        ref_data,
        err_msg="Windowed read from minted VRT differs from the source tile",
    )


# ---------------------------------------------------------------------------
# 3. Dynamic subset produces a smaller mosaic with fewer members
# ---------------------------------------------------------------------------


def test_mint_vrt_dynamic_subset(tmp_path):
    """Minting over a subset of tiles produces a smaller mosaic (fewer members)."""
    tiles = _write_tile_grid(tmp_path, n_cols=3, n_rows=1)

    vrt_all = mint_vrt(tiles)
    vrt_sub = mint_vrt(tiles[:2])  # only first 2 of 3

    with rasterio.open(vrt_all) as all_ds:
        with rasterio.open(vrt_sub) as sub_ds:
            assert sub_ds.width < all_ds.width, (
                f"Subset VRT ({sub_ds.width}px) should be narrower than "
                f"full VRT ({all_ds.width}px)"
            )

    # VRT XML references only the 2 member tiles (per band)
    members = _unique_source_filenames(vrt_sub)
    assert len(members) == 2, f"Expected 2 unique members, got {members}"
    for tile in tiles[:2]:
        assert os.path.basename(tile) in members, f"{tile} not in subset VRT"
    assert (
        os.path.basename(tiles[2]) not in members
    ), "Third tile must not appear in subset VRT"


# ---------------------------------------------------------------------------
# 4. Transient: VRT is NOT written inside the tiles directory
# ---------------------------------------------------------------------------


def test_mint_vrt_is_not_in_tiles_dir(tmp_path):
    """mint_vrt writes to a temp location, NOT alongside the tiles."""
    tiles = _write_tile_grid(tmp_path, n_cols=2)
    tiles_dir = os.path.realpath(os.path.dirname(tiles[0]))

    vrt_path = os.path.realpath(mint_vrt(tiles))
    assert not vrt_path.startswith(
        tiles_dir + os.sep
    ), f"VRT was written inside the tiles directory: {vrt_path!r}"


# ---------------------------------------------------------------------------
# 5. Caller-supplied out= destination
# ---------------------------------------------------------------------------


def test_mint_vrt_out_path(tmp_path):
    """When out= is given, the VRT is written to that exact path."""
    tiles = _write_tile_grid(tmp_path, n_cols=2)
    dest = str(tmp_path / "custom" / "my_mosaic.vrt")

    result = mint_vrt(tiles, out=dest)

    assert result == dest, f"Return value should equal out={dest!r}, got {result!r}"
    assert os.path.exists(dest), f"VRT not written at {dest!r}"
    with rasterio.open(dest) as ds:
        assert ds.width == _TILE_W * 2


# ---------------------------------------------------------------------------
# 6. No osgeo import in _mosaic.py (light-tier compliance)
# ---------------------------------------------------------------------------


def test_mint_vrt_no_osgeo():
    """_mosaic.py must not import osgeo — light-tier / Serverless compliance."""
    import re

    spec = importlib.util.find_spec("databricks.labs.gbx.ds._mosaic")
    assert spec is not None, "_mosaic module not found"
    src_text = pathlib.Path(spec.origin).read_text(encoding="utf-8")
    # Reject any actual import of osgeo (allow doc-string mentions like "no osgeo").
    osgeo_imports = re.findall(r"^(?:import|from)\s+osgeo\b.*$", src_text, re.MULTILINE)
    assert not osgeo_imports, f"_mosaic.py must not import osgeo: {osgeo_imports}"


# ---------------------------------------------------------------------------
# 7. minted_vrt context-manager cleans up its temp dir on exit
# ---------------------------------------------------------------------------


def test_minted_vrt_contextmanager_cleans_up(tmp_path):
    """minted_vrt yields a working VRT, then removes its temp dir on exit —
    leaving the member tiles intact."""
    from databricks.labs.gbx.ds._mosaic import minted_vrt

    tiles = _write_tile_grid(tmp_path, n_cols=2, n_rows=1)

    with minted_vrt(tiles) as vrt_path:
        assert os.path.exists(vrt_path), "VRT should exist inside the block"
        tmp_dir = os.path.dirname(vrt_path)
        assert os.path.isdir(tmp_dir)
        with rasterio.open(vrt_path) as ds:
            assert ds.count == 1
            assert ds.width == _TILE_W * 2  # two adjacent columns

    # After the block: transient VRT + its temp dir are gone; tiles untouched.
    assert not os.path.exists(vrt_path), "VRT temp file should be cleaned up"
    assert not os.path.exists(tmp_dir), "temp dir should be removed"
    for t in tiles:
        assert os.path.exists(t), "member tiles must NOT be removed by cleanup"


def test_minted_vrt_cleans_up_on_exception(tmp_path):
    """The temp dir is removed even when the with-body raises."""
    from databricks.labs.gbx.ds._mosaic import minted_vrt

    tiles = _write_tile_grid(tmp_path, n_cols=2, n_rows=1)
    tmp_dir = None
    try:
        with minted_vrt(tiles) as vrt_path:
            tmp_dir = os.path.dirname(vrt_path)
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert tmp_dir is not None, "with-block should have entered"
    assert not os.path.exists(tmp_dir), "temp dir must be removed on exception too"
