"""Two adjacent virtual tiles, one shared clip polygon spanning both, must
materialize partial slices that tile back together pixel-aligned at the seam.
This is the precondition for lazy mosaic-with-clip (later increment).

Geometry design
---------------
Raster: origin (10.0, 50.0), pixel size 0.001 deg (EPSG:4326), 512x256.
  col c has left-edge at x = 10.0 + c * 0.001
  col c has center  at x = 10.0 + (c + 0.5) * 0.001

Two 256-wide tiles:  left  = cols 0..255  (window col_off=0,   w=256)
                     right = cols 256..511 (window col_off=256, w=256)
The tile seam is at source column 256 (x = 10.256).

Polygon: we choose bounds that fall STRICTLY INSIDE target columns, not on
pixel edges, to avoid rasterio.mask's crop=True including an extra
boundary pixel as nodata.  A tiny inset (0.1 * pixel_size) achieves this:

    minx = left-edge of col 128  + 0.0001  → clearly inside col 128
    maxx = left-edge of col 384  − 0.0001  → clearly inside col 383

rasterio.mask (all_touched=False, crop=True) then:
  · left  tile clips to cols 128..255 → 128 px wide, all valid
  · right tile clips to cols 256..383 → 128 px wide, all valid
  Seam: left ends at col 256 (right-edge of 255), right starts at col 256.
  Total: 256 columns covering source cols 128..383.

Expected: np.hstack([left_arr, right_arr]) == full[0:H, 128:384]
"""

import numpy as np
import rasterio
import shapely.wkb
from shapely.geometry import box

from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

from . import _layouts

# Raster parameters
W, H, BS = 512, 256, 256

# Target column range (the polygon covers exactly these source columns).
# We aim for cols 128..383 (256 total, 128 per tile).
_COL_START = 128  # first included column
_COL_END = 384  # one-past-last included column (source cols 128..383)

# Tiny inset (0.1 * pixel_size) keeps polygon bounds off pixel edges so
# rasterio.mask never includes a boundary pixel as nodata.
_INSET = 0.0001


def _build_polygon(src_transform):
    """Return WKB bytes for a polygon that cleanly covers source cols 128..383."""
    px = abs(src_transform.a)
    minx = src_transform.c + _COL_START * px + _INSET
    maxx = src_transform.c + _COL_END * px - _INSET
    maxy = src_transform.f  # top of the raster
    miny = src_transform.f - H * abs(src_transform.e)  # bottom (all H rows)
    return shapely.wkb.dumps(box(minx, miny, maxx, maxy))


def _src_col_start(ds_clipped, src_transform):
    """Map clipped dataset left-edge to source column index (round to nearest)."""
    px = abs(src_transform.a)
    return round((ds_clipped.bounds.left - src_transform.c) / px)


def _src_col_end(ds_clipped, src_transform):
    """Map clipped dataset right-edge to source column index (round to nearest)."""
    px = abs(src_transform.a)
    return round((ds_clipped.bounds.right - src_transform.c) / px)


def test_adjacent_tiles_reassemble(tmp_path):
    """Clip two adjacent 256-wide tiles with one shared polygon; reassemble pixel-exact."""
    path = _layouts.write_tiled_gtiff(str(tmp_path / "a.tif"), W, H, BS)

    with rasterio.open(path) as src:
        src_transform = src.transform
        full = src.read(1)

    wkb = _build_polygon(src_transform)

    left_tile = VirtualTile(
        cellid=0,
        path=path,
        window=(0, 0, 256, 256),
        clip_polygon=wkb,
        clip_crs="EPSG:4326",
    )
    right_tile = VirtualTile(
        cellid=1,
        path=path,
        window=(256, 0, 256, 256),
        clip_polygon=wkb,
        clip_crs="EPSG:4326",
    )

    slices = []
    col_spans = []
    for tile in (left_tile, right_tile):
        with ot.open_tile(tile) as ds:
            arr = ds.read(1)
            c_start = _src_col_start(ds, src_transform)
            c_end = _src_col_end(ds, src_transform)
            slices.append(arr)
            col_spans.append((c_start, c_end))

    left_span, right_span = col_spans
    left_arr, right_arr = slices

    # (a) Each partial must be non-trivially sized.
    assert left_arr.shape[1] > 0, "left partial is zero-width"
    assert right_arr.shape[1] > 0, "right partial is zero-width"

    # (b) Heights must match so hstack is valid.
    assert (
        left_arr.shape[0] == right_arr.shape[0]
    ), f"Height mismatch: {left_arr.shape[0]} vs {right_arr.shape[0]}"

    # (c) Contiguous at seam — no gap, no overlap.
    assert left_span[1] == right_span[0], (
        f"Gap or overlap at seam: left ends at col {left_span[1]}, "
        f"right starts at col {right_span[0]}"
    )

    # (d) Pixel-exact reassembly: hstack reproduces the corresponding slice
    #     of the full raster with no masking artifacts (no stray nodata pixels).
    reassembled = np.hstack([left_arr, right_arr])
    col_start = left_span[0]
    col_end = right_span[1]
    expected = full[0 : left_arr.shape[0], col_start:col_end]

    assert (
        reassembled.shape == expected.shape
    ), f"Shape mismatch: reassembled {reassembled.shape} vs expected {expected.shape}"
    assert np.array_equal(reassembled, expected), (
        "Pixel values differ after reassembly — tiles are not contiguous or misaligned.\n"
        f"  left span:  cols {left_span[0]}..{left_span[1]}  shape={left_arr.shape}\n"
        f"  right span: cols {right_span[0]}..{right_span[1]}  shape={right_arr.shape}\n"
        f"  expected full[0:{left_arr.shape[0]}, {col_start}:{col_end}]"
    )
