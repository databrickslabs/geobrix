"""Tests for rst_h3_tessellate mode parameter (Spark-free core).

iter_tessellate_h3 yields (cellid_int, gtiff_bytes) 2-tuples.
"""

import numpy as np
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import tessellate as T


def _tile_4326(size=64, res_deg=0.01, origin=(-0.1, 51.5)):
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(
            origin[0], origin[1], res_deg, res_deg
        ),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def test_centroid_mode_partitions_pixels():
    """centroid: every valid pixel assigned to exactly one cell; union == all pixels; no overlap."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            results = list(T.iter_tessellate_h3(ds, resolution=9, mode="centroid"))

    seen = 0
    for _cellid, raster_bytes in results:
        with _serde.open_tile(raster_bytes) as ds:
            arr = ds.read(1, masked=True)
            seen += int((~arr.mask).sum())

    assert (
        seen == 64 * 64
    ), f"centroid chips must partition all valid pixels exactly once; got {seen}"


def test_centroid_mode_invalid_raises():
    """Unknown mode raises ValueError with a useful message."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            import pytest

            with pytest.raises(ValueError, match="mode must be one of"):
                list(T.iter_tessellate_h3(ds, resolution=9, mode="bad_mode"))


def test_covering_mode_unchanged():
    """Passing mode='covering' (explicit default) still yields cells (backward compat)."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            cells = list(T.iter_tessellate_h3(ds, resolution=9, mode="covering"))
    assert len(cells) > 0, "covering mode must yield at least one cell"


def test_covering_mode_is_overlap_set():
    """covering mode cell ids must exactly match h3.polygon_to_cells_experimental overlap set."""
    import h3
    from shapely.geometry import box

    tile = _tile_4326()
    with _serde.open_tile(tile) as ds:
        cells = {
            cellid
            for cellid, _ in T.iter_tessellate_h3(ds, resolution=9, mode="covering")
        }

    # tile extent: from_origin(-0.1, 51.5, 0.01, 0.01) → west=-0.1, north=51.5, 64px at 0.01deg
    minx, maxx = -0.1, -0.1 + 64 * 0.01
    miny, maxy = 51.5 - 64 * 0.01, 51.5
    shp = h3.geo_to_h3shape(box(minx, miny, maxx, maxy).__geo_interface__)
    oracle = {
        T._h3_str_to_signed_int64(c)
        for c in h3.polygon_to_cells_experimental(shp, 9, contain="overlap")
    }
    assert cells == oracle


def test_covering_chips_of_a_source_without_nodata_mark_the_fill_as_nodata():
    """Covering chips of a source without NoData carry the dtype fallback (65535).

    The clip fill outside each hexagon is then NoData, not valid zero pixels: every
    valid chip pixel is a source pixel with the source's value, and the source's
    real zero rows stay valid.
    """
    size = 64
    data = (np.arange(size * size, dtype="uint32") % 4000 + 1).astype("uint16")
    data = data.reshape(size, size)
    data[:4, :] = 0
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(-0.1, 51.5, 0.002, 0.002),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        with mf.open() as ds:
            inv = ~ds.transform
            chips = list(
                T.iter_tessellate(ds, 7, "h3", mode="covering", coverage="complete")
            )
    assert len(chips) > 1
    filled = zeros = 0
    for _, chip in chips:
        with _serde.open_tile(chip) as c:
            assert c.nodata == 65535.0
            valid = c.read_masks(1) != 0
            filled += int((~valid).sum())
            rows, cols = np.nonzero(valid)
            xs, ys = c.transform * (cols + 0.5, rows + 0.5)
            sc, sr = inv * (xs, ys)
            sr, sc = np.floor(sr).astype(int), np.floor(sc).astype(int)
            assert ((sr >= 0) & (sr < size) & (sc >= 0) & (sc < size)).all()
            values = c.read(1)[rows, cols]
            assert (values == data[sr, sc]).all(), "a fill pixel reads as valid data"
            zeros += int((values == 0).sum())
    assert filled > 0, "fixture must clip some pixels outside a hexagon"
    assert zeros >= 4 * size, "the real zero rows must stay valid"
