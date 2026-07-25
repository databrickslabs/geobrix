"""Tests for light-tier quadbin raster tessellation (iter_tessellate_quadbin).

Mirrors the H3 tessellate test structure (test_core_tessellate_modes.py).
Uses the same in-memory EPSG:4326 fixture: 64x64 pixels, 0.01 deg resolution,
origin (-0.1, 51.5) — a London-area tile small enough for fast polyfill.
"""

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core import tessellate as T


def _tile_4326(size=64, res_deg=0.01, origin=(-0.1, 51.5)):
    """Small EPSG:4326 raster mirroring the H3 tessellate test fixture."""
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
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# Covering mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_quadbin_covering_yields_tagged_chips():
    """covering over a small EPSG:4326 raster yields >=1 chip, each with a
    nonzero Long quadbin cell id and non-empty GTiff bytes."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_quadbin(ds, resolution=12, mode="covering"))
    assert chips, "covering must yield >=1 chip"
    for cellid, raster in chips:
        assert isinstance(cellid, int) and cellid != 0
        assert raster  # non-empty GTiff bytes


def test_iter_tessellate_quadbin_covering_default_mode():
    """Default mode (no mode arg) is covering and must yield >=1 chip."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_quadbin(ds, resolution=12))
    assert chips, "default mode must yield >=1 chip"


def test_iter_tessellate_quadbin_covering_cell_ids_are_nonzero_ints():
    """All yielded cell ids are non-zero Python ints (signed int64)."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_quadbin(ds, resolution=12, mode="covering"))
    assert chips
    for cellid, _ in chips:
        assert isinstance(cellid, int)
        assert cellid != 0, f"cell id must not be zero; got {cellid}"


# ---------------------------------------------------------------------------
# Centroid mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_quadbin_centroid_yields_chips():
    """centroid mode yields at least one chip (single-assign partition)."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            chips = list(T.iter_tessellate_quadbin(ds, resolution=12, mode="centroid"))
    assert chips, "centroid mode must yield >=1 chip"
    for cellid, raster in chips:
        assert isinstance(cellid, int) and cellid != 0
        assert raster


def test_iter_tessellate_quadbin_centroid_partitions_all_pixels():
    """centroid: every valid pixel is assigned to exactly one cell; union == all pixels."""
    from databricks.labs.gbx.pyrx import _serde

    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            results = list(
                T.iter_tessellate_quadbin(ds, resolution=12, mode="centroid")
            )

    seen = 0
    for _cellid, raster_bytes in results:
        with _serde.open_tile(raster_bytes) as chip_ds:
            arr = chip_ds.read(1, masked=True)
            seen += int((~arr.mask).sum())

    assert (
        seen == 64 * 64
    ), f"centroid chips must partition all valid pixels exactly once; got {seen}"


# ---------------------------------------------------------------------------
# Unknown mode
# ---------------------------------------------------------------------------


def test_iter_tessellate_quadbin_unknown_mode_raises():
    """An unrecognised mode string raises ValueError with a useful message."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="mode must be one of"):
                list(T.iter_tessellate_quadbin(ds, resolution=12, mode="bad_mode"))
