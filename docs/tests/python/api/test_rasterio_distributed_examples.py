"""Executes the 'Rasterio, Distributed' doc examples and asserts the rasterio side
and the pyrx side AGREE on synthesized rasters (Docker; api suite).

NDVI: np.allclose on the float32 output arrays.
Warp: both sides report EPSG:3857 after reprojecting.

Clip equivalence is not separately asserted here — the clip snippet pair
(CLIP_RASTERIO / CLIP_PYRX) appears on the docs page as illustrative code; the
pyrx call form is already exercised by the existing pyrx_clip_example test in
test_pyrx_functions.py, so we avoid duplicating a noisy equality assertion whose
border-pixel semantics diverge between rasterio.mask and GDAL Warp.
"""

import sys
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

sys.path.insert(0, str(Path(__file__).parent))
import rasterio_distributed_examples as ex  # noqa: E402


def _write_rgbnir(path, px=32):
    """Write a 2-band float32 GeoTIFF: band1=red, band2=nir, EPSG:4326.

    Values are chosen so nir > red everywhere (NDVI always positive) and
    no pixel has nir + red == 0, so the division is never degenerate and
    np.allclose works without equal_nan tricks.
    """
    red = np.linspace(10, 100, px * px, dtype="float32").reshape(px, px)
    nir = np.linspace(60, 200, px * px, dtype="float32").reshape(px, px)
    transform = from_bounds(-122.5, 37.7, -122.4, 37.8, px, px)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=px,
        height=px,
        count=2,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
    ) as ds:
        ds.write(red, 1)
        ds.write(nir, 2)


def test_warp_agrees(spark, tmp_path):
    """Both sides report EPSG:3857 after reprojecting."""
    p = str(tmp_path / "in.tif")
    _write_rgbnir(p)
    rio_epsg, pyrx_srid = ex.warp_both(spark, p)
    assert rio_epsg == 3857, f"rasterio target CRS EPSG was {rio_epsg}"
    assert pyrx_srid == 3857, f"pyrx rst_srid returned {pyrx_srid} after rst_transform"


def test_ndvi_agrees(spark, tmp_path):
    """pyrx rst_ndvi and rasterio/numpy NDVI agree on synthesized raster."""
    p = str(tmp_path / "in.tif")
    _write_rgbnir(p)
    rio_ndvi, pyrx_ndvi = ex.ndvi_both(spark, p)
    assert rio_ndvi.shape == pyrx_ndvi.shape, (
        f"shape mismatch: rasterio {rio_ndvi.shape} vs pyrx {pyrx_ndvi.shape}"
    )
    assert np.allclose(
        rio_ndvi, pyrx_ndvi, rtol=1e-4, atol=1e-4
    ), (
        f"NDVI arrays diverged: max abs diff = "
        f"{np.max(np.abs(rio_ndvi - pyrx_ndvi)):.6f}"
    )
