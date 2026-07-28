"""Executes the 'Rasterio, Distributed' doc examples and asserts the rasterio side
and the pyrx side AGREE on synthesized rasters (Docker; api suite).

NDVI: np.allclose on the float32 output arrays.
Warp: both sides report EPSG:3857 AND agree on reprojected geographic bounds (1 % tol).

Clip equivalence is not separately asserted here — the clip snippet pair
(CLIP_RASTERIO / CLIP_PYRX) appears on the docs page as illustrative code; the
pyrx call form is already exercised by the existing pyrx_clip_example test in
test_pyrx_functions.py, so we avoid duplicating a noisy equality assertion whose
border-pixel semantics diverge between rasterio.mask and GDAL Warp.
"""

import sys
from pathlib import Path

import numpy as np
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
    """Both sides agree on CRS and geographic bounds after reprojecting to EPSG:3857.

    rasterio side: ``calculate_default_transform`` does real warp math to derive
    the target grid (width, height, affine) — this is the same call a single-node
    pipeline would make before writing the reprojected file.

    pyrx side: ``rst_transform("tile", 3857)`` runs the distributed Arrow UDF and
    the result tile is read back to extract CRS, dimensions, and bounds.

    Assertions:
    (a) Both sides report EPSG:3857.
    (b) The reprojected bounds agree within 1 % of the rasterio extent in each axis
        (pixel-grid choices can differ by a pixel; bounds should track closely).
    """
    p = str(tmp_path / "in.tif")
    _write_rgbnir(p)
    r = ex.warp_both(spark, p)

    # (a) CRS agreement
    assert r["rio_epsg"] == 3857, f"rasterio target CRS EPSG was {r['rio_epsg']}"
    assert r["pyrx_epsg"] == 3857, (
        f"pyrx CRS after rst_transform: {r['pyrx_epsg']}"
    )

    # (b) Bounds agreement — each axis within 1 % of the rasterio extent.
    rio_b = r["rio_bounds"]   # (west, south, east, north) in EPSG:3857 metres
    pyrx_b = r["pyrx_bounds"]
    rio_width_m = abs(rio_b[2] - rio_b[0])
    rio_height_m = abs(rio_b[3] - rio_b[1])
    tol_m = 0.01  # 1 % tolerance factor
    for label, rio_val, pyrx_val, scale in [
        ("west",  rio_b[0], pyrx_b[0], rio_width_m),
        ("east",  rio_b[2], pyrx_b[2], rio_width_m),
        ("south", rio_b[1], pyrx_b[1], rio_height_m),
        ("north", rio_b[3], pyrx_b[3], rio_height_m),
    ]:
        assert abs(rio_val - pyrx_val) <= tol_m * scale, (
            f"bounds[{label}] diverged: rasterio={rio_val:.2f}m pyrx={pyrx_val:.2f}m "
            f"(diff={abs(rio_val - pyrx_val):.2f}m, tol={tol_m * scale:.2f}m)"
        )


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
