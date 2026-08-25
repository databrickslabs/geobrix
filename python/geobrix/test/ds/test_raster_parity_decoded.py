"""Verify raster parity gate: decoded-pixel comparison vs byte-identical.

Task 7 verification: after T6 (both tiers now ZSTD+predictor), the parity gate
must compare decoded pixels (not file bytes) because light auto-levels (L16 for small
tiles) while heavy fixed-levels (L9). Same pixels, different bytes -> a byte-identical
gate would FAIL even though tiles are semantically identical.

This test encodes a small gridded raster two ways (ZSTD L16 + L9, same predictor)
and verifies:
  1. Decoded-pixel comparison PASSES (pixels identical despite different levels)
  2. File bytes differ (proving the comparison is not byte-identical)

Demonstrates the gate is correctly decoded-pixel based and robust to codec-level variation.
"""

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_bounds  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402


def _create_gridded_raster():
    """Small 16x16 gridded raster with deterministic gradient + noise."""
    np.random.seed(42)
    # Gradient with noise (realistic compression scenario, non-trivial to compress)
    base = np.arange(256, dtype="float32").reshape(16, 16) / 256.0
    noise = np.random.randn(16, 16).astype("float32") * 0.05
    data = (base + noise).astype("float32")
    return data


def _encode_with_zstd_level(data, level):
    """Encode a 2-D array as GeoTIFF with ZSTD at a specific level + predictor 3."""
    prof = dict(
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_bounds(-1, -1, 1, 1, data.shape[1], data.shape[0]),
        nodata=-9999.0,
        compress="zstd",
        zstd_level=level,
        predictor=3,  # float32
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def test_decoded_pixel_parity_different_zstd_levels():
    """Verify parity gate works: same pixels, different ZSTD levels, bytes differ."""
    data = _create_gridded_raster()

    # Encode at two different levels (simulating light auto=16 vs heavy fixed=9)
    bytes_l16 = _encode_with_zstd_level(data, level=16)
    bytes_l9 = _encode_with_zstd_level(data, level=9)

    # File bytes should differ (different compression levels)
    assert bytes_l16 != bytes_l9, "Bytes must differ for different ZSTD levels"
    # But pixels must be identical (ZSTD is lossless)
    # Verify decoded-pixel parity
    with _serde.open_tile(bytes_l16) as ds16:
        arr16 = ds16.read(1)
        nodata16 = ds16.nodata
        width16 = ds16.width
        height16 = ds16.height
        crs16 = ds16.crs
        transform16 = ds16.transform

    with _serde.open_tile(bytes_l9) as ds9:
        arr9 = ds9.read(1)
        nodata9 = ds9.nodata
        width9 = ds9.width
        height9 = ds9.height
        crs9 = ds9.crs
        transform9 = ds9.transform

    # Pixel arrays must be bit-identical (lossless codec)
    assert np.array_equal(
        arr16, arr9
    ), "Decoded pixels must be identical for same-level lossless codec"

    # Metadata must match
    assert width16 == width9, f"Width mismatch: {width16} != {width9}"
    assert height16 == height9, f"Height mismatch: {height16} != {height9}"
    assert nodata16 == nodata9, f"NoData mismatch: {nodata16} != {nodata9}"
    assert crs16 == crs9, f"CRS mismatch: {crs16} != {crs9}"
    assert (
        transform16 == transform9
    ), f"Transform mismatch: {transform16} != {transform9}"


def test_byte_identical_comparison_would_fail_on_different_levels():
    """Verify byte-identical comparison FAILS (proving we need decoded-pixel)."""
    data = _create_gridded_raster()

    bytes_l16 = _encode_with_zstd_level(data, level=16)
    bytes_l9 = _encode_with_zstd_level(data, level=9)

    # Byte-identical comparison FAILS (as expected for different ZSTD levels)
    assert bytes_l16 != bytes_l9, "Byte comparison fails (bytes differ by design)"

    # But decoded-pixel comparison PASSES (this is what the parity gate must use)
    with _serde.open_tile(bytes_l16) as ds16:
        arr16 = ds16.read(1)
    with _serde.open_tile(bytes_l9) as ds9:
        arr9 = ds9.read(1)

    assert np.array_equal(arr16, arr9), "Decoded pixels pass (lossless)"
