"""Task 5 (SP2-CRS): bench CRS round-trip regressions — grouped_file, spec.

These tests are designed so they FAIL if the production edits are reverted:

- test_clip_geom_crs_str_esri_authority: calls grouped_file._clip_geom_from_source with
  an ESRI:54008 raster. Old code (to_epsg()->None -> crs.to_wkt()) returns WKT; new code
  (crs_to_canonical) returns "ESRI:54008". Reverted -> WKT string != "ESRI:54008" -> FAIL.

- test_tile_extent_size_srid_esri_authority: calls spec._tile_extent_size_srid with an
  ESRI:54008 DatasetReader. Old code (to_epsg()->None -> 0); new code
  (authority_srid_of -> 54008). Reverted -> result[-1] == 0 -> FAIL.

NOTE: bench/spec.py and bench/grouped_file.py transitively import pygx.functions -> quadbin.
When quadbin is not installed, these tests are skipped (pre-existing env limitation;
test_spec.py has the same issue). The regression guard is active in any environment with
quadbin installed.

For runner.py:650 and file_gbx_qa.py:297 (both inside Spark-requiring functions), the
canonical helper behavior is exercised transitively: authority_srid_of and crs_to_canonical
are verified by the two tests above for the same ESRI:54008 case.
"""

import numpy as np
import pytest
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds

# quadbin is an optional dep; bench/spec.py and bench/grouped_file.py both transitively
# import it at module level.  Skip gracefully when not installed (pre-existing limitation).
quadbin = pytest.importorskip(
    "quadbin", reason="quadbin not installed; bench CRS tests skipped"
)


def _esri_54008_crs():
    return CRS.from_authority("ESRI", 54008)


def _write_esri_raster(path) -> None:
    """Write a 4x4 float32 GTiff with ESRI:54008 CRS to path."""
    import rasterio

    crs = _esri_54008_crs()
    transform = from_bounds(-180, -90, 180, 90, 4, 4)
    with rasterio.open(
        str(path),
        "w",
        driver="GTiff",
        count=1,
        height=4,
        width=4,
        dtype="float32",
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(np.ones((1, 4, 4), dtype="float32"))


# ---------------------------------------------------------------------------
# grouped_file._clip_geom_from_source — BEHAVIOR-CHANGING (ESRI authority)
# ---------------------------------------------------------------------------


def test_clip_geom_crs_str_esri_authority(tmp_path):
    """grouped_file._clip_geom_from_source returns 'ESRI:54008', not WKT.

    Fail-on-revert: old code produces WKT (to_epsg() returns None for ESRI CRS);
    new code calls crs_to_canonical which returns 'ESRI:54008'.
    """
    tif = tmp_path / "esri_54008.tif"
    _write_esri_raster(tif)

    from databricks.labs.gbx.bench.grouped_file import _clip_geom_from_source

    _, crs_str = _clip_geom_from_source(tif)
    assert crs_str == "ESRI:54008", (
        f"Expected 'ESRI:54008', got {crs_str!r}; "
        "reverted code returns WKT via crs.to_wkt() because to_epsg() is None for ESRI CRS"
    )


def test_clip_geom_crs_str_epsg_preserved(tmp_path):
    """grouped_file._clip_geom_from_source returns 'EPSG:4326' for EPSG:4326 (behavior-preserving)."""
    import rasterio

    tif = tmp_path / "epsg_4326.tif"
    transform = from_bounds(-1, -1, 1, 1, 4, 4)
    with rasterio.open(
        str(tif),
        "w",
        driver="GTiff",
        count=1,
        height=4,
        width=4,
        dtype="float32",
        crs=CRS.from_epsg(4326),
        transform=transform,
    ) as dst:
        dst.write(np.ones((1, 4, 4), dtype="float32"))

    from databricks.labs.gbx.bench.grouped_file import _clip_geom_from_source

    _, crs_str = _clip_geom_from_source(tif)
    assert crs_str == "EPSG:4326"


# ---------------------------------------------------------------------------
# spec._tile_extent_size_srid — BEHAVIOR-CHANGING (ESRI authority)
# ---------------------------------------------------------------------------


def test_tile_extent_size_srid_esri_authority():
    """spec._tile_extent_size_srid returns srid=54008 for ESRI:54008 (not 0).

    Fail-on-revert: old code (to_epsg()) returns None for ESRI CRS, so srid=0;
    new code (authority_srid_of) returns 54008.
    """
    crs = _esri_54008_crs()
    transform = from_bounds(-180, -90, 180, 90, 4, 4)

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            count=1,
            height=4,
            width=4,
            dtype="float32",
            crs=crs,
            transform=transform,
        ) as dst:
            dst.write(np.ones((1, 4, 4), dtype="float32"))
        with mf.open() as src:
            from databricks.labs.gbx.bench.spec import _tile_extent_size_srid

            result = _tile_extent_size_srid(src)

    assert result[-1] == 54008, (
        f"Expected srid=54008 for ESRI:54008, got {result[-1]}; "
        "reverted code uses to_epsg() which returns None for ESRI CRS -> srid=0"
    )


def test_tile_extent_size_srid_epsg():
    """spec._tile_extent_size_srid returns srid=4326 for EPSG:4326 (behavior-preserving)."""
    transform = from_bounds(-180, -90, 180, 90, 4, 4)

    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            count=1,
            height=4,
            width=4,
            dtype="float32",
            crs=CRS.from_epsg(4326),
            transform=transform,
        ) as dst:
            dst.write(np.ones((1, 4, 4), dtype="float32"))
        with mf.open() as src:
            from databricks.labs.gbx.bench.spec import _tile_extent_size_srid

            result = _tile_extent_size_srid(src)

    assert result[-1] == 4326
