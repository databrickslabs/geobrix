"""Golden regression guard for iter_tessellate (all grids × modes).

Freezes the SHA-256 digest of the sorted cell-id list and the chip count for
each of the six (grid × mode) combinations, captured from the verified-correct
post-consolidation output.  Any future change to iter_tessellate's cell-id
set or count is caught here before it can silently break coverage/assignment
semantics downstream.

These are NOT live old-vs-new comparisons (the per-grid delegates are now thin
``yield from iter_tessellate(...)`` wrappers, so comparing them would be
comparing iter_tessellate to itself).  They are frozen constants, updated only
when a deliberate behavioral change is made and the new baseline verified
correct.

Behavioral correctness is covered by the 44 pre-existing tests in:
  test_core_tessellate.py, test_tessellate_{bng,quadbin}.py,
  test_core_tessellate_modes.py, test_tessellate_gridsystem.py.
"""

import hashlib

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

# ---------------------------------------------------------------------------
# Frozen golden baselines
# Captured 2026-09-09 from the verified-correct consolidated implementation.
# Update only when a deliberate behavioral change is made and re-verified.
# ---------------------------------------------------------------------------

_GOLDEN = {
    # (grid, mode): {"count": int, "digest": str}
    # digest = sha256(sorted(str(c) for c in cell_ids).join(","))[:16]
    ("h3", "covering"): {"count": 8623, "digest": "ed9af0e9521dba67"},
    ("h3", "centroid"): {"count": 1024, "digest": "2029ccd9fdd1a68d"},
    ("quadbin", "covering"): {"count": 30, "digest": "ab5d473d48ab0f7d"},
    ("quadbin", "centroid"): {"count": 30, "digest": "ab5d473d48ab0f7d"},
    ("bng", "covering"): {"count": 9, "digest": "3ad7e348dd33093d"},
    ("bng", "centroid"): {"count": 9, "digest": "3ad7e348dd33093d"},
    # Custom entries: captured 2026-09-10 from the verified-correct implementation.
    # Grid: no-CRS raster (32×32 px, origin (0,200), px=6.25) over bounds (0,0,200,200),
    # cell_splits=2, root_cell_size=200, srid=-1, resolution=1 → 4 cells of 100×100.
    # Sanity: 6.25m pixels are well within 100m cells; no boundary overlap.
    # Both modes emit 4 chips (one per cell). Cell IDs: 72057594037927936..39.
    # Digest = sha256("72057594037927936,72057594037927937,72057594037927938,72057594037927939")[:16]
    ("custom", "covering"): {"count": 4, "digest": "37d58d73c341e04b"},
    ("custom", "centroid"): {"count": 4, "digest": "37d58d73c341e04b"},
}

# Resolution used per grid (same as captured baseline)
_RES = {"h3": 9, "quadbin": 12, "bng": "1km"}


# ---------------------------------------------------------------------------
# Tile helpers (same fixtures used to capture the baseline)
# ---------------------------------------------------------------------------


def _tile_4326(size: int = 32) -> bytes:
    """Small EPSG:4326 tile (London-area, 32×32 px, 0.01 deg) for H3 and quadbin."""
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(-0.1, 51.5, 0.01, 0.01),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _tile_27700(size: int = 32) -> bytes:
    """Small EPSG:27700 tile (London-area, 32×32 px, 62.5 m) for BNG."""
    minx, miny, maxx, maxy = 529500, 179500, 531500, 181500
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    xres = (maxx - minx) / size
    yres = (maxy - miny) / size
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(minx, maxy, xres, yres),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


_TILE_FN = {"h3": _tile_4326, "quadbin": _tile_4326, "bng": _tile_27700}

# NOTE: "custom" is not in _RES/_TILE_FN; it's handled by test_iter_tessellate_custom_golden.


def _tile_custom(size: int = 32) -> bytes:
    """CRS-less tile matching the custom consolidation grid (bounds 0,0,200,200).

    Origin (0, 200), px=6.25 → 32×32 px covers (0,0)-(200,200) exactly.
    At resolution 1 (4 cells of 100×100): 6.25 m pixels are well within cell
    boundaries — no pixel straddles the x=100 or y=100 boundary.
    """
    data = np.arange(size * size, dtype="float32").reshape(size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs=None,  # CRS-less; custom grid uses srid=-1
        transform=rasterio.transform.from_origin(0.0, 200.0, 6.25, 6.25),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


_CUSTOM_CONF_FOR_CONSOLIDATION = {
    "bound_x_min": 0,
    "bound_x_max": 200,
    "bound_y_min": 0,
    "bound_y_max": 200,
    "cell_splits": 2,
    "root_cell_size_x": 200,
    "root_cell_size_y": 200,
    "srid": -1,
}


def _digest(cell_ids) -> str:
    """SHA-256 of comma-joined sorted string cell-ids, truncated to 16 hex chars."""
    sorted_strs = sorted(str(c) for c in cell_ids)
    return hashlib.sha256(",".join(sorted_strs).encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Golden regression tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_iter_tessellate_golden(grid, mode):
    """iter_tessellate must reproduce the frozen (count, cell-id digest) baseline.

    Catches any change to which cells are selected or their encoding.
    """
    tile = _TILE_FN[grid]()
    resolution = _RES[grid]
    golden = _GOLDEN[(grid, mode)]

    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            ids = [c for c, _ in iter_tessellate(ds, resolution, grid, mode)]

    assert (
        len(ids) == golden["count"]
    ), f"{grid}/{mode}: expected {golden['count']} chips, got {len(ids)}"
    assert _digest(ids) == golden["digest"], (
        f"{grid}/{mode}: cell-id set changed (count {len(ids)} matches but "
        f"digest {_digest(ids)!r} != frozen {golden['digest']!r})"
    )


# ---------------------------------------------------------------------------
# Error-path guards (behaviour, not golden)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("grid", ["h3", "quadbin", "bng"])
def test_iter_tessellate_invalid_mode_raises(grid):
    """iter_tessellate raises ValueError for an unrecognised mode string."""
    tile = _TILE_FN[grid]()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="mode must be one of"):
                list(iter_tessellate(ds, _RES[grid], grid, mode="bad"))


def test_iter_tessellate_invalid_grid_raises():
    """iter_tessellate raises ValueError for an unrecognised grid string."""
    tile = _tile_4326()
    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            with pytest.raises(ValueError, match="grid must be one of"):
                list(iter_tessellate(ds, 9, "xyz"))


# ---------------------------------------------------------------------------
# Custom-grid golden regression tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_iter_tessellate_custom_golden(mode):
    """iter_tessellate for custom grid must reproduce the frozen (count, digest) baseline.

    Custom grid requires an explicit ``conf`` parameter (CustomGridConf), so it
    is tested in a separate parametrize rather than the h3/quadbin/bng loop.
    Fixture: no-CRS 32×32 tile at origin (0, 200) px=6.25 → full coverage of
    bounds (0, 200) at res=1 gives 4 chips, one per cell.
    """
    from databricks.labs.gbx.pygx._custom import CustomGridConf

    tile = _tile_custom()
    resolution = 1
    golden = _GOLDEN[("custom", mode)]

    conf = CustomGridConf(
        bound_x_min=_CUSTOM_CONF_FOR_CONSOLIDATION["bound_x_min"],
        bound_x_max=_CUSTOM_CONF_FOR_CONSOLIDATION["bound_x_max"],
        bound_y_min=_CUSTOM_CONF_FOR_CONSOLIDATION["bound_y_min"],
        bound_y_max=_CUSTOM_CONF_FOR_CONSOLIDATION["bound_y_max"],
        cell_splits=_CUSTOM_CONF_FOR_CONSOLIDATION["cell_splits"],
        root_cell_size_x=_CUSTOM_CONF_FOR_CONSOLIDATION["root_cell_size_x"],
        root_cell_size_y=_CUSTOM_CONF_FOR_CONSOLIDATION["root_cell_size_y"],
        srid=_CUSTOM_CONF_FOR_CONSOLIDATION["srid"],
    )

    with MemoryFile(bytes(tile)) as mf:
        with mf.open() as ds:
            ids = [
                c for c, _ in iter_tessellate(ds, resolution, "custom", mode, conf=conf)
            ]

    assert (
        len(ids) == golden["count"]
    ), f"custom/{mode}: expected {golden['count']} chips, got {len(ids)}"
    assert _digest(ids) == golden["digest"], (
        f"custom/{mode}: cell-id set changed (count {len(ids)} matches but "
        f"digest {_digest(ids)!r} != frozen {golden['digest']!r})"
    )
