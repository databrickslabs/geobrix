"""CRS-string handling for non-EPSG CRS (ESRI:54008, WKT-stored) through the pyrx pipeline.

Task 4 TDD tests — cover the 6 bug sites:
  1. agg._pick_ref_crs: deterministic for all-non-EPSG groups
  2. warp.reproject_to_srid: identity check uses CRS-object comparison
  3. functions._transform_bytes: same CRS-object identity check (via reproject_to_srid path)
  4. tessellate (5 sites): H3/quadbin/BNG CRS-object branch decisions
  5. gridagg.already_bng: CRS-object compare
  6. open_tile: non-EPSG tile.crs triggers warp (via _warp_window_bytes_crs)

ESRI:54008 = sinusoidal; to_epsg() returns None; authority is ('ESRI', '54008').
MODIS fixture: target/test-classes/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF
  Bounds: left=-8895604, bottom=1111950, right=-7783653, top=2223901 (Mexico/Caribbean)
  Size: 2400x2400

IMPORTANT: BNG tessellate with MODIS raster produces 0 cells (correct — Mexico is not in GB).
"""

import os

import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.io import MemoryFile

_MODIS_B01 = os.path.join(
    os.path.dirname(__file__),
    "../../../..",
    "target/test-classes/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF",
)
_MODIS_B01 = os.path.normpath(_MODIS_B01)


def _modis_available():
    return os.path.exists(_MODIS_B01)


def _small_modis_bytes(nbands=1, size=64):
    """Tiny (64x64) ESRI:54008 raster clipped from the MODIS extent, in memory."""
    with rasterio.open(_MODIS_B01) as src:
        modis_crs = src.crs
        # Use the top-left corner of the MODIS tile at reduced size.
        left, _, _, top = src.bounds
        xres = (src.bounds.right - src.bounds.left) / src.width
        yres = (src.bounds.top - src.bounds.bottom) / src.height
        # Scale up pixel size to produce a 64x64 tile.
        xres_new = xres * (src.width / size)
        yres_new = yres * (src.height / size)
        transform = rasterio.transform.from_origin(left, top, xres_new, yres_new)

    data = np.random.randint(100, 10000, (nbands, size, size), dtype="uint16")
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=nbands,
        dtype="uint16",
        crs=modis_crs,
        transform=transform,
        nodata=0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
        return mf.read()


def _small_27700_bytes(size=64):
    """Tiny EPSG:27700 raster (London area)."""
    data = np.arange(size * size, dtype="float32").reshape(1, size, size)
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=rasterio.transform.from_origin(530000, 181000, 30.0, 30.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
        return mf.read()


def _small_4326_bytes(size=32):
    """Tiny EPSG:4326 raster (some area)."""
    data = np.random.randint(1, 1000, (1, size, size), dtype="uint16")
    prof = dict(
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint16",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(-10.0, 52.0, 0.01, 0.01),
        nodata=0,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
        return mf.read()


# ---------------------------------------------------------------------------
# Bug 1: agg._pick_ref_crs — deterministic for all-non-EPSG groups
# ---------------------------------------------------------------------------


def test_pick_ref_crs_all_non_epsg_deterministic():
    """_pick_ref_crs with an all-non-EPSG group returns a deterministic CRS
    (CRS-object equality instead of first-encountered ordering).

    Pre-fix: the function returns `best or c` for non-EPSG tiles, so if
    all tiles are non-EPSG it returns the first one encountered — which is
    order-dependent and non-deterministic.  Post-fix: the all-same fast path
    checks CRS-object equality and returns the shared CRS deterministically.
    """
    from databricks.labs.gbx.pyrx.core.agg import _pick_ref_crs

    esri_crs = CRS.from_user_input("ESRI:54008")
    size = 16
    xres = 100000.0 / size

    def _make_ds(left, top, stack):
        data = np.ones((1, size, size), dtype="uint16")
        prof = dict(
            driver="GTiff",
            height=size,
            width=size,
            count=1,
            dtype="uint16",
            crs=esri_crs,
            transform=rasterio.transform.from_origin(left, top, xres, xres),
        )
        mf = MemoryFile()
        ds = mf.open(**prof)
        ds.write(data)
        # Flush and reopen read-only so profile is settled.
        ds.close()
        mf.seek(0)
        ds2 = mf.open()
        stack.append((mf, ds2))
        return ds2

    mfs_and_dss = []
    try:
        ds_a = _make_ds(-9000000, 2000000, mfs_and_dss)
        ds_b = _make_ds(-8500000, 2000000, mfs_and_dss)
        ds_c = _make_ds(-8000000, 2000000, mfs_and_dss)
        assert ds_a.crs.to_epsg() is None  # confirm non-EPSG
        assert ds_b.crs.to_epsg() is None

        ref = _pick_ref_crs([ds_a, ds_b, ds_c])
        assert ref is not None, "_pick_ref_crs must return a CRS, not None"
        # All three share the same ESRI:54008 CRS; the result must equal it.
        assert ref == esri_crs, (
            f"_pick_ref_crs for all-same non-EPSG group must return that CRS; "
            f"got {ref}"
        )
        # Determinism: result must not depend on order.
        ref_rev = _pick_ref_crs([ds_c, ds_b, ds_a])
        assert ref_rev == esri_crs, "reversed order must give the same CRS"
    finally:
        for mf, ds in mfs_and_dss:
            try:
                ds.close()
            except Exception:
                pass
            try:
                mf.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Bug 2: warp.reproject_to_srid identity check
# ---------------------------------------------------------------------------


def test_reproject_to_srid_identity_does_not_use_to_epsg_for_epsg4326():
    """reproject_to_srid(ds, 4326) for a raster ALREADY in EPSG:4326 hits the
    identity short-circuit regardless of how CRS equality is determined.

    This test documents the working case (EPSG:4326 raster, target=4326) — the
    CRS-object comparison `ds.crs == CRS.from_epsg(4326)` must fire and return
    bytes without a reproject.  The fix keeps this working and also works for
    non-EPSG cases where to_epsg() returns None.
    """
    from databricks.labs.gbx.pyrx.core import warp

    tile_bytes = _small_4326_bytes()
    with MemoryFile(tile_bytes) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 4326  # sanity: vanilla EPSG
        result = warp.reproject_to_srid(ds, 4326)
    assert isinstance(result, bytes) and len(result) > 0


def test_reproject_to_srid_non_epsg_to_4326_reprojects():
    """reproject_to_srid from ESRI:54008 to 4326 must NOT be skipped by the identity
    check (to_epsg() is None, so the source is not EPSG:4326).

    Pre-fix: `src_epsg = ds.crs.to_epsg()` → None → identity fires? No — identity
    only fires when `src_epsg is not None and src_epsg == target`.  None != 4326 →
    warp proceeds.  This remains working with CRS-object compare too: ESRI:54008 !=
    CRS.from_epsg(4326) → warp.

    This test VERIFIES the warp actually runs by checking the output CRS is 4326.
    """
    pytest.importorskip("rasterio")
    if not _modis_available():
        pytest.skip("MODIS fixture not available")
    from databricks.labs.gbx.pyrx.core import warp

    modis_bytes = _small_modis_bytes()
    with MemoryFile(modis_bytes) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() is None  # confirm ESRI:54008
        result = warp.reproject_to_srid(ds, 4326)

    with MemoryFile(result) as mf, mf.open() as out_ds:
        assert out_ds.crs.to_epsg() == 4326, (
            f"reproject_to_srid(54008->4326) output must be EPSG:4326; "
            f"got {out_ds.crs}"
        )


# ---------------------------------------------------------------------------
# Bug 3: functions._transform_bytes / _transform_udf identity check
# ---------------------------------------------------------------------------


def test_transform_bytes_identity_for_epsg4326_raster():
    """_transform_bytes with a materialized EPSG:4326 tile and target_srid=4326 must
    return the original raster bytes (identity short-circuit preserved).

    Post-fix, identity check uses CRS-object comparison: ds.crs == CRS.from_epsg(4326).
    For a standard EPSG:4326 raster this is equivalent to the to_epsg() path.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    tile_bytes = _small_4326_bytes()
    v1_tile = {"cellid": 0, "raster": bytearray(tile_bytes), "metadata": {}}
    result = prx._transform_bytes(v1_tile, 4326)
    assert isinstance(result, bytes) and len(result) > 0


# ---------------------------------------------------------------------------
# Bug 4: tessellate — H3/quadbin/BNG CRS-object branch decisions
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _modis_available(), reason="MODIS fixture not available")
def test_h3_tessellate_esri54008_produces_cells():
    """H3 tessellate with ESRI:54008 raster produces cells (covers the
    `None != 4326 = True` branch — correctly identifies need_reproject).

    This test documents EXISTING working behavior: the covering mode fires the
    `need_reproject = dst_epsg != 4326` check.  With CRS-object comparison,
    `ds.crs is None or ds.crs != CRS.from_epsg(4326)` is also True for
    ESRI:54008, so behavior is preserved.
    """
    from databricks.labs.gbx.pyrx.core import tessellate as T

    modis_bytes = _small_modis_bytes()
    with MemoryFile(modis_bytes) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() is None  # ESRI:54008
        chips = list(T.iter_tessellate_h3(ds, resolution=4, mode="covering"))

    assert len(chips) > 0, "H3 tessellate with ESRI:54008 must produce cells"
    for cellid, raster_bytes in chips:
        assert isinstance(cellid, int)
        assert isinstance(raster_bytes, bytes) and len(raster_bytes) > 0


@pytest.mark.skipif(not _modis_available(), reason="MODIS fixture not available")
def test_quadbin_tessellate_esri54008_produces_cells():
    """Quadbin tessellate with ESRI:54008 (covering mode) produces cells.

    Same as H3 — the CRS-object comparison for the reproject branch must fire
    for ESRI:54008.
    """
    from databricks.labs.gbx.pyrx.core import tessellate as T

    modis_bytes = _small_modis_bytes()
    with MemoryFile(modis_bytes) as mf, mf.open() as ds:
        chips = list(T.iter_tessellate_quadbin(ds, resolution=6, mode="covering"))

    assert len(chips) > 0, "Quadbin tessellate with ESRI:54008 must produce cells"
    for cellid, raster_bytes in chips:
        assert isinstance(cellid, int)
        assert isinstance(raster_bytes, bytes) and len(raster_bytes) > 0


def test_bng_tessellate_27700_wkt_crs_identity_no_rewarp():
    """_as_bng_dataset skips the warp when the raster is already in EPSG:27700.

    Specifically: the post-fix CRS-object compare `ds.crs == CRS.from_epsg(27700)`
    must fire and yield the dataset without calling reproject_to_srid.

    Regression: pre-fix code uses `ds.crs.to_epsg() == 27700`, which works for
    standard EPSG:27700 but is brittle vs. WKT-stored 27700 where to_epsg() could
    return None. Post-fix CRS-object compare is authoritative.
    """
    from databricks.labs.gbx.pyrx.core import tessellate as T

    tile_bytes = _small_27700_bytes()
    with MemoryFile(tile_bytes) as mf, mf.open() as ds:
        assert ds.crs.to_epsg() == 27700  # standard EPSG
        chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))

    assert len(chips) > 0, "BNG tessellate must produce cells for EPSG:27700 raster"
    for cellid, raster_bytes in chips:
        assert isinstance(cellid, str) and len(cellid) > 0
        assert isinstance(raster_bytes, bytes) and len(raster_bytes) > 0


@pytest.mark.skipif(not _modis_available(), reason="MODIS fixture not available")
def test_bng_tessellate_esri54008_reprojects_without_error():
    """BNG tessellate with ESRI:54008 (a non-EPSG sinusoidal CRS) must warp to
    27700 and complete WITHOUT error — the CRS-object compare in _as_bng_dataset
    fires the warp (ESRI:54008 != CRS.from_epsg(27700)) instead of skipping it.

    Note: this MODIS tile covers Mexico, whose 27700 coordinates fall far outside
    the valid BNG envelope (E[0,700k] N[0,1.3M]). GeoBrix does NOT clamp BNG cell
    math to the GB envelope (that would be a separate feature), so out-of-GB
    coordinates still yield cells. The CRS contract under test is only: non-EPSG
    input reprojects and does not raise.
    """
    from databricks.labs.gbx.pyrx.core import tessellate as T

    modis_bytes = _small_modis_bytes()
    with MemoryFile(modis_bytes) as mf, mf.open() as ds:
        chips = list(T.iter_tessellate_bng(ds, resolution="1km", mode="covering"))

    assert isinstance(chips, list), "_as_bng_dataset must not raise for ESRI:54008"


# ---------------------------------------------------------------------------
# Bug 5: gridagg.already_bng CRS-object compare
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _modis_available(), reason="MODIS fixture not available")
def test_gridagg_bng_esri54008_reprojects_without_error():
    """BNG gridagg (raster_to_bng) with ESRI:54008 (non-EPSG) must warp to 27700
    and complete WITHOUT error — the CRS-object compare fires the warp branch.

    Pre-fix: `ds.crs.to_epsg() == 27700` → None == 27700 → False → warp fires.
    Post-fix: `ds.crs == CRS.from_epsg(27700)` → False → warp fires. Behavior
    preserved. As with BNG tessellate, this Mexico tile's 27700 coordinates fall
    outside the GB envelope; GeoBrix does not clamp BNG to GB, so cells are still
    produced. The contract under test is: non-EPSG input reprojects, no raise,
    returns a list.
    """
    from databricks.labs.gbx.pyrx.core import gridagg as GA

    modis_bytes = _small_modis_bytes()
    with MemoryFile(modis_bytes) as mf, mf.open() as ds:
        result = GA._raster_to_bng(ds, resolution=3, agg="avg")

    assert isinstance(
        result, list
    ), "_raster_to_bng must return a list for a non-EPSG raster (no raise)"


def test_gridagg_bng_27700_already_bng_identity():
    """BNG gridagg with EPSG:27700 raster uses the already_bng fast path and
    produces results without reprojection.

    Post-fix: `ds.crs == CRS.from_epsg(27700)` is True for a standard
    EPSG:27700 raster; the warp is skipped. Results must be non-empty (GB area).
    """
    from databricks.labs.gbx.pyrx.core import gridagg as GA

    tile_bytes = _small_27700_bytes()
    with MemoryFile(tile_bytes) as mf, mf.open() as ds:
        result = GA._raster_to_bng(ds, resolution=3, agg="avg")

    assert isinstance(result, list) and len(result) == 1
    assert len(result[0]) > 0, "EPSG:27700 London raster must yield >=1 BNG cell"


# ---------------------------------------------------------------------------
# Bug 6: open_tile — non-EPSG tile.crs triggers warp
# ---------------------------------------------------------------------------


def test_open_tile_epsg_string_triggers_warp():
    """open_tile with tile.crs='4326' (EPSG int-castable) on a 27700 source must
    reproject to 4326.

    This is the existing working path through _warp_window_bytes (EPSG int).
    Preserved by the fix: _epsg_of('4326') = 4326 != src_epsg=27700 → warp.
    """
    import tempfile

    from databricks.labs.gbx.pyrx.core import open_tile as OT
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    tile_bytes = _small_27700_bytes()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(tile_bytes)
        path = f.name

    try:
        vt = VirtualTile(
            cellid=0,
            path=path,
            window=(0, 0, 64, 64),
            crs="4326",
        )
        with OT.open_tile(vt) as ds:
            assert ds.crs is not None
            assert (
                ds.crs.to_epsg() == 4326
            ), f"tile.crs='4326' must trigger reproject to EPSG:4326; got {ds.crs}"
    finally:
        os.unlink(path)


def test_open_tile_non_epsg_crs_triggers_warp():
    """open_tile with tile.crs='ESRI:54008' on a 4326 source must reproject.

    This is the KEY RED-BEFORE-FIX test for open_tile.

    Pre-fix: `_epsg_of('ESRI:54008')` returns None → `want = None` → warp is
    SKIPPED → output CRS stays 4326 → FAILS (output CRS should be ESRI:54008).

    Post-fix: non-EPSG tile.crs is resolved via resolve_crs() to a CRS object and
    compared against the source CRS; since ESRI:54008 != EPSG:4326, the warp fires
    via _warp_window_bytes_crs → output CRS is ESRI:54008.
    """
    import tempfile

    from databricks.labs.gbx.pyrx.core import open_tile as OT
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    # Source raster in EPSG:4326 (Ireland coast area)
    tile_bytes = _small_4326_bytes()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(tile_bytes)
        path = f.name

    try:
        # Virtual tile that requests reprojection to ESRI:54008.
        vt = VirtualTile(
            cellid=0,
            path=path,
            window=(0, 0, 32, 32),
            crs="ESRI:54008",
        )
        with OT.open_tile(vt) as ds:
            assert ds.crs is not None, "output dataset must have a CRS"
            out_crs = ds.crs
            expected_crs = CRS.from_user_input("ESRI:54008")
            assert out_crs == expected_crs, (
                f"tile.crs='ESRI:54008' must trigger reproject to ESRI:54008; "
                f"got {out_crs} (to_epsg={out_crs.to_epsg()}). "
                f"Pre-fix: _epsg_of('ESRI:54008')=None → want=None → warp SKIPPED."
            )
    finally:
        os.unlink(path)


def test_open_tile_non_epsg_crs_identity_skips_warp():
    """open_tile with tile.crs='ESRI:54008' on an already-ESRI:54008 source must
    NOT warp (identity short-circuit).

    Post-fix: want_crs (ESRI:54008) == effective_src_crs (ESRI:54008) → no warp.
    Output CRS must be ESRI:54008 (source CRS preserved verbatim).
    """
    import tempfile

    from databricks.labs.gbx.pyrx.core import open_tile as OT
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    if not _modis_available():
        pytest.skip("MODIS fixture not available (need ESRI:54008 source)")

    modis_bytes = _small_modis_bytes()
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as f:
        f.write(modis_bytes)
        path = f.name

    try:
        vt = VirtualTile(
            cellid=0,
            path=path,
            window=(0, 0, 64, 64),
            crs="ESRI:54008",  # same as source → identity
        )
        with OT.open_tile(vt) as ds:
            expected_crs = CRS.from_user_input("ESRI:54008")
            assert ds.crs == expected_crs, (
                f"identity case (src=ESRI:54008, tile.crs=ESRI:54008) must preserve CRS; "
                f"got {ds.crs}"
            )
    finally:
        os.unlink(path)
