"""Unit tests for pygx h3_los_visible — pure-Python H3 line-of-sight.

Tests the driver-side ``h3_los_visible`` utility function (non-columnar;
works on plain Python dicts keyed by H3 cell strings).  All tests run
locally with no Databricks or Spark session required.

Run locally:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_viewshed.py
"""

import pytest

h3 = pytest.importorskip("h3", reason="h3 not installed")

from databricks.labs.gbx.pygx import h3_los_visible  # noqa: E402

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_RES = 11  # H3 resolution 11 — edge ~24 m, gives dense paths for LOS tests
_TOWER = h3.latlng_to_cell(37.769, -122.486, _RES)  # Golden Gate Park


def _flat_ground(cells):
    """Return a ground_z dict with 0.0 for every cell."""
    return {c: 0.0 for c in cells}


# ---------------------------------------------------------------------------
# (a) Flat terrain — every target visible
# ---------------------------------------------------------------------------


def test_flat_terrain_all_visible():
    """Flat surface (0 m) and flat ground: every cell in the disk is visible.

    With surface_z = ground_z = 0 and observer above the target, no
    intermediate cell's surface can exceed the sight ray.
    """
    disk = h3.grid_disk(_TOWER, 12)
    surf = {c: 0.0 for c in disk}
    ground = _flat_ground(disk)
    vis = h3_los_visible(_TOWER, 30.0, disk, surf, ground, target_height=2.0)
    assert vis == set(disk), "flat terrain: every cell in the disk must be visible"


# ---------------------------------------------------------------------------
# (b) Wall cell blocks targets behind it
# ---------------------------------------------------------------------------


def test_wall_blocks_cell_behind_it():
    """A very tall intermediate cell blocks all targets further along the path.

    The far cell is beyond the wall; path[1] is before the wall.  After
    setting the wall to 1000 m (far above any sight ray), the far cell must
    be occluded while cells before the wall remain visible.
    """
    far = h3.grid_disk(_TOWER, 12)[-1]
    path = h3.grid_path_cells(_TOWER, far)
    assert len(path) >= 4, "need at least 3 intermediate cells for this test"
    mid = path[len(path) // 2]

    surf = {c: 0.0 for c in path}
    surf[mid] = 1000.0  # wall far above any sight ray

    ground = _flat_ground(path)
    vis = h3_los_visible(_TOWER, 30.0, set(path), surf, ground, target_height=2.0)

    assert far not in vis, "cell behind the wall must be blocked"
    assert path[1] in vis, "cell before the wall must remain visible"


# ---------------------------------------------------------------------------
# (c) Taller observer clears a low obstacle that blocks a lower observer
# ---------------------------------------------------------------------------


def test_taller_observer_clears_obstacle():
    """A higher observer position allows the ray to clear the same obstacle.

    With a 20 m obstacle at the midpoint:
      - observer at 2 m is blocked (ray dips below the obstacle);
      - observer at 100 m is not blocked (ray stays well above).
    """
    far = h3.grid_disk(_TOWER, 12)[-1]
    path = h3.grid_path_cells(_TOWER, far)
    mid = path[len(path) // 2]

    surf = {c: 0.0 for c in path}
    surf[mid] = 20.0  # 20 m obstacle at the midpoint

    ground = _flat_ground(path)

    blocked = h3_los_visible(_TOWER, 2.0, {far}, surf, ground, target_height=2.0)
    cleared = h3_los_visible(_TOWER, 100.0, {far}, surf, ground, target_height=2.0)

    assert far not in blocked, "low observer (2 m) must be blocked by the 20 m obstacle"
    assert far in cleared, "tall observer (100 m) must clear the 20 m obstacle"


# ---------------------------------------------------------------------------
# (d) Target under canopy — surface just before target occludes the ray
# ---------------------------------------------------------------------------


def test_canopy_near_target_occludes_ground_receiver():
    """A high canopy cell just before the target blocks a ground-level receiver.

    Rigour: the target base is DTM + target_height (a receiver on the ground),
    so a canopy at path[-2] (just before the target) sits above the falling
    sight ray and blocks it.  If we incorrectly placed the target on top of
    the canopy (ground_z = canopy surface), it would read as visible — this
    test confirms the ground-based model is what the function uses.
    """
    far = h3.grid_disk(_TOWER, 10)[-1]
    path = h3.grid_path_cells(_TOWER, far)
    assert len(path) >= 3, "need at least one intermediate cell"
    just_before = path[-2]  # cell just before the target

    surf = {c: 0.0 for c in path}
    surf[just_before] = 40.0  # 40 m canopy immediately before the target

    ground = _flat_ground(path)  # target ground = 0 m (under the canopy)

    # Ground receiver (0 m + 1.6 m) behind a 40 m canopy -> blocked.
    vis = h3_los_visible(_TOWER, 30.0, {far}, surf, ground, target_height=1.6)
    assert far not in vis, "ground receiver behind canopy must be occluded"

    # Sanity: placing the target on the canopy top (ground_z = 40 m) would be
    # visible — confirms the ground-based model is what causes the occlusion.
    ground_on_canopy = {**ground, far: 40.0}
    vis_canopy = h3_los_visible(
        _TOWER, 30.0, {far}, surf, ground_on_canopy, target_height=1.6
    )
    assert far in vis_canopy, "target placed on canopy top should be visible (sanity)"


# ---------------------------------------------------------------------------
# (e) None/missing surface cell on the path is transparent (not a blocker)
# ---------------------------------------------------------------------------


def test_missing_surface_is_transparent():
    """A cell absent from surface_z (or mapped to None) does not block the ray.

    With an empty surface map, no intermediate cell contributes a blocker,
    so the target is visible as long as its ground_z is known.
    """
    far = h3.grid_disk(_TOWER, 8)[-1]
    path = h3.grid_path_cells(_TOWER, far)
    mid = path[len(path) // 2]

    # Case 1: surface_z is entirely empty -> all intermediates transparent.
    vis_empty = h3_los_visible(_TOWER, 30.0, {far}, {}, {far: 0.0}, target_height=2.0)
    assert far in vis_empty, "empty surface map -> target must be visible"

    # Case 2: surface_z has an explicit None entry at the intermediate cell.
    surf_none = {c: 0.0 for c in path}
    surf_none[mid] = None  # explicit NoData -> transparent
    ground = _flat_ground(path)
    vis_none = h3_los_visible(_TOWER, 30.0, {far}, surf_none, ground, target_height=2.0)
    assert (
        far in vis_none
    ), "None surface at intermediate -> target must still be visible"


# ---------------------------------------------------------------------------
# (f) Target with None ground_z is skipped (never fabricated to 0)
# ---------------------------------------------------------------------------


def test_groundless_target_is_skipped():
    """A target whose ground_z is None is skipped, not fabricated to sea level.

    A bare-earth gap (NoData DTM) must produce no entry in the visible set,
    not a spurious hit at z=0.  Other targets with known ground are unaffected.
    """
    disk = h3.grid_disk(_TOWER, 6)
    far = disk[-1]
    path = h3.grid_path_cells(_TOWER, far)

    surf = {c: 0.0 for c in path}
    ground = {c: 0.0 for c in path}
    ground[far] = None  # groundless target -> must be skipped

    vis = h3_los_visible(_TOWER, 30.0, set(path), surf, ground, target_height=2.0)

    assert far not in vis, "target with None ground_z must be absent from visible set"
    # Cells with known ground on the same path remain visible.
    assert path[1] in vis, "cells with known ground_z must still be assessed normally"
