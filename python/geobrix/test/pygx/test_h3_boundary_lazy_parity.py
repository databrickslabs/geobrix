"""Self-parity: lazy O(perimeter) h3 geom-aware path == polyfill-oracle path.

Task 5 of the boundary-as-line efficiency refactor.  h3 is light-only (no heavy
tier, no cross-tier parity), so the correctness oracle is h3's ORIGINAL
polyfill-based classifier, retained as ``_h3._classify_polyfill``.

The lazy path (``_h3.geom_expand`` for Polygon/MultiPolygon boundary/hole modes)
seeds from the polygon boundary ring(s) and classifies only cells near the
perimeter (O(perimeter)), instead of polyfilling the whole area.  Its output
MUST be byte-identical to the oracle:

    _dilate.geom_expand(kind, k, mode, _h3._classify_polyfill(g, res),
                        _h3._neighbors, coverage)

across ALL 6 modes × 3 coverages × {ring, loop} × {simple, holed, grid-aligned}
h3 fixtures.  This file is RED before the lazy impl (``_classify_polyfill`` does
not exist / ``geom_expand`` is not lazy) and GREEN after.

Run:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_h3_boundary_lazy_parity.py
"""

import h3
import pytest
from shapely import to_wkb
from shapely.geometry import Polygon, box
from shapely.ops import unary_union

from databricks.labs.gbx.pygx import _dilate
from databricks.labs.gbx.pygx import _h3 as _h3mod

# ---------------------------------------------------------------------------
# h3 polygon fixtures: (name, geometry, resolution)
#
# Kept small (London-area, res 7) so the h3-native per-cell membership stays
# quick, while still exercising every seed/admit path.
# ---------------------------------------------------------------------------

# simple: ~8 km London box, no holes.
_SIMPLE = (box(-0.12, 51.50, 0.00, 51.57), 7)

# holed: London-area box with a 0.1°×0.1° interior hole (donut topology).
_HOLED = (
    Polygon(
        [(-0.20, 51.45), (0.10, 51.45), (0.10, 51.65), (-0.20, 51.65)],
        [[(-0.10, 51.50), (0.00, 51.50), (0.00, 51.60), (-0.10, 51.60)]],
    ),
    7,
)


def _build_aligned(res=7):
    """Polygon whose edges lie EXACTLY on h3 cell boundaries (union of a 2-ring disk).

    This is the true grid-aligned stress case: shared cell/region edges make
    planar shapely tests on a reconstructed hexagon disagree with h3's geodesic
    polyfill, so only h3-native membership reproduces the oracle here.  A plain
    lat/lng box is NOT h3-aligned and would not exercise this divergence.
    """
    center = h3.latlng_to_cell(51.52, -0.07, res)
    disk = h3.grid_disk(center, 2)
    return unary_union(
        [Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(c)]) for c in disk]
    )


_ALIGNED = (_build_aligned(7), 7)

# multi-hole: London-area box with two disjoint interior holes.
_MULTIHOLE = (
    Polygon(
        [(-0.25, 51.42), (0.15, 51.42), (0.15, 51.68), (-0.25, 51.68)],
        [
            [(-0.15, 51.48), (-0.05, 51.48), (-0.05, 51.58), (-0.15, 51.58)],
            [(0.02, 51.55), (0.10, 51.55), (0.10, 51.63), (0.02, 51.63)],
        ],
    ),
    7,
)

_FIXTURES = {
    "simple": _SIMPLE,
    "holed": _HOLED,
    "aligned": _ALIGNED,
    "multihole": _MULTIHOLE,
}


def _oracle(kind, geom, res, k, mode, coverage):
    """Reference path: full O(area) polyfill classifier fed to _dilate.geom_expand."""
    cls = _h3mod._classify_polyfill(geom, int(res))
    return set(
        _dilate.geom_expand(kind, int(k), mode, cls, _h3mod._neighbors, coverage)
    )


@pytest.mark.parametrize("fixture", sorted(_FIXTURES))
@pytest.mark.parametrize("mode", _dilate.MODES)
@pytest.mark.parametrize("coverage", _dilate.COVERAGE)
@pytest.mark.parametrize("kind", ("ring", "loop"))
@pytest.mark.parametrize("k", (0, 1, 2, 3))
def test_h3_lazy_equals_polyfill_oracle(fixture, mode, coverage, kind, k):
    """Lazy h3 geom_expand output == polyfill-oracle output, byte-for-byte."""
    geom, res = _FIXTURES[fixture]
    wkb = to_wkb(geom)

    lazy = _h3mod.geom_expand(kind, wkb, res, k, mode, coverage)
    oracle = _oracle(kind, geom, res, k, mode, coverage)

    assert lazy == oracle, (
        f"lazy != oracle for {fixture}/{mode}/{coverage}/{kind}/k={k}: "
        f"|lazy|={len(lazy)} |oracle|={len(oracle)} "
        f"only_lazy={sorted(lazy - oracle)[:4]} "
        f"only_oracle={sorted(oracle - lazy)[:4]}"
    )
    assert all(isinstance(c, int) for c in lazy), "all cell ids must be Python int"
