"""Frozen golden corpus for geom-aware kring/kloop light-tier entrypoints.

Captures current behavior of all four grid systems (quadbin, h3, bng, custom)
across all 6 dilation modes × 3 coverages × {ring,loop} × k∈{0,1,2} × 6
fixture geometries, producing ``geomk_golden.json`` as the correctness anchor
for the boundary-as-line efficiency refactor.

Fixtures per grid are in the grid's native CRS:
  quadbin / h3 : WGS84 lon/lat (EPSG:4326)
  bng / custom  : EPSG:27700 (British National Grid metres)

The ``aligned`` fixture has edges on exact cell boundaries (quadbin: exact tile
edges at zoom 10; h3: union of a cell disk at resolution 7; bng/custom: corners
at exact 1 km multiples).  This is the most critical fixture — it exercises the
code paths a future perimeter shortcut would change.

Generate / update the committed JSON (run inside the Docker container):
    python python/geobrix/test/pygx/test_geomk_golden.py --generate

Run the frozen assertion (pure Python, no JAR, no Spark):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pygx/test_geomk_golden.py \\
        --log golden.log
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from shapely import to_wkb
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
    box,
)
from shapely.ops import unary_union

_HERE = Path(__file__).resolve().parent
_GOLDEN_FILE = _HERE / "geomk_golden.json"

# ===========================================================================
# Matrix constants
# ===========================================================================

_MODES = (
    "boundary-out",
    "boundary-in",
    "boundary-in-ignore-holes",
    "hole-in",
    "hole-out",
    "hole-out-ignore-geom",
)
_COVERAGES = ("coveras", "polyfill", "core")
_KINDS = ("ring", "loop")
_K_VALUES = (0, 1, 2)
_FIXTURES = ("simple", "holed", "aligned", "line", "point", "gc")

# ===========================================================================
# Per-grid resolution (single resolution for all fixtures in that grid)
# ===========================================================================

_QB_RES = 10  # quadbin zoom 10  — tiles ~0.35° wide
_H3_RES = 7  # h3 resolution 7  — cells ~1.22 km²
_BNG_RES = 3  # bng resolution 3 — 1 km cells (EPSG:27700)
_CUST_RES = 0  # custom grid res 0 — 1 000 m cells

# custom-grid constructor kwargs (matching parity tests)
_CUST_GRID_ARGS: dict = dict(
    bound_x_min=0,
    bound_x_max=1_000_000,
    bound_y_min=0,
    bound_y_max=1_000_000,
    cell_splits=2,
    root_cell_size_x=1000,
    root_cell_size_y=1000,
    srid=-1,
)

# ===========================================================================
# Fixtures — static (WGS84 lon/lat for quadbin/h3; EPSG:27700 for bng/custom)
# ===========================================================================


def _wkb(geom) -> bytes:
    """Return WKB bytes for a Shapely geometry."""
    return bytes(to_wkb(geom))


# --------------- quadbin (WGS84 lon/lat, res 10) ----------------------------

# simple: 4°×4° box near NYC — yields ~52 coveras cells at k=0
_QB_SIMPLE = box(-78.0, 39.0, -74.0, 43.0)

# holed: east-US box with 2°×3° interior hole (matching parity test)
_QB_HOLED = Polygon(
    [(-76.0, 38.0), (-72.0, 38.0), (-72.0, 43.0), (-76.0, 43.0)],
    [[(-75.0, 39.0), (-73.0, 39.0), (-73.0, 42.0), (-75.0, 42.0)]],
)

# aligned: union of tiles (291..299) × (377..383) at zoom 10 — exact tile edges
# computed lazily to avoid import at module load
_QB_ALIGNED: Polygon | None = None

# line, point, gc in the same area
_QB_LINE = LineString([(-77.0, 39.0), (-73.0, 43.0)])
_QB_POINT = Point(-75.0, 41.0)
_QB_GC = GeometryCollection(
    [
        Polygon(
            [(-77.0, 40.0), (-74.0, 40.0), (-74.0, 43.0), (-77.0, 43.0)],
            [[(-76.0, 41.0), (-75.0, 41.0), (-75.0, 42.0), (-76.0, 42.0)]],
        ),
        LineString([(-73.0, 43.0), (-71.0, 43.0)]),
        Point(-70.0, 44.0),
    ]
)


def _build_qb_aligned() -> Polygon:
    """Return a polygon whose edges lie on exact quadbin tile boundaries at zoom 10.

    Tiles x=291..299 × y=377..383 at zoom 10 cover roughly (-77.7°, 41.0°) →
    (-74.5°, 42.8°), just north of the simple fixture.  The bounding box of all
    their bounding boxes is aligned to the tile grid.
    """
    import quadbin

    cells = [
        quadbin.tile_to_cell((x, y, _QB_RES))
        for x in range(291, 300)
        for y in range(377, 384)
    ]
    bboxes = [quadbin.cell_to_bounding_box(c) for c in cells]  # [w, s, e, n]
    return box(
        min(b[0] for b in bboxes),  # westmost edge
        min(b[1] for b in bboxes),  # southmost edge
        max(b[2] for b in bboxes),  # eastmost edge
        max(b[3] for b in bboxes),  # northmost edge
    )


# --------------- h3 (WGS84 lon/lat, res 7) ----------------------------------

# simple: ~8 km × 8 km London box — ~15 coveras cells at k=0
_H3_SIMPLE = box(-0.12, 51.50, 0.00, 51.57)

# holed: larger London-area box with 0.1°×0.1° interior hole
_H3_HOLED = Polygon(
    [(-0.20, 51.45), (0.10, 51.45), (0.10, 51.65), (-0.20, 51.65)],
    [[(-0.10, 51.50), (0.00, 51.50), (0.00, 51.60), (-0.10, 51.60)]],
)

# aligned: computed lazily from h3 cell boundaries
_H3_ALIGNED: Polygon | MultiPolygon | None = None

_H3_LINE = LineString([(-0.15, 51.48), (0.05, 51.60)])
_H3_POINT = Point(-0.05, 51.52)
_H3_GC = GeometryCollection(
    [
        Polygon(
            [(-0.15, 51.47), (0.05, 51.47), (0.05, 51.55), (-0.15, 51.55)],
            [[(-0.08, 51.49), (-0.02, 51.49), (-0.02, 51.53), (-0.08, 51.53)]],
        ),
        LineString([(-0.12, 51.56), (0.07, 51.56)]),
        Point(0.10, 51.58),
    ]
)


def _build_h3_aligned() -> Polygon | MultiPolygon:
    """Return a polygon whose edges lie on exact h3 cell boundaries at resolution 7.

    Takes a 2-ring disk (~19 cells) centred on (51.52°N, 0.07°W), computes the
    union of their hexagonal boundary polygons.  The result is a Polygon or
    MultiPolygon with edges coinciding exactly with h3 cell edges.
    """
    import h3

    center = h3.latlng_to_cell(51.52, -0.07, _H3_RES)
    disk = h3.grid_disk(center, 2)
    polys = [
        Polygon([(lng, lat) for lat, lng in h3.cell_to_boundary(cell)]) for cell in disk
    ]
    return unary_union(polys)


# --------------- bng (EPSG:27700, res 3 = 1 km cells) -----------------------

# simple: 10 km × 10 km box near London — ~100 cells at k=0 polyfill
_BNG_SIMPLE = box(525000.0, 175000.0, 535000.0, 185000.0)

# holed: 60 km × 60 km box with 20 km × 20 km interior hole (matching parity test)
_BNG_HOLED = Polygon(
    [
        (500000.0, 150000.0),
        (560000.0, 150000.0),
        (560000.0, 210000.0),
        (500000.0, 210000.0),
    ],
    [
        [
            (520000.0, 170000.0),
            (540000.0, 170000.0),
            (540000.0, 190000.0),
            (520000.0, 190000.0),
        ]
    ],
)

# aligned: corners at exact 1 km multiples → edges on cell boundaries at res 3
_BNG_ALIGNED = box(530000.0, 180000.0, 534000.0, 184000.0)

_BNG_LINE = LineString([(525000.0, 175000.0), (535000.0, 185000.0)])
_BNG_POINT = Point(530000.0, 180000.0)
_BNG_GC = GeometryCollection(
    [
        Polygon(
            [
                (528000.0, 178000.0),
                (534000.0, 178000.0),
                (534000.0, 184000.0),
                (528000.0, 184000.0),
            ],
            [
                [
                    (529500.0, 180000.0),
                    (531000.0, 180000.0),
                    (531000.0, 182000.0),
                    (529500.0, 182000.0),
                ]
            ],
        ),
        LineString([(534500.0, 183000.0), (536000.0, 183000.0)]),
        Point(537000.0, 185000.0),
    ]
)

# --------------- custom grid (0..1 000 000 × 0..1 000 000, res 0 = 1 000 m) -

# simple: 5 km × 5 km box — ~25 cells at k=0 (coveras)
_CUST_SIMPLE = box(530000.0, 180000.0, 535000.0, 185000.0)

# holed: 60 km × 60 km outer with interior hole inset 500 m from cell lines
# (matching parity test — hCore non-empty: ~38×38 = 1 444 cells inside hole)
_CUST_HOLED = Polygon(
    [
        (500000.0, 100000.0),
        (560000.0, 100000.0),
        (560000.0, 160000.0),
        (500000.0, 160000.0),
    ],
    [
        [
            (510500.0, 110500.0),
            (549500.0, 110500.0),
            (549500.0, 149500.0),
            (510500.0, 149500.0),
        ]
    ],
)

# aligned: corners at exact 1 000 m multiples → edges on cell boundaries at res 0
_CUST_ALIGNED = box(530000.0, 180000.0, 534000.0, 184000.0)

_CUST_LINE = LineString([(530000.0, 180000.0), (540000.0, 190000.0)])
_CUST_POINT = Point(532000.0, 182000.0)
_CUST_GC = GeometryCollection(
    [
        Polygon(
            [
                (530000.0, 180000.0),
                (538000.0, 180000.0),
                (538000.0, 188000.0),
                (530000.0, 188000.0),
            ],
            [
                [
                    (532000.0, 182000.0),
                    (535000.0, 182000.0),
                    (535000.0, 185000.0),
                    (532000.0, 185000.0),
                ]
            ],
        ),
        LineString([(540000.0, 187000.0), (545000.0, 187000.0)]),
        Point(547000.0, 190000.0),
    ]
)

# ===========================================================================
# Fixture registry
# ===========================================================================


def _get_fixtures(grid: str) -> dict:
    """Return {fixture_name: shapely_geometry} for the given grid.

    Aligned fixtures for quadbin and h3 are built lazily on first call.
    """
    global _QB_ALIGNED, _H3_ALIGNED

    if grid == "quadbin":
        if _QB_ALIGNED is None:
            _QB_ALIGNED = _build_qb_aligned()
        return {
            "simple": _QB_SIMPLE,
            "holed": _QB_HOLED,
            "aligned": _QB_ALIGNED,
            "line": _QB_LINE,
            "point": _QB_POINT,
            "gc": _QB_GC,
        }
    if grid == "h3":
        if _H3_ALIGNED is None:
            _H3_ALIGNED = _build_h3_aligned()
        return {
            "simple": _H3_SIMPLE,
            "holed": _H3_HOLED,
            "aligned": _H3_ALIGNED,
            "line": _H3_LINE,
            "point": _H3_POINT,
            "gc": _H3_GC,
        }
    if grid == "bng":
        return {
            "simple": _BNG_SIMPLE,
            "holed": _BNG_HOLED,
            "aligned": _BNG_ALIGNED,
            "line": _BNG_LINE,
            "point": _BNG_POINT,
            "gc": _BNG_GC,
        }
    if grid == "custom":
        return {
            "simple": _CUST_SIMPLE,
            "holed": _CUST_HOLED,
            "aligned": _CUST_ALIGNED,
            "line": _CUST_LINE,
            "point": _CUST_POINT,
            "gc": _CUST_GC,
        }
    raise ValueError(f"unknown grid: {grid!r}")


# ===========================================================================
# Entrypoint dispatchers — call CURRENT (unmodified) light-tier code
# ===========================================================================


def _run_quadbin(geom, kind: str, k: int, mode: str, coverage: str) -> list:
    from databricks.labs.gbx.pygx import _quadbin

    wkb = _wkb(geom)
    if kind == "ring":
        return sorted(
            _quadbin.geometry_k_ring(wkb, _QB_RES, k, mode, coverage=coverage)
        )
    return sorted(_quadbin.geometry_k_loop(wkb, _QB_RES, k, mode, coverage=coverage))


def _run_h3(geom, kind: str, k: int, mode: str, coverage: str) -> list:
    from databricks.labs.gbx.pygx import _h3

    wkb = _wkb(geom)
    return sorted(_h3.geom_expand(kind, wkb, _H3_RES, k, mode, coverage))


def _run_bng(geom, kind: str, k: int, mode: str, coverage: str) -> list:
    from databricks.labs.gbx.pygx import _bng

    wkb = _wkb(geom)
    if kind == "ring":
        return sorted(_bng.geometry_k_ring_str(wkb, _BNG_RES, k, mode, coverage))
    return sorted(_bng.geometry_k_loop_str(wkb, _BNG_RES, k, mode, coverage))


def _run_custom(geom, kind: str, k: int, mode: str, coverage: str) -> list:
    from databricks.labs.gbx.pygx import _custom

    conf = _custom.CustomGridConf(**_CUST_GRID_ARGS)
    wkb = _wkb(geom)
    if kind == "ring":
        return sorted(
            _custom.geometry_k_ring(conf, wkb, _CUST_RES, k, mode, coverage=coverage)
        )
    return sorted(
        _custom.geometry_k_loop(conf, wkb, _CUST_RES, k, mode, coverage=coverage)
    )


_DISPATCHERS = {
    "quadbin": _run_quadbin,
    "h3": _run_h3,
    "bng": _run_bng,
    "custom": _run_custom,
}

_GRIDS = ("quadbin", "h3", "bng", "custom")


def _run_one(grid: str, geom, kind: str, k: int, mode: str, coverage: str) -> list:
    """Run one corpus entry.  Returns a sorted list of cell IDs (int or str)."""
    return _DISPATCHERS[grid](geom, kind, k, mode, coverage)


def _corpus_key(
    grid: str, mode: str, coverage: str, kind: str, k: int, fixture: str
) -> str:
    return f"{grid}|{mode}|{coverage}|{kind}|{k}|{fixture}"


# ===========================================================================
# Corpus generation
# ===========================================================================


def _generate_corpus() -> dict:
    """Iterate the full matrix and call the current light-tier entrypoints.

    Returns a dict mapping corpus_key -> sorted list of cell IDs.
    Degenerate combos (empty result) are recorded as [].
    """
    corpus: dict = {}
    total = (
        len(_GRIDS)
        * len(_MODES)
        * len(_COVERAGES)
        * len(_KINDS)
        * len(_K_VALUES)
        * len(_FIXTURES)
    )
    done = 0

    for grid in _GRIDS:
        fixtures = _get_fixtures(grid)
        for fixture_name, geom in fixtures.items():
            for mode in _MODES:
                for coverage in _COVERAGES:
                    for kind in _KINDS:
                        for k in _K_VALUES:
                            key = _corpus_key(
                                grid, mode, coverage, kind, k, fixture_name
                            )
                            try:
                                result = _run_one(grid, geom, kind, k, mode, coverage)
                                corpus[key] = result
                            except Exception as exc:
                                print(f"  WARN {key}: {exc}", file=sys.stderr)
                                corpus[key] = []
                            done += 1
                            if done % 100 == 0:
                                print(f"  {done}/{total} …", file=sys.stderr)

    return corpus


def _save_corpus(corpus: dict, path: Path) -> None:
    """Write corpus to JSON, sorted by key for stable diffs."""
    with open(path, "w") as f:
        json.dump(dict(sorted(corpus.items())), f, indent=2)
        f.write("\n")
    print(f"Wrote {len(corpus)} entries to {path}", file=sys.stderr)


def _load_corpus() -> dict:
    with open(_GOLDEN_FILE) as f:
        return json.load(f)


# ===========================================================================
# Frozen assertion test (pure Python, no JAR, no Spark)
# ===========================================================================


def test_geomk_golden() -> None:
    """Assert current light-tier output matches the committed golden corpus.

    Fails loudly on any mismatch so future refactors cannot silently change
    geom-aware cell-set behavior.
    """
    if not _GOLDEN_FILE.exists():
        pytest.skip(
            "Golden corpus JSON not found; run: "
            "python python/geobrix/test/pygx/test_geomk_golden.py --generate"
        )

    corpus = _load_corpus()
    mismatches: list[str] = []

    # Build fixture sets once per grid (reuses cached aligned fixtures)
    grid_fixtures = {grid: _get_fixtures(grid) for grid in _GRIDS}

    for key, expected in corpus.items():
        grid, mode, coverage, kind, k_str, fixture = key.split("|")
        geom = grid_fixtures[grid][fixture]
        actual = _run_one(grid, geom, kind, int(k_str), mode, coverage)
        if sorted(actual) != sorted(expected):
            mismatches.append(
                f"  {key}:\n"
                f"    expected ({len(expected)} cells): {expected[:4]}…\n"
                f"    actual   ({len(actual)} cells):   {actual[:4]}…"
            )

    if mismatches:
        preview = "\n".join(mismatches[:10])
        raise AssertionError(
            f"{len(mismatches)} of {len(corpus)} golden corpus entries diverged:\n{preview}"
        )


# ===========================================================================
# Generator entry point
# ===========================================================================

if __name__ == "__main__":
    if "--generate" not in sys.argv:
        print(
            "Usage: python python/geobrix/test/pygx/test_geomk_golden.py --generate\n"
            "  Generates geomk_golden.json in the same directory as this file."
        )
        sys.exit(1)

    print("Generating geom-aware golden corpus …", file=sys.stderr)
    corpus = _generate_corpus()
    _save_corpus(corpus, _GOLDEN_FILE)
    nonempty = sum(1 for v in corpus.values() if v)
    print(
        f"Done: {len(corpus)} total entries, {nonempty} non-empty, "
        f"{len(corpus) - nonempty} empty (degenerate combos recorded as []).",
        file=sys.stderr,
    )
