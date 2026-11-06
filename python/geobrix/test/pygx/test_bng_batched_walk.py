"""The batched BNG walks give what the one-cell-at-a-time walks give.

``polyfill`` floods a frontier of cells per step (array calls); ``k_loop``/``k_ring`` encode a whole ring in one
call; ``format`` slices the id's digits. Each is pinned against a plain scalar reference, so a change to the batched
code cannot move a cell, or the order the cells come out in.
"""

import random

import pytest

shapely = pytest.importorskip("shapely")
from shapely import Point, Polygon, box  # noqa: E402
from shapely.geometry import GeometryCollection, LineString, MultiPolygon  # noqa: E402

from databricks.labs.gbx.pygx import _bng  # noqa: E402

RESOLUTIONS = [1, 2, 3, 4, -2, -3, -4]


def _ref_format(cell_id):
    """The digit-list ``format`` the string-slice one replaced."""
    digits = _bng.cell_digits(cell_id)
    if len(digits) < 6:
        x = _bng._safe_digit_index(digits[3:5], len(_bng.LETTER_MAP) - 1)
        y = _bng._safe_digit_index(digits[1:3], len(_bng.LETTER_MAP[0]) - 1)
        return _bng.LETTER_MAP[x][y][0]
    q = max(0, min(len(_bng.QUADRANTS) - 1, digits[-1]))
    x = _bng._safe_digit_index(digits[3:5], len(_bng.LETTER_MAP) - 1)
    y = _bng._safe_digit_index(digits[1:3], len(_bng.LETTER_MAP[0]) - 1)
    coords = digits[5:-1]
    k = len(coords) // 2
    xs = "".join(str(d) for d in coords[:k])
    ys = "".join(str(d) for d in coords[k : 2 * k])
    return f"{_bng.LETTER_MAP[x][y]}{xs}{ys}{_bng.QUADRANTS[q]}"


def _ref_k_loop(cell_id, k):
    """The point-by-point ``k_loop`` the array one replaced."""
    digits = _bng.cell_digits(cell_id)
    resolution = _bng.get_resolution_from_digits(digits)
    edge = _bng.get_edge_size(resolution)
    x, y = _bng.get_x(digits, edge), _bng.get_y(digits, edge)
    xmin, xmax, ymin, ymax = x - k * edge, x + k * edge, y - k * edge, y + k * edge
    pts = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
    pts += [(xmin, yy) for yy in range(ymin + edge, ymax, edge)]
    pts += [(xmax, yy) for yy in range(ymin + edge, ymax, edge)]
    pts += [(xx, ymax) for xx in range(xmin + edge, xmax, edge)]
    pts += [(xx, ymin) for xx in range(xmin + edge, xmax, edge)]
    return [_bng.point_to_cell_id(px, py, resolution) for px, py in pts]


def _cells():
    rng = random.Random(7)
    out = []
    for res in RESOLUTIONS:
        for _ in range(40):
            # inside Great Britain, and a margin around it, so edge cells (negative or out-of-range ids) are in
            e, n = rng.uniform(-30000, 730000), rng.uniform(-30000, 1330000)
            out.append(_bng.point_to_cell_id(e, n, res))
    return out


def test_format_matches_digit_list_reference():
    ids = _cells() + [12, 1234, 99999, 123456, 10**12, -5, -1234567]
    assert [_bng.format(c) for c in ids] == [_ref_format(c) for c in ids]


@pytest.mark.parametrize("k", [0, 1, 2, 3])
def test_k_loop_and_k_ring_match_point_by_point(k):
    for cid in _cells():
        if not _bng.is_valid(cid):
            continue
        assert _bng.k_loop(cid, k) == _ref_k_loop(cid, k)
        ring = [cid] + [c for j in range(1, k + 1) for c in _ref_k_loop(cid, j)]
        assert _bng.k_ring(cid, k) == ring


def _geoms():
    rng = random.Random(3)
    out = []
    for _ in range(6):
        cx, cy = rng.uniform(100000, 600000), rng.uniform(100000, 1100000)
        r = rng.choice([300, 1500, 4000])
        out.append(
            Polygon(
                [
                    (
                        cx + r * rng.uniform(0.5, 1) * dx,
                        cy + r * rng.uniform(0.5, 1) * dy,
                    )
                    for dx, dy in [
                        (1, 0),
                        (0.7, 0.7),
                        (0, 1),
                        (-0.7, 0.7),
                        (-1, 0),
                        (0, -1),
                    ]
                ]
            )
        )
    out += [
        box(530000, 180000, 532000, 183000),  # aligned to the grid
        Polygon(
            [(0, 0), (5000, 0), (5000, 5000), (0, 5000)],
            [[(1000, 1000), (2500, 1000), (2500, 2500), (1000, 2500)]],
        ),  # a hole
        MultiPolygon(
            [box(100000, 100000, 103000, 103500), box(120000, 130000, 121500, 132000)]
        ),
        GeometryCollection(
            [
                box(300000, 300000, 303000, 303000),
                LineString([(300500, 299000), (304000, 304000)]),
            ]
        ),
        GeometryCollection(  # a collection nested in a collection adds no seed of its own
            [
                box(300000, 300000, 303000, 303000),
                GeometryCollection([box(303000, 300000, 306000, 303000)]),
            ]
        ),
        LineString([(400000, 400000), (404000, 407000)]),
        Point(400000.5, 400000.5),
        box(-3000, -3000, 4000, 4000),  # over the south-west edge of the grid
        box(690000, 1290000, 720000, 1320000),  # over the north-east edge
        box(2e6, 2e6, 2.001e6, 2.001e6),  # nowhere near the grid
    ]
    return out


def test_polyfill_batched_matches_cell_by_cell_in_order():
    for g in _geoms():
        for res in RESOLUTIONS + [-1]:
            if res == 4 and g.bounds[2] - g.bounds[0] > 4500:
                continue
            assert _bng.polyfill(g, res) == _bng._polyfill_scalar(g, res), (
                g.wkt[:60],
                res,
            )


def test_polyfill_non_finite_coordinate_raises():
    with pytest.raises(ValueError, match="Non-finite"):
        _bng.polyfill(LineString([(1, 1), (float("inf"), 2)]), 3)
