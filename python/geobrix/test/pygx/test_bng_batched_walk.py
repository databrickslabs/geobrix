"""The batched BNG walks give what the one-cell-at-a-time walks give.

``polyfill`` floods a frontier of cells per step (array calls); ``k_loop``/``k_ring`` encode a whole ring in one
call; ``format`` slices the id's digits. Each is pinned against a plain scalar reference, so a change to the batched
code cannot move a cell, or the order the cells come out in.
"""

import random

import numpy as np
import pytest

shapely = pytest.importorskip("shapely")
from shapely import Point, Polygon, box  # noqa: E402
from shapely.geometry import (  # noqa: E402
    GeometryCollection,
    LineString,
    MultiPoint,
    MultiPolygon,
)

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


def _ref_cell_origin(cell_id):
    """The digit-list origin the string-slice ``_cell_origin`` replaced."""
    digits = _bng.cell_digits(cell_id)
    resolution = _bng.get_resolution_from_digits(digits)
    edge = _bng.get_edge_size(resolution)
    return resolution, edge, _bng.get_x(digits, edge), _bng.get_y(digits, edge)


def test_cell_origin_matches_digit_list_reference():
    for cid in _cells() + [1, 12, 1234, 99999, 123456, -1234567890]:
        try:
            ref = _ref_cell_origin(cid)
        except (
            Exception
        ) as e:  # an id with no readable origin raises, and the same error
            with pytest.raises(type(e)):
                _bng._cell_origin(cid)
        else:
            assert _bng._cell_origin(cid) == ref


@pytest.mark.parametrize("k", [-1, 0, 1, 2, 3, 70])
def test_ring_strings_and_ids_match_the_encoder_and_format(k):
    """The ring ids come from the two axes of the square and the strings from the axes' letters and bins; both are
    pinned to the point-by-point encoder and the digit-list ``format``, for cells on the grid, on its edges and
    off it (a negative or out-of-range id takes the encoder and ``format`` instead)."""
    cells = _cells()[::3] if k == 70 else _cells()
    for cid in cells:
        try:
            ref_loop = _ref_k_loop(cid, k)
        except Exception:
            continue
        ref_ring = [cid] + [c for j in range(1, k + 1) for c in _ref_k_loop(cid, j)]
        assert _bng.k_loop(cid, k) == ref_loop
        assert _bng.k_loop_strs(cid, k) == [_ref_format(c) for c in ref_loop]
        assert _bng.k_ring(cid, k) == ref_ring
        assert _bng.k_ring_strs(cid, k) == [_ref_format(c) for c in ref_ring]


def test_ring_strings_on_the_letter_grid_edges():
    """Cells whose ring crosses a 100 km line, and the corners of the grid, where a letter or a bin leaves its
    range and the strings must come from ``format``."""
    pts = [
        (99900, 99900),
        (100100, 100100),
        (699900, 1299900),
        (500, 500),
        (0, 0),
        (-500, -500),
        (700500, 1300500),
        (-250000, 400000),
        (400000, -250000),
    ]
    for res in (1, 2, 3, 4, 5, 6):
        for e, n in pts:
            cid = _bng.point_to_cell_id(e, n, res)
            for k in (1, 2, 4):
                ids = _bng.k_ring(cid, k)
                assert _bng.k_ring_strs(cid, k) == [_ref_format(c) for c in ids]
                ids = _bng.k_loop(cid, k)
                assert _bng.k_loop_strs(cid, k) == [_ref_format(c) for c in ids]


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
        for res in RESOLUTIONS:
            if res == 4 and g.bounds[2] - g.bounds[0] > 4500:
                continue
            assert _bng.polyfill(g, res) == _bng._polyfill_scalar(g, res), (
                g.wkt[:60],
                res,
            )


@pytest.mark.parametrize("res", [2, 3, -4])
def test_polyfill_walks_side_by_side_match_the_one_cell_walk(res):
    """Floods run together (as tessellate's carved and border geometries do) give, each, the one-cell walk's list:
    their frontiers share the array calls, but not a visited set or an order. An empty geometry and a ``None`` in the
    list give ``[]`` and cost the others nothing."""
    geoms = [g for g in _geoms() if g.bounds[2] - g.bounds[0] < 9000][:6]
    walks = [None, Polygon(), *geoms, box(530000, 180000, 531500, 181500)]
    got = _bng._polyfill_walks(walks, res)
    want = [
        [] if g is None or g.is_empty else _bng._polyfill_scalar(g, res) for g in walks
    ]
    assert got == want
    assert any(want)


def test_polyfill_walks_seed_valid_at_another_resolution():
    far = (-1087613.8209951958, 1011086.41892763)
    seed = _bng.point_to_cell_id(*far, 3)
    _, _, x, y = _bng._cell_origin(seed)
    foreign = MultiPoint([far, (x + 5000, y + 5000)])
    ordinary = box(530000, 180000, 531500, 181500)
    got = _bng._polyfill_walks([ordinary, foreign], 3)
    assert got == [_bng._polyfill_scalar(g, 3) for g in (ordinary, foreign)]


def test_xy_valid_vec_matches_the_scalar_functions():
    """Origins and validity of Long ids as one array pass, for every resolution that follows the digit layout, with
    quadrant cells, off-grid cells, negative ids and ids that are not a cell at all."""
    rng = random.Random(12)
    for res in (1, 2, 3, 4, 5, 6, -2, -3, -4, -5, -6):
        ids = [
            _bng.point_to_cell_id(rng.uniform(-3e5, 1e6), rng.uniform(-3e5, 1.6e6), res)
            for _ in range(150)
        ]
        ids += [-c for c in ids[:30]]
        ndigits = 4 + 2 * (abs(res) if res >= -1 else abs(res) - 1)
        ids = [c for c in ids if 10 ** (ndigits - 1) <= abs(c) < 10**ndigits]
        x, y, valid = _bng._xy_valid_vec(np.abs(np.array(ids, dtype=np.int64)), res)
        for i, c in enumerate(ids):
            assert bool(valid[i]) == _bng.is_valid(c), (res, c)
            if valid[i]:
                assert (int(x[i]), int(y[i])) == _bng._cell_origin(c)[2:], (res, c)


def test_polyfill_500km_is_the_one_cell_walk(monkeypatch):
    """The degenerate 500km ids do not follow the digit layout, so that resolution IS the scalar walk (a parity
    assertion against itself would pass whatever it did)."""
    calls = []

    def fake(geometry, resolution):
        calls.append(resolution)
        return ["scalar"]

    monkeypatch.setattr(_bng, "_polyfill_scalar", fake)
    assert _bng.polyfill(box(100000, 100000, 200000, 200000), -1) == ["scalar"]
    assert calls == [-1]


def test_polyfill_seed_valid_at_another_resolution_walks_cell_by_cell():
    """A seed far outside the grid can make an id that is valid at a coarser resolution than the walk's (its digits
    say so): the one-cell walk builds that cell's square and neighbours at the id's own resolution, so the walk
    must give what it gives."""
    far = (-1087613.8209951958, 1011086.41892763)
    seed = _bng.point_to_cell_id(*far, 3)
    assert _bng._cell_origin(seed)[0] == 2 and _bng.is_valid(
        seed
    )  # the premise: valid, at resolution 2
    _, _, x, y = _bng._cell_origin(seed)
    # the 10 km square's centre is in the geometry (a square at resolution 3 from the same origin is not)
    g = MultiPoint([far, (x + 5000, y + 5000)])
    got = _bng.polyfill(g, 3)
    assert got == _bng._polyfill_scalar(g, 3)
    assert seed in got


def test_origin_valid_negative_id_reads_its_magnitude_like_the_scalar():
    """A negative id with the layout's digit count takes the array path: the scalar functions read the digits of the
    id's magnitude too (``cell_digits``), so the two agree."""
    pos = _bng.point_to_cell_id(400500.0, 300500.0, 3)
    for c in (-pos, pos):
        x, y, valid = _bng._origin_valid(np.array([c], dtype=np.int64), 3)
        assert bool(valid[0]) == _bng.is_valid(c)
        assert (int(x[0]), int(y[0])) == _bng._cell_origin(c)[2:]


def test_polyfill_non_finite_coordinate_raises():
    with pytest.raises(ValueError, match="Non-finite"):
        _bng.polyfill(LineString([(1, 1), (float("inf"), 2)]), 3)


def _ref_tessellate(geometry, resolution, keep_core_geom):
    """The cell-by-cell tessellate the batched one replaced: one square, one clip and one equality test per cell."""
    if geometry is None or geometry.is_empty:
        return []
    input_dimension = _bng._get_dimensions(geometry)
    radius = _bng.get_buffer_radius(resolution)
    polygonal = _bng._polygonal_part(geometry)
    carved = polygonal.buffer(-radius)
    if carved.is_empty:
        border_geom = geometry.buffer(radius * 1.01).simplify(0.01 * radius)
    else:
        border_geom = polygonal.boundary.buffer(radius * 1.01).simplify(0.01 * radius)
    core_set = set(_bng.polyfill(carved, resolution))
    border = [c for c in _bng.polyfill(border_geom, resolution) if c not in core_set]
    chips = []
    for cell in core_set:
        core_geom = _bng.cell_id_to_geometry(cell) if keep_core_geom else None
        chips.append((cell, True, core_geom))
    for cell in border:
        cell_geom = _bng.cell_id_to_geometry(cell)
        intersect = cell_geom.intersection(geometry)
        if intersect.is_empty:
            continue
        adjusted = _bng._clean_chip_geometry(intersect, cell_geom.boundary)
        if adjusted.is_empty:
            continue
        if _bng._equals_exact(adjusted, cell_geom, _bng._FULL_CELL_TOL):
            chips.append((cell, True, cell_geom if keep_core_geom else None))
        else:
            chips.append((cell, False, adjusted))
    return [
        (cell, core, chip)
        for (cell, core, chip) in chips
        if chip is None or _bng._get_dimensions(chip) == input_dimension
    ], core_set


def _tess_geoms():
    return [
        box(530000.5, 180000.5, 534000.5, 183000.5),
        box(530000, 180000, 534000, 183000),  # aligned to the grid
        # two cells in the middle are fully inside and a ring of them is within the border radius of the edge: a
        # border cell that is the whole cell, which is what the promotion test looks at
        box(530850, 180850, 533150, 182150),
        box(
            529000, 179000, 551000, 201000
        ),  # the same at 10 km: four whole cells within the border radius
        Polygon(
            [(530200, 180300), (534300, 180100), (533900, 183200), (530100, 182900)]
        ),
        Polygon(
            [(530000, 180000), (536000, 180000), (536000, 186000), (530000, 186000)],
            [[(532000, 182000), (534000, 182000), (534000, 184000), (532000, 184000)]],
        ),
        MultiPolygon(
            [box(530100, 180100, 532900, 182900), box(536100, 180100, 538400, 182900)]
        ),
        GeometryCollection(
            [
                box(530000, 180000, 533000, 183000),
                LineString([(533500, 180500), (536000, 184000)]),
            ]
        ),
    ]


def _same_tessellation(got, want):
    assert len(got) == len(want)
    for (cg, kg, chg), (cw, kw, chw) in zip(got, want):
        assert (cg, kg) == (cw, kw)
        assert (chg is None) == (chw is None)
        if chg is not None:
            assert _bng._equals_exact(chg, chw, 0)


@pytest.mark.parametrize("promote", [False, True])
@pytest.mark.parametrize("keep_core_geom", [False, True])
@pytest.mark.parametrize("res", [2, 3])
def test_tessellate_batched_matches_cell_by_cell(
    monkeypatch, res, keep_core_geom, promote
):
    """Element by element: the cell, whether it is core, and the chip's vertices. ``promote`` makes the whole-cell
    test order-insensitive, so a border cell that is the whole cell becomes core (shapely's own ``equals_exact``
    rejects the renormalised ring a clip returns, so the promotion branch is otherwise never taken).
    """
    if promote:
        monkeypatch.setattr(
            _bng, "_equals_exact", lambda a, b, tol: shapely.equals(a, b)
        )
    promoted = 0
    for g in _tess_geoms():
        want, core_set = _ref_tessellate(g, res, keep_core_geom)
        got = _bng.tessellate(g, res, keep_core_geom)
        _same_tessellation(got, want)
        promoted += sum(1 for c, core, _ in got if core and c not in core_set)
    if promote:
        assert promoted > 0  # the branch ran
