"""Oracle table for the cell aggregates (`gbx_<grid>_cellfill`, the cell unions and the BNG chip folds): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test.
A fill is judged on the cells it must hold (the BINARY payload is decoded from its documented layout in
oracle_checks.check_cellfill). The rings of neighbours come from the grids' own libraries: `h3.grid_ring`, the
`quadbin` k-ring, the 1 km squares of the OS grid counted out by hand (`TQ3080` is the cell whose south-west corner
is (530000, 180000)), and the custom grid's numbering (`cg_id`). A union is the shapely union of the cells'
polygons, a chip fold the shapely union or intersection of the chips written below.

A group is a list of rows `(cellid, value[, grid])`; the arguments after it are the group's constants k, method and
power. The error contract, once for the family: an empty group is NULL; a method that is neither `mean` nor `idw` and
a negative k raise naming the value; a NULL cell id beside BIGINT ids past 2**53 raises (Spark would hand the batch
over as float64 and round the ids to other cells).
"""

from databricks.labs.gbx.bench.oracle_tables.grid_scalar import (
    CUSTOM_GRID,
    H3_CELL,
    QB_C,
    QB_E,
    cg_id,
)
from databricks.labs.gbx.bench.oracles import Build, Case, builder

ONLY = ("session",)

H3F, QBF, BNF, CGF = (
    "gbx_h3_cellfill",
    "gbx_quadbin_cellfill",
    "gbx_bng_cellfill",
    "gbx_custom_cellfill",
)
QUN, BUN, BIN = (
    "gbx_quadbin_cellunion_agg",
    "gbx_bng_cellunion_agg",
    "gbx_bng_cellintersection_agg",
)


# ------------------------------------------------------------------------------------------------ the cells
def _h3_rings():
    import h3

    c = h3.int_to_str(H3_CELL)
    ring = lambda d: [h3.str_to_int(x) for x in h3.grid_ring(c, d)]  # noqa: E731
    return H3_CELL, ring(1), ring(2)


def _qb_rings():
    import quadbin

    inner = set(quadbin.k_ring(QB_C, 1))
    ring2 = set(quadbin.k_ring(QB_C, 2)) - inner
    return QB_C, sorted(inner - {QB_C}), sorted(ring2)


def _bng_rings():  # 1 km cells: one ring is the eight neighbours, two rings the sixteen beyond
    return "TQ3080", ["TQ3180", "TQ3081"], ["TQ3280", "TQ3082"]


def _cg_rings():  # CUSTOM_GRID: 1000 m root cells; resolution 0 is the root cell
    return (
        cg_id(530, 180, 0),
        [cg_id(531, 180, 0), cg_id(530, 181, 0)],
        [cg_id(532, 180, 0), cg_id(530, 182, 0)],
    )


_GRIDS = {
    "h3": (H3F, _h3_rings, None),
    "qb": (QBF, _qb_rings, None),
    "bng": (BNF, _bng_rings, None),
    "cg": (CGF, _cg_rings, CUSTOM_GRID),
}


def _group(prefix, members):
    """The rows of one group: (cellid, value) pairs. The custom grid is the group's first constant."""
    return list(members)


def _consts(prefix, *consts):
    """The constants after the group: the custom grid first (its third parameter), then k, method, power."""
    return ((dict(CUSTOM_GRID),) if prefix == "cg" else ()) + consts


def _register(prefix):
    """The builders of one grid: the cells are looked up when a row runs."""

    def far():  # a cell with no neighbour in the group
        c, r1, r2 = _GRIDS[prefix][1]()
        return r2[-1]

    @builder(f"{prefix}_fill_mean")
    def _mean(tmpdir):
        c, r1, _ = _GRIDS[prefix][1]()
        return _group(prefix, [(c, None), (r1[0], 10.0), (r1[1], 20.0)])

    @builder(f"{prefix}_fill_idw")
    def _idw(tmpdir):
        c, r1, r2 = _GRIDS[prefix][1]()
        return _group(prefix, [(c, None), (r1[0], 10.0), (r2[0], 20.0)])

    @builder(f"{prefix}_fill_dup")
    def _dup(tmpdir):
        c, r1, _ = _GRIDS[prefix][1]()
        return _group(
            prefix,
            [(c, None), (r1[0], 3.0), (r1[0], 9.0), (r1[0], 5.0), (r1[1], 1.0)],
        )

    @builder(f"{prefix}_fill_isolated")
    def _isolated(tmpdir):
        c, r1, r2 = _GRIDS[prefix][1]()
        return _group(prefix, [(r2[-1], None)])

    @builder(f"{prefix}_fill_null_id")
    def _null_id(tmpdir):
        c, r1, _ = _GRIDS[prefix][1]()
        if (
            prefix == "cg"
        ):  # a resolution-0 id is small; resolution 1 puts the resolution in bit 56, past 2**53
            c = cg_id(1061, 361, 1)
        return _group(prefix, [(c, 1.0), (None, None)])

    @builder(f"{prefix}_fill_all_null")
    def _all_null(tmpdir):
        return _group(prefix, [(None, None), (None, None)])


def _expect(prefix, which):
    def expect():
        c, r1, r2 = _GRIDS[prefix][1]()
        return {
            "mean": {c: 15.0, r1[0]: 10.0, r1[1]: 20.0},
            "idw": {c: 12.0, r1[0]: 10.0, r2[0]: 20.0},
            "dup": {
                c: 5.0,
                r1[0]: 9.0,
                r1[1]: 1.0,
            },  # the mean of 9 (the largest of 3, 9, 5) and 1
            "isolated": {r2[-1]: None},
        }[which]

    return expect


def _fill_rows(prefix):
    fn = _GRIDS[prefix][0]
    big_ids = prefix != "bng"
    _register(prefix)
    rows = (
        # The NULL cell takes the mean of its valid neighbours; valid cells pass through.
        Case(
            f"{prefix}_fill_mean",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix, 1, "mean", 2.0)),
            "cellfill",
            _expect(prefix, "mean"),
            ONLY,
        ),
        # k, method and power left out take 1, 'mean', 2.0.
        Case(
            f"{prefix}_fill_defaults",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix)),
            "cellfill",
            _expect(prefix, "mean"),
            ONLY,
        ),
        # idw at k = 2 weighs the ring-1 value 10 by 1 and the ring-2 value 20 by 1/4: (10 + 5) / 1.25 = 12.
        Case(
            f"{prefix}_fill_idw",
            fn,
            (Build(f"{prefix}_fill_idw"), *_consts(prefix, 2, "idw", 2.0)),
            "cellfill",
            _expect(prefix, "idw"),
            ONLY,
        ),
        # A repeated cell keeps its LARGEST value (9 of 3, 9, 5), also when it is neither the first nor the last row.
        Case(
            f"{prefix}_fill_dup",
            fn,
            (Build(f"{prefix}_fill_dup"), *_consts(prefix, 1, "mean", 2.0)),
            "cellfill",
            _expect(prefix, "dup"),
            ONLY,
        ),
        # A NULL cell with no valid neighbour within k rings stays NULL.
        Case(
            f"{prefix}_fill_isolated",
            fn,
            (Build(f"{prefix}_fill_isolated"), *_consts(prefix, 1, "mean", 2.0)),
            "cellfill",
            _expect(prefix, "isolated"),
            ONLY,
        ),
        Case(
            f"{prefix}_fill_empty",
            fn,
            (Build(f"{prefix}_fill_all_null"), *_consts(prefix, 1, "mean", 2.0)),
            "null",
            None,
            ONLY,
        ),
        Case(
            f"{prefix}_fill_bad_method",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix, 1, "median", 2.0)),
            "raises",
            "median",
            ONLY,
        ),
        Case(
            f"{prefix}_fill_negative_k",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix, -1, "mean", 2.0)),
            "raises",
            "-1",
            ONLY,
        ),
    )
    if big_ids:
        rows += (
            Case(
                f"{prefix}_fill_null_beside_big_id",
                fn,
                (Build(f"{prefix}_fill_null_id"), *_consts(prefix, 1, "mean", 2.0)),
                "raises",
                "2**53",
                ONLY,
            ),
        )
    return rows


CELL_AGGREGATE = tuple(row for p in _GRIDS for row in _fill_rows(p))


# ------------------------------------------------------------------------------------------------ quadbin union
def _qb_boundary(cell):
    import quadbin
    import shapely

    return shapely.Polygon(quadbin.cell_to_boundary(cell))


def _qb_union_wkt(*cells):
    import shapely

    return lambda: shapely.unary_union([_qb_boundary(c) for c in cells]).wkt


CELL_AGGREGATE += (
    Case(
        "qu_agg_two_cells",
        QUN,
        ((QB_C, QB_E),),
        "geom_equals",
        _qb_union_wkt(QB_C, QB_E),
        ONLY,
    ),
    # A repeated cell counts once.
    Case(
        "qu_agg_dup",
        QUN,
        ((QB_C, QB_E, QB_C),),
        "geom_equals",
        _qb_union_wkt(QB_C, QB_E),
        ONLY,
    ),
    Case("qu_agg_all_null", QUN, ((None, None),), "null", None, ONLY),
    Case("qu_agg_null_beside_big", QUN, ((QB_C, None),), "raises", "2**53", ONLY),
)


# ------------------------------------------------------------------------------------------------ BNG chip folds
# The cell TQ3080 is (530000, 180000)..(531000, 181000). West chip: the western 600 m, east chip: the eastern 600 m
# (they overlap on x 530400..530600); the two strips are 300 m wide at the two edges, which do not meet.
def _box(x0, x1):
    import shapely

    return shapely.box(x0, 180000, x1, 181000)


def _chip(geom, core=False, cell="TQ3080"):
    import shapely

    return {
        "cellid": cell,
        "core": core,
        "chip": None if geom is None else shapely.to_wkb(geom),
    }


@builder("chips_west_east")
def _chips_west_east(tmpdir):
    return [_chip(_box(530000, 530600)), _chip(_box(530400, 531000))]


@builder("chips_west_core")
def _chips_west_core(tmpdir):
    return [_chip(_box(530000, 530600)), _chip(None, core=True)]


@builder("chips_strips")
def _chips_strips(tmpdir):
    return [_chip(_box(530000, 530300)), _chip(_box(530700, 531000))]


@builder("chips_null")
def _chips_null(tmpdir):
    return [None, None]


def _box_wkt(x0, x1):
    return lambda: _box(x0, x1).wkt


CELL_AGGREGATE += (
    Case(
        "bu_agg_two_chips",
        BUN,
        (Build("chips_west_east"),),
        "geom_equals",
        _box_wkt(530000, 531000),
        ONLY,
    ),
    # A core chip has no geometry and stands for the whole cell.
    Case(
        "bu_agg_core_chip",
        BUN,
        (Build("chips_west_core"),),
        "geom_equals",
        _box_wkt(530000, 531000),
        ONLY,
    ),
    Case("bu_agg_null_chips", BUN, (Build("chips_null"),), "null", None, ONLY),
    Case(
        "bi_agg_two_chips",
        BIN,
        (Build("chips_west_east"),),
        "geom_equals",
        _box_wkt(530400, 530600),
        ONLY,
    ),
    # A core chip is the whole cell and comes back as it is, the same early exit as the scalar `gbx_bng_cellintersection`.
    Case(
        "bi_agg_core_chip",
        BIN,
        (Build("chips_west_core"),),
        "geom_equals",
        _box_wkt(530000, 531000),
        ONLY,
    ),
    # Strips that do not meet intersect in nothing: NULL.
    Case("bi_agg_disjoint", BIN, (Build("chips_strips"),), "null", None, ONLY),
    Case("bi_agg_null_chips", BIN, (Build("chips_null"),), "null", None, ONLY),
)
