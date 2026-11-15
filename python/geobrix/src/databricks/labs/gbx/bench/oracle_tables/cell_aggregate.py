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

from dataclasses import replace

from databricks.labs.gbx.bench.oracle_tables._uc_tile import Adapt, recipe, twin
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


# ------------------------------------------------------------------------------------------------ the array forms
# `gbx_<grid>_cellfill_array` is the scalar twin of each aggregate: the group is one ARRAY of (cellid, value) structs and
# the answer an array of the same structs, ascending, not the BINARY payload. Its rows are the aggregate's, run on the
# session and on UC with the same expectation. Not twinned: a NULL cell id beside ids past 2**53 raises in the aggregate
# (its float64 batches round them); a row UDF reads the ids exactly, so the array form skips the NULL id (pinned below).
H3FA, QBFA, BNFA, CGFA = (
    "gbx_h3_cellfill_array",
    "gbx_quadbin_cellfill_array",
    "gbx_bng_cellfill_array",
    "gbx_custom_cellfill_array",
)


def _array_rows(prefix, fn):
    rows = [
        c
        for c in CELL_AGGREGATE
        if c.cid.startswith(f"{prefix}_fill_") and not c.cid.endswith("beside_big_id")
    ]
    return tuple(
        replace(c, surfaces=("session", "uc")) for c in twin(tuple(rows), fn, Adapt())
    )


for _prefix, _fn in (("h3", H3FA), ("qb", QBFA), ("bng", BNFA), ("cg", CGFA)):
    CELL_AGGREGATE += _array_rows(_prefix, _fn)


def _null_id_expect(prefix):
    c = _GRIDS[prefix][1]()[0]
    if (
        prefix == "cg"
    ):  # the cell of the builder: resolution 1 puts the resolution in bit 56, past 2**53
        c = cg_id(1061, 361, 1)
    return {c: 1.0}


CELL_AGGREGATE += tuple(
    Case(
        f"{p}_fill_null_beside_big_id__{fn[4:]}",
        fn,
        (Build(f"{p}_fill_null_id"), *_consts(p, 1, "mean", 2.0)),
        "cellfill",
        lambda p=p: _null_id_expect(p),  # the NULL id is skipped, the valid cell stays
        ("session", "uc"),
    )
    for p, fn in (("h3", H3FA), ("qb", QBFA), ("cg", CGFA))
)


# Rows only the array forms have: a NULL array, an empty one, k = 0, a method in capitals; the pentagon row is H3 only.
_FA = {"h3": H3FA, "qb": QBFA, "bng": BNFA, "cg": CGFA}


def _extra_array_rows(prefix, fn):
    both = ("session", "uc")
    return (
        Case(
            f"{prefix}_fill_array_null__{fn[4:]}",
            fn,
            (None, *_consts(prefix, 1, "mean", 2.0)),
            "null",
            None,
            both,
        ),
        Case(
            f"{prefix}_fill_array_empty__{fn[4:]}",
            fn,
            ([], *_consts(prefix, 1, "mean", 2.0)),
            "null",
            None,
            both,
        ),
        # k = 0 fills nothing: the NULL cell stays NULL and the valid cells pass through
        Case(
            f"{prefix}_fill_array_k0__{fn[4:]}",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix, 0, "mean", 2.0)),
            "cellfill",
            lambda p=prefix: {
                _GRIDS[p][1]()[0]: None,
                _GRIDS[p][1]()[1][0]: 10.0,
                _GRIDS[p][1]()[1][1]: 20.0,
            },
            both,
        ),
        # the method is read in any case, as the aggregate reads it
        Case(
            f"{prefix}_fill_array_method_case__{fn[4:]}",
            fn,
            (Build(f"{prefix}_fill_mean"), *_consts(prefix, 1, "MEAN", 2.0)),
            "cellfill",
            _expect(prefix, "mean"),
            both,
        ),
    )


for _prefix, _fn in _FA.items():
    CELL_AGGREGATE += _extra_array_rows(_prefix, _fn)

# A radius whose ring ((2k+1)^2 neighbours of one NULL cell) is over the 64 MiB cap is refused with a Fix BEFORE any ring
# is built, through a large k; many NULL cells at a small k are not refused (the fill holds one ring at a time). H3 is not listed: its UC form is a SQL kernel over the product's h3_kringdistances, which cannot pre-check.
CELL_AGGREGATE += tuple(
    Case(
        f"uc_{p}_cellfill_array_over_cap",
        fn,
        (Build(f"{p}_fill_mean"), *_consts(p, 100_000, "mean", 2.0)),
        "uc_refused",
        None,
        ("uc",),
    )
    for p, fn in (("qb", QBFA), ("bng", BNFA), ("cg", CGFA))
)


@builder("h3_fill_pentagon")
def _h3_fill_pentagon(tmpdir):
    """A pentagon with no value and its five neighbours valued 1..5: the ring around a pentagon has five cells, not six."""
    import h3

    pent = h3.get_pentagons(8)[0]
    ring = sorted(h3.str_to_int(c) for c in h3.grid_ring(pent, 1))
    assert len(ring) == 5
    return [(h3.str_to_int(pent), None)] + [
        (c, float(i + 1)) for i, c in enumerate(ring)
    ]


def _h3_pentagon_expect():
    import h3

    pent = h3.get_pentagons(8)[0]
    ring = sorted(h3.str_to_int(c) for c in h3.grid_ring(pent, 1))
    want = {h3.str_to_int(pent): 3.0}  # the mean of 1..5
    want.update({c: float(i + 1) for i, c in enumerate(ring)})
    return want


CELL_AGGREGATE += (
    Case(
        "h3_fill_pentagon__h3_cellfill_array",
        H3FA,
        (Build("h3_fill_pentagon"), 1, "mean", 2.0),
        "cellfill",
        _h3_pentagon_expect,
        ("session", "uc"),
    ),
)


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


# ------------------------------------------------------------------------------------------------ BNG chip arrays
# `gbx_bng_cellunion_array` and `gbx_bng_cellintersection_array` are the array twins of the two chip aggregates: the
# aggregate's rows run on the session and on UC. The chips of one cell are an array; chips of several cells are a caller's
# mistake, and the array form answers NULL where the aggregate's fold is order dependent (pinned below).
BUA, BIA = "gbx_bng_cellunion_array", "gbx_bng_cellintersection_array"
CELL_AGGREGATE += tuple(
    replace(c, surfaces=("session", "uc"))
    for fn, prefix in ((BUA, "bu_agg"), (BIA, "bi_agg"))
    for c in twin(
        tuple(r for r in CELL_AGGREGATE if r.cid.startswith(prefix)), fn, Adapt()
    )
)


def _ewkb(geom):
    import shapely

    return shapely.to_wkb(shapely.set_srid(geom, 27700), include_srid=True)


@builder("chips_west_east_ewkb")
def _chips_west_east_ewkb(tmpdir):
    """The chips as gbx_bng_tessellate writes them on UC: EWKB with SRID 27700."""
    import shapely

    return [
        {**c, "chip": _ewkb(shapely.from_wkb(c["chip"]))}
        for c in _chips_west_east(tmpdir)
    ]


@builder("chips_two_cells")
def _chips_two_cells(tmpdir):
    return [
        _chip(_box(530000, 530600)),
        _chip(_box(530400, 531000), cell="TQ3180"),
    ]


@builder("chips_with_null_element")
def _chips_with_null_element(tmpdir):
    return [None, *_chips_west_east(tmpdir)]


@builder("chips_west_core_other_cell")
def _chips_west_core_other_cell(tmpdir):
    """A core chip of TQ3180 beside a chip of TQ3080: the first core chip names the polygon."""
    return [_chip(_box(530000, 530600)), _chip(None, core=True, cell="TQ3180")]


def _box_wkt_xy(x0, y0, x1, y1):
    import shapely

    return lambda: shapely.box(x0, y0, x1, y1).wkt


CELL_AGGREGATE += (
    Case(
        "bu_array_ewkb_chips",
        BUA,
        (Build("chips_west_east_ewkb"),),
        "geom_equals",
        _box_wkt(530000, 531000),
        ("session", "uc"),
    ),
    Case(
        "bi_array_ewkb_chips",
        BIA,
        (Build("chips_west_east_ewkb"),),
        "geom_equals",
        _box_wkt(530400, 530600),
        ("session", "uc"),
    ),
    # a NULL element beside chips is skipped
    Case(
        "bu_array_null_element",
        BUA,
        (Build("chips_with_null_element"),),
        "geom_equals",
        _box_wkt(530000, 531000),
        ("session", "uc"),
    ),
    Case(
        "bi_array_null_element",
        BIA,
        (Build("chips_with_null_element"),),
        "geom_equals",
        _box_wkt(530400, 530600),
        ("session", "uc"),
    ),
    Case("bu_array_null", BUA, (None,), "null", None, ("session", "uc")),
    Case("bi_array_empty", BIA, ([],), "null", None, ("session", "uc")),
    # chips of two cells: NULL (the aggregate folds them in the order they arrive)
    Case(
        "bu_array_two_cells",
        BUA,
        (Build("chips_two_cells"),),
        "null",
        None,
        ("session", "uc"),
    ),
    Case(
        "bi_array_two_cells",
        BIA,
        (Build("chips_two_cells"),),
        "null",
        None,
        ("session", "uc"),
    ),
    # a core chip stands for its own cell, whatever the other chips say
    Case(
        "bu_array_core_names_the_cell",
        BUA,
        (Build("chips_west_core_other_cell"),),
        "geom_equals",
        _box_wkt_xy(531000, 180000, 532000, 181000),
        ("session", "uc"),
    ),
)


def _ewkb_on_uc(row):
    """The array forms write their geometry as plain WKB in a session and as EWKB with SRID 27700 on UC (the SQL kernel
    ends in st_asewkb, as the UC chip functions write their chips). A geometry row becomes two: the session keeps the
    vertex check, the UC row also pins the SRID."""
    if (
        row.fn not in (BUA, BIA)
        or row.kind != "geom_equals"
        or row.surfaces != ("session", "uc")
    ):
        return (row,)
    want = row.expect
    return (
        replace(row, surfaces=("session",)),
        replace(
            row,
            cid=f"{row.cid}_ewkb",
            kind="geom_srid",
            expect=lambda: (want() if callable(want) else want, 27700),
            surfaces=("uc",),
        ),
    )


CELL_AGGREGATE = tuple(r for row in CELL_AGGREGATE for r in _ewkb_on_uc(row))


# ------------------------------------------------------------------------------------------------ recipes
# The statements a user writes at table scale, where no function is needed: the native route is the fastest form and has
# no 64 MiB array in it. Each is a recipe row (one whole statement, `{q}` the schema qualifier) judged against the same
# expectation as the aggregate it replaces. Only the H3 fill can run on the host (through the ring table of
# bench.product_stand_ins); the union recipes call the product geometry aggregate and a UC Python function, so the
# workspace pass runs them and a host test parses their text.

# C1: the H3 fill with no collect and no ceiling. `{ring}` is the number of rings (the distance cut), `{value}` the fill:
# the mean of the valid neighbours, or the inverse-distance mean.
_C1 = (
    "SELECT sort_array(collect_list(struct(cellid, value))) AS out FROM ("
    "WITH src AS (SELECT * FROM VALUES {rows} AS t(grp, cellid, value)), "
    "g AS (SELECT grp, cellid, max(value) AS value FROM src GROUP BY grp, cellid), "
    "holes AS (SELECT grp, cellid FROM g WHERE value IS NULL), "
    "nb AS (SELECT h.grp, h.cellid, n.cellid AS nb, n.distance AS d FROM holes h "
    "LATERAL VIEW inline(h3_kringdistances(h.cellid, {ring})) n WHERE n.distance >= 1), "
    "f AS (SELECT nb.grp, nb.cellid, avg(g.value) AS mean_v, "
    "sum(g.value * pow(nb.d, -2.0)) / sum(pow(nb.d, -2.0)) AS idw_v FROM nb "
    "JOIN g ON g.grp = nb.grp AND g.cellid = nb.nb AND g.value IS NOT NULL GROUP BY nb.grp, nb.cellid) "
    "SELECT g.grp, g.cellid, coalesce(g.value, f.{value}) AS value FROM g "
    "LEFT JOIN f ON f.grp = g.grp AND f.cellid = g.cellid) WHERE grp = 'a'"
)
_H3_C, _H3_R1A, _H3_R1B = 613196570331971583, 613196570340360191, 613196570344554495
_H3_R2 = 613196570401177599  # the first ring-2 cell of _H3_C, the one the builder h3_fill_idw uses


def _h3_rows(*pairs):
    return ", ".join(
        f"('a', CAST({c} AS BIGINT), CAST({'NULL' if v is None else v} AS DOUBLE))"
        for c, v in pairs
    )


CELL_AGGREGATE += (
    recipe(
        "recipe_h3_fill_mean",
        H3FA,
        _C1.format(
            rows=_h3_rows((_H3_C, None), (_H3_R1A, 10.0), (_H3_R1B, 20.0)),
            ring=1,
            value="mean_v",
        ),
        "cellfill",
        {_H3_C: 15.0, _H3_R1A: 10.0, _H3_R1B: 20.0},
    ),
    # two rings and the inverse-distance mean: (10 / 1 + 20 / 4) / (1 + 1 / 4) = 12
    recipe(
        "recipe_h3_fill_idw",
        H3FA,
        _C1.format(
            rows=_h3_rows((_H3_C, None), (_H3_R1A, 10.0), (_H3_R2, 20.0)),
            ring=2,
            value="idw_v",
        ),
        "cellfill",
        {_H3_C: 12.0, _H3_R1A: 10.0, _H3_R2: 20.0},
    ),
)

# C2: the union of quadbin cells in a large group, native. The cell polygon is the one Python call per cell.
_C2 = (
    "SELECT st_asbinary(st_union_agg(st_geomfromewkb({{q}}gbx_quadbin_aswkb(c)))) AS out "
    "FROM VALUES (CAST({a} AS BIGINT)), (CAST({b} AS BIGINT)) AS t(c)"
)

# C3: the union of the chips of each BNG cell, native; a core chip makes the answer the cell polygon.
_WEST = "0103000000010000000500000000000000503120410000000000f905410000000050312041000000004018064100000000a02c2041000000004018064100000000a02c20410000000000f9054100000000503120410000000000f90541"
_EAST = "0103000000010000000500000000000000703420410000000000f905410000000070342041000000004018064100000000c02f2041000000004018064100000000c02f20410000000000f9054100000000703420410000000000f90541"
_C3 = (
    "SELECT CASE WHEN bool_or(core) THEN st_asbinary(st_geomfromewkb({{q}}gbx_bng_aswkb(cellid))) "
    "ELSE st_asbinary(st_union_agg(st_geomfromewkb(chip))) END AS out FROM VALUES "
    "('TQ3080', false, X'{west}'), ('TQ3080', false, X'{east}') AS t(cellid, core, chip) GROUP BY cellid"
)
_C3_CORE = (
    "SELECT CASE WHEN bool_or(core) THEN st_asbinary(st_geomfromewkb({{q}}gbx_bng_aswkb(cellid))) "
    "ELSE st_asbinary(st_union_agg(st_geomfromewkb(chip))) END AS out FROM VALUES "
    "('TQ3080', false, X'{west}'), ('TQ3080', true, CAST(NULL AS BINARY)) AS t(cellid, core, chip) GROUP BY cellid"
)

CELL_AGGREGATE += (
    recipe(
        "recipe_qb_union_native",
        QUN,
        _C2.format(a=QB_C, b=QB_E),
        "geom_equals",
        _qb_union_wkt(QB_C, QB_E),
    ),
    recipe(
        "recipe_bng_chip_union_native",
        BUN,
        _C3.format(west=_WEST, east=_EAST),
        "geom_equals",
        _box_wkt(530000, 531000),
    ),
    recipe(
        "recipe_bng_chip_union_core",
        BUN,
        _C3_CORE.format(west=_WEST),
        "geom_equals",
        _box_wkt(530000, 531000),
    ),
)


# --- the custom grid as a column ---------------------------------------------------------------------------------------
# A grid held in a table is a column, not a constant: each group reads the grid of its own rows, at every level. Under a
# grid of 2000 m root cells (500 to a row) the same ids are other cells: 180531 is the east neighbour of 180530, but
# 181530 is two rows north, so the NULL cell takes 10 alone.
def _grid_sql(grid):
    """A custom grid struct as SQL text, typed as gbx_custom_grid builds it."""
    longs = ("bound_x_min", "bound_x_max", "bound_y_min", "bound_y_max")
    return "named_struct({})".format(
        ", ".join(f"'{k}', {v}{'L' if k in longs else ''}" for k, v in grid.items())
    )


_CG_C, (_CG_R1A, _CG_R1B), _ = _cg_rings()
_CG_COARSE = {**CUSTOM_GRID, "root_cell_size_x": 2000, "root_cell_size_y": 2000}


def _cg_grid_rows(key, grid):
    return ", ".join(
        f"('{key}', {c}L, CAST({'NULL' if v is None else v} AS DOUBLE), {_grid_sql(grid)})"
        for c, v in ((_CG_C, None), (_CG_R1A, 10.0), (_CG_R1B, 20.0))
    )


_CGF_GROUPS = (
    "SELECT out FROM (SELECT key, {{q}}gbx_custom_cellfill(cellid, value, grid) AS out FROM VALUES {rows} "
    "AS t(key, cellid, value, grid) GROUP BY key) WHERE key = '{key}'"
)
CELL_AGGREGATE += (
    recipe(
        "cgf_grid_column",
        CGF,
        "SELECT {{q}}gbx_custom_cellfill(cellid, value, grid) AS out FROM VALUES {rows} AS t(key, cellid, value, grid)".format(
            rows=_cg_grid_rows("a", CUSTOM_GRID)
        ),
        "cellfill",
        {_CG_C: 15.0, _CG_R1A: 10.0, _CG_R1B: 20.0},
        ONLY,
    ),
    # two groups, two grids: each fills through its own
    recipe(
        "cgf_grid_column_per_group_fine",
        CGF,
        _CGF_GROUPS.format(
            rows=_cg_grid_rows("a", _CG_COARSE)
            + ", "
            + _cg_grid_rows("b", CUSTOM_GRID),
            key="b",
        ),
        "cellfill",
        {_CG_C: 15.0, _CG_R1A: 10.0, _CG_R1B: 20.0},
        ONLY,
    ),
    recipe(
        "cgf_grid_column_per_group_coarse",
        CGF,
        _CGF_GROUPS.format(
            rows=_cg_grid_rows("a", _CG_COARSE)
            + ", "
            + _cg_grid_rows("b", CUSTOM_GRID),
            key="a",
        ),
        "cellfill",
        {_CG_C: 10.0, _CG_R1A: 10.0, _CG_R1B: 20.0},
        ONLY,
    ),
)
