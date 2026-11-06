"""Oracle table for the vector aggregates (`gbx_st_asmvt`, `gbx_st_coverageisvalid`, `gbx_st_coverageinvalidedges`):
inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test: an
MVT layer is decoded with mapbox_vector_tile and held to the features that went in (geometry in tile coordinates and
the attributes), and a coverage is judged by shapely's own `coverage_is_valid` and `coverage_invalid_edges`.

An aggregate's first argument is its group's rows; the arguments after it are the group's constants. A NULL geometry is
skipped. A group with no usable geometry is NULL for the coverage functions and an empty layer for the MVT.
"""

from dataclasses import replace

from databricks.labs.gbx.bench.oracle_tables._uc_tile import Adapt, twin
from databricks.labs.gbx.bench.oracles import BUILDERS, Build, Case, builder

ONLY = ("session",)
MVT, CVALID, CEDGES = (
    "gbx_st_asmvt",
    "gbx_st_coverageisvalid",
    "gbx_st_coverageinvalidedges",
)

# --- st_asmvt ----------------------------------------------------------------------------------------------------
POLY = "POLYGON((10 10, 100 10, 100 100, 10 100, 10 10))"
POINT = "POINT(5 7)"
A_ATTRS, B_ATTRS = {"name": "a", "n": 3}, {"name": "b", "n": 4}


def _wkb(wkt):
    import shapely

    return shapely.from_wkt(wkt).wkb


@builder("mvt_two_wkb")
def _mvt_two_wkb(tmpdir):
    return [(_wkb(POLY), A_ATTRS), (_wkb(POINT), B_ATTRS)]


@builder("mvt_two_wkt")
def _mvt_two_wkt(tmpdir):
    return [(POLY, A_ATTRS), (POINT, B_ATTRS)]


@builder("mvt_two_ewkt")
def _mvt_two_ewkt(tmpdir):
    return [("SRID=4326;" + POLY, A_ATTRS), ("SRID=4326;" + POINT, B_ATTRS)]


@builder("mvt_two_ewkb")
def _mvt_two_ewkb(tmpdir):
    import shapely

    def ewkb(wkt):
        return shapely.to_wkb(
            shapely.set_srid(shapely.from_wkt(wkt), 4326), include_srid=True
        )

    return [(ewkb(POLY), A_ATTRS), (ewkb(POINT), B_ATTRS)]


@builder("mvt_with_null_geom")
def _mvt_with_null_geom(tmpdir):
    return [(None, {"name": "x", "n": 9}), *_mvt_two_wkb(tmpdir)]


@builder("mvt_only_null_geom")
def _mvt_only_null_geom(tmpdir):
    return [(None, {"name": "x", "n": 9})]


TWO = {"layer": "lyr", "features": [(POLY, A_ATTRS), (POINT, B_ATTRS)]}
VECTOR_AGGREGATE = (
    Case(
        "asmvt_two_features", MVT, (Build("mvt_two_wkb"), "lyr"), "mvt_layer", TWO, ONLY
    ),
    # the four formats: WKT in tile coordinates gives the same layer
    Case("asmvt_wkt", MVT, (Build("mvt_two_wkt"), "lyr"), "mvt_layer", TWO, ONLY),
    Case("asmvt_ewkt", MVT, (Build("mvt_two_ewkt"), "lyr"), "mvt_layer", TWO, ONLY),
    Case("asmvt_ewkb", MVT, (Build("mvt_two_ewkb"), "lyr"), "mvt_layer", TWO, ONLY),
    Case(
        "asmvt_null_geom_skipped",
        MVT,
        (Build("mvt_with_null_geom"), "lyr"),
        "mvt_layer",
        TWO,
        ONLY,
    ),
    Case(
        "asmvt_layer_name",
        MVT,
        (Build("mvt_two_wkb"), "roads"),
        "mvt_layer",
        {"layer": "roads", "features": TWO["features"]},
        ONLY,
    ),
    Case(
        "asmvt_no_feature",
        MVT,
        (Build("mvt_only_null_geom"), "lyr"),
        "mvt_layer",
        {"layer": "lyr", "features": []},
        ONLY,
    ),
)


# --- the coverage functions ----------------------------------------------------------------------------------------
# a and b share the edge x = 1; ov overlaps a on x 0.5..1; gap leaves a 0.1 wide gap between itself and a.
def _box(x0, x1):
    import shapely

    return shapely.box(x0, 0, x1, 1)


@builder("cov_clean")
def _cov_clean(tmpdir):
    return [_box(0, 1).wkb, _box(1, 2).wkb]


@builder("cov_clean_wkt")
def _cov_clean_wkt(tmpdir):
    return [_box(0, 1).wkt, _box(1, 2).wkt]


@builder("cov_overlap")
def _cov_overlap(tmpdir):
    return [_box(0, 1).wkb, _box(0.5, 1.5).wkb]


@builder("cov_gap")
def _cov_gap(tmpdir):
    return [_box(0, 1).wkb, _box(1.1, 2).wkb]


@builder("cov_null")
def _cov_null(tmpdir):
    return [None, None]


def _ewkb_box(x0, x1, srid):
    import shapely

    return shapely.to_wkb(shapely.set_srid(_box(x0, x1), srid), include_srid=True)


def _edges_of_overlap_wkt():
    import shapely

    return shapely.line_merge(
        shapely.union_all(shapely.coverage_invalid_edges([_box(0, 1), _box(0.5, 1.5)]))
    ).wkt


VECTOR_AGGREGATE += (
    Case("covvalid_clean", CVALID, (Build("cov_clean"), 0.0), "exact", True, ONLY),
    Case("covvalid_wkt", CVALID, (Build("cov_clean_wkt"), 0.0), "exact", True, ONLY),
    Case("covvalid_overlap", CVALID, (Build("cov_overlap"), 0.0), "exact", False, ONLY),
    # a gap narrower than gap_width is invalid; with a width of 0 it is not a gap
    Case(
        "covvalid_gap_narrower", CVALID, (Build("cov_gap"), 0.2), "exact", False, ONLY
    ),
    Case("covvalid_gap_ignored", CVALID, (Build("cov_gap"), 0.0), "exact", True, ONLY),
    Case(
        "covvalid_gap_width_left_out", CVALID, (Build("cov_gap"),), "exact", True, ONLY
    ),
    Case("covvalid_empty", CVALID, (Build("cov_null"), 0.0), "null", None, ONLY),
    Case(
        "covedges_overlap",
        CEDGES,
        (Build("cov_overlap"), 0.0),
        "geom_equals",
        _edges_of_overlap_wkt,
        ONLY,
    ),
    # a valid coverage has no invalid edge: an empty multi-line, as WKB
    Case(
        "covedges_clean",
        CEDGES,
        (Build("cov_clean"), 0.0),
        "exact",
        lambda: __import__("shapely").from_wkt("MULTILINESTRING EMPTY").wkb,
        ONLY,
    ),
    Case("covedges_empty", CEDGES, (Build("cov_null"), 0.0), "null", None, ONLY),
)


# --- the array twins ----------------------------------------------------------------------------------------------
# `gbx_st_asmvt_array`, `gbx_st_coverageisvalid_array` and `gbx_st_coverageinvalidedges_array` are the scalar twins of the
# three aggregates: the group is one ARRAY, and the answer is the aggregate's, so each twin row is the aggregate's row run
# on the session and on UC with the same expectation. Not twinned: the rows whose members are WKT text (a UC array holds
# WKB, a BINARY), which stay aggregate rows. The attributes of a feature are JSON text, as in every UC form (a struct of
# any fields has no UC type).
MVTA, CVALIDA, CEDGESA = (
    "gbx_st_asmvt_array",
    "gbx_st_coverageisvalid_array",
    "gbx_st_coverageinvalidedges_array",
)
BOTH = ("session", "uc")


def _as_features(builder_name):
    """The `(geom, attrs)` members of an aggregate group as the array's structs: the attributes as JSON text."""
    import json

    def build(tmpdir):
        return [
            {"geom": g, "attrs": None if a is None else json.dumps(a)}
            for g, a in BUILDERS[builder_name](tmpdir)
        ]

    return build


for _n in ("mvt_two_wkb", "mvt_with_null_geom", "mvt_only_null_geom"):
    builder(f"{_n}__features")(_as_features(_n))


def _feature_args(args):
    return (Build(f"{args[0].name}__features"), *args[1:])


VECTOR_AGGREGATE += tuple(
    replace(c, surfaces=BOTH)
    for c in twin(
        tuple(VECTOR_AGGREGATE),
        MVTA,
        Adapt(args=_feature_args),
        "asmvt_two_features",
        "asmvt_null_geom_skipped",
        "asmvt_layer_name",
        "asmvt_no_feature",
    )
) + tuple(
    replace(c, surfaces=BOTH)
    for fn, cids in (
        (CVALIDA, [c.cid for c in VECTOR_AGGREGATE if c.fn == CVALID]),
        (CEDGESA, [c.cid for c in VECTOR_AGGREGATE if c.fn == CEDGES]),
    )
    for c in twin(
        tuple(VECTOR_AGGREGATE),
        fn,
        Adapt(),
        *[x for x in cids if x != "covvalid_wkt"],
    )
)

# the invalid edges of the overlapping pair cov_overlap, shapely's own answer written out
OVERLAP_EDGES = (
    "MULTILINESTRING ((0 0, 0.5 0), (0.5 1, 0 1), (0.5 0, 1 0), (0.5 1, 0.5 0), (1 1, 0.5 1), "
    "(1 0, 1.5 0), (1 0, 1 1), (1.5 1, 1 1))"
)

# Rows only the UC form has: a NULL array is NULL, and an empty one has no usable member; a member that is no geometry is
# NULL (an aggregate raises); the attributes must be a JSON object; a gap width that is no usable number raises.
VECTOR_AGGREGATE += (
    Case("uc_asmvt_array_null", MVTA, (None, "lyr"), "null", None, ("uc",)),
    Case(
        "uc_asmvt_array_empty",
        MVTA,
        ([], "lyr"),
        "mvt_layer",
        {"layer": "lyr", "features": []},
        ("uc",),
    ),
    Case(
        "uc_asmvt_array_no_attrs",
        MVTA,
        ([{"geom": _wkb(POINT), "attrs": None}], "lyr"),
        "mvt_layer",
        {"layer": "lyr", "features": [(POINT, {})]},
        ("uc",),
    ),
    Case(
        "uc_asmvt_array_attrs_not_json",
        MVTA,
        ([{"geom": _wkb(POINT), "attrs": "{1, a}"}], "lyr"),
        "uc_refused",
        "JSON",
        ("uc",),
    ),
    Case("uc_covvalid_array_null", CVALIDA, (None, 0.0), "null", None, ("uc",)),
    Case("uc_covvalid_array_empty", CVALIDA, ([], 0.0), "null", None, ("uc",)),
    Case(
        "uc_covvalid_array_member_not_a_geometry",
        CVALIDA,
        ([b"not a geometry", _box(0, 1).wkb], 0.0),
        "null",
        None,
        ("uc",),
    ),
    Case(
        "uc_covvalid_array_bad_gap",
        CVALIDA,
        ([_box(0, 1).wkb], -1.0),
        "uc_refused",
        "gap_width",
        ("uc",),
    ),
    Case("uc_covedges_array_null", CEDGESA, (None, 0.0), "null", None, ("uc",)),
    Case(
        "uc_covedges_array_bad_gap",
        CEDGESA,
        ([_box(0, 1).wkb], float("nan")),
        "uc_refused",
        "gap_width",
        ("uc",),
    ),
    # the SRID of the members is the SRID of the edges
    Case(
        "uc_covedges_array_keeps_srid",
        CEDGESA,
        ([_ewkb_box(0, 1, 3857), _ewkb_box(0.5, 1.5, 3857)], 0.0),
        "geom_srid",
        (OVERLAP_EDGES, 3857),
        ("uc",),
    ),
)
