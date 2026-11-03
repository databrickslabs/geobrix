"""Oracle table for the vector aggregates (`gbx_st_asmvt`, `gbx_st_coverageisvalid`, `gbx_st_coverageinvalidedges`):
inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test: an
MVT layer is decoded with mapbox_vector_tile and held to the features that went in (geometry in tile coordinates and
the attributes), and a coverage is judged by shapely's own `coverage_is_valid` and `coverage_invalid_edges`.

An aggregate's first argument is its group's rows; the arguments after it are the group's constants. A NULL geometry is
skipped. A group with no usable geometry is NULL for the coverage functions and an empty layer for the MVT.
"""

from databricks.labs.gbx.bench.oracles import Build, Case, builder

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
