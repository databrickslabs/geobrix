"""Oracle table for the raster aggregates (`gbx_rst_*_agg`) and the scalar twins of the cell rasterizers
(`gbx_rst_<grid>_rasterize`): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test:
numpy for the per-pixel statistics of `oracle_refs.combine`, inverse-distance weights, bins and a plane; shapely for the
burn of a feature; the h3, quadbin and OS grid definitions (and the custom grid's numbering) for the cell a pixel centre
is in (`oracle_checks._pixel_cells`). The rows of `gbx_rst_merge_agg` and `gbx_rst_bng_rasterize_agg` are those of
MVP_A, which the shape test also runs through every aggregate shape.

An aggregate's first argument is its group's rows; the arguments after it are the group's constants. The aggregates
return the GeoTIFF bytes, so a group with no usable member is NULL (bytes have no place for an error tile). A tile
that cannot be read, and an error tile, are skipped; tiles of different shapes raise. A group over the memory budget
(the read cap) is folded by streaming; one streaming cannot cover raises with a Fix naming the vrt route.

The cell rasterizers: a repeated cell burns its LARGEST value, whatever order the rows arrive in; a NULL value in a typed
column never wins against a number, and a cell with nothing but NULL burns NaN; a NULL cell id is skipped, except that a
BIGINT id column holding a NULL reaches an aggregate as float64, so ids past 2**53 would be rounded to other cells: that
raises. The scalar twins take the group as an array, where a NULL cell id is simply skipped.
"""

from dataclasses import replace

from databricks.labs.gbx.bench.oracle_tables._uc_tile import Adapt, recipe, twin
from databricks.labs.gbx.bench.oracle_tables.grid_scalar import (
    CUSTOM_GRID,
    H3_CELL,
    QB_C,
    cg_id,
)
from databricks.labs.gbx.bench.oracles import (
    BUILDERS,
    CANVAS,
    MVP_A,
    NAN,
    OMITTED,
    UPSTREAM_ERROR,
    Build,
    Case,
    _build,
    _gtiff,
    _tile,
    builder,
)

ONLY = ("session",)


def _surfaces(fn):
    """The surfaces of a scalar twin's rows: the H3 twin is also a Unity Catalog function."""
    return ("session", "uc") if fn == "gbx_rst_h3_rasterize" else ONLY


MA, BR = "gbx_rst_merge_agg", "gbx_rst_bng_rasterize_agg"
CA, FB, DB, RZ = (
    "gbx_rst_combineavg_agg",
    "gbx_rst_frombands_agg",
    "gbx_rst_derivedband_agg",
    "gbx_rst_rasterize_agg",
)
GP, BP, DT = (
    "gbx_rst_gridfrompoints_agg",
    "gbx_rst_binpoints_agg",
    "gbx_rst_dtmfromgeoms_agg",
)
H3A, QBA, CGA = (
    "gbx_rst_h3_rasterize_agg",
    "gbx_rst_quadbin_rasterize_agg",
    "gbx_rst_custom_rasterize_agg",
)
MT, BT = "gbx_rst_merge_tvf", "gbx_rst_bng_rasterize_tvf"
BRT, H3T, QBT, CGT = (
    "gbx_rst_bng_rasterize",
    "gbx_rst_h3_rasterize",
    "gbx_rst_quadbin_rasterize",
    "gbx_rst_custom_rasterize",
)

# --- the raster aggregates over tiles -------------------------------------------------------------------------------
# aa_stack (raster_array_op): three aligned 8 x 6 tiles on a 10 m grid with NoData holes.
STACK = Build("aa_stack")


@builder("ra_sum_pair")
def _sum_pair(tmpdir):
    import numpy as np

    a = np.arange(12, dtype="float64").reshape(1, 3, 4)
    return [_tile(_gtiff(a, "EPSG:3857")), _tile(_gtiff(a * 10 + 1, "EPSG:3857"))]


@builder("ra_bands_unordered")
def _bands_unordered(tmpdir):
    """(tile, band_index) rows: band 3, 1 and 2 in that order, so the stack must be sorted by the index."""
    import numpy as np

    base = np.arange(12, dtype="float64").reshape(1, 3, 4)
    tiles = [_tile(_gtiff(base + 100 * k, "EPSG:3857")) for k in (1, 2, 3)]
    return [(tiles[2], 3), (tiles[0], 1), (tiles[1], 2)]


@builder("ra_bands_null_index")
def _bands_null_index(tmpdir):
    rows = _bands_unordered(tmpdir)
    return [*rows, (rows[0][0], None)]  # a NULL index skips the row


@builder("ra_far_apart")
def _far_apart(tmpdir):
    """Two 4 x 4 tiles on a 10 m grid, 10^7 m apart: a mosaic of 10^12 pixels, nearly all NoData."""
    import numpy as np

    px = np.arange(16, dtype="float64").reshape(1, 4, 4)
    return [
        _tile(_gtiff(px, "EPSG:3857", origin=(0.0, 40.0))),
        _tile(_gtiff(px, "EPSG:3857", origin=(1.0e7, 1.0e7))),
    ]


@builder("ra_stack_misshaped")
def _misshaped(tmpdir):
    import numpy as np

    return [
        _build("aa_stack", tmpdir)[0],
        _tile(_gtiff(np.zeros((1, 5, 7)), "EPSG:3857")),
    ]


SUM2 = (
    "def sum2(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs): "
    "out_ar[:] = in_ar[0] + in_ar[1]"
)

RASTER_AGGREGATE = (
    # --- the rows of the two aggregates that MVP_A also runs through every shape
    *(c for c in MVP_A if c.fn in (MA, BR)),
    # A group whose members add up to more than the memory budget is folded by streaming, so the result is the
    # unconstrained one (READ_CAP_ROWS lowers the cap to 3000 bytes; the mosaic of quad_tiles is 4 KiB, its members
    # 1 KiB each, the stack's members 192 bytes each)
    Case(
        "merge_agg_streams_under_budget",
        MA,
        (Build("quad_tiles"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    Case(
        "combineavg_agg_streams_under_budget",
        CA,
        (STACK,),
        "ref",
        "combine_avg",
        ONLY,
    ),
    # What streaming cannot cover raises with a Fix: never an executor OOM, never a NULL (which reads as an empty group).
    # Tiles 10^7 m apart make a mosaic past what a merge can stream (the Fix names the VRT-mosaic route, mint_vrt); and a
    # tile in another CRS over the read cap (READ_CAP_ROWS: 150 bytes, a member of aa_mixed_crs is 192) must be decoded
    # whole to be reprojected (the Fix names merge_by_key(method="vrt"): the members need no RAM there; as the members are
    # not on one CRS, it first says to bring them to one with gbx_rst_transform and gbx_rst_resample_to_res).
    Case(
        "merge_agg_over_budget_refused",
        MA,
        (Build("ra_far_apart"),),
        "raises",
        "mint_vrt",
        ONLY,
    ),
    Case(
        "merge_agg_member_over_cap_refused",
        MA,
        (Build("aa_mixed_crs"),),
        "raises",
        "merge_by_key",
        ONLY,
    ),
    # The encoded result of a bytes aggregate comes back whole in one row, so it is bounded by the same cap (READ_CAP_ROWS:
    # 2000 bytes; the streamed COG of quad_tiles is about 2.4 KB): over it the aggregate raises with the VRT-mosaic route,
    # not merge_by_key, which returns BINARY bounded by the same cap.
    Case(
        "merge_agg_output_over_cap_refused",
        MA,
        (Build("quad_tiles"),),
        "raises",
        "mint_vrt",
        ONLY,
    ),
    # --- combineavg: the mean of the valid values at each pixel, against numpy
    Case("ca_agg_stack", CA, (STACK,), "ref", "combine_avg", ONLY),
    Case(
        "ca_agg_corrupt_member",
        CA,
        (Build("aa_stack_corrupt"),),
        "ref",
        "combine_avg",
        ONLY,
    ),
    Case(
        "ca_agg_error_member",
        CA,
        (Build("aa_stack_error"),),
        "ref",
        "combine_avg",
        ONLY,
    ),
    Case("ca_agg_virtual", CA, (Build("aa_stack_paths"),), "ref", "combine_avg", ONLY),
    Case(
        "ca_agg_all_error",
        CA,
        (Build("tile_error_pair"),),
        "error_group",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "ca_agg_misshaped", CA, (Build("ra_stack_misshaped"),), "raises", "align", ONLY
    ),
    # --- frombands: the first band of each tile, in ascending band_index order whatever the arrival order
    Case(
        "fb_agg_order", FB, (Build("ra_bands_unordered"),), "ref", "agg_frombands", ONLY
    ),
    Case(
        "fb_agg_null_index",
        FB,
        (Build("ra_bands_null_index"),),
        "ref",
        "agg_frombands",
        ONLY,
    ),
    # --- derivedband: a pixel function over the group's tiles as bands
    Case(
        "db_agg_sum",
        DB,
        (Build("ra_sum_pair"), SUM2, "sum2"),
        "ref",
        "agg_derived_sum",
        ONLY,
    ),
    # a member over the read cap raises with the Fix, as the merge and the combine do: never a NULL, which reads as an
    # empty group (READ_CAP_ROWS: 20 bytes; a member of ra_sum_pair is 48)
    Case(
        "db_agg_member_over_cap_refused",
        DB,
        (Build("ra_sum_pair"), SUM2, "sum2"),
        "raises",
        "read cap",
        ONLY,
    ),
)


# --- rasterize_agg: features burned in the canonical order (WKB bytes, then value), the last write winning ---------
def _feature(x0, x1, value):
    import shapely

    return (shapely.box(x0, 0, x1, 2).wkb, value)


@builder("rz_overlap")
def _rz_overlap(tmpdir):
    # A covers x 0..3, B x 2..4: they overlap on 2..3. A NULL geometry and a NULL value are skipped.
    return [
        _feature(0, 3, 1.0),
        _feature(2, 4, 2.0),
        (None, 9.0),
        (_feature(0, 1, 0)[0], None),
    ]


@builder("rz_overlap_rev")
def _rz_overlap_rev(tmpdir):
    return _rz_overlap(tmpdir)[::-1]


@builder("rz_nan_feature")
def _rz_nan_feature(tmpdir):
    # a NaN-valued feature over x 1..4: its WKB sorts after every other, so a burned NaN would be the last write
    return [*_rz_overlap(tmpdir), _feature(1, 4, NAN)]


@builder("rz_none")
def _rz_none(tmpdir):
    return [(None, 1.0)]


# xmin, ymin, xmax, ymax, width_px, height_px, out_srid: 4 x 2 pixels of 1 unit
RZ_CANVAS = (0.0, 0.0, 4.0, 2.0, 4, 2, 4326)
RASTER_AGGREGATE += (
    Case(
        "rz_agg_overlap",
        RZ,
        (Build("rz_overlap"), *RZ_CANVAS),
        "ref",
        "agg_rasterize",
        ONLY,
    ),
    Case(
        "rz_agg_overlap_rev",
        RZ,
        (Build("rz_overlap_rev"), *RZ_CANVAS),
        "ref",
        "agg_rasterize",
        ONLY,
    ),
    # a NaN value is a missing one, like a NULL: the feature is skipped at every level
    Case(
        "rz_agg_nan_skipped",
        RZ,
        (Build("rz_nan_feature"), *RZ_CANVAS),
        "ref",
        "agg_rasterize",
        ONLY,
    ),
    Case("rz_agg_empty", RZ, (Build("rz_none"), *RZ_CANVAS), "null", None, ONLY),
)


# --- gridfrompoints_agg, binpoints_agg, dtmfromgeoms_agg: interpolation and binning against numpy -------------------
@builder("gp_points")
def _gp_points(tmpdir):
    return [
        ("POINT(0.25 0.5)", 10.0),
        ("POINT(1.5 0.5)", 20.0),
        ("POINT(1.0 0.5)", None),
        (None, 5.0),
    ]


@builder("gp_none")
def _gp_none(tmpdir):
    return [(None, 1.0)]


GP_CANVAS = (
    0.0,
    0.0,
    2.0,
    1.0,
    2,
    1,
)  # xmin, ymin, xmax, ymax, width_px, height_px: two 1 x 1 pixels
RASTER_AGGREGATE += (
    # IDW at the pixel centres: (0.5, 0.5) weighs the points at distance 0.25 and 1.0 by 16 and 1. A NULL value and a NULL
    # point are dropped.
    Case(
        "gp_agg_idw",
        GP,
        (Build("gp_points"), *GP_CANVAS, 4326),
        "ref",
        "agg_gridfrompoints",
        ONLY,
    ),
    Case(
        "gp_agg_power_max_pts",
        GP,
        (Build("gp_points"), *GP_CANVAS, 4326, 1.0, 1),
        "ref",
        "agg_gridfrompoints",
        ONLY,
    ),
    Case("gp_agg_empty", GP, (Build("gp_none"), *GP_CANVAS, 4326), "null", None, ONLY),
    # out_srid left NULL and out_crs set: the output is in the CRS asked for
    Case(
        "gp_agg_out_crs",
        GP,
        (Build("gp_points"), *GP_CANVAS, None, 2.0, 12, "EPSG:3857"),
        "tile_meta",
        {"crs": "EPSG:3857", "width": 2, "height": 1},
        ONLY,
    ),
    Case(
        "gp_agg_out_srid_and_crs",
        GP,
        (Build("gp_points"), *GP_CANVAS, 4326, 2.0, 12, "EPSG:3857"),
        "raises",
        "out_srid",
        ONLY,
    ),
)


@builder("bp_points")
def _bp_points(tmpdir):
    return [(0.5, 0.5, 1.0), (0.6, 0.4, 5.0), (1.5, 0.5, 7.0)]


@builder("bp_outside")
def _bp_outside(tmpdir):
    return [(9.0, 9.0, 1.0)]


BP_CANVAS = (0.0, 0.0, 2.0, 1.0, 2, 1, 4326)  # extent, width_px, height_px, srid
RASTER_AGGREGATE += (
    Case(
        "bp_agg_max",
        BP,
        (Build("bp_points"), *BP_CANVAS, "max"),
        "ref",
        "agg_binpoints",
        ONLY,
    ),
    Case(
        "bp_agg_mean",
        BP,
        (Build("bp_points"), *BP_CANVAS, "mean"),
        "ref",
        "agg_binpoints",
        ONLY,
    ),
    # the statistic left out is the maximum
    Case(
        "bp_agg_default_statistic",
        BP,
        (Build("bp_points"), *BP_CANVAS),
        "ref",
        "agg_binpoints",
        ONLY,
    ),
    # points outside the extent leave every pixel NoData
    Case(
        "bp_agg_outside",
        BP,
        (Build("bp_outside"), *BP_CANVAS, "max"),
        "ref",
        "agg_binpoints",
        ONLY,
    ),
)


# --- binpoints as partial / finalize --------------------------------------------------------------------------------
# `gbx_rst_binpoints_partial` gives the pixel a point falls in (a SQL expression, the session's half-open floor rule);
# a native GROUP BY reduces the points of each pixel; `gbx_rst_binpoints_finalize` writes the reduced pixels as the tile
# `gbx_rst_binpoints_agg` makes for the same points. The partial's rows pin the floor rule (the east and south edges are
# left out, the north and west edges are in); the finalize rows are the session aggregate's rows with the group reduced
# (twin) plus its own; the recipe rows are the whole statement against the session's reference over the same points.
BPP, BPF = "gbx_rst_binpoints_partial", "gbx_rst_binpoints_finalize"
BOTH = ("session", "uc")
_P10 = (0.0, 0.0, 10.0, 10.0, 10, 10)  # extent and size: ten 1 x 1 pixels each way


def _partial(cid, args, expect, kind="exact"):
    return Case(cid, BPP, args, kind, expect, BOTH)


RASTER_AGGREGATE += (
    # a point in a pixel: the column counts from the west, the row from the north
    _partial("bp_partial_cells", (1.5, 8.5, *_P10), (1, 1)),
    _partial("bp_partial_south_west_pixel", (0.5, 0.5, *_P10), (0, 9)),
    # the west and north edges belong to their pixel
    _partial("bp_partial_west_edge", (0.0, 5.0, *_P10), (0, 5)),
    _partial("bp_partial_north_edge", (5.0, 10.0, *_P10), (5, 0)),
    _partial("bp_partial_north_west_corner", (0.0, 10.0, *_P10), (0, 0)),
    # the east and south edges are in no pixel (half-open): never clamped into the last one
    _partial("bp_partial_east_edge", (10.0, 5.0, *_P10), None, "null"),
    _partial("bp_partial_south_edge", (5.0, 0.0, *_P10), None, "null"),
    _partial("bp_partial_south_east_corner", (10.0, 0.0, *_P10), None, "null"),
    # just inside the east edge is the last column
    _partial("bp_partial_inside_east_edge", (9.999999, 5.0, *_P10), (9, 5)),
    _partial("bp_partial_just_west_of_canvas", (-1e-9, 5.0, *_P10), None, "null"),
    _partial("bp_partial_outside_x", (11.0, 5.0, *_P10), None, "null"),
    _partial("bp_partial_outside_y", (5.0, 11.0, *_P10), None, "null"),
    _partial("bp_partial_negative", (-0.5, 5.0, *_P10), None, "null"),
    # a canvas away from the origin, with negative coordinates
    _partial(
        "bp_partial_negative_canvas",
        (-4.5, 4.5, -5.0, -5.0, 5.0, 5.0, 10, 10),
        (0, 0),
    ),
    # one pixel: any point inside is (0, 0), the far edges are out
    _partial("bp_partial_one_pixel", (0.5, 0.5, 0.0, 0.0, 1.0, 1.0, 1, 1), (0, 0)),
    _partial(
        "bp_partial_one_pixel_east_edge",
        (1.0, 0.5, 0.0, 0.0, 1.0, 1.0, 1, 1),
        None,
        "null",
    ),
    # pixels that are not square: 8 x 2 over 4 x 2 pixels
    _partial("bp_partial_oblong", (5.0, 0.5, 0.0, 0.0, 8.0, 2.0, 4, 2), (2, 1)),
    # a coordinate that is no number is in no pixel, as in the session (a NaN floors to 0 in SQL: it must not)
    _partial("bp_partial_nan_x", (NAN, 5.0, *_P10), None, "null"),
    _partial("bp_partial_nan_y", (5.0, NAN, *_P10), None, "null"),
    _partial("bp_partial_infinite_x", (float("inf"), 5.0, *_P10), None, "null"),
    _partial("bp_partial_negative_infinite_y", (5.0, -float("inf"), *_P10), None, "null"),
    _partial("bp_partial_null_x", (None, 5.0, *_P10), None, "null"),
    _partial("bp_partial_null_y", (5.0, None, *_P10), None, "null"),
    _partial("bp_partial_null_extent", (5.0, 5.0, None, 0.0, 10.0, 10.0, 10, 10), None, "null"),
    _partial("bp_partial_null_size", (5.0, 5.0, 0.0, 0.0, 10.0, 10.0, 10, None), None, "null"),
    # a wrong canvas raises with a Fix, whatever the point is
    _partial("bp_partial_zero_width", (5.0, 5.0, 0.0, 0.0, 10.0, 10.0, 0, 10), "above 0", "raises"),
    _partial("bp_partial_inverted_extent", (5.0, 5.0, 10.0, 0.0, 0.0, 10.0, 10, 10), "xmax above xmin", "raises"),
    _partial("bp_partial_empty_extent", (5.0, 5.0, 0.0, 10.0, 10.0, 10.0, 10, 10), "ymax above ymin", "raises"),
)  # fmt: skip


# the finalize rows of the session aggregate's rows: the group is reduced to its pixels first (the numpy reference
# `oracle_refs.bin_statistic`), then written; the reference is the pixels written out
def _register_cells(points, stat, grid):
    """The builder name of the reduced pixels (col, row, value) of a points builder under a statistic, on the grid
    (xmin, ymin, xmax, ymax, width_px, height_px)."""
    name = f"bpc_{points}_{stat}_{'_'.join(str(g) for g in grid)}"
    if name not in BUILDERS:

        def reduce(tmpdir, points=points, stat=stat, grid=grid):
            from databricks.labs.gbx.bench import oracle_refs

            pts = BUILDERS[points](tmpdir)
            out, mask = oracle_refs.bin_statistic(
                [p[0] for p in pts],
                [p[1] for p in pts],
                [p[2] for p in pts],
                grid,
                stat,
            )
            return [
                (int(c), int(r), float(out[r, c])) for r, c in zip(*(~mask).nonzero())
            ]

        BUILDERS[name] = reduce
    return name


def _finalize_args(args):
    points, canvas = args[0].name, args[1:8]
    stat = args[8] if len(args) > 8 else "max"
    return (Build(_register_cells(points, stat, tuple(canvas[:6]))), *canvas)


RASTER_AGGREGATE += tuple(
    replace(c, surfaces=BOTH)
    for c in twin(
        tuple(r for r in RASTER_AGGREGATE if r.cid.startswith("bp_agg_")),
        BPF,
        Adapt(args=_finalize_args, expect=lambda e: "binpoints_cells"),
    )
)

_F2 = (0.0, 0.0, 2.0, 1.0, 2, 1, 4326)  # two 1 x 1 pixels


def _fin(cid, cells, kind, expect, *canvas):
    return Case(cid, BPF, (cells, *(canvas or _F2)), kind, expect, BOTH)


RASTER_AGGREGATE += (
    _fin("bp_fin_pixels", [(0, 0, 1.5), (1, 0, 2.5)], "ref", "binpoints_cells"),
    # a pixel in no struct is NoData
    _fin("bp_fin_one_pixel", [(1, 0, 2.5)], "ref", "binpoints_cells"),
    # nothing reduced (the points were all outside the canvas): an all-NoData tile, as the aggregate gives
    _fin("bp_fin_empty", [], "ref", "binpoints_cells"),
    # a NULL or NaN value leaves the pixel NoData; a NULL element and one with a NULL key are skipped
    _fin("bp_fin_null_value", [(0, 0, None), (1, 0, 3.0)], "ref", "binpoints_cells"),
    _fin("bp_fin_nan_value", [(0, 0, NAN), (1, 0, 3.0)], "ref", "binpoints_cells"),
    _fin("bp_fin_null_element", [None, (1, 0, 3.0)], "ref", "binpoints_cells"),
    _fin("bp_fin_null_key", [(None, 0, 9.0), (0, None, 9.0), (1, 0, 3.0)], "ref", "binpoints_cells"),
    # a pixel given twice keeps its largest value, whatever the order
    _fin("bp_fin_repeated_pixel", [(0, 0, 1.0), (0, 0, 4.0), (0, 0, 2.0)], "ref", "binpoints_cells"),
    _fin("bp_fin_negative_value", [(0, 0, -3.5), (1, 0, 0.0)], "ref", "binpoints_cells"),
    _fin(
        "bp_fin_props",
        [(0, 0, 1.0)],
        "tile_meta",
        {"crs": "EPSG:4326", "dtype": "float32", "width": 2, "height": 1, "count": 1, "nodata": -9999.0, "materialized": True},
    ),
    # cells from another canvas raise with a Fix naming the partial, never drop quietly
    _fin("bp_fin_cell_outside_canvas", [(2, 0, 1.0)], "raises", "outside the 2 x 1 canvas"),
    _fin("bp_fin_row_outside_canvas", [(0, 1, 1.0)], "raises", "outside the 2 x 1 canvas"),
    _fin("bp_fin_negative_cell", [(-1, 0, 1.0)], "raises", "outside the 2 x 1 canvas"),
    _fin("bp_fin_zero_width", [(0, 0, 1.0)], "raises", "above 0", 0.0, 0.0, 2.0, 1.0, 0, 1, 4326),
    _fin("bp_fin_inverted_extent", [(0, 0, 1.0)], "raises", "xmax above xmin", 2.0, 0.0, 0.0, 1.0, 2, 1, 4326),
    Case("bp_fin_null_cells", BPF, (None, *_F2), "null", None, BOTH),
    Case("bp_fin_null_extent", BPF, ([(0, 0, 1.0)], None, 0.0, 2.0, 1.0, 2, 1, 4326), "null", None, BOTH),
    Case("bp_fin_null_srid", BPF, ([(0, 0, 1.0)], 0.0, 0.0, 2.0, 1.0, 2, 1, None), "null", None, BOTH),
    # a canvas over the cap is refused before it is allocated, with the Fix that names the session aggregate
    Case("uc_bp_fin_over_cap_refused", BPF, ([(0, 0, 1.0)], 0.0, 0.0, 1.0, 1.0, 100000, 100000, 4326), "uc_refused", None, ("uc",)),
)  # fmt: skip


# --- recipe D1: partial -> native GROUP BY -> finalize, against the session's reference over the same points -------------
# Ten points on a 10 x 10 canvas of 1 x 1 pixels. Pixel (2, 7) holds four (1, 2, 4, 10: an even count, so the median is
# between two values, and p90 is between the two largest), pixel (9, 0) two, pixel (5, 5) one. The rest are left out: the
# east edge, the south edge, west of the canvas.
_D1_POINTS = [
    (2.5, 2.5, 1.0), (2.1, 2.9, 2.0), (2.9, 2.1, 4.0), (2.5, 2.5, 10.0),
    (9.5, 9.5, 3.0), (9.9, 9.1, 5.0),
    (5.5, 4.5, 7.0),
    (10.0, 5.0, 99.0), (5.0, 0.0, 99.0), (-1.0, 5.0, 99.0),
]  # fmt: skip
_D1_CANVAS = "0D, 0D, 10D, 10D, 10, 10"


def _d1_value(v):
    if v is None:
        return "CAST(NULL AS DOUBLE)"
    if v != v:
        return "CAST('NaN' AS DOUBLE)"
    return f"{v!r}D"


def _d1_values(points):
    return ", ".join(
        f"({_d1_value(x)}, {_d1_value(y)}, {_d1_value(z)})" for x, y, z in points
    )


# the reduction of each statistic; a NULL or NaN z is dropped, as a NULL is by the aggregate (the NaN rule is pinned in
# test_uc_binpoints.py: it differs from the session's for count, mean, median and percentile)
D1_REDUCE = {
    "max": "max(z)",
    "min": "min(z)",
    "mean": "avg(z)",
    "count": "CAST(count(*) AS DOUBLE)",
    "median": "median(z)",
    "percentile:90": "percentile(z, 0.9)",
}
D1 = (
    "WITH b AS (SELECT {{q}}gbx_rst_binpoints_partial(x, y, {canvas}) AS px, z FROM {source}), "
    "c AS (SELECT px.col AS col, px.row AS row, {reduce} AS value FROM b "
    "WHERE px IS NOT NULL AND z IS NOT NULL AND NOT isnan(z) GROUP BY px.col, px.row) "
    "SELECT {{q}}gbx_rst_binpoints_finalize(collect_list(struct(col, row, value)), {canvas}, 4326) AS out FROM c"
)


def d1_sql(statistic, points=_D1_POINTS, source=None, canvas=_D1_CANVAS, srid=4326):
    """The recipe's statement for a statistic over ``points`` (or over ``source``, a table or view of x, y, z)."""
    return D1.format(
        canvas=canvas,
        source=source or f"VALUES {_d1_values(points)} AS t(x, y, z)",
        reduce=D1_REDUCE[statistic],
    ).replace("4326) AS out", f"{srid}) AS out")


def _d1(cid, statistic, points=_D1_POINTS):
    kept = [p for p in points if p[2] is not None and p[2] == p[2]]
    return recipe(
        cid,
        BP,
        d1_sql(statistic, points),
        "ref",
        "agg_binpoints",
        args=(kept, 0.0, 0.0, 10.0, 10.0, 10, 10, 4326, statistic),
    )


RASTER_AGGREGATE += tuple(
    _d1(f"recipe_d1_{stat.split(':')[0]}{'_90' if ':' in stat else ''}", stat)
    for stat in D1_REDUCE
)


@builder("dt_plane_points")
def _dt_plane_points(tmpdir):
    # z = x + 2 y at the corners of the 10 x 10 square: any triangulation holds the plane
    return [
        f"POINT Z ({x} {y} {x + 2 * y})"
        for x, y in ((0, 0), (10, 0), (0, 10), (10, 10))
    ]


@builder("dt_flat_point")
def _dt_flat_point(tmpdir):
    return ["POINT (1 1)"]


DT_CANVAS = (
    0.0,
    0.0,
    10.0,
    10.0,
    10,
    10,
    4326,
)  # extent, width_px, height_px, out_srid
RASTER_AGGREGATE += (
    Case(
        "dt_agg_plane",
        DT,
        (Build("dt_plane_points"), None, 0.0, 0.0, *DT_CANVAS),
        "ref",
        "agg_dtm_plane",
        ONLY,
    ),
    # a point without Z cannot give a height
    Case(
        "dt_agg_no_z",
        DT,
        (Build("dt_flat_point"), None, 0.0, 0.0, *DT_CANVAS),
        "raises",
        "Z",
        ONLY,
    ),
)


# --- the cell rasterizers: H3, quadbin, custom grid (BNG's rows are MVP_A's) ---------------------------------------
def _rings():
    """(center, [three ring-1 cells]) of each grid, from its own library or numbering."""
    import h3
    import quadbin

    h3_ring = [h3.str_to_int(x) for x in h3.grid_ring(h3.int_to_str(H3_CELL), 1)][:3]
    qb_ring = sorted(set(quadbin.k_ring(QB_C, 1)) - {QB_C})[:3]
    cg_ring = [cg_id(531, 180, 0), cg_id(530, 181, 0), cg_id(531, 181, 0)]
    return {
        "h3": (H3_CELL, h3_ring),
        "qb": (QB_C, qb_ring),
        "cg": (cg_id(530, 180, 0), cg_ring),
    }


# grid -> (function, twin, resolution of the cells, canvas: out_srid, pixel_size, extent, width, height, mode, kring_pad)
_CELLS = {
    "h3": (
        H3A,
        H3T,
        8,
        (4326, None, -122.43, 37.76, -122.405, 37.785, 10, 10, "centroids", 0),
    ),
    # the canvas covers QB_C and the three ring cells (San Francisco), square 0.08875-degree pixels, 4 x 3 to a cell
    "qb": (
        QBA,
        QBT,
        10,
        (4326, None, -123.05, 37.7186, -122.34, 38.2511, 8, 6, "centroids", 0),
    ),
    "cg": (
        CGA,
        CGT,
        0,
        (27700, None, 529000.0, 179000.0, 533000.0, 182000.0, 8, 6, "centroids", 0),
    ),
}


def _members(prefix, kind):
    c = _rings()[prefix]
    cells = [c[0], *c[1]]
    return {
        # a repeated cell keeps its largest value (3 of 3, 1); a NULL value in a typed column and a NaN burn NaN
        "dup_max": [
            (cells[0], 3.0),
            (cells[0], 1.0),
            (cells[1], 2.0),
            (cells[2], None),
            (cells[3], NAN),
        ],
        # a NaN beside numbers never wins, wherever it arrives
        "nan_beside": [
            (cells[0], NAN),
            (cells[0], 4.0),
            (cells[0], 1.0),
            (cells[1], 2.0),
            (cells[1], NAN),
        ],
        "null_id": [(cells[0], 1.0), (None, 2.0)],
        # no value given: every covered cell is present, 1.0
        "presence": [(cells[0], OMITTED), (cells[1], OMITTED), (cells[0], OMITTED)],
        # a custom-grid id of resolution 0 is small; resolution 1 puts the resolution in bit 56, past 2**53
        "big_null": [
            (cg_id(1061, 361, 1) if prefix == "cg" else cells[0], 1.0),
            (None, 2.0),
        ],
        "empty": [(None, 1.0), (None, 2.0)],
    }[kind]


def _group(prefix, kind):
    return _members(prefix, kind)  # the custom grid is the first constant, not a column


def _pixels(prefix, kind):
    """The canvas as numpy gives it: each pixel centre's cell (by the library) looked up in the largest value of the
    group's cells."""

    def expect():
        import numpy as np

        from databricks.labs.gbx.bench.oracle_checks import _pixel_cells

        best = {}
        for c, v in _members(prefix, kind):
            if c is None:
                continue
            v = 1.0 if v is OMITTED else (None if v is None or v != v else v)
            cur = best.get(c, "unset")
            if cur == "unset" or (v is not None and (cur is None or v > cur)):
                best[c] = v
        out_srid, _px, x0, y0, x1, y1, w, h, _m, _k = _CELLS[prefix][3]
        cx = x0 + (np.arange(w) + 0.5) * (x1 - x0) / w
        cy = y1 - (np.arange(h) + 0.5) * (y1 - y0) / h
        gx, gy = np.meshgrid(cx, cy)
        keys = _pixel_cells(
            {"h3": "h3", "qb": "quadbin", "cg": "custom"}[prefix],
            _CELLS[prefix][2],
            gx.ravel(),
            gy.ravel(),
        )
        vals = [
            NAN if (k in best and best[k] is None) else best.get(k, -9999.0)
            for k in keys
        ]
        pixels = np.array(vals, dtype="float64").reshape(h, w)
        assert (
            pixels != -9999.0
        ).any(), f"{prefix}: the canvas holds no cell of the group"
        return pixels.tolist()

    return expect


def _cells_cases(prefix):
    fn, twin, _res, canvas = _CELLS[prefix]
    name = {"h3": "h3r", "qb": "qbr", "cg": "cgr"}[prefix]
    tname = {"h3": "h3rt", "qb": "qbrt", "cg": "cgrt"}[prefix]
    for kind in ("dup_max", "nan_beside", "null_id", "big_null", "empty"):
        builder(f"{name}_{kind}")(lambda tmpdir, p=prefix, k=kind: _group(p, k))
        builder(f"{tname}_{kind}")(lambda tmpdir, p=prefix, k=kind: list(_group(p, k)))
    builder(f"{name}_presence")(lambda tmpdir, p=prefix: _group(p, "presence"))
    grid_arg = (dict(CUSTOM_GRID),) if prefix == "cg" else ()
    rows = []
    for kind in ("dup_max", "nan_beside"):
        rows.append(
            Case(
                f"{name}_{kind}",
                fn,
                (Build(f"{name}_{kind}"), *grid_arg, *canvas),
                "pixels",
                _pixels(prefix, kind),
                ONLY,
            )
        )
        rows.append(
            Case(
                f"{tname}_{kind}",
                twin,
                (Build(f"{tname}_{kind}"), *grid_arg, *canvas),
                "pixels",
                _pixels(prefix, kind),
                _surfaces(twin),
            )
        )
    rows += [
        # a NULL cell id beside BIGINT ids past 2**53: the aggregate refuses it; the twin's array skips it
        Case(
            f"{name}_null_beside_big_id",
            fn,
            (Build(f"{name}_big_null"), *grid_arg, *canvas),
            "raises",
            "2**53",
            ONLY,
        ),
        Case(
            f"{tname}_null_cell_skipped",
            twin,
            (Build(f"{tname}_null_id"), *grid_arg, *canvas),
            "pixels",
            _pixels(prefix, "null_id"),
            _surfaces(twin),
        ),
        Case(
            f"{name}_empty",
            fn,
            (Build(f"{name}_empty"), *grid_arg, *canvas),
            "null",
            None,
            ONLY,
        ),
        Case(
            f"{tname}_empty",
            twin,
            (Build(f"{tname}_empty"), *grid_arg, *canvas),
            "null",
            None,
            _surfaces(twin),
        ),
    ]
    return tuple(rows)


for _p in _CELLS:
    RASTER_AGGREGATE += _cells_cases(_p)

# out_crs: the output CRS asked for by its name when out_srid is NULL (the trailing 13th argument of H3 and quadbin)
RASTER_AGGREGATE += (
    Case(
        "h3r_out_crs",
        H3A,
        (Build("h3r_dup_max"), None, *_CELLS["h3"][3][1:], "EPSG:4326"),
        "pixels",
        _pixels("h3", "dup_max"),
        ONLY,
    ),
    Case(
        "qbr_out_crs",
        QBA,
        (Build("qbr_dup_max"), None, *_CELLS["qb"][3][1:], "EPSG:4326"),
        "pixels",
        _pixels("qb", "dup_max"),
        ONLY,
    ),
    # out_srid and out_crs set together raise, as in every rasterizer (the canvas row carries an out_srid of 4326)
    Case(
        "h3r_out_srid_and_crs",
        H3A,
        (Build("h3r_dup_max"), *_CELLS["h3"][3], "EPSG:3857"),
        "raises",
        "Fix: set one",
        ONLY,
    ),
    Case(
        "qbr_out_srid_and_crs",
        QBA,
        (Build("qbr_dup_max"), *_CELLS["qb"][3], "EPSG:3857"),
        "raises",
        "Fix: set one",
        ONLY,
    ),
    # ... and the scalar twins refuse it the same way
    Case(
        "h3rt_out_srid_and_crs",
        H3T,
        (Build("h3rt_dup_max"), *_CELLS["h3"][3], "EPSG:3857"),
        "raises",
        "Fix: set one",
        _surfaces(H3T),
    ),
    Case(
        "qbrt_out_srid_and_crs",
        QBT,
        (Build("qbrt_dup_max"), *_CELLS["qb"][3], "EPSG:3857"),
        "raises",
        "Fix: set one",
        ONLY,
    ),
    # No value given (an untyped NULL: SQL `NULL`, Python `None`) is the presence mask: every covered cell burns 1.0,
    # where a NULL in a typed column burns NaN (h3r_dup_max).
    Case(
        "h3r_value_omitted",
        H3A,
        (Build("h3r_presence"), *_CELLS["h3"][3]),
        "pixels",
        _pixels("h3", "presence"),
        ONLY,
    ),
    # A NULL out_srid is the custom grid's CRS; a grid without one has no CRS to fall back to.
    Case(
        "cgr_out_srid_from_grid",
        CGA,
        (Build("cgr_dup_max"), dict(CUSTOM_GRID), None, *_CELLS["cg"][3][1:]),
        "pixels",
        _pixels("cg", "dup_max"),
        ONLY,
    ),
    Case(
        "cgr_grid_no_crs",
        CGA,
        (
            Build("cgr_dup_max"),
            {**CUSTOM_GRID, "srid": -1},
            None,
            *_CELLS["cg"][3][1:],
        ),
        "raises",
        "supply out_srid",
        ONLY,
    ),
)

# --- the BNG twin: the rows of br_dup_max and br_nan_beside_numbers, on the canvas of MVP_A -------------------------
BNG_CELLS = [
    ("TQ3080", 3.0),
    ("TQ3080", 1.0),
    ("TQ3180", 2.0),
    ("TQ3081", None),
    ("TQ3181", NAN),
]
BNG_NAN = [
    ("TQ3080", NAN),
    ("TQ3080", 4.0),
    ("TQ3080", 1.0),
    ("TQ3180", 2.0),
    ("TQ3180", NAN),
]
RASTER_AGGREGATE += (
    # a typed NULL burns NaN on every tier; the last column holds no cell
    Case(
        "brt_dup_max",
        BRT,
        (BNG_CELLS, *CANVAS),
        "pixels",
        [[NAN, NAN, -9999.0], [3.0, 2.0, -9999.0]],
        ONLY,
    ),
    Case(
        "brt_nan_beside_numbers",
        BRT,
        (BNG_NAN, *CANVAS),
        "pixels",
        [[-9999.0, -9999.0, -9999.0], [4.0, 2.0, -9999.0]],
        ONLY,
    ),
    Case(
        "brt_null_cell_skipped",
        BRT,
        ([(None, 1.0), ("TQ3080", 5.0)], *CANVAS),
        "pixels",
        [[-9999.0, -9999.0, -9999.0], [5.0, -9999.0, -9999.0]],
        ONLY,
    ),
    Case("brt_empty", BRT, ([(None, 1.0)], *CANVAS), "null", None, ONLY),
    # a non-NULL out_crs is refused: the output is always EPSG:27700
    Case(
        "brt_out_crs_refused",
        BRT,
        (BNG_CELLS, *CANVAS, "EPSG:3857"),
        "raises",
        "out_crs",
        ONLY,
    ),
)


# --- the table functions of the two MVP_A aggregates: `gbx_x(TABLE(<members>) PARTITION BY (key), <constants>)` --------
# The group of a case is the partition of key `a`; the harness calls the function with `oracles.table_call_sql` (the one
# input mode of a function that reads a table argument). The expectations are those of the aggregate rows they are
# made from (MVP_A: the shapes of the merge and the rasterize all meet the same rows), so the table function and the
# grouped aggregate are held to the same pixels. A partition with no answer gives NO ROW (the harness reads it as NULL):
# the aggregate's NULL.
_MVP = {c.cid: c for c in MVP_A}
_CANVAS_ARGS = CANVAS[
    2:8
]  # xmin, ymin, xmax, ymax, width, height: the table function has no out_srid / mode / pad
RASTER_AGGREGATE += (
    Case("mt_quad", MT, _MVP["ma_quad"].args, "merged_equals", Build("tile_32"), ONLY),
    # a partition of nothing but error tiles has no mosaic: no row
    Case(
        "mt_error_only",
        MT,
        _MVP["ma_error_only"].args,
        "error_group",
        UPSTREAM_ERROR,
        ONLY,
    ),
    # a partition past what a merge can build raises with the Fix, as the aggregate does (never a missing group)
    Case(
        "mt_over_budget_refused",
        MT,
        (Build("ra_far_apart"),),
        "raises",
        "mint_vrt",
        ONLY,
    ),
    *(
        Case(
            cid,
            BT,
            (_MVP[src].args[0], *_CANVAS_ARGS),
            _MVP[src].kind,
            _MVP[src].expect,
            ONLY,
        )
        for cid, src in (
            ("bt_dup_max", "br_dup_max"),
            ("bt_nan_beside_numbers", "br_nan_beside_numbers"),
            ("bt_empty", "br_empty"),
        )
    ),
)
