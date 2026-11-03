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

from databricks.labs.gbx.bench.oracle_tables.grid_scalar import (
    CUSTOM_GRID,
    H3_CELL,
    QB_C,
    cg_id,
)
from databricks.labs.gbx.bench.oracles import (
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
    # whole to be reprojected (the Fix names merge_by_key(method="vrt"): the members need no RAM there).
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
                ONLY,
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
            ONLY,
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
            ONLY,
        ),
    ]
    return tuple(rows)


for _p in _CELLS:
    RASTER_AGGREGATE += _cells_cases(_p)

# out_crs: the output CRS asked for by its name, also when out_srid is NULL (the trailing 13th argument of H3 and quadbin)
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
