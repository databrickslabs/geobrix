"""Oracle table for the functions that take an ARRAY of tiles (`gbx_rst_combine*`, `gbx_rst_merge`,
`gbx_rst_frombands`): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from independent numpy references (oracle_refs.combine: the statistic of the valid values at each
pixel, written from its definition) or from the fixtures' definitions: the four 16 x 16 windows of `tile_32` merge back
into `tile_32`, and the bands of `frombands` are the first bands of the tiles, in array order. The stack is three
aligned 8 x 6 tiles on a 10 m grid with NoData holes: pixel (0, 0) is NoData in all three, pixels (0, 1) and (1, 1) in one,
and (2, 2) is a valid 0 (a valid zero counts).

The error contract, once for the family: a NULL or empty array is NULL; an array of nothing but error tiles is the first
member's error tile; an error tile or a tile that cannot be read inside an array is skipped (the others combine); tiles
of different shapes raise; a group over the memory budget is folded window by window; one whose single row of members
does not fit the budget is the row's error tile with a `Fix:`."""

from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _build,
    _gtiff,
    _tile,
    builder,
)

STATS = ("avg", "count", "max", "median", "min", "stddev", "sum")
CB = {s: f"gbx_rst_combine{s}" for s in STATS}
MG, FB = "gbx_rst_merge", "gbx_rst_frombands"
STACK, STACK_REV = Build("aa_stack"), Build("aa_stack_rev")
ONLY = ("session",)


def _stack_arrays():
    import numpy as np

    base = np.arange(48, dtype="float64").reshape(6, 8)
    a, b, c = base.copy(), base * 2 + 1, base % 7
    a[0, 0] = a[0, 1] = -9999.0
    b[0, 0] = b[1, 1] = -9999.0
    c[0, 0] = -9999.0
    c[2, 2] = 0.0
    return [x[np.newaxis] for x in (a, b, c)]


def _stack_tiles():
    return [_tile(_gtiff(x, "EPSG:3857")) for x in _stack_arrays()]


@builder("aa_stack")
def _aa_stack(tmpdir):
    return _stack_tiles()


@builder("aa_stack_rev")
def _aa_stack_rev(tmpdir):
    return _stack_tiles()[::-1]


@builder("aa_stack_corrupt")
def _aa_stack_corrupt(tmpdir):
    return [*_stack_tiles(), _build("tile_corrupt", tmpdir)]


@builder("aa_stack_mid_corrupt")
def _aa_stack_mid_corrupt(tmpdir):
    a, _, c = _stack_tiles()
    return [a, _build("tile_corrupt", tmpdir), c]


@builder("aa_stack_error")
def _aa_stack_error(tmpdir):
    a, b, c = _stack_tiles()
    return [a, _build("tile_error", tmpdir), b, c]


@builder("aa_stack_null_element")
def _aa_stack_null_element(tmpdir):
    a, b, c = _stack_tiles()
    return [a, None, b, c]


@builder("aa_stack_paths")
def _aa_stack_paths(tmpdir):
    import os

    out = []
    for i, t in enumerate(_stack_tiles()):
        path = os.path.join(tmpdir, f"aa_stack_{i}.tif")
        with open(path, "wb") as fh:
            fh.write(t["raster"])
        out.append(_tile(None, path, None))
    return out


@builder("aa_misshaped")
def _aa_misshaped(tmpdir):
    import numpy as np

    return [
        _stack_tiles()[0],
        _tile(_gtiff(np.zeros((1, 5, 7)), "EPSG:3857")),
    ]


@builder("aa_mixed_crs")
def _aa_mixed_crs(tmpdir):
    """Two 8 x 6 tiles on a 10 m grid: the west one in UTM 33N, the east one in UTM 33S, half a pixel (5 m) past the
    west one's east edge. The zones share their meridian and differ only in the false northing (10,000,000 m), so the
    east tile, put in the 33N CRS, spans x 500085..500165 (the half pixel keeps the expected size clear of the
    projection library's rounding): on the west tile's pixel grid it grows to 500080..500170, 9 pixels.
    """
    import numpy as np

    pixels = np.arange(48, dtype="float64").reshape(1, 6, 8)
    return [
        _tile(_gtiff(pixels, "EPSG:32633", origin=(500000.0, 5000060.0))),
        _tile(_gtiff(pixels + 100, "EPSG:32733", origin=(500085.0, 15000060.0))),
    ]


@builder("aa_quad_corrupt")
def _aa_quad_corrupt(tmpdir):
    return [*_build("quad_tiles", tmpdir), _build("tile_corrupt", tmpdir)]


@builder("aa_quad_error")
def _aa_quad_error(tmpdir):
    return [_build("tile_error", tmpdir), *_build("quad_tiles", tmpdir)]


RASTER_ARRAY_OP = (
    # --- the combine family: the statistic of the valid values at each pixel, against numpy
    *(
        Case(f"combine{s}_stack", CB[s], (STACK,), "ref", f"combine_{s}", ONLY)
        for s in STATS
    ),
    Case(
        "combineavg_virtual",
        CB["avg"],
        (Build("aa_stack_paths"),),
        "ref",
        "combine_avg",
        ONLY,
    ),
    # a NULL or empty array is NULL; the avg and the shared stat reducer are two code paths
    *(
        Case(
            f"combine{s}_no_tiles",
            CB[s],
            (None,),
            "null",
            None,
            ONLY,
            variants=(([],),),
        )
        for s in ("avg", "max")
    ),
    # an array of nothing but error tiles is the first member's error tile, as it is
    *(
        Case(
            f"combine{s}_all_error",
            CB[s],
            (Build("tile_error_pair"),),
            "error_tile",
            UPSTREAM_ERROR,
            ONLY,
        )
        for s in ("avg", "max")
    ),
    # a member that cannot be read, an error tile and a NULL element are skipped: the others combine
    *(
        Case(
            f"combine{s}_corrupt_member",
            CB[s],
            (Build("aa_stack_corrupt"),),
            "ref",
            f"combine_{s}",
            ONLY,
        )
        for s in ("avg", "max")
    ),
    Case(
        "combineavg_error_member",
        CB["avg"],
        (Build("aa_stack_error"),),
        "ref",
        "combine_avg",
        ONLY,
    ),
    Case(
        "combineavg_null_element",
        CB["avg"],
        (Build("aa_stack_null_element"),),
        "ref",
        "combine_avg",
        ONLY,
    ),
    # tiles of different shapes cannot be stacked: the call raises (align them with rst_align_to first)
    *(
        Case(
            f"combine{s}_misshaped",
            CB[s],
            (Build("aa_misshaped"),),
            "raises",
            "align",
            ONLY,
        )
        for s in ("avg", "sum")
    ),
    # a stack too wide to fold one row of it in the budget is the row's error tile with a Fix (READ_CAP_ROWS lowers the
    # cap to 1 KiB; a row of the two 32-pixel-wide members of pair_aligned needs 1280 bytes of the 512 a window may use)
    Case(
        "array_decoded_over_cap",
        CB["avg"],
        (Build("pair_aligned"),),
        "error_fix",
        None,
        ONLY,
    ),
    # an array whose members add up to more than the memory budget is folded by streaming, window by window, and gives
    # the result of an unconstrained call (READ_CAP_ROWS lowers the budget to 3000 bytes: the 4 KiB mosaic of quad_tiles,
    # and a stack of three 8 x 6 tiles that is folded in windows of rows)
    Case(
        "merge_array_streams_under_budget",
        MG,
        (Build("quad_tiles"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    *(
        Case(
            f"combine{s}_array_streams_under_budget",
            CB[s],
            (STACK,),
            "ref",
            f"combine_{s}",
            ONLY,
        )
        for s in ("avg", "sum")
    ),
    # --- merge: the four windows of tile_32 merge back into it
    Case(
        "merge_quad",
        MG,
        (Build("quad_tiles"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    Case(
        "merge_virtual_quad",
        MG,
        (Build("quad_paths"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    Case("merge_no_tiles", MG, (None,), "null", None, ONLY, variants=(([],),)),
    Case(
        "merge_all_error",
        MG,
        (Build("tile_error_pair"),),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "merge_corrupt_member",
        MG,
        (Build("aa_quad_corrupt"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    Case(
        "merge_error_member",
        MG,
        (Build("aa_quad_error"),),
        "merged_equals",
        Build("tile_32"),
        ONLY,
    ),
    # tiles in other CRSs are mosaicked in one of them (the smallest EPSG code), none is dropped: the 8-pixel tile and
    # the 9 pixels the other one grows to, from x 500000 to 500170, are 17 pixels wide; the light tier's reprojection
    # can add a pixel on an edge (the projection library's rounding), so 17 or 18 wide and 6 or 7 high (a dropped tile gives 8 wide)
    Case(
        "merge_mixed_crs",
        MG,
        (Build("aa_mixed_crs"),),
        "tile_meta",
        {"crs": "EPSG:32633", "width": (17, 18), "height": (6, 7), "count": 1},
        ONLY,
    ),
    # --- frombands: one band per tile, in array order
    Case("frombands_three", FB, (STACK,), "ref", "frombands", ONLY),
    Case("frombands_array_order", FB, (STACK_REV,), "ref", "frombands", ONLY),
    Case("frombands_virtual", FB, (Build("aa_stack_paths"),), "ref", "frombands", ONLY),
    Case("frombands_no_tiles", FB, (None,), "null", None, ONLY, variants=(([],),)),
    Case(
        "frombands_all_error",
        FB,
        (Build("tile_error_pair"),),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "frombands_corrupt_member",
        FB,
        (Build("aa_stack_mid_corrupt"),),
        "ref",
        "frombands",
        ONLY,
    ),
)
