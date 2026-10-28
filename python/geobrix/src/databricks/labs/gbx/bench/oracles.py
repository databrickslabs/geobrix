"""Family oracle tables: inputs -> expected, run at every implementation level and surface.

One table per function family. Host tests, container parity runs and the cluster and
Unity Catalog passes all read these rows, so a level is "correct" exactly when its rows pass.
"""

from __future__ import annotations

import base64
import functools
import json
import math
import os
import tempfile
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

MERC_PROJ = (
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 "
    "+units=m +nadgrids=@null +wktext +no_defs"
)
# An error tile (a swallowed failure) carries its diagnostic under this one metadata key on
# every level. The light tier defines it (`_serde.build_error_tile`); the JVM tier conforms.
ERROR_TILE_KEY = "last_error"
UPSTREAM_ERROR = "upstream failure"
CUSTOM_TMERC = "+proj=tmerc +lat_0=0 +lon_0=1.5 +k=0.9996 +x_0=500000 +y_0=0 +ellps=WGS84 +units=m +no_defs"


@dataclass(frozen=True)
class Build:
    name: str  # a fixture built by _build(name)


@dataclass(frozen=True)
class Case:
    cid: str
    fn: str
    args: Tuple[Any, ...]
    # exact | exact_rows | null | raises | uc_refused | error_tile | tile_props | geom_props
    # exact_rows is one batch: `args` is a tuple of argument rows, `expect` one value per row.
    # Generators also use no_rows | windows | cells, and error_tile means exactly one row.
    # The DataFrame helpers' rst_viewshed_towers uses towers (see MVP_H).
    # windows_batch is one task over several tiles: `args` is a tuple of argument rows, as for exact_rows.
    # Aggregates use merged_equals | pixels | error_group | null; `args[0]` is the group's rows.
    kind: str
    # exact: the value. raises: a substring the error message must contain (the offending
    # value), so a missing function cannot pass. tile_props: the target CRS, or (CRS, SRID).
    # error_tile: when set, the reason that must come out unchanged.
    expect: Any = None
    surfaces: Tuple[str, ...] = ("session", "uc")
    geom_srid: int = (
        4326  # the integer SRID UC GEOMETRY args are built with (never a CRS string)
    )
    # exact | null only: more argument rows with the same expectation (one batch on the session surface,
    # one handler call each on UC). The UC warehouse statement is the primary `args` only.
    variants: Tuple[Tuple[Any, ...], ...] = ()


# Rows a JVM level is known to fail until it conforms to the light contract, as {row: {level: reason}}.
# The parity run expects exactly these (level, row) pairs to fail, strictly, and a row that starts to
# pass must be dropped from here. Empty: every JVM level conforms.
HEAVY_PENDING: Dict[str, Dict[str, str]] = {}

B, T, M = "gbx_bng_eastnorthasbng", "gbx_rst_transformcrs", "gbx_st_makevalid"
SQUARE = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
MVP_S: Tuple[Case, ...] = (
    Case("bng_1km", B, (530000.0, 180000.0, 3), "exact", "TQ3080"),
    Case("bng_1km_str", B, (530000.0, 180000.0, "1km"), "exact", "TQ3080"),
    Case("bng_5km_quad", B, (530000.0, 180000.0, -3), "exact", "TQ38SW"),
    Case("bng_100km", B, (530000.0, 180000.0, 1), "exact", "TQ"),
    Case("bng_1m", B, (530000.0, 180000.0, 6), "exact", "TQ3000080000"),
    Case("bng_500km", B, (530000.0, 180000.0, "500km"), "exact", "T"),
    Case("bng_edinburgh", B, (325000.0, 673000.0, 3), "exact", "NT2573"),
    Case("bng_origin", B, (0.0, 0.0, 3), "exact", "SV0000"),
    Case("bng_null_e", B, (None, 180000.0, 3), "null"),
    Case("bng_nan_e", B, (float("nan"), 180000.0, 3), "null"),
    # A non-finite coordinate is NULL like NaN, on every level: there is no right cell for it.
    Case(
        "bng_inf_e",
        B,
        (float("inf"), 180000.0, 3),
        "null",
        variants=(
            (float("-inf"), 180000.0, 3),
            (530000.0, float("inf"), 3),
            (530000.0, float("-inf"), 3),
            (float("inf"), float("inf"), 3),
        ),
    ),
    Case("bng_null_res", B, (530000.0, 180000.0, None), "null"),
    # A NULL in an INT column makes pandas hand the whole batch over as floats (3.0): the
    # valid rows around the NULL must still answer, row by row.
    # (Session only: the batch is a pandas UDF artifact. A UC Python function runs row by row, and
    # bng_null_res already pins its NULL.)
    Case(
        "bng_mixed_null_res",
        B,
        (
            (530000.0, 180000.0, 3),
            (530000.0, 180000.0, None),
            (325000.0, 673000.0, 3),
        ),
        "exact_rows",
        ("TQ3080", None, "NT2573"),
        ("session",),
    ),
    Case("bng_bad_res", B, (530000.0, 180000.0, "3km"), "raises", "3km"),
    # A resolution BNG does not define is a parameter error on every level, never a clamped cell.
    Case("bng_bad_res_int", B, (530000.0, 180000.0, 1001), "raises", "1001"),
    Case("bng_bad_res_digits", B, (530000.0, 180000.0, "1001"), "raises", "1001"),
    # A non-number easting is a parameter error too (only NaN and out-of-range values are data errors).
    # (Session only: UC types easting DOUBLE, so the engine's cast sees 'abc' before any handler runs. The
    # warehouse Statement API has ANSI off, where that cast is a silent NULL; with ANSI on the engine's
    # own error names the value. Either way it is the product's behaviour, not GeoBrix's to pin.)
    Case(
        "bng_bad_easting",
        B,
        ("abc", 180000.0, 3),
        "raises",
        "abc",
        ("session",),
    ),
    # A UC STRING parameter receives an INT literal as its digit string, so every surface
    # accepts the digit-string form of the integer resolution.
    Case("bng_digit_res", B, (530000.0, 180000.0, "3"), "exact", "TQ3080"),
    Case("bng_digit_res_quad", B, (530000.0, 180000.0, "-3"), "exact", "TQ38SW"),
    Case("tcrs_epsg", T, (Build("tile_4326"), "EPSG:3857"), "tile_props", "EPSG:3857"),
    Case(
        "tcrs_virtual",
        T,
        (Build("tile_virtual"), "EPSG:3857"),
        "tile_props",
        "EPSG:3857",
        ("session",),
    ),
    Case(
        "tcrs_virtual_uc",
        T,
        (Build("tile_virtual"), "EPSG:3857"),
        "uc_refused",
        None,
        ("uc",),
    ),
    # The target CRS forms UC accepts: an int SRID, EPSG:<n> or ESRI:<n> (converted to the int).
    # Anything else is refused on UC with a Fix: line; the session accepts every CRS spelling.
    Case(
        "tcrs_int_srid",
        T,
        (Build("tile_4326"), "3857"),
        "tile_props",
        ("EPSG:3857", 3857),
    ),
    Case(
        "tcrs_epsg4326",
        T,
        (Build("tile_3857"), "EPSG:4326"),
        "tile_props",
        ("EPSG:4326", 4326),
    ),
    Case(
        "tcrs_esri102003",
        T,
        (Build("tile_4326"), "ESRI:102003"),
        "tile_props",
        ("ESRI:102003", 102003),
    ),
    Case(
        "tcrs_wkt",
        T,
        (Build("tile_4326"), Build("wkt_3857")),
        "tile_props",
        "EPSG:3857",
        ("session",),
    ),
    Case(
        "tcrs_wkt_uc",
        T,
        (Build("tile_4326"), Build("wkt_3857")),
        "uc_refused",
        None,
        ("uc",),
    ),
    Case(
        "tcrs_proj4",
        T,
        (Build("tile_4326"), MERC_PROJ),
        "tile_props",
        "EPSG:3857",
        ("session",),
    ),
    Case(
        "tcrs_proj4_uc", T, (Build("tile_4326"), MERC_PROJ), "uc_refused", None, ("uc",)
    ),
    Case(
        "tcrs_custom_src",
        T,
        (Build("tile_custom"), "EPSG:4326"),
        "tile_props",
        "EPSG:4326",
        ("session",),
    ),
    Case(
        "tcrs_custom_src_uc",
        T,
        (Build("tile_custom"), "EPSG:4326"),
        "uc_refused",
        None,
        ("uc",),
    ),
    Case("tcrs_null_tile", T, (None, "EPSG:3857"), "null"),
    # A tile with neither raster nor path and no diagnostic has nothing to transform: NULL.
    Case(
        "tcrs_empty_tile",
        T,
        (Build("tile_empty"), "EPSG:3857"),
        "null",
        None,
        ("session",),
    ),
    # An error tile IN is the same error tile OUT on every level (the upstream reason survives), and a
    # tile with no raster never reaches GDAL.
    Case(
        "tcrs_error_tile_in",
        T,
        (Build("tile_error"), "EPSG:3857"),
        "error_tile",
        UPSTREAM_ERROR,
        ("session",),
    ),
    Case("tcrs_null_crs", T, (Build("tile_4326"), None), "null"),
    Case(
        "tcrs_corrupt",
        T,
        (Build("tile_corrupt"), "EPSG:3857"),
        "error_tile",
        None,
        ("session",),
    ),
    # A target CRS that cannot be parsed is data too: an error tile, on the SQL and the Column API.
    Case(
        "tcrs_bad_crs",
        T,
        (Build("tile_4326"), "EPSG:not-a-crs"),
        "error_tile",
        None,
        ("session",),
    ),
    # A malformed string is refused too, never a NULL: with ANSI off (the warehouse Statement API) the
    # product casts a bad CRS string to a silent NULL, so UC checks the form before any CRS use.
    Case(
        "tcrs_bad_crs_uc",
        T,
        (Build("tile_4326"), "EPSG:not-a-crs"),
        "uc_refused",
        None,
        ("uc",),
    ),
    Case(
        "tcrs_corrupt_uc",
        T,
        (Build("tile_corrupt"), "EPSG:3857"),
        "null",
        None,
        ("uc",),
    ),
    Case(
        "mv_bowtie",
        M,
        ("POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))",),
        "geom_props",
        {"valid": True, "area": 0.5},
    ),
    Case(
        "mv_square_ogc", M, (SQUARE, "ogc"), "geom_props", {"valid": True, "area": 1.0}
    ),
    Case("mv_null", M, (None,), "null"),
    Case("mv_bad_level", M, (SQUARE, "bogus"), "raises", "bogus"),
)


# The towers of the DataFrame helpers' towers row, as (id, x, y) on the 200 m DSM grid of `dsm_tiles_edge`: in one tile, in
# one tile with the ridge at the edge of its window, 20 m from a ridge that is inside its window (and so across two
# tiles), on the corner of four tiles, in a tile of NoData, on the west edge of the DSM (inside it, within half a
# pixel of its edge), 10 m west of the DSM (outside it, with a tile in reach), and in no tile's reach.
TOWERS = (
    ("t1", 100.0, 100.0),
    ("t2", 300.0, 300.0),
    ("ridge", 260.0, 100.0),
    ("corner", 200.0, 200.0),
    ("nodata", 500.0, 100.0),
    ("edge", 0.0, 100.0),
    ("beyond", -10.0, 100.0),
    ("outside", 1000.0, 1000.0),
)
DSM_TILE_M = 200.0
TOWERS_TOLERANCE = (
    0.005  # of total_px: GDAL builds differ by a pixel or so along a visibility edge
)

# The DataFrame helpers in pyrx/helpers.py (a case's `fn` is the helper's name, not a SQL name).
# The first arg builds the input rows; the rest are the helper's arguments after the DataFrame and,
# for the by-key helpers, the method / stat. A towers row (`rst_viewshed_towers`) takes the DSM manifest
# `(tx, ty, path)` instead of tiles, then the towers (id, x, y[, mast]; a mast column is passed by name), the radius
# and the mast height; its `expect` is {tower: (visible_px, total_px)}, or None for a tower on no tile (no row), or
# for kind "raises" the text the error must contain.
MVP_H: Tuple[Case, ...] = (
    Case(
        "h_retile",
        "retile_virtual",
        (Build("tile_virtual"), 32, 32),
        "virtual_windows",
        [(0, 0, 32, 32), (32, 0, 32, 32), (0, 32, 32, 16), (32, 32, 32, 16)],
        ("session",),
    ),
    Case(
        "h_retile_materialized",
        "retile_virtual",
        (Build("tile_4326"), 32, 32),
        "raises",
        "takes virtual tiles",
        ("session",),
    ),
    # An error tile in is the same error tile out; a by-key helper drops it as the aggregates do,
    # so a group of nothing else has no raster.
    Case(
        "h_retile_error_tile",
        "retile_virtual",
        (Build("tile_error"), 32, 32),
        "error_tile",
        UPSTREAM_ERROR,
        ("session",),
    ),
    Case(
        "h_merge_error_tile",
        "merge_by_key",
        (Build("tile_error"), "fold"),
        "null",
        None,
        ("session",),
    ),
    Case(
        "h_tess",
        "bng_tessellate_virtual",
        (Build("tile_27700_path"), 3),
        "virtual_cells",
        {"TQ3080", "TQ3180", "TQ3081", "TQ3181"},
        ("session",),
    ),
    # Every source pixel lands in some cell's window, also on a grid the cells do not line up with.
    Case(
        "h_tess_cover",
        "bng_tessellate_virtual",
        (Build("tile_27700_unaligned"), 3),
        "virtual_cover",
        None,
        ("session",),
    ),
    # A tile that is already clipped is cut to the cells inside its clip, not re-opened to all of them.
    Case(
        "h_tess_clip",
        "bng_tessellate_virtual",
        (Build("tile_27700_clipped"), 3),
        "virtual_cells",
        {"TQ3080"},
        ("session",),
    ),
    Case(
        "h_merge_fold",
        "merge_by_key",
        (Build("quad_tiles"), "fold"),
        "merged_equals",
        Build("tile_32"),
        ("session",),
    ),
    Case(
        "h_merge_vrt",
        "merge_by_key",
        (Build("quad_paths"), "vrt"),
        "merged_equals",
        Build("tile_32"),
        ("session",),
    ),
    Case(
        "h_combine_avg",
        "combine_by_key",
        (Build("pair_aligned"), "avg"),
        "pixel_ratio",
        1.5,
        ("session",),
    ),
    Case(
        "h_combine_max",
        "combine_by_key",
        (Build("pair_aligned"), "max"),
        "pixel_ratio",
        2.0,
        ("session",),
    ),
    # Tiles on different grids are refused for avg as for max, never averaged cell by cell.
    Case(
        "h_combine_avg_shifted",
        "combine_by_key",
        (Build("pair_shifted"), "avg"),
        "raises",
        "different geotransform",
        ("session",),
    ),
    # The viewshed is the window around the tower: 161 x 161 for 80 m, whole when the DSM ends sooner (the "edge" tower
    # has half of it), and total_px is its pixel count. The ground is flat except a ridge on the west edge of the odd
    # columns: it hides what is behind it for "ridge" and for nobody else. A tile of NoData is flat to the viewshed.
    # A tower on no tile has no row, wherever its window reaches.
    Case(
        "h_towers",
        "rst_viewshed_towers",
        (Build("dsm_tiles_edge"), TOWERS, 80.0, 20.0),
        "towers",
        {
            "t1": (20081, 25921),
            "t2": (20081, 25921),
            "ridge": (18691, 25921),
            "corner": (19286, 25921),
            "nodata": (20081, 25921),
            "edge": (10121, 13041),
            "beyond": None,
            "outside": None,
        },
        ("session",),
    ),
    # A NULL mast height is 0.0, at every level: the second tower is the first with its mast given as 0, the third is
    # taller and sees more past the ridge.
    Case(
        "h_towers_null_mast",
        "rst_viewshed_towers",
        (
            Build("dsm_tiles_edge"),
            (
                ("null", 260.0, 100.0, None),
                ("zero", 260.0, 100.0, 0.0),
                ("tall", 260.0, 100.0, 20.0),
            ),
            80.0,
            "mast",
        ),
        "towers",
        {"null": (18477, 25921), "zero": (18477, 25921), "tall": (18691, 25921)},
        ("session",),
    ),
    # A tile that cannot be read fails the query at every level, naming the tile; never a plausible-looking row.
    Case(
        "h_towers_garbage_tile",
        "rst_viewshed_towers",
        (Build("dsm_tiles_garbage"), (("corner", 200.0, 200.0),), 80.0, 20.0),
        "raises",
        "dsm_1_1.tif",
        ("session",),
    ),
    # ... also a bad block in the middle of a tile that is whole otherwise: the last row reads, and GDAL reads the
    # rest of that strip as zeros unless something asks it whether it could.
    Case(
        "h_towers_garbled_tile",
        "rst_viewshed_towers",
        (Build("dsm_tiles_garbled"), (("corner", 200.0, 200.0),), 80.0, 20.0),
        "raises",
        ("dsm_1_1.tif", "Read failed"),
        ("session",),
    ),
    Case(
        "h_towers_truncated_tile",
        "rst_viewshed_towers",
        (Build("dsm_tiles_truncated"), (("corner", 200.0, 200.0),), 80.0, 20.0),
        "raises",
        (
            "dsm_1_1.tif",
            "Read failed",
        ),  # the light read says "Read failed" and no tile; the others name it
        ("session",),
    ),
)


# Generators: functions that return many rows per input (and the array form of tessellate).
# A case's rows are what `LATERAL fn(args)` (or `inline(fn(args))`) gives for it; see `run_rows`.
RT, TE, TS = "gbx_rst_retile", "gbx_bng_tessellateexplode", "gbx_bng_tessellate"
# 2800 x 1800 m, offset 100 m into the cells it covers: six 1 km cells, every one a border cell.
OFFSET = "POLYGON((530100 180100, 532900 180100, 532900 181900, 530100 181900, 530100 180100))"
# 3500 x 3500 m starting 100 m outside the cells it covers: 25 cells, and the 4 in the middle are core
# (a core cell is one the carved-in geometry reaches; a cell fully inside can still be a border cell).
COVER = "POLYGON((529900 179900, 533400 179900, 533400 183400, 529900 183400, 529900 179900))"
_SIX = {
    "TQ3080": 810000.0,
    "TQ3180": 900000.0,
    "TQ3280": 810000.0,
    "TQ3081": 810000.0,
    "TQ3181": 900000.0,
    "TQ3281": 810000.0,
}
_TE: Tuple[Case, ...] = (
    Case(
        "te_offset",
        TE,
        (OFFSET, 3),
        "cells",
        {"cells": _SIX, "core": 0},
        geom_srid=27700,
    ),
    Case(
        "te_cover",
        TE,
        (COVER, 3),
        "cells",
        {"count": 25, "core": 4, "core_chips": "null"},
        geom_srid=27700,
    ),
    Case(
        "te_cover_keep",
        TE,
        (COVER, 3, True),
        "cells",
        {"count": 25, "core": 4, "core_chips": 1.0e6},
        geom_srid=27700,
    ),
    Case("te_null", TE, (None, 3), "no_rows"),
    # A NULL keep_core_geom is the default, false: an optional argument given as NULL is not a NULL result.
    Case(
        "te_null_keep",
        TE,
        (OFFSET, 3, None),
        "cells",
        {"cells": _SIX, "core": 0},
        geom_srid=27700,
    ),
    # A resolution BNG does not define is a parameter error: it raises on every level, never zero rows.
    Case("te_bad_res", TE, (OFFSET, "3km"), "raises", "3km", geom_srid=27700),
    # UC takes geometry in SRID 27700 only (BNG cells are 27700): any other is refused with a Fix.
    Case(
        "te_wrong_srid_uc", TE, (OFFSET, 3), "uc_refused", None, ("uc",), geom_srid=4326
    ),
)
MVP_G: Tuple[Case, ...] = (
    Case(
        "rt_32",
        RT,
        (Build("tile_4326"), 32, 32),
        "windows",
        [(0, 0, 32, 32), (32, 0, 32, 32), (0, 32, 32, 16), (32, 32, 32, 16)],
        ("session",),
    ),
    # Two tiles of one size that differ in one pixel, in ONE task: each gets its own windows. A shape that keeps
    # the last tile's dataset open for the next call must tell them apart by their bytes, and the edit sits
    # where a sample of the bytes cannot see it.
    Case(
        "rt_edit_pair",
        RT,
        (
            (Build("tile_edit_a"), 256, 256),
            (Build("tile_edit_b"), 256, 256),
        ),
        "windows_batch",
        None,
        ("session",),
    ),
    # A window of 0 pixels never advances: a parameter error that raises on every level and says how to fix it
    # (it used to hang the light tier and give an error tile on the JVM).
    Case(
        "rt_zero_window",
        RT,
        (Build("tile_4326"), 0, 32),
        "raises",
        "tile_width",
        ("session",),
    ),
    # A NULL window size is data: the same one error tile on every level that judges errors, never a bare exception.
    Case(
        "rt_null_window",
        RT,
        (Build("tile_4326"), None, 32),
        "error_tile",
        None,
        ("session",),
    ),
    # A NULL tile has nothing to split: no rows, as explode(NULL) gives.
    Case("rt_null", RT, (None, 32, 32), "no_rows", None, ("session",)),
    # An error tile in comes out as the same error tile (the upstream reason survives); a tile that
    # cannot be read is one error tile, never a failed task.
    Case(
        "rt_error_in",
        RT,
        (Build("tile_error"), 32, 32),
        "error_tile",
        UPSTREAM_ERROR,
        ("session",),
    ),
    Case(
        "rt_corrupt",
        RT,
        (Build("tile_corrupt"), 32, 32),
        "error_tile",
        None,
        ("session",),
    ),
    *_TE,
    # The array form returns the same chips as one value (a session function: UC reaches it through the TVF).
    *(
        replace(c, cid=c.cid.replace("te_", "ts_"), fn=TS, surfaces=("session",))
        for c in _TE
        if c.kind != "uc_refused"
    ),
)


# Aggregates (a case's `fn` is the aggregate; see `run_groups`). `args[0]` is the group's rows: tiles
# for the merge, (cellid, value) pairs for the rasterize; the other args are its per-group constants.
MA, BR = "gbx_rst_merge_agg", "gbx_rst_bng_rasterize_agg"
# out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, mode, kring_pad: 3 x 2 pixels of 1 km, the
# last column with no cell in it
CANVAS = (27700, None, 530000.0, 180000.0, 533000.0, 182000.0, 3, 2, "centroids", 0)
# What a NULL value burns is the one thing the tiers do not share (raster-functions.mdx, rst_bng_rasterize_agg):
# NaN on the lightweight tier, 1.0 on the heavyweight tier. A shape says which it follows (`run_groups`).
NULL_BURN = "<the burn of a NULL value>"
NAN = float("nan")
MVP_A: Tuple[Case, ...] = (
    Case(
        "ma_quad",
        MA,
        (Build("quad_tiles"),),
        "merged_equals",
        Build("tile_32"),
        ("session",),
    ),
    # An aggregate over nothing but error tiles gives the first member's error tile as it is (two members, so the
    # one-member shortcut of the heavy aggregate does not decide), where the output is a tile; NULL where it is
    # bytes, which have no place for a reason.
    Case(
        "ma_error_only",
        MA,
        (Build("tile_error_pair"),),
        "error_group",
        UPSTREAM_ERROR,
        ("session",),
    ),
    # A repeated cell keeps its largest value, also when the larger value arrives first (3.0 before 1.0), so
    # "the last one wins" fails. A NaN-only cell burns NaN on every tier. The last column holds no cell.
    Case(
        "br_dup_max",
        BR,
        (
            (
                ("TQ3080", 3.0),
                ("TQ3080", 1.0),
                ("TQ3180", 2.0),
                ("TQ3081", None),
                ("TQ3181", NAN),
            ),
        )
        + CANVAS,
        "pixels",
        [[NULL_BURN, NAN, -9999.0], [3.0, 2.0, -9999.0]],
        ("session",),
    ),
    # A NaN value beside numbers in one cell never wins (the largest number is burned), wherever it arrives; a cell
    # of nothing but NaN burns NaN (br_dup_max).
    Case(
        "br_nan_beside_numbers",
        BR,
        (
            (
                ("TQ3080", NAN),
                ("TQ3080", 4.0),
                ("TQ3080", 1.0),
                ("TQ3180", 2.0),
                ("TQ3180", NAN),
            ),
        )
        + CANVAS,
        "pixels",
        [[-9999.0, -9999.0, -9999.0], [4.0, 2.0, -9999.0]],
        ("session",),
    ),
    # Rows whose cell id is NULL carry no cell: nothing to burn, so the group has no raster.
    Case(
        "br_empty",
        BR,
        (((None, 1.0), (None, 2.0)),) + CANVAS,
        "null",
        None,
        ("session",),
    ),
)


def _tile_bytes(
    crs_value: str, w: int = 64, h: int = 48, origin=None, px=None
) -> bytes:
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.compression import creation_opts
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    data = (np.arange(w * h, dtype="float32").reshape(1, h, w) % 97).astype("float32")
    data[0, :4, :4] = -9999.0
    geographic = crs_value.upper().startswith("EPSG:4326")
    transform = (
        from_origin(origin[0], origin[1], px, px)
        if origin
        else (
            from_origin(-1.0, 52.0, 0.01, 0.01)
            if geographic
            else from_origin(400000.0, 5760000.0, 100.0, 100.0)
        )
    )
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        nodata=-9999.0,
        crs=resolve_crs(crs_value),
        transform=transform,
        **creation_opts("float32", compress="zstd"),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data)
        return mf.read()


def _tile(raster=None, path=None, window=None) -> dict:
    return {
        "cellid": 0,
        "raster": raster,
        "path": path,
        "path_mode": None,
        "window": window,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": {},
    }


# The sampling of the retile probes' cache key (pyrx/_mvp_forms.py, RetileKernel): the first and the last
# 64 KB and 64 blocks of 4 KB between them, nothing at all of a raster up to 512 KB. An edit that sits
# between two blocks of a raster above that is invisible to a key made of the sample.
_SAMPLE_HEAD, _SAMPLE_BLOCK, _SAMPLE_BLOCKS, _SAMPLE_WHOLE = 65536, 4096, 64, 524288


@functools.lru_cache(maxsize=1)
def _edit_pair() -> Tuple[bytes, bytes]:
    """Two 512 x 512 int32 GTiffs (1 MiB each, uncompressed) that differ in ONE pixel, chosen so that the
    bytes that differ lie between two sampled blocks (see _SAMPLE_*)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.compression import creation_opts
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    n = 512
    data = np.random.default_rng(7).integers(0, 1000, size=(1, n, n), dtype="int32")
    profile = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="int32",
        crs=resolve_crs("EPSG:4326"),
        transform=from_origin(-1.0, 52.0, 0.01, 0.01),
        **creation_opts("int32", compress="none"),
    )

    def encode(arr) -> bytes:
        with MemoryFile() as mf:
            with mf.open(**profile) as ds:
                ds.write(arr)
            return mf.read()

    a = encode(data)
    start = a.find(data[0, 0, :8].tobytes())  # where the pixels begin
    span = len(a) - 2 * _SAMPLE_HEAD - _SAMPLE_BLOCK
    block = 20  # the midpoint of the gap between two sampled blocks
    gap_lo = _SAMPLE_HEAD + span * block // (_SAMPLE_BLOCKS - 1) + _SAMPLE_BLOCK
    gap_hi = _SAMPLE_HEAD + span * (block + 1) // (_SAMPLE_BLOCKS - 1)
    idx = (
        (gap_lo + gap_hi) // 2 - start
    ) // 4  # the pixel whose 4 bytes lie in the gap
    edited = data.copy()
    edited[0, idx // n, idx % n] += 100000
    b = encode(edited)
    assert (
        len(a) == len(b) and len(a) > _SAMPLE_WHOLE
    ), "the pair must be one size, above the sampled limit"
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    assert (
        diff and gap_lo <= diff[0] and diff[-1] < gap_hi
    ), "the edit must lie between two sampled blocks"
    return a, b


def _build(name: str, tmpdir: str):
    if name == "tile_edit_a":
        return _tile(_edit_pair()[0])
    if name == "tile_edit_b":
        return _tile(_edit_pair()[1])
    if name == "tile_4326":
        return _tile(_tile_bytes("EPSG:4326"))
    if name == "tile_3857":
        return _tile(_tile_bytes("EPSG:3857"))
    if name == "wkt_3857":
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        return resolve_crs(
            "EPSG:3857"
        ).to_wkt()  # a CRS-as-WKT INPUT value (not a stored canonical string)
    if name == "tile_custom":
        return _tile(_tile_bytes(CUSTOM_TMERC))
    if name == "tile_corrupt":
        return _tile(b"II*\x00not-a-real-tiff")
    if name == "tile_empty":
        return _tile()
    if name == "tile_error_pair":  # two error tiles with the same reason
        return [_build("tile_error", tmpdir), _build("tile_error", tmpdir)]
    if (
        name == "tile_error"
    ):  # what a failed upstream function returns: no raster, no path, the reason
        return {**_tile(), "metadata": {ERROR_TILE_KEY: UPSTREAM_ERROR}}
    if name == "tile_virtual":
        path = os.path.join(tmpdir, "oracle_4326.tif")
        if not os.path.exists(path):
            with open(path, "wb") as fh:
                fh.write(_tile_bytes("EPSG:4326"))
        return _tile(
            None, path, {"col_off": 0, "row_off": 0, "width": 64, "height": 48}
        )
    return _build_helper_input(name, tmpdir)


def _dsm_tile_is_whole(path: str, size: int) -> bool:
    """True when `path` opens as a `size` x `size` raster whose last row reads: a copy that an interrupted run left
    half-written does not."""
    import rasterio
    from rasterio.windows import Window

    try:
        with rasterio.open(path) as ds:
            last = ds.read(1, window=Window(0, size - 1, size, 1))
            return (ds.width, ds.height) == (size, size) and last.shape == (1, size)
    except Exception:  # noqa: BLE001 - absent, not a raster, cut short: write it again
        return False


def dsm_tile_rows(
    tmpdir: str, grid: int = 2, size: int = 200, nodata_at: Sequence = ()
) -> List[Tuple[int, int, str]]:
    """The `(tx, ty, path)` manifest of a DSM written under `tmpdir`: a `grid` x `grid` block of tiles of `size`
    pixels at 1 m in EPSG:3857, tile (tx, ty) covering x in [tx*size, (tx+1)*size) and y likewise, plus the
    tiles in `nodata_at`, which hold NoData only. The ground is flat at 10 m, and every odd-`tx` tile has a
    30 m ridge on its 5 westernmost columns. A tile that is already there and whole is kept, so a re-run writes
    nothing. `tmpdir` may be a UC Volume, which does not reliably rename: each tile is written to a local file,
    copied (content only) to its path, and the local file removed.
    """
    import shutil

    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.compression import creation_opts
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    os.makedirs(tmpdir, exist_ok=True)
    cells = {(tx, ty) for tx in range(grid) for ty in range(grid)} | set(nodata_at)
    rows = []
    with tempfile.TemporaryDirectory(prefix="gbx-dsm-") as scratch:
        for tx, ty in sorted(cells):
            p = os.path.join(tmpdir, f"dsm_{tx}_{ty}.tif")
            if not _dsm_tile_is_whole(p, size):
                z = np.full((1, size, size), 10.0, dtype="float32")
                if tx % 2 == 1:
                    z[0, :, :5] = 40.0  # a 30 m ridge along x = tx*size .. tx*size + 5
                if (tx, ty) in nodata_at:
                    z[:] = -9999.0
                prof = dict(
                    driver="GTiff",
                    width=size,
                    height=size,
                    count=1,
                    dtype="float32",
                    nodata=-9999.0,
                    crs=resolve_crs("EPSG:3857"),
                    transform=from_origin(tx * size, (ty + 1) * size, 1.0, 1.0),
                    **creation_opts("float32", compress="zstd"),
                )
                local = os.path.join(scratch, os.path.basename(p))
                with rasterio.open(local, "w", **prof) as ds:
                    ds.write(z)
                shutil.copyfile(local, p)  # content only: a Volume takes no copystat
                os.remove(local)
            rows.append((tx, ty, p))
    return rows


def _build_helper_input(name: str, tmpdir: str):
    """The fixtures of the helper cases (tiles for the DataFrame helpers)."""
    if name == "dsm_tiles":  # four tiles, 400 x 400 m
        return dsm_tile_rows(tmpdir)
    if (
        name == "dsm_tiles_edge"
    ):  # the same four, and a tile of NoData east of them at (2, 0)
        return dsm_tile_rows(tmpdir, nodata_at=((2, 0),))
    if name in ("dsm_tiles_garbage", "dsm_tiles_truncated", "dsm_tiles_garbled"):
        # The four, tile (1, 1) not a raster / cut short / with one block gone bad in the middle of an otherwise whole
        # file (rows 150-159: inside the window of a tower on the corner at (200, 200) with 80 m, and not the last row).
        rows = dsm_tile_rows(os.path.join(tmpdir, name))
        path = next(p for tx, ty, p in rows if (tx, ty) == (1, 1))
        data = bytearray(open(path, "rb").read())
        if name.endswith("garbled"):
            import rasterio

            with rasterio.open(path) as ds:
                at = int(ds.get_tag_item("BLOCK_OFFSET_0_15", "TIFF", bidx=1))
                n = int(ds.get_tag_item("BLOCK_SIZE_0_15", "TIFF", bidx=1))
            data[at : at + n] = b"\xff" * n
        if name.endswith("garbage"):
            data = bytearray(b"not a raster")
        if name.endswith("truncated"):
            data = data[: len(data) // 2]
        with open(path, "wb") as fh:
            fh.write(data)
        return rows
    if name == "tile_32":
        return _tile(_tile_bytes("EPSG:4326", 32, 32))
    if name == "quad_tiles":
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.pyrx.core import tiling

        with MemoryFile(_tile_bytes("EPSG:4326", 32, 32)) as mf, mf.open() as ds:
            return [
                _tile(
                    raster,
                    window=dict(zip(("col_off", "row_off", "width", "height"), win)),
                )
                for win, raster in tiling.iter_retile_located(ds, 16, 16)
            ]
    if name == "tile_27700_path":
        path = os.path.join(tmpdir, "oracle_27700.tif")
        if not os.path.exists(path):
            with open(path, "wb") as fh:
                fh.write(
                    _tile_bytes(
                        "EPSG:27700", 20, 20, origin=(530000.0, 182000.0), px=100.0
                    )
                )
        return _tile(None, path, None)
    if name == "tile_27700_unaligned":  # 33 m pixels from an origin off the 1 km grid
        path = os.path.join(tmpdir, "oracle_27700_unaligned.tif")
        if not os.path.exists(path):
            with open(path, "wb") as fh:
                fh.write(
                    _tile_bytes(
                        "EPSG:27700", 60, 60, origin=(530010.0, 182010.0), px=33.0
                    )
                )
        return _tile(None, path, None)
    if name == "tile_27700_clipped":  # inside cell TQ3080 only
        from shapely.geometry import box

        t = _build("tile_27700_path", tmpdir)
        t["clip_polygon"] = box(530100.0, 180100.0, 530900.0, 180900.0).wkb
        t["clip_crs"] = "EPSG:27700"
        return t
    if name == "quad_paths":
        out = []
        for i, t in enumerate(_build("quad_tiles", tmpdir)):
            p = os.path.join(tmpdir, f"quad_{i}.tif")
            with open(p, "wb") as fh:
                fh.write(t["raster"])
            out.append(_tile(None, p, None))
        return out
    if name == "pair_aligned":
        import numpy as np
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.pyrx.core.compression import write_gtiff

        a = _tile_bytes("EPSG:4326", 32, 32)
        with MemoryFile(a) as mf, mf.open() as ds:
            prof, arr = ds.profile.copy(), ds.read()
        return [
            _tile(a),
            _tile(
                write_gtiff(
                    prof, np.where(arr == -9999.0, arr, arr * 2).astype("float32")
                )
            ),
        ]
    if name == "pair_shifted":  # the same grid, moved six pixels east
        from affine import Affine
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.pyrx.core.compression import write_gtiff

        a = _tile_bytes("EPSG:4326", 32, 32)
        with MemoryFile(a) as mf, mf.open() as ds:
            prof, arr = ds.profile.copy(), ds.read()
        prof["transform"] = prof["transform"] * Affine.translation(6, 0)
        return [_tile(a), _tile(write_gtiff(prof, arr))]
    raise KeyError(name)


def resolve_arg(arg: Any, tmpdir: str) -> Any:
    return _build(arg.name, tmpdir) if isinstance(arg, Build) else arg


def _field(row, name):
    if row is None:
        return None
    try:
        return row[name]
    except (KeyError, TypeError, IndexError, ValueError):
        return getattr(row, name, None)


def _raster_of(tile) -> Optional[bytes]:
    if _field(tile, "raster") is not None:
        return bytes(_field(tile, "raster"))
    p = _field(tile, "path")
    return open(p, "rb").read() if p else None


def _reference_warp(src, target_crs: str):
    """Independent nearest-neighbour reference: rasterio.warp and numpy only.

    It must not call the gbx warp code, or an implementation level would be checked against
    itself. Returns (width, height, masked first band).
    """
    import numpy as np
    from rasterio.crs import CRS
    from rasterio.warp import Resampling, calculate_default_transform, reproject

    dst_crs = CRS.from_user_input(target_crs)
    transform, width, height = calculate_default_transform(
        src.crs, dst_crs, src.width, src.height, *src.bounds
    )
    nodata = src.nodata
    dst = np.full((height, width), 0 if nodata is None else nodata, dtype=src.dtypes[0])
    reproject(
        source=src.read(1),
        destination=dst,
        src_transform=src.transform,
        src_crs=src.crs,
        src_nodata=nodata,
        dst_transform=transform,
        dst_crs=dst_crs,
        dst_nodata=nodata,
        resampling=Resampling.nearest,
    )
    band = (
        np.ma.masked_equal(dst, nodata)
        if nodata is not None
        else np.ma.masked_array(dst)
    )
    return width, height, band


def _check_tile(case: Case, out, args) -> Optional[str]:
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import crs_equal

    raster = _field(out, "raster")
    if raster is None:
        return f"{case.cid}: no raster in the result (metadata={_field(out, 'metadata')!r})"
    want_crs, want_srid = (
        case.expect if isinstance(case.expect, tuple) else (case.expect, None)
    )
    with MemoryFile(_raster_of(args[0])) as src_mf, src_mf.open() as src:
        want_w, want_h, w = _reference_warp(src, want_crs)
    with MemoryFile(bytes(raster)) as a, a.open() as got:
        if not crs_equal(got.crs, want_crs):
            return f"{case.cid}: CRS {got.crs} is not {want_crs}"
        from databricks.labs.gbx.core.crs import authority_srid_of

        if want_srid is not None and authority_srid_of(got.crs) != want_srid:
            return f"{case.cid}: SRID {authority_srid_of(got.crs)} is not {want_srid}"
        if abs(got.width - want_w) > 1 or abs(got.height - want_h) > 1:
            return (
                f"{case.cid}: {got.width}x{got.height} vs reference {want_w}x{want_h}"
            )
        g = got.read(1, masked=True)
        # Nearest-neighbour pixels must agree, not just their mean. A grid one pixel larger or
        # smaller than the reference is compared over the rows and columns both have (same
        # origin), so a size difference never turns the pixel check off.
        rows, cols = min(g.shape[0], w.shape[0]), min(g.shape[1], w.shape[1])
        same = (g[:rows, :cols].filled(-1) == w[:rows, :cols].filled(-1)).mean()
        if same < 0.99:
            return f"{case.cid}: only {same:.1%} of pixels match the reference warp"
        if g.count() == 0 or abs(float(g.mean()) - float(w.mean())) > 0.01 * max(
            1.0, abs(float(w.mean()))
        ):
            return f"{case.cid}: valid mean {g.mean()} vs reference {w.mean()}"
    return None


def _check_geom(case: Case, out) -> Optional[str]:
    from databricks.labs.gbx._geom import parse_geom

    g = parse_geom(out if not isinstance(out, (bytearray, memoryview)) else bytes(out))
    if g is None:
        return f"{case.cid}: no geometry"
    if (
        bool(g.is_valid) != case.expect["valid"]
        or abs(g.area - case.expect["area"]) > 1e-9
    ):
        return f"{case.cid}: valid={g.is_valid} area={g.area}, want {case.expect}"
    return None


def check(case: Case, out, args) -> Optional[str]:
    """None when `out` meets the case; else a one-line reason."""
    if case.kind == "exact":
        return (
            None
            if out == case.expect
            else f"{case.cid}: got {out!r}, want {case.expect!r}"
        )
    if case.kind == "exact_rows":
        return (
            None
            if list(out) == list(case.expect)
            else f"{case.cid}: got {list(out)!r}, want {list(case.expect)!r}"
        )
    if case.kind == "null":
        return None if out is None else f"{case.cid}: got {out!r}, want NULL"
    if case.kind in ("raises", "uc_refused"):
        return f"{case.cid}: returned {out!r}, want an error"
    if case.kind == "error_tile":
        meta = _field(out, "metadata") or {}
        ok = (
            out is not None
            and _field(out, "raster") is None
            and _field(out, "path") is None
            and bool(meta.get(ERROR_TILE_KEY))
        )
        if ok and case.expect is not None and meta.get(ERROR_TILE_KEY) != case.expect:
            return f"{case.cid}: the reason is {meta.get(ERROR_TILE_KEY)!r}, want {case.expect!r} unchanged"
        return (
            None
            if ok
            else f"{case.cid}: got {out!r}, want an error tile (no raster, no path, metadata[{ERROR_TILE_KEY!r}] set)"
        )
    if case.kind == "tile_props":
        return _check_tile(case, out, args)
    if case.kind == "geom_props":
        return _check_geom(case, out)
    if case.kind == "merged_equals":
        return _check_merged(case, out)
    if case.kind == "pixels":
        return _check_pixels(case, out)
    raise ValueError(case.kind)


def check_error(case: Case, exc: BaseException) -> Optional[str]:
    if case.kind == "raises":
        # a tuple is alternatives: levels that say it differently (a light read failure names no tile)
        wanted = case.expect if isinstance(case.expect, tuple) else (case.expect,)
        if any(str(w) in str(exc) for w in wanted):
            return None
        return f"{case.cid}: error does not mention {case.expect!r}: {type(exc).__name__}: {str(exc)[:300]}"
    if case.kind == "uc_refused":
        return (
            None
            if "Fix:" in str(exc)
            else f"{case.cid}: refused without a 'Fix:' line: {exc}"
        )
    return f"{case.cid}: raised {type(exc).__name__}: {str(exc)[:300]}"


def _raster_bytes(out) -> Optional[bytes]:
    """The raster bytes of a result that is bytes, a tile struct, or NULL."""
    if out is None:
        return None
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    raster = _field(out, "raster")
    return None if raster is None else bytes(raster)


def _check_merged(case: Case, out) -> Optional[str]:
    """A merged raster has the expected grid and the expected pixels (`case.expect` names the fixture)."""
    import numpy as np
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import crs_equal

    got_b = _raster_bytes(out)
    want_b = _build(case.expect.name, tempfile.gettempdir())["raster"]
    if got_b is None:
        return f"{case.cid}: no merged raster"
    with (
        MemoryFile(got_b) as a,
        a.open() as got,
        MemoryFile(want_b) as b,
        b.open() as want,
    ):
        if (got.shape, got.transform) != (want.shape, want.transform) or not crs_equal(
            got.crs, want.crs
        ):
            return f"{case.cid}: {got.shape}@{got.transform} in {got.crs} vs {want.shape}@{want.transform} in {want.crs}"
        return (
            None
            if np.array_equal(got.read(), want.read())
            else f"{case.cid}: pixels differ"
        )


def _check_pixels(case: Case, out) -> Optional[str]:
    """The first band of the raster (bytes or a tile) is `case.expect`, pixel by pixel (NaN equals NaN), and it sits
    where the canvas of the case puts it: its transform, its CRS and its NoData value. A canvas aggregate's
    arguments are the group, out_srid, pixel_size, xmin, ymin, xmax, ymax, width, height, ...
    """
    import numpy as np
    from affine import Affine
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import crs_equal

    b = _raster_bytes(out)
    if b is None:
        return f"{case.cid}: no raster"
    srid, (xmin, ymin, xmax, ymax, width, height) = case.args[1], case.args[3:9]
    want_transform = Affine(
        (xmax - xmin) / width, 0.0, xmin, 0.0, -(ymax - ymin) / height, ymax
    )
    with MemoryFile(b) as m, m.open() as ds:
        got = ds.read(1).astype("float64")
        where = (ds.transform, ds.crs, ds.nodata)
    if not (where[0] == want_transform and crs_equal(where[1], f"EPSG:{srid}")):
        return f"{case.cid}: placed at {where[0]} in {where[1]}, want {want_transform} in EPSG:{srid}"
    if where[2] != -9999.0:
        return f"{case.cid}: NoData is {where[2]}, want -9999.0"
    ok = got.shape == np.shape(case.expect) and np.array_equal(
        got, np.asarray(case.expect, dtype="float64"), equal_nan=True
    )
    return None if ok else f"{case.cid}: pixels {got.tolist()}, want {case.expect}"


def _check_error_group(case: Case, out, tile_out: bool) -> Optional[str]:
    """A group of nothing but error tiles: the error tile (reason `case.expect`) when the result is a tile,
    NULL when it is bytes (they have no place for a reason)."""
    if tile_out:
        return check(replace(case, kind="error_tile"), out, None)
    return None if out is None else f"{case.cid}: got bytes, want NULL"


def _check_helper(case: Case, rows, tiles) -> Optional[str]:
    """Judge the collected rows of one helper call (`tiles` are the input tiles)."""
    import numpy as np
    from rasterio.io import MemoryFile

    if case.kind == "raises":
        return f"{case.cid}: returned {len(rows)} rows, want an error"
    ts = [r["tile"] for r in rows] if rows and "tile" in rows[0].asDict() else []
    if case.kind == "error_tile":
        return (
            check(case, ts[0], None)
            if len(ts) == 1
            else f"{case.cid}: {len(ts)} rows, want the one error tile"
        )
    if case.kind == "null":
        return (
            None
            if rows and all(r["raster"] is None for r in rows)
            else f"{case.cid}: want a NULL raster, got {len(rows)} rows"
        )
    if case.kind == "virtual_windows":
        if any(t["raster"] is not None for t in ts):
            return f"{case.cid}: a sub-tile carries bytes"
        wins = sorted(
            (
                t["window"]["col_off"],
                t["window"]["row_off"],
                t["window"]["width"],
                t["window"]["height"],
            )
            for t in ts
        )
        return None if wins == sorted(case.expect) else f"{case.cid}: windows {wins}"
    if case.kind == "virtual_cover":
        import rasterio

        with rasterio.open(tiles[0]["path"]) as src:
            seen = np.zeros((src.height, src.width), dtype=bool)
        for t in ts:
            w = t["window"]
            seen[
                w["row_off"] : w["row_off"] + w["height"],
                w["col_off"] : w["col_off"] + w["width"],
            ] = True
        return (
            None
            if seen.all()
            else f"{case.cid}: {int((~seen).sum())} of {seen.size} source pixels are in no cell window"
        )
    if case.kind == "virtual_cells":
        from databricks.labs.gbx.pygx import _bng

        cells = {_bng.format(t["cellid"]) for t in ts}
        ok = cells == case.expect and all(
            t["raster"] is None and t["clip_crs"] == "EPSG:27700" for t in ts
        )
        return None if ok else f"{case.cid}: cells {cells}"
    if case.kind == "merged_equals":
        return _check_merged(case, rows[0]["raster"] if rows else None)
    if case.kind == "pixel_ratio":
        with (
            MemoryFile(bytes(rows[0]["raster"])) as a,
            a.open() as got,
            MemoryFile(tiles[0]["raster"]) as b,
            b.open() as base,
        ):
            x = base.read(1, masked=True)
            with np.errstate(
                divide="ignore", invalid="ignore"
            ):  # a valid 0 divides to NaN
                ratio = got.read(1)[~x.mask] / x.compressed()
        ratio = ratio[np.isfinite(ratio)]
        return (
            None
            if np.allclose(ratio, case.expect, rtol=1e-6)
            else f"{case.cid}: ratio {np.unique(ratio)[:5]}"
        )
    raise ValueError(case.kind)


def run_towers(spark, case: Case, tmpdir: str, level: Optional[str] = None):
    """`rst_viewshed_towers` over the case's DSM and towers, at `level` (None: the default one).
    Returns {tower: (visible_px, total_px)}; a tower with no row is not in it."""
    from databricks.labs.gbx.pyrx.functions import rst_viewshed_towers

    manifest_rows, towers, radius, height = case.args
    tiles = spark.createDataFrame(
        resolve_arg(manifest_rows, tmpdir), "tx int, ty int, path string"
    )
    masts = len(towers[0]) == 4  # a per-tower mast column, named by `height`
    sites = spark.createDataFrame(
        list(towers),
        "tower_cellid string, x double, y double"
        + (f", {height} double" if masts else ""),
    )
    got = rst_viewshed_towers(
        sites, tiles, radius, height, x0=0.0, y0=0.0, tile_m=DSM_TILE_M, _levels=level
    )
    return {r["tower_cellid"]: (r["visible_px"], r["total_px"]) for r in got.collect()}


def towers_differ(want: Dict, got: Dict) -> Optional[str]:
    """None when every tower of `want` ({tower: (visible_px, total_px) or None for no row}) is in `got` within
    TOWERS_TOLERANCE of its total_px, and `got` holds no other; else the towers that differ, with both counts.
    """
    bad = []
    for k in sorted(set(want) | set(got)):
        w, g = want.get(k), got.get(k)
        if (w is None) != (g is None) or (
            w is not None
            and any(abs(g[i] - w[i]) > TOWERS_TOLERANCE * w[1] for i in (0, 1))
        ):
            bad.append(f"{k}: want {w}, got {g}")
    return "; ".join(bad) or None


def run_helpers(
    spark, cases, tmpdir: Optional[str] = None, level: Optional[str] = None
):
    """Build a (key, tile) DataFrame from the case, call the helper by name, check the rows.
    A towers case runs at `level` (None: the default one)."""
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.pyrx import helpers

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    out = []
    for case in cases:
        if "session" not in case.surfaces:
            continue
        if case.fn == "rst_viewshed_towers":
            try:
                got = run_towers(spark, case, tmpdir, level)
                if case.kind == "raises":
                    out.append(
                        (case, f"{case.cid}: returned {len(got)} rows, want an error")
                    )
                else:
                    msg = towers_differ(case.expect, got)
                    out.append((case, msg and f"{case.cid}: {msg}"))
            except Exception as exc:  # noqa: BLE001
                out.append((case, check_error(case, exc)))
            continue
        first = resolve_arg(case.args[0], tmpdir)
        tiles = first if isinstance(first, list) else [first]
        df = spark.createDataFrame(
            [("a", t) for t in tiles], f"key string, tile {TILE_SQL}"
        )
        fn, extra = getattr(helpers, case.fn), list(case.args[1:])
        try:
            if case.fn == "merge_by_key":
                res = fn(df, ["key"], method=extra[0]).collect()
            elif case.fn == "combine_by_key":
                res = fn(df, ["key"], stat=extra[0]).collect()
            else:
                res = fn(df, "tile", *extra).collect()
            out.append((case, _check_helper(case, res, tiles)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            out.append((case, check_error(case, exc)))
    return out


_SQL = {
    "DOUBLE": "double",
    "INT": "int",
    "STRING": "string",
    "BINARY": "binary",
    "BOOLEAN": "boolean",
}


def _ddl_type(value, param) -> str:
    from databricks.labs.gbx.manifest import TILE_SQL

    if isinstance(value, dict) or "TILE" in param.session:
        return TILE_SQL
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, (bytes, bytearray)):
        return "binary"
    if isinstance(value, str):
        return "string"
    return _SQL[param.session[0]]


def _argument_rows(case: Case, tmpdir: str) -> List[List[Any]]:
    raw = case.args if case.kind == "exact_rows" else (case.args, *case.variants)
    return [[resolve_arg(a, tmpdir) for a in row] for row in raw]


def _verdict(case: Case, outs, rows) -> Optional[str]:
    """`check` for the whole batch: exact_rows judges the list, any other kind each row (variants included)."""
    if case.kind == "exact_rows":
        return check(case, outs, rows[0])
    return next(filter(None, (check(case, o, r) for o, r in zip(outs, rows))), None)


def run_session(
    spark,
    cases: Sequence[Case],
    name_of: Callable[[str], str] = lambda f: f,
    tmpdir: Optional[str] = None,
    surface: str = "session",
    column_of: Optional[Callable[[str], Callable]] = None,
) -> List[Tuple[Case, Optional[str]]]:
    """Run each case through SQL (`name_of(fn)(cols...)`) and check it. One result per case.

    With `column_of` (SQL name -> Column function) the case runs through the Python Column
    API instead of SQL, so both APIs are held to the same rows.
    """
    from databricks.labs.gbx import manifest

    if tmpdir is None:  # virtual-tile fixtures live here; removed when the run ends
        with tempfile.TemporaryDirectory(prefix="gbx-oracle-") as d:
            return run_session(spark, cases, name_of, d, surface, column_of)
    man = manifest.load()
    results = []
    for case in cases:
        if surface not in case.surfaces:
            continue
        rows = _argument_rows(case, tmpdir)
        args = rows[0]
        fn = man.functions[case.fn]
        params = fn.params
        # A column is typed from its first non-NULL value; an all-NULL column from the manifest.
        schema = ", ".join(
            f"c{i} "
            + _ddl_type(next((r[i] for r in rows if r[i] is not None), None), params[i])
            for i in range(len(args))
        )
        # On the UC surface GEOMETRY params get an integer-SRID constructor, never a CRS string.
        cols = ", ".join(
            (
                f"st_geomfromwkt(c{i}, {case.geom_srid})"
                if surface == "uc" and params[i].uc == "GEOMETRY"
                else f"c{i}"
            )
            for i in range(len(args))
        )
        call = f"{name_of(case.fn)}({cols})"
        if surface == "uc" and fn.returns_uc == "GEOMETRY":
            call = f"st_asewkb({call})"
        try:
            df = spark.createDataFrame([tuple(r) for r in rows], schema)
            if column_of is None:
                df.createOrReplaceTempView("_gbx_oracle")
                got = spark.sql(f"SELECT {call} AS out FROM _gbx_oracle").collect()
            else:
                from pyspark.sql import functions as F

                fcols = [F.col(f"c{i}") for i in range(len(args))]
                got = df.select(column_of(case.fn)(*fcols).alias("out")).collect()
            outs = [r["out"] for r in got]
            results.append((case, _verdict(case, outs, rows)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            results.append((case, check_error(case, exc)))
    return results


_WINDOW_KEYS = ("col_off", "row_off", "width", "height")


def _window_of(tile, src) -> Tuple[int, int, int, int]:
    """Where `tile` was cut from `src`, read from the pixels: its origin in the source grid and its size.

    The JVM tier does not fill the tile's `window` field, so the window is the geotransform's, which
    every level shares. A level that fills `window` has it held to the same numbers.
    """
    from rasterio.io import MemoryFile

    with MemoryFile(bytes(_field(tile, "raster"))) as mf, mf.open() as got:
        col = round((got.transform.c - src.transform.c) / src.transform.a)
        row = round((got.transform.f - src.transform.f) / src.transform.e)
        return col, row, got.width, got.height


def _check_window_batch(case: Case, tiles: list, arg_rows) -> Optional[str]:
    """Several tiles through one task: every output piece is a window of ONE of them, and together the pieces
    are exactly the windows of all of them (a piece's pixel sum must be the sum of that window of its tile).

    The pieces carry no link to the tile they came from, so the expectation is the multiset over all the
    tiles: a dataset kept from the first tile and cut again for the second gives the first tile's window twice
    and the second's edited one never.
    """
    from collections import Counter

    from rasterio.io import MemoryFile
    from rasterio.windows import Window

    if any(_field(t, "raster") is None for t in tiles):
        return f"{case.cid}: a piece has no raster"
    want: Counter = Counter()
    for tile, tw, th in (r[:3] for r in arg_rows):
        with MemoryFile(_raster_of(tile)) as mf, mf.open() as src:
            for row in range(0, src.height, th):
                for col in range(0, src.width, tw):
                    win = (
                        col,
                        row,
                        min(tw, src.width - col),
                        min(th, src.height - row),
                    )
                    want[
                        (
                            win,
                            float(src.read(1, window=Window(*win), masked=True).sum()),
                        )
                    ] += 1
    got: Counter = Counter()
    with MemoryFile(_raster_of(arg_rows[0][0])) as mf, mf.open() as src:
        for t in tiles:
            with MemoryFile(bytes(_field(t, "raster"))) as o, o.open() as piece:
                got[(_window_of(t, src), float(piece.read(1, masked=True).sum()))] += 1
    if got == want:
        return None
    return f"{case.cid}: {sum((want - got).values())} expected pieces missing {sorted((want - got))[:2]}, unexpected {sorted((got - want))[:2]}"


def _check_rows(case: Case, rows: list, args) -> Optional[str]:
    """Judge the rows a generator produced for one case."""
    from databricks.labs.gbx._geom import parse_geom

    tiles = [
        _field(r, "tile") or r for r in rows
    ]  # one `tile` column (JVM), or the tile fields (light)
    if case.kind == "no_rows":
        return None if not rows else f"{case.cid}: {len(rows)} rows, want none"
    if case.kind == "error_tile":
        if len(rows) != 1:
            return f"{case.cid}: {len(rows)} rows, want the one error tile"
        return check(case, tiles[0], args)
    if case.kind == "raises":
        return f"{case.cid}: returned {len(rows)} rows, want an error"
    if case.kind == "windows_batch":
        return _check_window_batch(case, tiles, args)
    if case.kind == "windows":
        from rasterio.io import MemoryFile
        from rasterio.windows import Window

        if any(_field(t, "raster") is None for t in tiles):
            return f"{case.cid}: a sub-tile has no raster"
        with MemoryFile(_raster_of(args[0])) as mf, mf.open() as src:
            seen = []
            for t in tiles:
                win = _window_of(t, src)
                declared = _field(t, "window")
                if (
                    declared is not None
                    and tuple(declared[k] for k in _WINDOW_KEYS) != win
                ):
                    return f"{case.cid}: window field {tuple(declared[k] for k in _WINDOW_KEYS)}, pixels say {win}"
                want = float(src.read(1, window=Window(*win), masked=True).sum())
                with MemoryFile(bytes(_field(t, "raster"))) as o, o.open() as got:
                    if abs(float(got.read(1, masked=True).sum()) - want) > 1e-6 * max(
                        1.0, abs(want)
                    ):
                        return f"{case.cid}: pixel sum differs in window {win}"
                seen.append(win)
        return (
            None
            if sorted(seen) == sorted(case.expect)
            else f"{case.cid}: windows {sorted(seen)}, want {sorted(case.expect)}"
        )
    if case.kind == "cells":
        e = case.expect
        got = {
            (_field(r, "cellid"), bool(_field(r, "core"))): _field(r, "chip")
            for r in rows
        }
        if len(got) != len(rows):
            return f"{case.cid}: a cell is returned twice"
        if "cells" in e:
            areas = {
                cid: parse_geom(bytes(ch)).area
                for (cid, _), ch in got.items()
                if ch is not None
            }
            if set(areas) != set(e["cells"]) or any(
                abs(areas[k] - v) > 1e-6 * v for k, v in e["cells"].items()
            ):
                return f"{case.cid}: cells/areas {areas}, want {e['cells']}"
        if "count" in e and len(got) != e["count"]:
            return f"{case.cid}: {len(got)} cells, want {e['count']}"
        cores = [ch for (_, core), ch in got.items() if core]
        if len(cores) != e["core"]:
            return f"{case.cid}: {len(cores)} core cells, want {e['core']}"
        if e.get("core_chips") == "null" and any(ch is not None for ch in cores):
            return f"{case.cid}: core chips carry geometry with keep_core_geom off"
        if isinstance(e.get("core_chips"), float) and any(
            abs(parse_geom(bytes(ch)).area - e["core_chips"]) > 1e-3 for ch in cores
        ):
            return f"{case.cid}: core chips are not whole cells"
        return None
    raise ValueError(case.kind)


def run_rows(
    spark,
    cases,
    shape_sql: str = "SELECT t.* FROM _gbx_oracle, LATERAL {fn}({args}) t",
    name_of: Callable[[str], str] = lambda f: f,
    tmpdir: Optional[str] = None,
    surface: str = "session",
    pad: bool = False,
):
    """Generators: `shape_sql` (with {fn} and {args}) yields the rows a case checks; one shape per form.

    `pad` fills the optional arguments a case leaves out with their defaults, for a function of fixed arity.
    """
    from databricks.labs.gbx import manifest

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    man = manifest.load()
    out = []
    for case in cases:
        if surface not in case.surfaces:
            continue
        batch = case.kind == "windows_batch"
        arg_rows = [
            [resolve_arg(a, tmpdir) for a in row]
            for row in (case.args if batch else (case.args,))
        ]
        args = arg_rows[0]
        params = man.functions[case.fn].params
        schema = ", ".join(
            f"c{i} {_ddl_type(v, params[i])}" for i, v in enumerate(args)
        )
        cols = ", ".join(
            (
                f"st_geomfromwkt(c{i}, {case.geom_srid})"
                if surface == "uc" and params[i].uc == "GEOMETRY"
                else f"c{i}"
            )
            for i in range(len(args))
        )
        if pad:
            cols = ", ".join([cols] + [p.default for p in params[len(args) :]])
        try:
            df = spark.createDataFrame([tuple(r) for r in arg_rows], schema)
            # one partition: the rows of a batch case go through ONE task, one after the other
            (df.coalesce(1) if batch else df).createOrReplaceTempView("_gbx_oracle")
            rows = spark.sql(shape_sql.format(fn=name_of(case.fn), args=cols)).collect()
            out.append((case, _check_rows(case, rows, arg_rows if batch else args)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            out.append((case, check_error(case, exc)))
    return out


def _typed_literal(value, param, case: Case) -> str:
    """`value` as a SQL literal of the parameter's type (a bare `1000.0` is a DECIMAL); NULL stays untyped."""
    lit = _sql_literal(value, param, case)
    return lit if value is None else f"CAST({lit} AS {_SQL[param.session[0]]})"


def group_width(params) -> int:
    """How many leading parameters one row of an aggregate's group fills: a tile, or a (cellid, value) pair."""
    return 1 if params[0].session == ("TILE",) else 2


def _burn_nulls(expect, null_burn: float):
    """`expect` (rows of pixels) with the marker of a NULL value's burn replaced by what this tier burns."""
    return [[null_burn if v == NULL_BURN else v for v in row] for row in expect]


def run_groups(
    spark,
    cases,
    shape_sql: str = "SELECT key, {fn}({args}) AS out FROM _gbx_oracle GROUP BY key",
    tmpdir: Optional[str] = None,
    null_burn: float = NAN,
):
    """Aggregates: `args[0]` is the group's rows, the rest are its constants. The table `_gbx_oracle` has one
    row per member: a key, the member's columns c0.., then the constants as columns. `shape_sql` takes `{fn}`,
    `{src}` (that table) and `{args}` (the member columns, then the constants as literals: a Catalyst aggregate
    reads them when it finishes, with no row to look in) and returns `(key, out)`. `null_burn` is what a NULL
    value burns on the tier the shape runs.
    """
    from pyspark.sql.types import StructType

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manifest import TILE_SQL

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    man = manifest.load()
    out = []
    for case in cases:
        if "session" not in case.surfaces:
            continue
        params = man.functions[case.fn].params
        group = resolve_arg(case.args[0], tmpdir)
        group = (
            [group] if isinstance(group, dict) else group
        )  # one tile is a group of one
        consts = [resolve_arg(a, tmpdir) for a in case.args[1:]]
        if group_width(params) == 1:
            rows, first = [("a", t, *consts) for t in group], [f"c0 {TILE_SQL}"]
        else:
            rows, first = [("a", c, v, *consts) for c, v in group], [
                "c0 string",
                "c1 double",
            ]
        n0 = len(first)
        schema = ", ".join(
            ["key string"]
            + first
            + [
                f"c{i + n0} {_ddl_type(v, params[i + n0])}"
                for i, v in enumerate(consts)
            ]
        )
        members = ", ".join(f"c{i}" for i in range(n0))
        literals = [
            _typed_literal(v, params[i + n0], case) for i, v in enumerate(consts)
        ]
        want = (
            replace(case, expect=_burn_nulls(case.expect, null_burn))
            if case.kind == "pixels"
            else case
        )
        try:
            spark.createDataFrame(rows, schema).createOrReplaceTempView("_gbx_oracle")
            df = spark.sql(
                shape_sql.format(
                    fn=case.fn,
                    src="_gbx_oracle",
                    args=", ".join([members] + literals),
                )
            )
            got = df.collect()
            value = got[0]["out"] if got else None
            if case.kind == "error_group":
                tile_out = isinstance(df.schema["out"].dataType, StructType)
                out.append((case, _check_error_group(want, value, tile_out)))
            else:
                out.append((case, check(want, value, case.args)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            out.append((case, check_error(case, exc)))
    return out


# --- Unity Catalog surface -----------------------------------------------------------------------


def _uc_value(arg, param, case: Case):
    """A session-shaped oracle argument as the UC function receives it."""
    if arg is None or isinstance(arg, dict):
        return arg
    if param.uc == "GEOMETRY" and isinstance(arg, str):
        from shapely import set_srid, to_wkb

        from databricks.labs.gbx._geom import parse_geom

        return to_wkb(set_srid(parse_geom(arg), case.geom_srid), include_srid=True)
    if param.uc == "STRING" and not isinstance(arg, str):
        return str(arg)  # UC has one STRING type: an INT literal arrives as its digits
    return arg


def run_handlers(cases, tmpdir: Optional[str] = None):
    """L4 without Spark: call the UC handlers with Python values. GEOMETRY params get EWKB bytes."""
    from databricks.labs.gbx import manifest

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    man = manifest.load()
    out = []
    for case in cases:
        fn = man.functions[case.fn]
        if "uc" not in case.surfaces or "L4" not in fn.levels:
            continue
        handler = manifest.resolve_ref(fn.levels["L4"]["handler"])
        try:
            msgs = []
            for row in (case.args, *case.variants):
                args = [resolve_arg(a, tmpdir) for a in row]
                args += [  # the SQL wrapper supplies the DEFAULTs
                    None if p.default is None else p.default.strip("'")
                    for p in fn.params[len(args) :]
                ]
                args = [_uc_value(a, p, case) for a, p in zip(args, fn.params)]
                msgs.append(check(case, handler(*args), args))
            out.append((case, next(filter(None, msgs), None)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            out.append((case, check_error(case, exc)))
    return out


def run_handlers_rows(cases, tmpdir: Optional[str] = None):
    """UC table functions without Spark: call the handler behind the TVF and check its elements as rows."""
    from databricks.labs.gbx import manifest

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    man = manifest.load()
    out = []
    for case in cases:
        fn = man.functions[case.fn]
        if "uc" not in case.surfaces or "tvf_over" not in fn.levels.get("L4", {}):
            continue
        inner = man.functions[fn.levels["L4"]["tvf_over"]]
        handler = manifest.resolve_ref(inner.levels["L4"]["handler"])
        args = [resolve_arg(a, tmpdir) for a in case.args]
        args += [
            None if p.default is None else p.default.strip("'")
            for p in inner.params[len(args) :]
        ]
        args = [_uc_value(a, p, case) for a, p in zip(args, inner.params)]
        args = [
            a == "true" if p.uc == "BOOLEAN" and isinstance(a, str) else a
            for a, p in zip(args, inner.params)
        ]
        try:
            out.append((case, _check_rows(case, list(handler(*args) or []), args)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            out.append((case, check_error(case, exc)))
    return out


def _sql_literal(value, param, case: Case) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, dict):
        raster = (
            "NULL" if value["raster"] is None else "X'" + value["raster"].hex() + "'"
        )
        path = "NULL" if value["path"] is None else "'" + value["path"] + "'"
        return (
            f"named_struct('cellid', 0L, 'raster', {raster}, 'path', {path}, "
            "'path_mode', CAST(NULL AS STRING), "
            "'window', CAST(NULL AS struct<col_off:int,row_off:int,width:int,height:int>), "
            "'clip_polygon', CAST(NULL AS BINARY), 'clip_crs', CAST(NULL AS STRING), "
            "'crs', CAST(NULL AS STRING), 'metadata', CAST(map() AS map<string,string>))"
        )
    if param.uc == "GEOMETRY":
        return f"st_geomfromwkt('{value}', {case.geom_srid})"  # integer SRID only (product CRS facts)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, float) and math.isnan(value):
        return "CAST('NaN' AS DOUBLE)"
    if isinstance(value, float) and math.isinf(value):
        return (
            "CAST('Infinity' AS DOUBLE)" if value > 0 else "CAST('-Infinity' AS DOUBLE)"
        )
    return repr(value)


def uc_sql(cases, catalog: str, schema: str, tmpdir: str) -> List[Tuple[str, str]]:
    """(cid, statement) per UC case: one SELECT each, so a refusal can't hide another case."""
    from databricks.labs.gbx import manifest

    man = manifest.load()
    stmts = []
    for case in cases:
        if "uc" not in case.surfaces:
            continue
        fn = man.functions[case.fn]
        lits = [
            _sql_literal(resolve_arg(a, tmpdir), p, case)
            for a, p in zip(case.args, fn.params)
        ]
        call = f"`{catalog}`.`{schema}`.{case.fn}({', '.join(lits)})"
        if fn.returns_uc == "GEOMETRY":
            call = f"st_asewkb({call})"
        stmts.append((case.cid, f"SELECT {call} AS out"))
    return stmts


_CID_MARK = "-- cid: "


def uc_sql_text(pairs: Sequence[Tuple[str, str]]) -> str:
    """The statement file gbx:uc:sql reads: a `-- cid: <id>` line, then the statement."""
    return "\n\n".join(f"{_CID_MARK}{cid}\n{stmt};" for cid, stmt in pairs) + "\n"


def parse_uc_sql(text: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for line in text.splitlines():
        if line.startswith(_CID_MARK):
            pairs.append((line[len(_CID_MARK) :].strip(), ""))
        elif pairs and line.strip() and not line.startswith("--"):
            cid, stmt = pairs[-1]
            pairs[-1] = (cid, f"{stmt}\n{line}" if stmt else line)
    return [(cid, stmt.strip().rstrip(";").strip()) for cid, stmt in pairs]


_INTS = {"BYTE", "TINYINT", "SHORT", "SMALLINT", "INT", "LONG", "BIGINT"}
_FLOATS = {"FLOAT", "DOUBLE", "DECIMAL"}
_BINARY_FIELDS = ("raster", "clip_polygon")  # the BINARY fields of a tile struct


def decode_uc_cell(raw, type_name: str, *, decode_binary: bool = True):
    """One cell of a Statement API JSON_ARRAY result -> a Python value.

    The API returns every cell as a string: BINARY as base64, DOUBLE as text (NaN included), and
    STRUCT / MAP / ARRAY as JSON text.
    """
    if raw is None:
        return None
    t = type_name.upper()
    if t == "BINARY":
        return base64.b64decode(raw) if decode_binary else raw
    if t in _INTS:
        return int(raw)
    if t in _FLOATS:
        return float(raw)
    if t == "BOOLEAN":
        return str(raw).lower() == "true"
    if t in ("STRUCT", "MAP", "ARRAY"):
        value = json.loads(raw) if isinstance(raw, str) else raw
        if decode_binary and isinstance(value, dict):
            for k in _BINARY_FIELDS:
                if isinstance(value.get(k), str):
                    value[k] = base64.b64decode(value[k])
        return value
    return raw


_BYTES_KEY = "$bytes"


def _jsonable(obj):
    if isinstance(obj, (bytes, bytearray)):
        return {_BYTES_KEY: base64.b64encode(bytes(obj)).decode("ascii")}
    if isinstance(obj, dict):
        return {k: _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    return obj


def _from_json(obj):
    if isinstance(obj, dict):
        if set(obj) == {_BYTES_KEY}:
            return base64.b64decode(obj[_BYTES_KEY])
        return {k: _from_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_from_json(v) for v in obj]
    return obj


def dump_uc_results(results: dict, path: str) -> None:
    """`{cid: {"value": ..., "wall_s": [...]} | {"error": message}}` as JSON; bytes are written as base64."""
    with open(path, "w") as fh:
        json.dump(_jsonable(results), fh, indent=1)


def load_uc_results(path: str) -> dict:
    with open(path) as fh:
        return _from_json(json.load(fh))


def check_uc_results(cases, results: dict, tmpdir: str) -> List[str]:
    """results: {cid: {"value": <decoded>} | {"error": "<message>"}} written by gbx-uc-sql --out.

    A UC case with no result is a failure too: a statement that never ran must not pass.
    """
    failures = []
    by_id = {c.cid: c for c in cases}
    for cid, r in results.items():
        case = by_id[cid]
        args = [resolve_arg(a, tmpdir) for a in case.args]
        msg = (
            check_error(case, RuntimeError(r["error"]))
            if "error" in r
            else check(case, r["value"], args)
        )
        if msg:
            failures.append(msg)
    failures += [
        f"{c.cid}: no result"
        for c in cases
        if "uc" in c.surfaces and c.cid not in results
    ]
    return failures
