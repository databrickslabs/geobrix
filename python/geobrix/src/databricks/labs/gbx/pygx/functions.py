"""pygx light GridX API — quadbin SQL functions (Serverless-safe).

Registers the gbx_quadbin_* SQL functions (point->cell, resolution, k-ring,
distance, polyfill, geometry/centroid EWKB, cell-union, tessellate) plus the
gbx_quadbin_cellunion_agg grouped aggregator, and exposes Column wrappers so
light <-> heavy is a one-line import swap.

Signatures mirror databricks.labs.gbx.gridx.grid.functions. Register once with
gx.register(spark), then use on columns. Serverless-safe: spark.udf.register
plus Column expressions only — no _jvm / spark.conf / RDD access.
"""

from typing import List, Optional, Union

import numpy as np
import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf, udf, udtf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

from databricks.labs.gbx import _register

from . import _bng, _custom, _env, _quadbin
from ._geom import parse_geom
from ._serde import BNG_CHIP_SCHEMA, CUSTOM_GRID_SCHEMA, QUADBIN_CELL_SCHEMA

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


# --- impl rule: scalar-output -> pandas_udf ; array-output -> plain @udf --------------------
# Scalar/bounded-output functions (pointascell/resolution/distance + the single-
# geometry aswkb/centroid/cellunion) use pandas_udf: pointascell/resolution are
# numpy-vectorized (a real win), the rest amortize the Arrow batch transfer, and a
# batch of scalars is bounded in memory. ARRAY-returning functions (kring/polyfill/
# tessellate) use a PLAIN @udf instead: a scalar pandas_udf buffers a whole Arrow
# batch (~10k rows) of output at once, so variable-length array outputs (polyfill
# can emit up to ~1M cells/row; tessellate many chips/row) risk worker OOM at scale
# — a plain @udf streams row-by-row (peak memory ~ one row's output). All wrap the
# same scalar _quadbin oracle, so cross-tier parity holds either way.


@pandas_udf(LongType())
def _pointascell_udf(lon: pd.Series, lat: pd.Series, res: pd.Series) -> pd.Series:
    lons = lon.to_numpy(dtype=np.float64)
    lats = lat.to_numpy(dtype=np.float64)
    ress = res.to_numpy()
    out = np.empty(len(lons), dtype=np.int64)
    # point_as_cell_vec needs a fixed z per call; group rows by their resolution
    # (typically a single literal => one pass). Bit-identical to the scalar path.
    for z in np.unique(ress):
        mask = ress == z
        out[mask] = _quadbin.point_as_cell_vec(lons[mask], lats[mask], int(z))
    return pd.Series(out)


@pandas_udf(IntegerType())
def _resolution_udf(cell: pd.Series) -> pd.Series:
    cells = cell.to_numpy(dtype=np.int64)
    return pd.Series(_quadbin.resolution_vec(cells).astype(np.int32))


@pandas_udf(IntegerType())
def _distance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    # Same-resolution-or-error; Chebyshev on cell_to_tile coords. Looped per
    # element (cell_to_tile is scalar in the lib); batched Arrow transfer is the win.
    return pd.Series(
        [int(_quadbin.distance(int(x), int(y))) for x, y in zip(a, b)],
        dtype="int32",
    )


# --- single-geometry UDFs (pandas_udf, bounded scalar output) --------------------------------
# aswkb/centroid/cellunion return ONE geometry per row (bounded output), so a
# pandas_udf batch is memory-safe; they loop over the batch calling the scalar
# _quadbin oracle (the win is the batched Arrow transfer). NULL preserved per row.


@pandas_udf(BinaryType())
def _aswkb_udf(cell: pd.Series) -> pd.Series:
    return pd.Series([_quadbin.as_wkb(int(c)) if c is not None else None for c in cell])


@pandas_udf(BinaryType())
def _centroid_udf(cell: pd.Series) -> pd.Series:
    return pd.Series(
        [_quadbin.centroid(int(c)) if c is not None else None for c in cell]
    )


@pandas_udf(BinaryType())
def _cellunion_udf(cells: pd.Series) -> pd.Series:
    # cells is a Series of arrays (ARRAY<LONG>); per-row union. None/empty -> None.
    return pd.Series(
        [
            (_quadbin.cell_union(list(cs)) if cs is not None and len(cs) else None)
            for cs in cells
        ]
    )


# --- array-returning UDFs (PLAIN @udf, row-by-row for scale safety) --------------------------
# kring/polyfill/tessellate emit variable-length arrays per row; a scalar pandas_udf
# would buffer a whole Arrow batch of these at once (OOM risk at scale), so they are
# plain row-at-a-time UDFs. NULL geom -> NULL (heavy propagateNull).


def _kring(cell, k):
    if cell is None or k is None:
        return None
    return _quadbin.k_ring(int(cell), int(k))


def _polyfill(geom, res):
    if geom is None:
        return None
    return _quadbin.polyfill(geom, int(res))


def _tessellate(geom, res):
    if geom is None:
        return None
    return [
        {"cell": int(c), "geom": gm} for (c, gm) in _quadbin.tessellate(geom, int(res))
    ]


# --- grouped-aggregate pandas UDF -----------------------------------------------------------
# (pd.Series) -> bytes is detected as GROUPED_AGG (Series-to-Scalar) by PySpark 3+.
# Returns the unioned cell-coverage geometry as BINARY (atomic) directly.


@pandas_udf(BinaryType())
def _cellunion_agg_udf(cell: pd.Series) -> Optional[bytes]:
    return _quadbin.cell_union([int(c) for c in cell if c is not None])


# ============================================================================
# BNG (British National Grid) — pure-Python port of gridx/grid/BNG.scala.
# Cell ids are STRING in the public surface; geometry outputs are plain WKB
# (EPSG:27700 coordinates, NO SRID — heavy uses JTS.toWKB, not toEWKB).
#
# Same impl-by-shape rule as quadbin: scalar/bounded -> pandas_udf (batched
# Arrow transfer is the win); ARRAY-returning -> plain @udf (row-by-row,
# OOM-safe at scale); explode -> @udtf (SQL-LATERAL only); grouped-agg ->
# grouped-aggregate pandas_udf (returns the dissolved chip as BINARY — PySpark
# grouped-agg cannot return a StructType; see the agg note below).
# ============================================================================


# --- scalar / bounded-output -> pandas_udf ----------------------------------


@pandas_udf(StringType())
def _bng_pointascell_udf(geom: pd.Series, res: pd.Series) -> pd.Series:
    # pointascell takes a GEOMETRY (WKB/EWKB/WKT/EWKT) and uses its centroid as
    # the EPSG:27700 easting/northing (heavy BNG_PointAsCell semantics).
    out = []
    for g, r in zip(geom, res):
        if g is None or r is None:
            out.append(None)
            continue
        pg = parse_geom(g)
        if pg is None or pg.is_empty:
            out.append(None)
            continue
        c = pg.centroid
        out.append(_bng.point_as_cell(c.x, c.y, _norm_res(r)))
    return pd.Series(out)


@pandas_udf(StringType())
def _bng_eastnorthasbng_udf(e: pd.Series, n: pd.Series, res: pd.Series) -> pd.Series:
    # eastnorthasbng takes SCALAR EPSG:27700 eastings/northings (not a geometry).
    out = []
    for ee, nn, r in zip(e, n, res):
        if ee is None or nn is None or r is None:
            out.append(None)
            continue
        out.append(_bng.point_as_cell(float(ee), float(nn), _norm_res(r)))
    return pd.Series(out)


@pandas_udf(DoubleType())
def _bng_cellarea_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.area(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(LongType())
def _bng_distance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                int(_bng.distance(_bng.parse(x), _bng.parse(y)))
                if x is not None and y is not None
                else None
            )
            for x, y in zip(a, b)
        ]
    )


@pandas_udf(LongType())
def _bng_euclideandistance_udf(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                int(_bng.euclidean_distance(_bng.parse(x), _bng.parse(y)))
                if x is not None and y is not None
                else None
            )
            for x, y in zip(a, b)
        ]
    )


@pandas_udf(BinaryType())
def _bng_aswkb_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_aswkb(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(StringType())
def _bng_aswkt_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_aswkt(_bng.parse(c)) if c is not None else None for c in cellid]
    )


@pandas_udf(BinaryType())
def _bng_centroid_udf(cellid: pd.Series) -> pd.Series:
    return pd.Series(
        [_bng.cell_centroid(_bng.parse(c)) if c is not None else None for c in cellid]
    )


# --- chip-pair scalar ops -> pandas_udf returning BNG_CHIP_SCHEMA ------------
# cellintersection/cellunion take two chip structs {cellid, core, chip}; the
# _bng ops operate on (cellid_str, core, shapely|None) tuples and apply the
# left-hand rule (different cellid -> empty; either core -> that chip).


def _chip_tuple(struct_row):
    """Map a chip struct row (positional (id, core, chip)) to (cellid, core, shapely|None).

    Fields are read POSITIONALLY — the documented chip-struct contract — so
    callers may name the struct fields anything (e.g. SQL ``struct(c, isCore,
    geom)``), matching the heavy reader's by-position/type field detection.
    """
    cellid, core, raw = struct_row[0], struct_row[1], struct_row[2]
    chip = parse_geom(raw) if raw is not None else None
    return (cellid, bool(core), chip)


def _to_positional(rec):
    """Coerce one struct element to a positional (id, core, chip) tuple.

    A struct element may arrive as a dict (grouped-agg Series), a Spark Row, or
    a plain sequence (scalar DataFrame rows). Dicts/Rows preserve struct-field
    declaration order in their values.
    """
    if isinstance(rec, dict):
        return tuple(rec.values())
    if hasattr(rec, "__fields__"):  # pyspark Row
        return tuple(rec)
    return tuple(rec)


def _chip_records(struct_col):
    """Iterate per-row positional (id, core, chip) tuples from a struct column.

    A StructType column arrives as a pandas DataFrame (scalar pandas_udf: one
    column per struct field) or as a Series of dict/Row elements (grouped-agg).
    """
    if isinstance(struct_col, pd.DataFrame):
        return [tuple(rec) for rec in struct_col.itertuples(index=False, name=None)]
    return [_to_positional(rec) for rec in struct_col]


def _chip_to_row(result):
    cellid, core, chip = result
    chip_wkb = None
    if chip is not None and not chip.is_empty:
        from shapely import to_wkb as _to_wkb

        chip_wkb = _to_wkb(chip)
    return {"cellid": cellid, "core": bool(core), "chip": chip_wkb}


@pandas_udf(BNG_CHIP_SCHEMA)
def _bng_cellunion_udf(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    rows = [
        _chip_to_row(_bng.cell_union(_chip_tuple(lt), _chip_tuple(rt)))
        for lt, rt in zip(_chip_records(left), _chip_records(right))
    ]
    return pd.DataFrame(rows, columns=["cellid", "core", "chip"])


@pandas_udf(BNG_CHIP_SCHEMA)
def _bng_cellintersection_udf(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    rows = [
        _chip_to_row(_bng.cell_intersection(_chip_tuple(lt), _chip_tuple(rt)))
        for lt, rt in zip(_chip_records(left), _chip_records(right))
    ]
    return pd.DataFrame(rows, columns=["cellid", "core", "chip"])


# --- array-output -> plain @udf (row-by-row, scale-safe) --------------------


def _bng_kring(cellid, k):
    if cellid is None or k is None:
        return None
    return _bng.k_ring_str(cellid, int(k))


def _bng_kloop(cellid, k):
    if cellid is None or k is None:
        return None
    return _bng.k_loop_str(cellid, int(k))


def _bng_polyfill(geom, res):
    if geom is None or res is None:
        return None
    return _bng.polyfill_str(geom, _norm_res(res))


def _bng_geomkring(geom, res, k):
    if geom is None or res is None or k is None:
        return None
    return sorted(_bng.geometry_k_ring_str(geom, _norm_res(res), int(k)))


def _bng_geomkloop(geom, res, k):
    if geom is None or res is None or k is None:
        return None
    return sorted(_bng.geometry_k_loop_str(geom, _norm_res(res), int(k)))


def _bng_tessellate(geom, res):
    if geom is None or res is None:
        return None
    return [
        {"cellid": c, "core": bool(core), "chip": chip}
        for (c, core, chip) in _bng.tessellate_str(geom, _norm_res(res))
    ]


# --- explode UDTFs (SQL-LATERAL only) ---------------------------------------


@udtf(returnType="cellid: string")
class _BngKRingExplode:
    def eval(self, cellid, k):
        if cellid is None or k is None:
            return
        for c in _bng.k_ring_str(cellid, int(k)):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngKLoopExplode:
    def eval(self, cellid, k):
        if cellid is None or k is None:
            return
        for c in _bng.k_loop_str(cellid, int(k)):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngGeomKRingExplode:
    def eval(self, geom, res, k):
        if geom is None or res is None or k is None:
            return
        for c in sorted(_bng.geometry_k_ring_str(geom, _norm_res(res), int(k))):
            yield (c,)


@udtf(returnType="cellid: string")
class _BngGeomKLoopExplode:
    def eval(self, geom, res, k):
        if geom is None or res is None or k is None:
            return
        for c in sorted(_bng.geometry_k_loop_str(geom, _norm_res(res), int(k))):
            yield (c,)


@udtf(returnType="cellid: string, core: boolean, chip: binary")
class _BngTessellateExplode:
    def eval(self, geom, res):
        if geom is None or res is None:
            return
        for c, core, chip in _bng.tessellate_str(geom, _norm_res(res)):
            yield (c, bool(core), chip)


# --- grouped-agg pandas_udf returning BNG_CHIP_SCHEMA ------------------------
# Fold a group's chips (same cellid) into one dissolved chip via the chip op.


# NOTE: PySpark grouped-aggregate pandas UDFs cannot return a StructType
# (NOT_IMPLEMENTED for struct return). Heavy BNG_CellUnionAgg returns a chip
# STRUCT<cellid, core, chip>, but the light grouped-agg can only emit an atomic
# type — so it returns the dissolved chip geometry as plain WKB BINARY (the
# load-bearing field), exactly like the quadbin cellunion_agg. The group key is
# the cellid (callers GROUP BY cellid), and core is recoverable from whether the
# chip equals the full cell, so no information is lost for the swap. Cross-tier
# parity (Task 9) compares the decoded chip geometry.


def _fold_chip_geom(chip, op):
    acc = None
    for row in _chip_records(chip):
        if row is None:
            continue
        cur = _chip_tuple(row)
        acc = cur if acc is None else op(acc, cur)
    if acc is None:
        return None
    cellid, core, geom = acc
    # A core chip carries None geometry in the array form; materialize the full
    # cell polygon so the dissolved output is always a real geometry.
    if (geom is None or geom.is_empty) and core and cellid is not None:
        geom = _bng.cell_id_to_geometry(_bng.parse(cellid))
    if geom is None or geom.is_empty:
        return None
    from shapely import to_wkb as _to_wkb

    return _to_wkb(geom)


@pandas_udf(BinaryType())
def _bng_cellunion_agg_udf(chip: pd.DataFrame) -> Optional[bytes]:
    return _fold_chip_geom(chip, _bng.cell_union)


@pandas_udf(BinaryType())
def _bng_cellintersection_agg_udf(chip: pd.DataFrame) -> Optional[bytes]:
    return _fold_chip_geom(chip, _bng.cell_intersection)


def _norm_res(res):
    """Normalize a SQL resolution arg (Int index or resolutionMap string).

    _bng.get_resolution validates Int ±1..±6 / resolutionMap keys and rejects
    metres-as-Int; the _bng wrapper fns call it again, so pass the raw value
    through with only numpy-scalar unwrapping.
    """
    if isinstance(res, (np.integer,)):
        return int(res)
    return res


# ============================================================================
# Custom gridding (gbx_custom_*) — pure-Python port of CustomGridSystem.scala +
# GridConf.scala. Cell ids are BIGINT; geometry outputs are plain WKB (NO SRID —
# heavy uses JTS.toWKB, the grid srid is metadata only).
#
# Same impl-by-shape rule as quadbin/BNG: scalar/bounded geometry ops ->
# pandas_udf (batched Arrow transfer is the win); ARRAY-returning ops (polyfill/
# kring) -> plain @udf (row-by-row, OOM-safe at scale). The grid SPEC builder is
# a validating @udf (Resolved decision 1) returning CUSTOM_GRID_SCHEMA so build-
# time bad-bounds errors match heavy Custom_Grid.eval's require(...).
# ============================================================================


# --- custom-grid spec builder -> validating @udf returning CUSTOM_GRID_SCHEMA ---
# Resolved decision 1 (spec 2026-06-14): eager validation matches heavy
# Custom_Grid.eval's require(...) (error-at-build-time parity). The 7-arg form
# defaults srid to -1 (no CRS), as in Custom_Grid.builder; the Column wrapper
# always supplies all 8.
@udf(returnType=CUSTOM_GRID_SCHEMA)
def _custom_grid_udf(x_min, x_max, y_min, y_max, splits, root_x, root_y, srid=-1):
    x_min, x_max = int(x_min), int(x_max)
    y_min, y_max = int(y_min), int(y_max)
    splits, root_x, root_y = int(splits), int(root_x), int(root_y)
    srid = -1 if srid is None else int(srid)
    if not x_max > x_min:
        raise ValueError(
            f"gbx_custom_grid: bound_x_max ({x_max}) must be greater than "
            f"bound_x_min ({x_min})"
        )
    if not y_max > y_min:
        raise ValueError(
            f"gbx_custom_grid: bound_y_max ({y_max}) must be greater than "
            f"bound_y_min ({y_min})"
        )
    if splits < 2:
        raise ValueError(f"gbx_custom_grid: cell_splits must be >= 2; got {splits}")
    if root_x <= 0:
        raise ValueError(f"gbx_custom_grid: root_cell_size_x must be > 0; got {root_x}")
    if root_y <= 0:
        raise ValueError(f"gbx_custom_grid: root_cell_size_y must be > 0; got {root_y}")
    return (x_min, x_max, y_min, y_max, splits, root_x, root_y, srid)


def _custom_first_coord(pg):
    """First coordinate (x, y) of a geometry — heavy uses geom.getCoordinate.

    Not the centroid (unlike BNG); for a POINT it's its xy, for any other geom
    it's the first vertex. shapely.get_coordinates returns vertices in ring/seq
    order, so [0] is the geometry's first coordinate.
    """
    from shapely import get_coordinates as _get_coordinates

    coords = _get_coordinates(pg)
    first = coords[0]
    return float(first[0]), float(first[1])


# --- consuming scalar UDFs -> pandas_udf (bounded scalar) -------------------


@pandas_udf(LongType())
def _custom_pointascell_udf(
    point: pd.Series, grid: pd.Series, res: pd.Series
) -> pd.Series:
    out = []
    for g, spec, r in zip(point, _custom_grid_records(grid), res):
        if g is None or spec is None or r is None:
            out.append(None)
            continue
        pg = parse_geom(g)
        if pg is None or pg.is_empty:
            out.append(None)
            continue
        conf = _custom.conf_from_row(spec)
        x, y = _custom_first_coord(pg)
        out.append(_custom.point_to_cell_id(conf, x, y, int(r)))
    return pd.Series(out)


@pandas_udf(BinaryType())
def _custom_cellaswkb_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                _custom.cell_aswkb(_custom.conf_from_row(s), int(c))
                if c is not None and s is not None
                else None
            )
            for c, s in zip(cell, _custom_grid_records(grid))
        ]
    )


@pandas_udf(StringType())
def _custom_cellaswkt_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                _custom.cell_aswkt(_custom.conf_from_row(s), int(c))
                if c is not None and s is not None
                else None
            )
            for c, s in zip(cell, _custom_grid_records(grid))
        ]
    )


@pandas_udf(BinaryType())
def _custom_centroid_udf(cell: pd.Series, grid: pd.Series) -> pd.Series:
    return pd.Series(
        [
            (
                _custom.cell_centroid(_custom.conf_from_row(s), int(c))
                if c is not None and s is not None
                else None
            )
            for c, s in zip(cell, _custom_grid_records(grid))
        ]
    )


def _custom_grid_records(grid):
    """Iterate per-row grid-spec records from a struct column.

    A StructType column arrives at a scalar pandas_udf as a pandas DataFrame
    (one column per struct field); _custom.conf_from_row accepts a Row/dict, so
    map each DataFrame row to a dict keyed by the struct field names. A null
    struct surfaces as an all-null row -> None.
    """
    if isinstance(grid, pd.DataFrame):
        recs = []
        for rec in grid.to_dict(orient="records"):
            recs.append(None if all(v is None for v in rec.values()) else rec)
        return recs
    return list(grid)


# --- array-output -> plain @udf (row-by-row, scale-safe) --------------------


def _custom_polyfill(geom, grid, res):
    if geom is None or grid is None or res is None:
        return None
    return _custom.polyfill(_custom.conf_from_row(grid), parse_geom(geom), int(res))


def _custom_kring(cell, grid, k):
    if cell is None or grid is None or k is None:
        return None
    return _custom.k_ring(_custom.conf_from_row(grid), int(cell), int(k))


def _registrar_groups() -> List[_register.Group]:
    quadbin = {
        "gbx_quadbin_pointascell": lambda s: s.udf.register(
            "gbx_quadbin_pointascell", _pointascell_udf
        ),
        "gbx_quadbin_resolution": lambda s: s.udf.register(
            "gbx_quadbin_resolution", _resolution_udf
        ),
        "gbx_quadbin_distance": lambda s: s.udf.register(
            "gbx_quadbin_distance", _distance_udf
        ),
        "gbx_quadbin_aswkb": lambda s: s.udf.register("gbx_quadbin_aswkb", _aswkb_udf),
        "gbx_quadbin_centroid": lambda s: s.udf.register(
            "gbx_quadbin_centroid", _centroid_udf
        ),
        "gbx_quadbin_cellunion": lambda s: s.udf.register(
            "gbx_quadbin_cellunion", _cellunion_udf
        ),
        "gbx_quadbin_kring": lambda s: s.udf.register(
            "gbx_quadbin_kring", _kring, ArrayType(LongType())
        ),
        "gbx_quadbin_polyfill": lambda s: s.udf.register(
            "gbx_quadbin_polyfill", _polyfill, ArrayType(LongType())
        ),
        "gbx_quadbin_tessellate": lambda s: s.udf.register(
            "gbx_quadbin_tessellate", _tessellate, ArrayType(QUADBIN_CELL_SCHEMA)
        ),
        "gbx_quadbin_cellunion_agg": lambda s: s.udf.register(
            "gbx_quadbin_cellunion_agg", _cellunion_agg_udf
        ),
    }
    bng = {
        "gbx_bng_pointascell": lambda s: s.udf.register(
            "gbx_bng_pointascell", _bng_pointascell_udf
        ),
        "gbx_bng_eastnorthasbng": lambda s: s.udf.register(
            "gbx_bng_eastnorthasbng", _bng_eastnorthasbng_udf
        ),
        "gbx_bng_cellarea": lambda s: s.udf.register(
            "gbx_bng_cellarea", _bng_cellarea_udf
        ),
        "gbx_bng_distance": lambda s: s.udf.register(
            "gbx_bng_distance", _bng_distance_udf
        ),
        "gbx_bng_euclideandistance": lambda s: s.udf.register(
            "gbx_bng_euclideandistance", _bng_euclideandistance_udf
        ),
        "gbx_bng_aswkb": lambda s: s.udf.register("gbx_bng_aswkb", _bng_aswkb_udf),
        "gbx_bng_aswkt": lambda s: s.udf.register("gbx_bng_aswkt", _bng_aswkt_udf),
        "gbx_bng_centroid": lambda s: s.udf.register(
            "gbx_bng_centroid", _bng_centroid_udf
        ),
        "gbx_bng_cellintersection": lambda s: s.udf.register(
            "gbx_bng_cellintersection", _bng_cellintersection_udf
        ),
        "gbx_bng_cellunion": lambda s: s.udf.register(
            "gbx_bng_cellunion", _bng_cellunion_udf
        ),
        "gbx_bng_kring": lambda s: s.udf.register(
            "gbx_bng_kring", _bng_kring, ArrayType(StringType())
        ),
        "gbx_bng_kloop": lambda s: s.udf.register(
            "gbx_bng_kloop", _bng_kloop, ArrayType(StringType())
        ),
        "gbx_bng_polyfill": lambda s: s.udf.register(
            "gbx_bng_polyfill", _bng_polyfill, ArrayType(StringType())
        ),
        "gbx_bng_geomkring": lambda s: s.udf.register(
            "gbx_bng_geomkring", _bng_geomkring, ArrayType(StringType())
        ),
        "gbx_bng_geomkloop": lambda s: s.udf.register(
            "gbx_bng_geomkloop", _bng_geomkloop, ArrayType(StringType())
        ),
        "gbx_bng_tessellate": lambda s: s.udf.register(
            "gbx_bng_tessellate", _bng_tessellate, ArrayType(BNG_CHIP_SCHEMA)
        ),
        "gbx_bng_kringexplode": lambda s: s.udtf.register(
            "gbx_bng_kringexplode", _BngKRingExplode
        ),
        "gbx_bng_kloopexplode": lambda s: s.udtf.register(
            "gbx_bng_kloopexplode", _BngKLoopExplode
        ),
        "gbx_bng_geomkringexplode": lambda s: s.udtf.register(
            "gbx_bng_geomkringexplode", _BngGeomKRingExplode
        ),
        "gbx_bng_geomkloopexplode": lambda s: s.udtf.register(
            "gbx_bng_geomkloopexplode", _BngGeomKLoopExplode
        ),
        "gbx_bng_tessellateexplode": lambda s: s.udtf.register(
            "gbx_bng_tessellateexplode", _BngTessellateExplode
        ),
        "gbx_bng_cellunion_agg": lambda s: s.udf.register(
            "gbx_bng_cellunion_agg", _bng_cellunion_agg_udf
        ),
        "gbx_bng_cellintersection_agg": lambda s: s.udf.register(
            "gbx_bng_cellintersection_agg", _bng_cellintersection_agg_udf
        ),
    }
    custom = {
        "gbx_custom_grid": lambda s: s.udf.register(
            "gbx_custom_grid", _custom_grid_udf
        ),
        "gbx_custom_pointascell": lambda s: s.udf.register(
            "gbx_custom_pointascell", _custom_pointascell_udf
        ),
        "gbx_custom_cellaswkb": lambda s: s.udf.register(
            "gbx_custom_cellaswkb", _custom_cellaswkb_udf
        ),
        "gbx_custom_cellaswkt": lambda s: s.udf.register(
            "gbx_custom_cellaswkt", _custom_cellaswkt_udf
        ),
        "gbx_custom_centroid": lambda s: s.udf.register(
            "gbx_custom_centroid", _custom_centroid_udf
        ),
        "gbx_custom_polyfill": lambda s: s.udf.register(
            "gbx_custom_polyfill", _custom_polyfill, ArrayType(LongType())
        ),
        "gbx_custom_kring": lambda s: s.udf.register(
            "gbx_custom_kring", _custom_kring, ArrayType(LongType())
        ),
    }
    return [
        (lambda: _env.assert_quadbin_available(), quadbin),
        (lambda: _env.assert_bng_available(), bng),
        (lambda: _env.assert_custom_available(), custom),
    ]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Register the pygx grid SQL functions (Serverless-safe: udf/udtf only).

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_bng_polyfill``) or short names
            (``bng_polyfill``), case-insensitively. ``None`` registers everything;
            ``[]`` registers nothing. An unrecognized name raises ``ValueError``.
            A sub-module's availability guard runs only when >=1 of its functions
            is selected.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)


# --- Column wrappers (mirror heavy gridx.grid.functions) ------------------------------------


def quadbin_pointascell(
    longitude: ColLike, latitude: ColLike, resolution: ColLike
) -> Column:
    """Quadbin cell (LONG) for the WGS84 longitude/latitude point at `resolution`."""
    return f.call_function(
        "gbx_quadbin_pointascell", _col(longitude), _col(latitude), _col(resolution)
    )


def quadbin_resolution(cell: ColLike) -> Column:
    """Resolution (INT) of a quadbin cell."""
    return f.call_function("gbx_quadbin_resolution", _col(cell))


def quadbin_kring(cell: ColLike, k: ColLike) -> Column:
    """ARRAY<LONG> of cells within ring distance `k` of `cell` (includes center)."""
    return f.call_function("gbx_quadbin_kring", _col(cell), _col(k))


def quadbin_distance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Chebyshev grid distance (INT) between two same-resolution cells."""
    return f.call_function("gbx_quadbin_distance", _col(cellid1), _col(cellid2))


def quadbin_polyfill(geom: ColLike, resolution: ColLike) -> Column:
    """ARRAY<LONG> of cells covering the geometry's envelope at `resolution`."""
    return f.call_function("gbx_quadbin_polyfill", _col(geom), _col(resolution))


def quadbin_aswkb(cell: ColLike) -> Column:
    """Cell boundary polygon as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_aswkb", _col(cell))


def quadbin_centroid(cell: ColLike) -> Column:
    """Cell centroid point as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_centroid", _col(cell))


def quadbin_cellunion(cells: ColLike) -> Column:
    """Union of an ARRAY<LONG> of cell boundaries as EWKB (SRID 4326) BINARY."""
    return f.call_function("gbx_quadbin_cellunion", _col(cells))


def quadbin_tessellate(geom: ColLike, resolution: ColLike) -> Column:
    """ARRAY<STRUCT<cell:LONG, geom:BINARY>> chips clipping the geometry per cell."""
    return f.call_function("gbx_quadbin_tessellate", _col(geom), _col(resolution))


def quadbin_cellunion_agg(cellid: ColLike) -> Column:
    """Aggregator: union a group's cell boundaries into one EWKB (SRID 4326) BINARY."""
    return _cellunion_agg_udf(_col(cellid))


# --- BNG Column wrappers (mirror heavy gridx.bng.functions) ----------------------------------
# Cell ids are STRING; geometry outputs are plain WKB (EPSG:27700, no SRID).


def bng_pointascell(geom: ColLike, resolution: ColLike) -> Column:
    """BNG cell id (STRING) for the centroid of a geometry (EPSG:27700)."""
    return f.call_function("gbx_bng_pointascell", _col(geom), _col(resolution))


def bng_eastnorthasbng(e: ColLike, n: ColLike, resolution: ColLike) -> Column:
    """BNG cell id (STRING) for scalar EPSG:27700 eastings/northings at `resolution`."""
    return f.call_function("gbx_bng_eastnorthasbng", _col(e), _col(n), _col(resolution))


def bng_cellarea(cellid: ColLike) -> Column:
    """Cell area in square KILOMETRES (DOUBLE)."""
    return f.call_function("gbx_bng_cellarea", _col(cellid))


def bng_distance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Manhattan grid distance (LONG) between two BNG cells (edge-size units)."""
    return f.call_function("gbx_bng_distance", _col(cellid1), _col(cellid2))


def bng_euclideandistance(cellid1: ColLike, cellid2: ColLike) -> Column:
    """Chebyshev grid distance (LONG) between two BNG cells (edge-size units)."""
    return f.call_function("gbx_bng_euclideandistance", _col(cellid1), _col(cellid2))


def bng_aswkb(cellid: ColLike) -> Column:
    """Cell boundary polygon as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_bng_aswkb", _col(cellid))


def bng_aswkt(cellid: ColLike) -> Column:
    """Cell boundary polygon as WKT (STRING)."""
    return f.call_function("gbx_bng_aswkt", _col(cellid))


def bng_centroid(cellid: ColLike) -> Column:
    """Cell centroid point as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_bng_centroid", _col(cellid))


def bng_cellintersection(left_chip: ColLike, right_chip: ColLike) -> Column:
    """Per-cell chip intersection STRUCT<cellid, core, chip> (left-hand rule)."""
    return f.call_function(
        "gbx_bng_cellintersection", _col(left_chip), _col(right_chip)
    )


def bng_cellunion(left_chip: ColLike, right_chip: ColLike) -> Column:
    """Per-cell chip union STRUCT<cellid, core, chip> (left-hand rule)."""
    return f.call_function("gbx_bng_cellunion", _col(left_chip), _col(right_chip))


def bng_kring(cellid: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> of cells within ring distance `k` (includes center)."""
    return f.call_function("gbx_bng_kring", _col(cellid), _col(k))


def bng_kloop(cellid: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> of the hollow ring of cells at exact distance `k`."""
    return f.call_function("gbx_bng_kloop", _col(cellid), _col(k))


def bng_polyfill(geom: ColLike, resolution: ColLike) -> Column:
    """ARRAY<STRING> of cells whose centroid is contained by the geometry."""
    return f.call_function("gbx_bng_polyfill", _col(geom), _col(resolution))


def bng_geomkring(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> k-ring around a geometry's covering chips."""
    return f.call_function("gbx_bng_geomkring", _col(geom), _col(resolution), _col(k))


def bng_geomkloop(geom: ColLike, resolution: ColLike, k: ColLike) -> Column:
    """ARRAY<STRING> k-loop around a geometry's covering chips."""
    return f.call_function("gbx_bng_geomkloop", _col(geom), _col(resolution), _col(k))


def bng_tessellate(geom: ColLike, resolution: ColLike) -> Column:
    """ARRAY<STRUCT<cellid:STRING, core:BOOL, chip:BINARY>> chips per cell."""
    return f.call_function("gbx_bng_tessellate", _col(geom), _col(resolution))


def bng_cellunion_agg(input_chip: ColLike) -> Column:
    """Aggregator: union a group's same-cell chips into one dissolved-chip WKB BINARY.

    (Light grouped-agg cannot return a STRUCT; emits the dissolved chip geometry,
    keyed by the group's cellid — see the registration note.)
    """
    return f.call_function("gbx_bng_cellunion_agg", _col(input_chip))


def bng_cellintersection_agg(input_chip: ColLike) -> Column:
    """Aggregator: intersect a group's same-cell chips into one dissolved-chip WKB BINARY."""
    return f.call_function("gbx_bng_cellintersection_agg", _col(input_chip))


# The five *explode functions are SQL-LATERAL-only table functions in the light
# tier — they have no Python DataFrame Column form (unlike the heavy tier). They
# exist here as importable names for binding parity, but raise NotImplementedError
# pointing to the registered UDTF; invoke them via SQL LATERAL instead.

_EXPLODE_HINT = (
    "Light BNG {name} has no Python Column form; invoke the registered UDTF via "
    "SQL LATERAL, e.g. SELECT t.* FROM <df>, LATERAL {udtf}(...) t"
)


def bng_kringexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT cellid FROM gbx_bng_kringexplode(cellid, k)."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(name="bng_kringexplode", udtf="gbx_bng_kringexplode")
    )


def bng_kloopexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT cellid FROM gbx_bng_kloopexplode(cellid, k)."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(name="bng_kloopexplode", udtf="gbx_bng_kloopexplode")
    )


def bng_geomkringexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_geomkringexplode(geom, res, k) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_geomkringexplode", udtf="gbx_bng_geomkringexplode"
        )
    )


def bng_geomkloopexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_geomkloopexplode(geom, res, k) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_geomkloopexplode", udtf="gbx_bng_geomkloopexplode"
        )
    )


def bng_tessellateexplode(*args, **kwargs) -> Column:
    """SQL-LATERAL-only: SELECT t.* FROM <df>, LATERAL gbx_bng_tessellateexplode(geom, res) t."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(
            name="bng_tessellateexplode", udtf="gbx_bng_tessellateexplode"
        )
    )


# --- Custom-grid Column wrappers (mirror heavy gridx.custom.functions) ------------------------
# Cell ids are BIGINT; geometry outputs are plain WKB (NO SRID). custom_grid ALWAYS
# supplies all 8 args (defaulting srid=-1) so the validating @udf sees a fixed arity.


def custom_grid(
    x_min: ColLike,
    x_max: ColLike,
    y_min: ColLike,
    y_max: ColLike,
    cell_splits: ColLike,
    root_x: ColLike,
    root_y: ColLike,
    srid: ColLike = -1,
) -> Column:
    """Build a custom-grid spec STRUCT (validated eagerly). srid=-1 means no CRS."""
    return f.call_function(
        "gbx_custom_grid",
        _col(x_min),
        _col(x_max),
        _col(y_min),
        _col(y_max),
        _col(cell_splits),
        _col(root_x),
        _col(root_y),
        _col(srid),
    )


def custom_pointascell(point: ColLike, grid: ColLike, resolution: ColLike) -> Column:
    """Custom-grid cell ID (BIGINT) for the first coordinate of a geometry at `resolution`."""
    return f.call_function(
        "gbx_custom_pointascell", _col(point), _col(grid), _col(resolution)
    )


def custom_cellaswkb(cell: ColLike, grid: ColLike) -> Column:
    """Cell footprint polygon as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_custom_cellaswkb", _col(cell), _col(grid))


def custom_cellaswkt(cell: ColLike, grid: ColLike) -> Column:
    """Cell footprint polygon as WKT (STRING)."""
    return f.call_function("gbx_custom_cellaswkt", _col(cell), _col(grid))


def custom_centroid(cell: ColLike, grid: ColLike) -> Column:
    """Cell centroid point as plain WKB (no SRID) BINARY."""
    return f.call_function("gbx_custom_centroid", _col(cell), _col(grid))


def custom_polyfill(geom: ColLike, grid: ColLike, resolution: ColLike) -> Column:
    """ARRAY<BIGINT> of cells whose center is contained by the geometry."""
    return f.call_function(
        "gbx_custom_polyfill", _col(geom), _col(grid), _col(resolution)
    )


def custom_kring(cell: ColLike, grid: ColLike, k: ColLike) -> Column:
    """ARRAY<BIGINT> of cells within Chebyshev ring distance `k` (includes center)."""
    return f.call_function("gbx_custom_kring", _col(cell), _col(grid), _col(k))
