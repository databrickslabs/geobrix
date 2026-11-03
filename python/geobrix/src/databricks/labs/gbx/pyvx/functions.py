"""pyvx light VectorX API — MVT, legacy-geometry, TIN, and CRS functions (Serverless-safe).

Covers the MVT aggregator/pyramid (gbx_st_asmvt, gbx_st_asmvt_pyramid), legacy
Mosaic-geometry decoding (gbx_st_legacyaswkb), constrained-Delaunay TIN
generators (gbx_st_triangulate, gbx_st_interpolateelevation{bbox,geom}), and
the CRS family (gbx_st_crs, gbx_st_setcrs, gbx_st_transformcrs).

Signatures mirror databricks.labs.gbx.vectorx.functions so light <-> heavy is a
one-line import swap. Register once with vx.register(spark), then use on columns.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import Union

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf, udtf
from pyspark.sql.types import BinaryType, BooleanType

from . import _legacy, _mvt, _tin

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


# --- st_asmvt: grouped-aggregate pandas UDF -------------------------------------------------
# Type hints (pd.Series, pd.Series) -> bytes are detected as GROUPED_AGG (Series-to-Scalar)
# by PySpark 3+.  Each series element for a struct column arrives as a plain dict.


@pandas_udf(BinaryType())
def _asmvt_udf(geom: pd.Series, attrs: pd.Series, layer: pd.Series) -> bytes:
    """Grouped-agg: encode one group's features into a single MVT layer blob."""
    layer_name = "layer"
    if layer is not None and len(layer) > 0 and layer.iloc[0] is not None:
        layer_name = str(layer.iloc[0])
    # Pass each geom (WKB/EWKB bytes or WKT/EWKT str) through untouched; encode_layer
    # routes it through the shared _geom.parse_geom contract. Empty/None geoms are
    # dropped there (parse_geom -> None / is_empty), so no per-encoding length check here.
    feats = [
        {"geometry": g, "properties": a} for g, a in zip(geom, attrs) if g is not None
    ]
    return _mvt.encode_layer(feats, layer_name=layer_name)


@pandas_udf(BooleanType())
def _coverageisvalid_udf(geom: pd.Series, gap_width: pd.Series = None) -> bool:
    """Grouped-agg: is this group's polygon set a valid coverage?"""
    from . import _coverage as _cov

    gw = 0.0
    if gap_width is not None and len(gap_width) > 0 and gap_width.iloc[0] is not None:
        gw = float(gap_width.iloc[0])
    return _cov.coverage_is_valid_agg(list(geom), gw)


@pandas_udf(BinaryType())
def _coverageinvalidedges_udf(geom: pd.Series, gap_width: pd.Series = None) -> bytes:
    """Grouped-agg: union of this coverage's invalid edges (EWKB; empty when clean)."""
    from . import _coverage as _cov

    gw = 0.0
    if gap_width is not None and len(gap_width) > 0 and gap_width.iloc[0] is not None:
        gw = float(gap_width.iloc[0])
    return _cov.coverage_invalid_edges_agg(list(geom), gw)


def _legacyaswkb_impl(geom):
    """Scalar: decode a legacy Mosaic struct row to ISO WKB (Z + holes)."""
    return _legacy.legacy_to_wkb(geom)


# --- st_asmvt_pyramid: Python UDTF ----------------------------------------------------------
# Returns one (z, x, y, mvt_bytes) row per tile the input feature intersects.
# Defined before use so the helper is available when the @udtf decorator runs at import.


def _mvt_tile_return():
    from ._serde import TILE_SCHEMA

    return TILE_SCHEMA


@udtf(returnType=_mvt_tile_return())
class _AsMvtPyramidUDTF:
    def eval(self, geom, attrs, min_z: int, max_z: int, layer_name=None, extent=None):
        if (
            min_z is None or max_z is None
        ):  # a NULL required argument: no rows, as explode(NULL)
            return
        ln = "layer" if layer_name is None else str(layer_name)
        ex = _mvt.DEFAULT_EXTENT if extent is None else int(extent)
        # Yield incrementally — never build the full list (fan-out OOM guard).
        for z, x, y, blob in _mvt.pyramid_tiles(
            geom, attrs, int(min_z), int(max_z), ln, ex
        ):
            yield (z, x, y, blob)


# --- st_triangulate: constrained-Delaunay TIN UDTF -----------------------------------------
# Returns one 2D-WKB triangle (Polygon) per row.  Constrained mode (default) uses scipy +
# Sloan recovery; conforming mode is heavy-only and raises.


def _geoms_from_array(arr):
    """Decode an ARRAY<BINARY|STRING> of geometries via the shared parse_geom
    contract (WKB/EWKB/WKT/EWKT)."""
    from ._geom import parse_geom

    out = []
    for g in arr or []:
        geom = parse_geom(g)
        if geom is not None:
            out.append(geom)
    return out


def _all_xyz(geom):
    """Recursively flatten any geometry to a list of (x, y, z) coordinate tuples.

    Handles multi-part geometries (MULTIPOINT/MULTILINESTRING/etc. expose
    ``.geoms``, not ``.coords``) and pads 2D coordinates with z=0.0. Used to
    extract triangulation/elevation sites from the points array, so a single
    MULTIPOINT element doesn't crash on a missing ``.coords``.
    """
    if hasattr(geom, "geoms"):
        return [c for g in geom.geoms for c in _all_xyz(g)]
    return [(c[0], c[1], c[2] if len(c) == 3 else 0.0) for c in geom.coords]


def _validate_mode(mode):
    m = (mode or "constrained").lower()
    if m == "conforming":
        raise NotImplementedError(
            "mode='conforming' (Steiner-point conforming Delaunay) is heavy-only; "
            "use the heavyweight vectorx tier, or mode='constrained' in light."
        )
    if m != "constrained":
        raise ValueError(f"mode must be 'constrained' or 'conforming'; got {mode!r}")
    return m


def _triangulate_schema():
    from ._serde import TRIANGLE_SCHEMA

    return TRIANGLE_SCHEMA


@udtf(returnType=_triangulate_schema())
class _TriangulateUDTF:
    def eval(
        self,
        points,
        breaklines,
        merge_tolerance,
        snap_tolerance,
        split_point_finder,
        mode=None,
    ):
        _validate_mode(mode)
        if (
            merge_tolerance is None
            or snap_tolerance is None
            or split_point_finder is None
        ):
            return  # a NULL required argument: no rows, as explode(NULL)
        import numpy as np
        from shapely import to_wkb
        from shapely.geometry import Polygon

        pt_geoms = _geoms_from_array(points)
        if not pt_geoms:
            return
        coords = np.array([xyz for g in pt_geoms for xyz in _all_xyz(g)], dtype=float)
        bls = [np.array(g.coords, dtype=float) for g in _geoms_from_array(breaklines)]
        for t in _tin.triangulate(
            coords, bls, float(merge_tolerance), float(snap_tolerance)
        ):
            yield (to_wkb(Polygon([(p[0], p[1]) for p in t])),)


# --- st_interpolateelevation{bbox,geom}: barycentric Z over the constrained TIN -------------
# One POINT Z WKB row per in-hull grid cell center (column-major); outside-hull cells dropped.


def _elevation_schema():
    from ._serde import ELEVATION_SCHEMA

    return ELEVATION_SCHEMA


def _emit_elevation(points, breaklines, mt, st, spf, mode, cell_iter, srid):
    _validate_mode(mode)
    if mt is None or st is None or spf is None:
        return  # a NULL required argument: no rows, as explode(NULL)
    import numpy as np
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point

    pt_geoms = _geoms_from_array(points)
    if not pt_geoms:
        return
    coords = np.array([xyz for g in pt_geoms for xyz in _all_xyz(g)], dtype=float)
    bls = [np.array(g.coords, dtype=float) for g in _geoms_from_array(breaklines)]
    tris = _tin.triangulate(coords, bls, float(mt), float(st))
    for x, y in cell_iter:
        z = _tin.interpolate_z(tris, x, y)
        if z is None:
            continue
        p = Point(x, y, z)
        if srid:
            p = set_srid(p, int(srid))
        yield (to_wkb(p, output_dimension=3),)


@udtf(returnType=_elevation_schema())
class _InterpElevBBoxUDTF:
    def eval(
        self,
        points,
        breaklines,
        merge_tolerance,
        snap_tolerance,
        split_point_finder,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        srid,
        mode=None,
    ):
        if None in (xmin, ymin, xmax, ymax, width_px, height_px, srid):
            _validate_mode(mode)
            return  # a NULL required argument: no rows, as explode(NULL)
        yield from _emit_elevation(
            points,
            breaklines,
            merge_tolerance,
            snap_tolerance,
            split_point_finder,
            mode,
            _tin.grid_bbox(
                float(xmin),
                float(ymin),
                float(xmax),
                float(ymax),
                int(width_px),
                int(height_px),
            ),
            int(srid),
        )


@udtf(returnType=_elevation_schema())
class _InterpElevGeomUDTF:
    def eval(
        self,
        points,
        breaklines,
        merge_tolerance,
        snap_tolerance,
        split_point_finder,
        grid_origin,
        grid_cols,
        grid_rows,
        cell_size_x,
        cell_size_y,
        mode=None,
    ):
        from shapely import get_srid

        from ._geom import parse_geom

        if None in (grid_origin, grid_cols, grid_rows, cell_size_x, cell_size_y):
            _validate_mode(mode)
            return  # a NULL required argument: no rows, as explode(NULL)
        og = parse_geom(grid_origin)
        ox, oy = (og.x, og.y) if og is not None else (0.0, 0.0)
        srid = get_srid(og) if og is not None else 0
        yield from _emit_elevation(
            points,
            breaklines,
            merge_tolerance,
            snap_tolerance,
            split_point_finder,
            mode,
            _tin.grid_geom(
                ox,
                oy,
                int(grid_cols),
                int(grid_rows),
                float(cell_size_x),
                float(cell_size_y),
            ),
            int(srid),
        )


def _light_st_asmvt(geom: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into an MVT protobuf blob (BINARY).

    geom: per-row geometry (WKB, EWKB, WKT, or EWKT) in tile-local coordinates.
    attrs:    per-row attribute struct (native-typed in the output tile).
    layer_name: constant MVT layer name (plain str -> literal).
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return _asmvt_udf(_col(geom), _col(attrs), _col(layer_name))


def _light_st_coverageisvalid(geom: ColLike, gap_width: float = 0.0) -> Column:
    """Column (grouped-agg): valid-coverage boolean over the grouped polygon set."""
    return _coverageisvalid_udf(_col(geom), f.lit(gap_width))


def _light_st_coverageinvalidedges(geom: ColLike, gap_width: float = 0.0) -> Column:
    """Column (grouped-agg): union of the coverage's invalid edges (BINARY)."""
    return _coverageinvalidedges_udf(_col(geom), f.lit(gap_width))


def __getattr__(name):
    """A removed public name: say where it went (no alias keeps working)."""
    from databricks.labs.gbx import _moved

    return _moved.removed_name(__name__, name)
