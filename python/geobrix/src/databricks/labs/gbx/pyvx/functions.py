"""pyvx light VectorX API — MVT, legacy-geometry, TIN, and CRS functions (Serverless-safe).

Covers the MVT aggregator/pyramid (gbx_st_asmvt, gbx_st_asmvt_pyramid), legacy
Mosaic-geometry decoding (gbx_st_legacyaswkb), constrained-Delaunay TIN
generators (gbx_st_triangulate, gbx_st_interpolateelevation{bbox,geom}), and
the CRS family (gbx_st_crs, gbx_st_setcrs, gbx_st_transformcrs).

Signatures mirror databricks.labs.gbx.vectorx.functions so light <-> heavy is a
one-line import swap. Register once with vx.register(spark), then use on columns.
"""

from typing import List, Optional, Union

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf, udtf
from pyspark.sql.types import BinaryType, StringType

from databricks.labs.gbx import _register

from . import _env, _legacy, _mvt, _tin

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


def _registrar_groups() -> List[_register.Group]:
    mvt = {
        "gbx_st_asmvt": lambda s: s.udf.register("gbx_st_asmvt", _asmvt_udf),
        "gbx_st_asmvt_pyramid": lambda s: s.udtf.register(
            "gbx_st_asmvt_pyramid", _AsMvtPyramidUDTF
        ),
    }
    legacy = {
        "gbx_st_legacyaswkb": lambda s: s.udf.register(
            "gbx_st_legacyaswkb", _legacyaswkb_impl, BinaryType()
        ),
    }
    tin = {
        "gbx_st_triangulate": lambda s: s.udtf.register(
            "gbx_st_triangulate", _TriangulateUDTF
        ),
        "gbx_st_interpolateelevationbbox": lambda s: s.udtf.register(
            "gbx_st_interpolateelevationbbox", _InterpElevBBoxUDTF
        ),
        "gbx_st_interpolateelevationgeom": lambda s: s.udtf.register(
            "gbx_st_interpolateelevationgeom", _InterpElevGeomUDTF
        ),
    }

    def _reg_pmtiles(s):
        from databricks.labs.gbx.pmtiles import register_pmtiles_agg

        register_pmtiles_agg(s)

    pmtiles = {"gbx_pmtiles_agg": _reg_pmtiles}

    def _reg_gbx_st_crs(s):
        from . import _crs as _c

        s.udf.register("gbx_st_crs", _c.st_crs, StringType())

    def _reg_gbx_st_setcrs(s):
        from . import _crs as _c

        s.udf.register("gbx_st_setcrs", _c._udf_st_setcrs, BinaryType())

    def _reg_gbx_st_transformcrs(s):
        from databricks.labs.gbx.core import proj_grids
        from databricks.labs.gbx.pyrx import _env

        from . import _crs as _c

        _grid_dirs = tuple(
            proj_grids.get_registered_dirs()
        )  # captured at build → pickled

        def _transformcrs_udf(geom, target_crs, source_crs=None):
            _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
            return _c._udf_st_transformcrs(geom, target_crs, source_crs)

        s.udf.register("gbx_st_transformcrs", _transformcrs_udf, BinaryType())

    crs = {
        "gbx_st_crs": _reg_gbx_st_crs,
        "gbx_st_setcrs": _reg_gbx_st_setcrs,
        "gbx_st_transformcrs": _reg_gbx_st_transformcrs,
    }

    def _reg_gbx_st_shiftlongitude(s):
        from . import _antimeridian as _am

        s.udf.register(
            "gbx_st_shiftlongitude", _am._udf_st_shiftlongitude, BinaryType()
        )

    def _reg_gbx_st_wrapx(s):
        from . import _antimeridian as _am

        s.udf.register("gbx_st_wrapx", _am._udf_st_wrapx, BinaryType())

    def _reg_gbx_st_split(s):
        from . import _antimeridian as _am

        s.udf.register("gbx_st_split", _am._udf_st_split, BinaryType())

    antimeridian = {
        "gbx_st_shiftlongitude": _reg_gbx_st_shiftlongitude,
        "gbx_st_wrapx": _reg_gbx_st_wrapx,
        "gbx_st_split": _reg_gbx_st_split,
    }
    return [
        (lambda: _env.assert_mvt_available(), mvt),
        (lambda: _env.assert_legacy_available(), legacy),
        (lambda: _env.assert_tin_available(), tin),
        (lambda: None, pmtiles),
        (lambda: _env.assert_crs_available(), crs),
        (lambda: True, antimeridian),
    ]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Register the pyvx VectorX SQL functions (Serverless-safe: udf/udtf only).

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_st_asmvt``) or short names (``st_asmvt``),
            case-insensitively. ``None`` registers everything; ``[]`` registers
            nothing. An unrecognized name raises ``ValueError``. A sub-module's
            availability guard runs only when >=1 of its functions is selected.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)
    globals()["_gbx_registered"] = True


def st_asmvt_pyramid(
    geom: ColLike,
    attrs: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    layer_name: Union[ColLike, None] = None,
    extent: Union[ColLike, None] = None,
):
    """Generator: one (z,x,y,mvt_bytes) row per intersecting tile across [min_z,max_z].

    In the light tier the pyramid generator is a Python UDTF and is invoked only via
    SQL LATERAL — it has no Python DataFrame Column form (unlike the heavy tier, which
    exposes a Column API for this generator). Calling this wrapper directly raises
    NotImplementedError; instead register and call it as a SQL LATERAL table function:

        SELECT t.* FROM features, LATERAL gbx_st_asmvt_pyramid(geom, attrs, 0, 12, 'layer', 4096) t

    The output schema (z,x,y,mvt_bytes) matches the heavyweight generator and feeds
    gbx_pmtiles_agg downstream, so the two tiers are interchangeable at the SQL level.
    """
    raise NotImplementedError(
        "Light st_asmvt_pyramid has no Python Column form; invoke the registered UDTF as a "
        "SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_st_asmvt_pyramid(geom, attrs, min_z, max_z, layer, extent) t"
    )


def st_triangulate(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    split_point_finder,
    mode: ColLike = "constrained",
):
    """Constrained Delaunay triangulation. Invoke via SQL LATERAL:
    SELECT t.* FROM <df>, LATERAL gbx_st_triangulate(points_array, breaklines_array, mt, st, spf, mode) t
    mode='conforming' is heavy-only."""
    raise NotImplementedError(
        "Light st_triangulate has no Python Column form; invoke the registered UDTF via SQL LATERAL."
    )


def st_interpolateelevationbbox(
    points_array,
    breaklines_array,
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
    mode: ColLike = "constrained",
):
    """Interpolate barycentric Z over the constrained TIN at bbox grid centers. Invoke via SQL LATERAL:
    SELECT t.* FROM <df>, LATERAL gbx_st_interpolateelevationbbox(
        points_array, breaklines_array, mt, st, spf, xmin, ymin, xmax, ymax, width_px, height_px, srid, mode) t
    One POINT Z WKB per in-hull cell (outside-hull dropped). mode='conforming' is heavy-only.
    """
    raise NotImplementedError(
        "Light st_interpolateelevationbbox has no Python Column form; "
        "invoke the registered UDTF via SQL LATERAL."
    )


def st_interpolateelevationgeom(
    points_array,
    breaklines_array,
    merge_tolerance,
    snap_tolerance,
    split_point_finder,
    grid_origin,
    grid_cols,
    grid_rows,
    cell_size_x,
    cell_size_y,
    mode: ColLike = "constrained",
):
    """Interpolate barycentric Z over the constrained TIN at origin-grid centers. Invoke via SQL LATERAL:
    SELECT t.* FROM <df>, LATERAL gbx_st_interpolateelevationgeom(
        points_array, breaklines_array, mt, st, spf, origin, cols, rows, cell_x, cell_y, mode) t
    SRID is taken from the origin EWKB. One POINT Z WKB per in-hull cell (outside-hull dropped).
    mode='conforming' is heavy-only."""
    raise NotImplementedError(
        "Light st_interpolateelevationgeom has no Python Column form; "
        "invoke the registered UDTF via SQL LATERAL."
    )


def st_asmvt(geom: ColLike, attrs: ColLike, layer_name: ColLike) -> Column:
    """Aggregator: encode a group of features into an MVT protobuf blob (BINARY).

    geom: per-row geometry (WKB, EWKB, WKT, or EWKT) in tile-local coordinates.
    attrs:    per-row attribute struct (native-typed in the output tile).
    layer_name: constant MVT layer name (plain str -> literal).
    """
    if isinstance(layer_name, str):
        layer_name = f.lit(layer_name)
    return _asmvt_udf(_col(geom), _col(attrs), _col(layer_name))


def st_legacyaswkb(geom: ColLike) -> Column:
    """Decode a legacy Mosaic geometry struct to ISO WKB (Z + holes preserved)."""
    return f.call_function("gbx_st_legacyaswkb", _col(geom))


def _crs_col(x: ColLike) -> Column:
    """Coerce a CRS argument: plain str -> f.lit (CRS string literal, not column name)."""
    if isinstance(x, str):
        return f.lit(x)
    return _col(x)


def st_crs(geom: ColLike) -> Column:
    """Return the canonical CRS string embedded in a geometry's SRID, or NULL.

    Reads the integer SRID from EWKB / EWKT and classifies it via the authoritative
    PROJ code sets (``EPSG:<n>`` or ``ESRI:<n>``). Returns NULL for plain WKB / WKT
    geometries with no embedded SRID.

    Args:
        geom: BINARY (WKB / EWKB) or STRING (WKT / EWKT) geometry column.

    Returns:
        STRING column: canonical CRS string (e.g. ``'EPSG:4326'``, ``'ESRI:54008'``),
        or NULL when no SRID is embedded.
    """
    return f.call_function("gbx_st_crs", _col(geom))


def st_setcrs(geom: ColLike, crs: ColLike) -> Column:
    """Stamp a CRS on a geometry without reprojecting (SQL surface, BINARY output).

    Assigns the EPSG or ESRI SRID to the geometry. Authority-less CRS strings
    (WKT / PROJ4 with no authority code) are rejected because a geometry SRID
    must be an integer.

    The SQL UDF always returns BINARY (EWKB), regardless of input encoding.
    For the medium-preserving Python core (bytes -> bytes, str -> str) use
    ``pyvx._crs.st_setcrs`` directly.

    Args:
        geom: BINARY (WKB / EWKB) or STRING (WKT / EWKT) geometry column.
        crs:  Column, CRS string literal (e.g. ``'EPSG:32633'``), or integer
              SRID. A plain Python str is treated as a CRS literal, not a column
              name. WKT / PROJ4 strings raise at execution time.

    Returns:
        BINARY column: EWKB geometry with the new SRID stamped.
    """
    return f.call_function("gbx_st_setcrs", _col(geom), _crs_col(crs))


def st_transformcrs(
    geom: ColLike,
    target_crs: ColLike,
    source_crs: Optional[ColLike] = None,
) -> Column:
    """Reproject a geometry to the target CRS (SQL surface, BINARY output).

    Source CRS resolution order:
    1. Embedded SRID from the geometry (EWKB / EWKT).
    2. Explicit ``source_crs`` column / literal (for plain WKB / WKT inputs).
    3. No source CRS resolvable -> input returned UNCHANGED (never-error).

    The SQL UDF always returns BINARY (WKB / EWKB), regardless of input encoding.
    For the medium-preserving Python core use ``pyvx._crs.st_transformcrs``.

    Args:
        geom:       BINARY (WKB / EWKB) or STRING (WKT / EWKT) geometry column.
        target_crs: Column, CRS string literal (e.g. ``'EPSG:32633'``), or
                    integer SRID. A plain Python str is treated as a CRS literal,
                    not a column name.
        source_crs: Optional Column, CRS string literal, or integer SRID —
                    explicit source CRS for plain (SRID-less) geometries.

    Returns:
        BINARY column: reprojected geometry (EWKB when target has an authority
        code, plain WKB when authority-less), or the original bytes when no
        source CRS is resolvable.
    """
    if source_crs is None:
        return f.call_function("gbx_st_transformcrs", _col(geom), _crs_col(target_crs))
    return f.call_function(
        "gbx_st_transformcrs", _col(geom), _crs_col(target_crs), _crs_col(source_crs)
    )


def st_shiftlongitude(geom: ColLike) -> Column:
    """Shift longitude from [-180,180] to [0,360] (SQL surface, BINARY output)."""
    return f.call_function("gbx_st_shiftlongitude", _col(geom))


def st_wrapx(geom: ColLike, wrap_x_origin: ColLike, wrap_direction: ColLike) -> Column:
    """Wrap X coordinates around an origin by a direction (SQL surface, BINARY output).

    PostGIS ST_WrapX semantics: wrap_direction < 0 moves coordinates with
    x >= wrap_x_origin by wrap_direction; wrap_direction > 0 moves coordinates
    with x <= wrap_x_origin by wrap_direction.

    Args:
        geom: BINARY (WKB / EWKB) or STRING (WKT / EWKT) geometry column.
        wrap_x_origin: X threshold value (numeric literal or column).
        wrap_direction: Amount to shift qualifying coordinates (numeric literal or column).

    Returns:
        BINARY column: EWKB geometry with wrapped X coordinates.
    """
    return f.call_function(
        "gbx_st_wrapx", _col(geom), _col(wrap_x_origin), _col(wrap_direction)
    )


def st_split(input_geom: ColLike, blade_geom: ColLike) -> Column:
    """Split a geometry by a blade; returns a GEOMETRYCOLLECTION (BINARY).

    MULTI inputs are decomposed — each part split individually, all pieces
    recollected into a single GEOMETRYCOLLECTION (pure shapely, no second engine).

    Args:
        input_geom: BINARY (WKB / EWKB) or STRING (WKT / EWKT) geometry column.
        blade_geom: BINARY (WKB / EWKB) or STRING (WKT / EWKT) blade geometry.

    Returns:
        BINARY column: EWKB GEOMETRYCOLLECTION of the split pieces.
    """
    return f.call_function("gbx_st_split", _col(input_geom), _col(blade_geom))
