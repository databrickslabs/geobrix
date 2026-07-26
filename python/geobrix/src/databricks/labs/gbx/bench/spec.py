"""Benchmark function registry.

Each FnSpec binds a pyrx function to: its SQL name, category, supported timing
modes, default args, and two call adapters:
  core_fn(ds, args) -> Any           # pure-core: rasterio DatasetReader in
  col_fn(tile_col, args) -> Column   # spark-path: build a Spark Column

Only standard ds-in functions live here for the representative set. Special-
shaped functions (rasterize_geom, aggregators, multi-output tiling) get their
own adapters in a later task and are added with the appropriate `modes`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List

import shapely.geometry
import shapely.wkb
from pyspark.sql import functions as F

from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import analysis as analysis_core
from databricks.labs.gbx.pyrx.core import (
    coords,
    derivedband,
    edit,
    features,
    focal,
    gridagg,
    indices,
    mapalgebra,
    ops,
    resample,
    terrain,
    tessellate,
    tiling,
    tin,
    warp,
    xyz,
)

# Fixed 3x3 normalised mean kernel for rst_convolve. Hardcoded identically here
# (Python core_fn + col_fn) and in the Scala BenchDispatch case so the two engines
# convolve with the same coefficients; the bench args carry only `kernel_size` for
# documentation (a 2-D kernel cannot ride the stringly-typed Scala args map).
_CONVOLVE_KERNEL = [[1.0 / 9.0] * 3 for _ in range(3)]

# --- Task 6: complex-arg constants (geometry / expression / band-map / func) -
# Each of these rides identically through the pyrx core_fn, the pyrx col_fn, AND
# the Scala BenchDispatch case (same as the convolve kernel) — a stringly-typed
# bench args map cannot carry a band-map, an expression's variable bindings, or a
# Python pixel-function body, so they are hardcoded in both engines.

# rst_index: the generic dispatcher needs a formula name + a band-map wiring the
# formula's named bands to 1-based indices. "ndvi" needs red+nir, both present in
# the 2-band corpus. Hardcoded identically here and in the Scala case.
_INDEX_NAME = "ndvi"
_INDEX_BAND_MAP = {"red": 1, "nir": 2}

# rst_mapalgebra: band 1 of the (single) input binds to A; "A*2" is CRS- and
# band-count-independent. Same expression on both engines.
_MAPALGEBRA_EXPR = "A*2"

# rst_derivedband: a GDAL VRT Python pixel-function body. Both engines exec this
# exact source in-process and fill out_ar in place, so the algorithm is identical
# (not merely analogous). A trivial per-pixel mean of all input bands.
_DERIVEDBAND_FUNC_NAME = "mean_bands"
_DERIVEDBAND_PYFUNC = (
    "import numpy as np\n"
    "def mean_bands(in_ar, out_ar, xoff, yoff, xsize, ysize,\n"
    "               raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
    "    stack = np.array(in_ar, dtype='float64')\n"
    "    out_ar[:] = stack.mean(axis=0)\n"
)

# rst_clip (timing-only): clip needs a geometry. No single literal geometry can
# intersect every tile across the multi-CRS corpus (EPSG:4326 degrees, 3857 /
# 32618 / 27700 metres), so clip is timing-only and never compared. A FIXED box
# (e.g. box(-500,-500,500,500)) is out-of-extent for a UTM (EPSG:32618) tile and
# makes rasterio.mask raise "Input shapes do not overlap raster." Instead the
# core_fn derives the cutline per-tile from ``ds.bounds`` (shrunk 50% about the
# tile center, in the tile's own CRS) so every tile gets an in-extent clip. The
# col_fn (spark-path) is mode-filtered out (clip is pure-core-only), so the
# static WKB below is unused at runtime and kept only for the col_fn signature.
_CLIP_GEOM_WKB = shapely.geometry.box(-500.0, -500.0, 500.0, 500.0).wkb


def _shrunk_bounds_box_wkb(ds) -> bytes:
    """WKB of the tile's bounds box shrunk 50% about its center, in the tile CRS.

    Guarantees an in-extent cutline for ``rst_clip`` timing on every corpus CRS.
    """
    left, bottom, right, top = ds.bounds
    cx, cy = (left + right) / 2.0, (bottom + top) / 2.0
    hw, hh = (right - left) / 4.0, (top - bottom) / 4.0
    return shapely.geometry.box(cx - hw, cy - hh, cx + hw, cy + hh).wkb


def _tile_center_xy(ds) -> tuple:
    """World (x, y) of the tile center in the tile's own CRS (in-extent observer)."""
    left, bottom, right, top = ds.bounds
    return ((left + right) / 2.0, (bottom + top) / 2.0)


# rst_sample (timing-only): sample needs a POINT in-extent for the tile. No single
# world point is in-extent across the multi-CRS corpus, so sample is timing-only
# and never compared. (0, 0) is a valid POINT for the timing call (out-of-extent
# points return null, which is fine for timing).
_SAMPLE_POINT_WKB = shapely.geometry.Point(0.0, 0.0).wkb

# rst_viewshed (timing-only): needs an observer point; like sample, no single
# world point is in-extent across the multi-CRS corpus, and a fixed (0, 0)
# observer falls outside the UTM/3857 tile x/y ranges (xrspatial raises
# "observer (0,0) outside raster x_range"). The core_fn derives the observer
# from the tile center (``ds.bounds``) so it is in-extent on every CRS.
# Additionally the pyrx binding documents a parity divergence (xrspatial CPU
# line-of-sight scan vs GDAL's GVM_Edge sweep with curvature), so the binary
# masks are not byte-equal even at a shared observer — timing-only on both
# counts. The col_fn is mode-filtered out (viewshed is pure-core-only), so the
# static WKB below is unused at runtime and kept only for the col_fn signature.
_VIEWSHED_OBSERVER_WKB = shapely.geometry.Point(0.0, 0.0).wkb

# rst_color_relief (timing-only): heavy reads a gdaldem color-table FILE and runs
# GDAL DEMProcessing color-relief (its own interpolation), while pyrx parses the
# same file and interpolates with np.interp — different interpolation engines, so
# the RGB(A) bytes are not value-identical. It also needs a real file path on the
# executor, which the static args map cannot synthesize per-engine. Timing-only.
# The runner does not invoke color_relief's core_fn for fingerprinting (it is
# fingerprint=False); the path below is a documented placeholder for the args dict
# and is created on demand by the core_fn.
_COLOR_RAMP_TEXT = "nv 0 0 0\n0% 0 0 255\n50% 0 255 0\n100% 255 0 0\n"


def _ds_to_gtiff_bytes(ds) -> bytes:
    """Re-serialize an open rasterio DatasetReader to GTiff bytes.

    ``mapalgebra`` (and rst_mapalgebra) consume raster *bytes*, but the bench
    runner hands core_fn an already-open ``DatasetReader``. Round-trip the open
    dataset through an in-memory GTiff so the single-input map-algebra call gets
    the bytes it expects, byte-for-byte the same grid the column path sees.
    """
    from rasterio.io import MemoryFile

    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(ds.read())
        return mf.read()


def _tile_extent_size_srid(ds) -> tuple:
    """(xmin, ymin, xmax, ymax, width_px, height_px, srid) from an open tile.

    The geometry-input constructors (rst_rasterize / rst_gridfrompoints /
    rst_dtmfromgeoms) burn/interpolate into a NEW raster whose extent, size, and
    CRS must match the tile the geometry was derived from so the output grid
    aligns on every corpus CRS (the heavy tier reads the SAME extent from the same
    tile, so the two grids are pixel-comparable). ``srid`` falls back to 0 when the
    tile carries no EPSG (none of the corpus tiles do, but stay defensive).
    """
    left, bottom, right, top = ds.bounds
    epsg = ds.crs.to_epsg() if ds.crs is not None else None
    return (
        float(left),
        float(bottom),
        float(right),
        float(top),
        int(ds.width),
        int(ds.height),
        int(epsg) if epsg is not None else 0,
    )


def _color_table_path() -> str:
    """Write the synthetic gdaldem color ramp to a temp file and return its path.

    color_relief is timing-only (see ``_COLOR_RAMP_TEXT``); this only needs to
    exist so the timing call succeeds.
    """
    import tempfile

    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="bench_color_", delete=False
    )
    f.write(_COLOR_RAMP_TEXT)
    f.close()
    return f.name


@dataclass(frozen=True)
class FnSpec:
    name: str
    sql_name: str
    category: str
    modes: tuple
    args: dict = field(default_factory=dict)
    min_bands: int = 1
    core_fn: Callable = None  # (ds, args) -> Any
    col_fn: Callable = None  # (tile_col, args) -> Column
    core: bool = False  # in the fast "core" benchmark set
    fingerprint: bool = (
        True  # emit a comparable output fingerprint; False = timing-only
    )
    # What the runners feed the core_fn / heavy dispatch for this function:
    #   "tile"  (default): the opened raster -- rasterio DatasetReader (pyrx) /
    #            GDAL Dataset (heavy). Preserves every pre-existing function.
    #   "bytes": the corpus tile's RAW BYTES -- core_fn(raster_bytes, args);
    #            the heavy dispatch opens the bytes itself (vsimem). Used by the
    #            reader/constructor fns whose input is content, not an open ds
    #            (rst_tryopen, rst_fromcontent).
    #   "path":  the corpus tile's FILE PATH -- core_fn(path, args); the heavy
    #            dispatch opens the path itself (rst_fromfile). pure-core only,
    #            since the spark-path tile DataFrame carries no path column.
    #   "geometry": the opened raster tile PLUS the tile's geometry set from the
    #            geometry corpus -- core_fn(ds, args, geom) where ``geom`` is the
    #            tile's ``manifest.GeometrySet`` (boxes/points/zpoints as WKB +
    #            burn values, in the tile CRS). Used by geometry-input functions
    #            (rst_clip / rst_rasterize / rst_dtmfromgeoms) and geometry
    #            aggregators. Geometry is in-extent for the tile and identical
    #            across both engines (write-once-read-both via geometry.json).
    input_kind: str = "tile"
    # How the runner fingerprints this function's output. The default ``"auto"``
    # preserves every pre-existing function: the runner inspects the output value
    # (bytes -> raster, list-of-bytes -> raster_collection, list-of-scalars ->
    # scalar_list, else scalar). Functions whose output shape the auto-detector
    # cannot classify declare an explicit kind:
    #   "dggs_grid":  a discrete-global-grid output (the bucket-B grid fns). The
    #                 core_fn returns a per-band list of cell records
    #                 (``gridagg.raster_to_grid``) or, for tessellation, a single
    #                 band wrapping ``[(cellid, bytes)]`` -- a list-of-lists that
    #                 the auto-detector would mis-read as a scalar_list. The runner
    #                 routes it through ``fingerprint_dggs_grid``.
    #   "vector":     a vector-feature output (the bucket-B vector fns) routed
    #                 through ``fingerprint_vector``.
    #   "collection": force the raster_collection fingerprint.
    # Honored only by the pure-core fingerprint pass (the spark-path mode never
    # fingerprints its output).
    fingerprint_kind: str = "auto"
    # Optional per-function relative tolerance for the fingerprint comparison.
    # ``None`` (default) means use the comparator's global ``REL_TOL``. A function
    # whose two engines run genuinely different algorithms with a small, inherent
    # numeric spread (e.g. rst_contour: GDAL's contour generator vs the lightweight
    # marching-squares segmentation differ ~1.5% on segment measures/attrs) sets a
    # looser per-fn tol so the comparator does not flag that inherent spread as a
    # divergence -- WITHOUT loosening the strict global tol for every other function.
    rel_tol: float | None = None
    # Repo-relative file paths whose CONTENT defines this function's heavy+light
    # behavior (the pyrx core module(s) the core_fn calls, plus the heavy Scala
    # RST_<Name> expression and any shared heavy helper it delegates to). This
    # powers change-aware benchmarking: a changed file -> the set of functions to
    # re-bench. Deliberately EXCLUDES the bench harness (this file, BenchDispatch)
    # so editing the registry does not mark every function stale.
    sources: tuple = ()
    # Route this function's pure-core sweep to the Great-Britain-overlapping corpus
    # tile (``TileEntry.role == "bng_gb"``) instead of the ordinary sweep tiles.
    # The BNG raster->grid / tessellate functions reproject the tile to EPSG:27700
    # internally and drop out-of-GB pixels, so on the NYC-ish sweep tiles they bin
    # an EMPTY grid (a vacuous both-empty cross-tier match). Set on those fns so
    # they bench REAL cells; every other fn leaves it False and skips the GB tile,
    # keeping its tile selection unchanged.
    gb_tile: bool = False


_BOTH = ("pure-core", "spark-path")

# --- source-path groups (DRY) ----------------------------------------------
# Editing the registry must NOT re-bench everything, so the harness files
# (spec.py / BenchDispatch.scala) are never listed. Each tuple is the set of
# repo-relative files whose content defines a function's heavy+light behavior.
_PYRX = "python/geobrix/src/databricks/labs/gbx/pyrx/core/"
_HEAVY = "src/main/scala/com/databricks/labs/gbx/rasterx/expressions/"
_OPS = "src/main/scala/com/databricks/labs/gbx/rasterx/operations/"
_GDAL = "src/main/scala/com/databricks/labs/gbx/rasterx/gdal/"
_NODATA = _PYRX + "_nodata.py"
# tile (de)serialization — the reader/constructor fns (fromcontent/fromfile)
# have no dedicated core module; their behavior is the _serde tile round-trip.
_PYRX_SERDE = "python/geobrix/src/databricks/labs/gbx/pyrx/_serde.py"
_DEM_HELPER = _HEAVY + "dem/RST_DEMProcessingHelper.scala"
_PIXEL_COMBINE = _OPS + "PixelCombineRasters.scala"
_GDAL_BLOCK = _GDAL + "GDALBlock.scala"

# pyrx core module + shared _nodata, by module (added to per-function sources
# together with that function's heavy Scala expression / helpers).
_ACCESSORS_LIGHT = (_PYRX + "accessors.py", _NODATA)
_TERRAIN_LIGHT = (_PYRX + "terrain.py", _NODATA)
_INDICES_LIGHT = (_PYRX + "indices.py", _NODATA)
_EDIT_LIGHT = (_PYRX + "edit.py", _NODATA)
_FOCAL_LIGHT = (_PYRX + "focal.py", _NODATA)
_FEATURES_LIGHT = (_PYRX + "features.py", _NODATA)
# TIN / IDW constructors (gridfrompoints -> idw_grid, dtmfromgeoms ->
# delaunay_dtm) live in core/tin.py; it does NOT import _nodata (grep-confirmed).
_TIN_LIGHT = (_PYRX + "tin.py",)
_MAPALGEBRA_LIGHT = (_PYRX + "mapalgebra.py", _NODATA)
# these core modules do NOT import _nodata (grep-confirmed)
_WARP_LIGHT = (_PYRX + "warp.py",)
_COORDS_LIGHT = (_PYRX + "coords.py",)
_TILING_LIGHT = (_PYRX + "tiling.py",)
_XYZ_LIGHT = (_PYRX + "xyz.py",)
_OPS_LIGHT = (_PYRX + "ops.py",)
_ANALYSIS_LIGHT = (_PYRX + "analysis.py",)
_RESAMPLE_LIGHT = (_PYRX + "resample.py",)
_DERIVEDBAND_LIGHT = (_PYRX + "derivedband.py",)
# multi-tile reducers (bucket C, group C3) live in core/agg.py; it does NOT
# import _nodata (grep-confirmed).
_AGG_LIGHT = (_PYRX + "agg.py",)
# bucket B, group B-grid (DGGS). raster->grid lives in core/gridagg.py; H3
# tessellation in core/tessellate.py (which delegates the per-cell clip to
# core/edit.py). The shared GridX index helpers the heavy expressions call are
# gridx/grid/{H3,Quadbin}.scala (RST_H3_RasterToGrid -> H3.pointToCellID;
# RST_Quadbin_RasterToGrid -> Quadbin; RST_H3_Tessellate -> RasterTessellate ->
# H3). Neither light module imports _nodata (grep-confirmed).
_GRIDX = "src/main/scala/com/databricks/labs/gbx/gridx/grid/"
_GRIDAGG_LIGHT = (_PYRX + "gridagg.py",)
# BNG raster->grid / rasterize / tessellate additionally route cell math through
# pygx._bng (the single source of truth), so the BNG bench sources add it.
_PYGX = "python/geobrix/src/databricks/labs/gbx/pygx/"
_BNG_LIB = _PYGX + "_bng.py"
_QUADBIN_LIB = _PYGX + "_quadbin.py"
_TESSELLATE_LIGHT = (_PYRX + "tessellate.py", _PYRX + "edit.py")
# H3 rasterize aggregator (cellid,value rows -> one tile, pixel-centroid burn).
# Light burn math + gridspec port live in core/cellraster.py.
_CELLRASTER_LIGHT = (_PYRX + "cellraster.py",)

# --- rst_h3_rasterize_agg: fixed deterministic cell set + EXPLICIT grid --------
# PARITY CONTRACT (must stay byte-for-byte in sync with the Scala BenchDispatch
# h3Aggregate branch -- see BenchDispatch.scala "H3 rasterize aggregate"):
#   1. CELL SET RECIPE: the res-9 H3 cell at (lat, lng) = (40.7128, -74.0060)
#      (NYC), grid-disk'd to k=10 -> 331 cells. The Python `h3` lib and the Scala
#      `com.uber.h3core` lib share the H3 C spec, so latlng_to_cell/geoToH3 +
#      grid_disk/kRing yield the IDENTICAL cell-id set (order-independent). The
#      bench runner ASSERTS this cross-tier sameness is plausible by checking the
#      cell count + the center cell id (test_runner exercises the builder).
#   2. EXPLICIT GRID: derived ONCE via cellraster.compute_gridspec(cells,
#      srid=4326, pixel_size=0.0025, kring_pad=1) and HARDCODED below. Both tiers
#      receive these exact grid constants (NOT auto-derived per tier), so the
#      burn lands on the identical canvas and the masks/fingerprints compare.
# A FIXED pixel_size (0.0025, an exact decimal) is used instead of the auto-
# derived hexagon-edge pixel so the grid constants are unambiguous literals with
# no float-formatting drift between Python and Scala.
_H3RAGG_RES = 9
_H3RAGG_CENTER_LAT = 40.7128
_H3RAGG_CENTER_LNG = -74.0060
_H3RAGG_K = 10
_H3RAGG_SRID = 4326
_H3RAGG_PIXEL_SIZE = 0.0025
_H3RAGG_KRING_PAD = 1
_H3RAGG_MODE = "centroids"
# Hardcoded explicit grid (== compute_gridspec(cells, 4326, 0.0025, kring_pad=1)).
# Mirrored EXACTLY in BenchDispatch.scala. If the recipe above changes, recompute
# (see docs/scripts or just re-run compute_gridspec) and update BOTH files.
_H3RAGG_XMIN = -74.055
_H3RAGG_YMIN = 40.6825
_H3RAGG_XMAX = -73.9575
_H3RAGG_YMAX = 40.7425
_H3RAGG_WIDTH = 39
_H3RAGG_HEIGHT = 24
_H3RAGG_N_CELLS = 331  # grid_disk(center, k=10) count -- a cross-tier sanity check.


def h3_rasterize_cells() -> List[int]:
    """The fixed deterministic H3 cell-id set the bench rasterizes (both tiers).

    The res-9 cell at the fixed NYC lat/lng, grid-disk'd to k=10. Returns signed
    Spark-Long cell ids (h3.str_to_int gives the unsigned 64-bit id; values >
    2^63 wrap negative, which is exactly what the Spark LONG column carries and
    the heavy tier sign-extends back). Deterministic + order-stable (sorted).
    """
    import h3

    center = h3.latlng_to_cell(_H3RAGG_CENTER_LAT, _H3RAGG_CENTER_LNG, _H3RAGG_RES)
    cells = sorted(h3.grid_disk(center, _H3RAGG_K))
    out = []
    for c in cells:
        v = h3.str_to_int(c)
        # Map unsigned 64-bit -> signed Spark Long (two's complement).
        out.append(v - (1 << 64) if v >= (1 << 63) else v)
    return out


# The explicit grid 8-tuple passed identically to both tiers (xmin..height, srid).
_H3RAGG_GRID = (
    _H3RAGG_XMIN,
    _H3RAGG_YMIN,
    _H3RAGG_XMAX,
    _H3RAGG_YMAX,
    _H3RAGG_WIDTH,
    _H3RAGG_HEIGHT,
    _H3RAGG_SRID,
)

# --- rst_quadbin_rasterize_agg: fixed deterministic cell set (PARITY CONTRACT) --
# Must stay byte-for-byte in sync with the Scala BenchDispatch quadbinRagg* block
# (search "rst_quadbin_rasterize_agg: fixed cell set"):
#   CELL SET RECIPE: the zoom-15 quadbin cell at (lat, lng) = (40.7128, -74.0060)
#   (NYC), kRing'd (Chebyshev) to k=3 -> (2*3+1)^2 = 49 cells. The Python `quadbin`
#   lib and the Scala Quadbin share the CARTO spec, so pointToCell + kRing yield
#   the IDENTICAL cell-id set (order-independent). Unlike H3, the quadbin/BNG
#   aggregators AUTO-DERIVE the grid from the cell set (deterministic given the
#   fixed set), so no explicit-grid constants are hardcoded -- the 2-arg overload
#   (cellid, value) is used on both tiers.
_QBRAGG_ZOOM = 15
_QBRAGG_CENTER_LAT = 40.7128
_QBRAGG_CENTER_LNG = -74.0060
_QBRAGG_K = 3
_QBRAGG_SRID = 4326
_QBRAGG_N_CELLS = (2 * _QBRAGG_K + 1) * (2 * _QBRAGG_K + 1)  # 49 (cross-tier check)


def quadbin_rasterize_cells() -> List[int]:
    """The fixed deterministic quadbin cell-id set the bench rasterizes (both tiers).

    The zoom-15 NYC cell, kRing'd to k=3. ``pygx._quadbin`` returns signed Spark
    Longs (the CARTO cell ids already fit the signed 64-bit space at zoom 15), the
    SAME ids the Scala Quadbin.kRing yields. Deterministic + order-stable (sorted).
    """
    from databricks.labs.gbx.pygx import _quadbin as _qb

    center = _qb.point_as_cell(_QBRAGG_CENTER_LNG, _QBRAGG_CENTER_LAT, _QBRAGG_ZOOM)
    return sorted(_qb.k_ring(center, _QBRAGG_K))


# --- rst_bng_rasterize_agg: fixed deterministic cell set (PARITY CONTRACT) ------
# Must stay in sync with the Scala BenchDispatch bngRagg* block (search
# "rst_bng_rasterize_agg: fixed cell set"):
#   CELL SET RECIPE: the res-3 (1km) BNG cell at British National Grid
#   easting/northing (530000, 180000) (central London -- BNG expects EPSG:27700
#   eastings/northings, NOT WGS84 lon/lat), kRing'd to k=3 -> 49 cells. BNG cell
#   ids are OS grid REFERENCE STRINGS (pygx._bng.format of the internal Long id),
#   the SAME string ids the Scala BNG.format emits -- so the aggregator streams a
#   STRING cellid column. BNG is 27700-native (srid forced to 27700); the 2-arg
#   overload auto-derives the 27700 grid from the cell set.
#   resolution 3 == "1km" (BNG.resolutionMap); NEVER metres-as-Int.
_BNGRAGG_RES = 3
_BNGRAGG_EASTING = 530000.0
_BNGRAGG_NORTHING = 180000.0
_BNGRAGG_K = 3
_BNGRAGG_SRID = 27700
_BNGRAGG_N_CELLS = (2 * _BNGRAGG_K + 1) * (2 * _BNGRAGG_K + 1)  # 49 (cross-tier check)


def bng_rasterize_cells() -> List[str]:
    """The fixed deterministic BNG cell-id set the bench rasterizes (both tiers).

    The res-3 (1km) central-London cell, kRing'd to k=3. Returns OS grid REFERENCE
    STRINGS (``pygx._bng.format``), the SAME string ids the Scala BNG.format emits
    and the light ``rst_bng_rasterize_agg`` UDF parses back via ``pygx._bng.parse``.
    Deterministic + order-stable (sorted).
    """
    from databricks.labs.gbx.pygx import _bng

    center = _bng.point_to_cell_id(_BNGRAGG_EASTING, _BNGRAGG_NORTHING, _BNGRAGG_RES)
    return sorted(_bng.format(c) for c in _bng.k_ring(center, _BNGRAGG_K))


REGISTRY: Dict[str, FnSpec] = {
    "rst_width": FnSpec(
        "rst_width",
        "gbx_rst_width",
        "accessor",
        _BOTH,
        {},
        core=True,
        core_fn=lambda ds, a: accessors.width(ds),
        col_fn=lambda t, a: prx.rst_width(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Width.scala",),
    ),
    "rst_avg": FnSpec(
        "rst_avg",
        "gbx_rst_avg",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Avg.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.avg(ds),
        col_fn=lambda t, a: prx.rst_avg(t),
    ),
    "rst_slope": FnSpec(
        "rst_slope",
        "gbx_rst_slope",
        "terrain",
        _BOTH,
        {"unit": "degrees"},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_Slope.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.slope(ds, a["unit"]),
        col_fn=lambda t, a: prx.rst_slope(t, a["unit"]),
    ),
    "rst_ndvi": FnSpec(
        "rst_ndvi",
        "gbx_rst_ndvi",
        "band-math",
        _BOTH,
        {"red_band": 1, "nir_band": 2},
        min_bands=2,
        sources=_INDICES_LIGHT + (_HEAVY + "RST_NDVI.scala",),
        core=True,
        core_fn=lambda ds, a: indices.ndvi(ds, a["red_band"], a["nir_band"]),
        col_fn=lambda t, a: prx.rst_ndvi(t, a["red_band"], a["nir_band"]),
    ),
    "rst_transform": FnSpec(
        "rst_transform",
        "gbx_rst_transform",
        "warp",
        _BOTH,
        {"target_srid": 3857},
        sources=_WARP_LIGHT + (_HEAVY + "RST_Transform.scala",),
        core=True,
        core_fn=lambda ds, a: warp.reproject_to_srid(ds, a["target_srid"]),
        col_fn=lambda t, a: prx.rst_transform(t, a["target_srid"]),
    ),
    # --- accessors (scalar; accessors.py; empty args) ---
    "rst_height": FnSpec(
        "rst_height",
        "gbx_rst_height",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Height.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.height(ds),
        col_fn=lambda t, a: prx.rst_height(t),
    ),
    "rst_numbands": FnSpec(
        "rst_numbands",
        "gbx_rst_numbands",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_NumBands.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.numbands(ds),
        col_fn=lambda t, a: prx.rst_numbands(t),
    ),
    "rst_min": FnSpec(
        "rst_min",
        "gbx_rst_min",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Min.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.minimum(ds),
        col_fn=lambda t, a: prx.rst_min(t),
    ),
    "rst_max": FnSpec(
        "rst_max",
        "gbx_rst_max",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Max.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.maximum(ds),
        col_fn=lambda t, a: prx.rst_max(t),
    ),
    "rst_median": FnSpec(
        "rst_median",
        "gbx_rst_median",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Median.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.median(ds),
        col_fn=lambda t, a: prx.rst_median(t),
    ),
    "rst_pixelcount": FnSpec(
        "rst_pixelcount",
        "gbx_rst_pixelcount",
        "accessor",
        _BOTH,
        {},
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_PixelCount.scala",),
        core=True,
        core_fn=lambda ds, a: accessors.pixelcount(ds),
        col_fn=lambda t, a: prx.rst_pixelcount(t),
    ),
    # --- terrain (bytes; terrain.py) ---
    "rst_aspect": FnSpec(
        "rst_aspect",
        "gbx_rst_aspect",
        "terrain",
        _BOTH,
        {"trigonometric": False, "zero_for_flat": False},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_Aspect.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.aspect(
            ds, a["trigonometric"], a["zero_for_flat"]
        ),
        col_fn=lambda t, a: prx.rst_aspect(t, a["trigonometric"], a["zero_for_flat"]),
    ),
    "rst_hillshade": FnSpec(
        "rst_hillshade",
        "gbx_rst_hillshade",
        "terrain",
        _BOTH,
        {"azimuth": 315.0, "altitude": 45.0, "z_factor": 1.0},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_Hillshade.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.hillshade(
            ds, a["azimuth"], a["altitude"], a["z_factor"]
        ),
        col_fn=lambda t, a: prx.rst_hillshade(
            t, a["azimuth"], a["altitude"], a["z_factor"]
        ),
    ),
    "rst_tri": FnSpec(
        "rst_tri",
        "gbx_rst_tri",
        "terrain",
        _BOTH,
        {},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_TRI.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.tri(ds),
        col_fn=lambda t, a: prx.rst_tri(t),
    ),
    "rst_tpi": FnSpec(
        "rst_tpi",
        "gbx_rst_tpi",
        "terrain",
        _BOTH,
        {},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_TPI.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.tpi(ds),
        col_fn=lambda t, a: prx.rst_tpi(t),
    ),
    "rst_roughness": FnSpec(
        "rst_roughness",
        "gbx_rst_roughness",
        "terrain",
        _BOTH,
        {},
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_Roughness.scala", _DEM_HELPER),
        core=True,
        core_fn=lambda ds, a: terrain.roughness(ds),
        col_fn=lambda t, a: prx.rst_roughness(t),
    ),
    # --- band-math (bytes; indices.py) ---
    "rst_ndwi": FnSpec(
        "rst_ndwi",
        "gbx_rst_ndwi",
        "band-math",
        _BOTH,
        {"green_idx": 1, "nir_idx": 2},
        min_bands=2,
        sources=_INDICES_LIGHT + (_HEAVY + "spectral/RST_NDWI.scala",),
        core=True,
        core_fn=lambda ds, a: indices.ndwi(ds, a["green_idx"], a["nir_idx"]),
        col_fn=lambda t, a: prx.rst_ndwi(t, a["green_idx"], a["nir_idx"]),
    ),
    "rst_nbr": FnSpec(
        "rst_nbr",
        "gbx_rst_nbr",
        "band-math",
        _BOTH,
        {"nir_idx": 1, "swir_idx": 2},
        min_bands=2,
        sources=_INDICES_LIGHT + (_HEAVY + "spectral/RST_NBR.scala",),
        core=True,
        core_fn=lambda ds, a: indices.nbr(ds, a["nir_idx"], a["swir_idx"]),
        col_fn=lambda t, a: prx.rst_nbr(t, a["nir_idx"], a["swir_idx"]),
    ),
    # --- warp (bytes; warp.py) ---
    "rst_to_webmercator": FnSpec(
        "rst_to_webmercator",
        "gbx_rst_to_webmercator",
        "warp",
        _BOTH,
        {"resampling": "bilinear"},
        sources=_WARP_LIGHT + (_HEAVY + "web/RST_ToWebMercator.scala",),
        core=True,
        core_fn=lambda ds, a: warp.reproject_to_srid(
            ds, 3857, resampling=a["resampling"]
        ),
        col_fn=lambda t, a: prx.rst_to_webmercator(t, a["resampling"]),
    ),
    # --- scalar accessors (no args; accessors.py) -------------------------------
    # All core=False. Most produce a cross-engine-identical scalar/array
    # fingerprint, so they run both modes. Two exceptions run pure-core-only.
    # WHY pure-core-only (no spark-path): both are fingerprint-suppressed
    # (fingerprint=False), so a spark-path cell would be timing-only and never
    # cross-engine-compared (spark-path is a noop-write timing pass that emits no
    # fingerprint). Pure-core captures their timing without spending the
    # spark-path/cluster budget on an uncomparable `na` cell; the heavy column()
    # dispatch keeps an exhaustive form for them only so its match is total.
    #   - rst_memsize: heavy returns the on-disk file size while the lightweight
    #     side opens a vsimem MemoryFile (no file size), so the values cannot be
    #     made identical; its fingerprint is suppressed in the scorecard.
    #   - rst_type: a per-band string array; BenchFingerprint has no
    #     array-of-strings constructor to match on the heavy side.
    "rst_srid": FnSpec(
        "rst_srid",
        "gbx_rst_srid",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.srid(ds),
        col_fn=lambda t, a: prx.rst_srid(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_SRID.scala",),
        core=False,
    ),
    "rst_pixelwidth": FnSpec(
        "rst_pixelwidth",
        "gbx_rst_pixelwidth",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.pixelwidth(ds),
        col_fn=lambda t, a: prx.rst_pixelwidth(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_PixelWidth.scala",),
        core=False,
    ),
    "rst_pixelheight": FnSpec(
        "rst_pixelheight",
        "gbx_rst_pixelheight",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.pixelheight(ds),
        col_fn=lambda t, a: prx.rst_pixelheight(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_PixelHeight.scala",),
        core=False,
    ),
    "rst_upperleftx": FnSpec(
        "rst_upperleftx",
        "gbx_rst_upperleftx",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.upperleftx(ds),
        col_fn=lambda t, a: prx.rst_upperleftx(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_UpperLeftX.scala",),
        core=False,
    ),
    "rst_upperlefty": FnSpec(
        "rst_upperlefty",
        "gbx_rst_upperlefty",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.upperlefty(ds),
        col_fn=lambda t, a: prx.rst_upperlefty(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_UpperLeftY.scala",),
        core=False,
    ),
    "rst_scalex": FnSpec(
        "rst_scalex",
        "gbx_rst_scalex",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.scalex(ds),
        col_fn=lambda t, a: prx.rst_scalex(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_ScaleX.scala",),
        core=False,
    ),
    "rst_scaley": FnSpec(
        "rst_scaley",
        "gbx_rst_scaley",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.scaley(ds),
        col_fn=lambda t, a: prx.rst_scaley(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_ScaleY.scala",),
        core=False,
    ),
    "rst_skewx": FnSpec(
        "rst_skewx",
        "gbx_rst_skewx",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.skewx(ds),
        col_fn=lambda t, a: prx.rst_skewx(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_SkewX.scala",),
        core=False,
    ),
    "rst_skewy": FnSpec(
        "rst_skewy",
        "gbx_rst_skewy",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.skewy(ds),
        col_fn=lambda t, a: prx.rst_skewy(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_SkewY.scala",),
        core=False,
    ),
    "rst_rotation": FnSpec(
        "rst_rotation",
        "gbx_rst_rotation",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.rotation(ds),
        col_fn=lambda t, a: prx.rst_rotation(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Rotation.scala",),
        core=False,
    ),
    "rst_isempty": FnSpec(
        "rst_isempty",
        "gbx_rst_isempty",
        "accessor",
        _BOTH,
        {},
        # Coerce the bool to 1.0/0.0 so the scalar fingerprint matches the heavy
        # side, which serializes RST_IsEmpty as ofScalar(1.0/0.0).
        core_fn=lambda ds, a: 1.0 if accessors.isempty(ds) else 0.0,
        col_fn=lambda t, a: prx.rst_isempty(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "RST_IsEmpty.scala",),
        core=False,
    ),
    "rst_getnodata": FnSpec(
        "rst_getnodata",
        "gbx_rst_getnodata",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.getnodata(ds),
        col_fn=lambda t, a: prx.rst_getnodata(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_GetNoData.scala",),
        core=False,
    ),
    "rst_format": FnSpec(
        "rst_format",
        "gbx_rst_format",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda ds, a: accessors.format(ds),
        col_fn=lambda t, a: prx.rst_format(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Format.scala",),
        core=False,
    ),
    "rst_type": FnSpec(
        "rst_type",
        "gbx_rst_type",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.type(ds),
        col_fn=lambda t, a: prx.rst_type(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Type.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_memsize": FnSpec(
        "rst_memsize",
        "gbx_rst_memsize",
        "accessor",
        ("pure-core",),
        {},
        # No accessors.memsize: use the in-memory raster buffer length from the
        # open dataset (deterministic; timing-only, fingerprint suppressed).
        core_fn=lambda ds, a: int(ds.read().nbytes),
        col_fn=lambda t, a: prx.rst_memsize(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_MemSize.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- coordinate / index accessors (Task 3) ----------------------------------
    # raster->world is the forward geotransform (pure affine): rasterio.xy and
    # GDAL.toWorldCoord agree for any pixel index in any CRS, so these compare in
    # both modes. The pixel index {x:64, y:64} is inside every corpus tile (256px
    # and 512px). The non-suffixed `...coord` returns the pair [x, y] (a list, not
    # a dict) so the fingerprint is a scalar_list matching the heavy ofArray(x, y).
    "rst_rastertoworldcoordx": FnSpec(
        "rst_rastertoworldcoordx",
        "gbx_rst_rastertoworldcoordx",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        core_fn=lambda ds, a: coords.raster_to_world_x(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_rastertoworldcoordx(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_RasterToWorldCoordX.scala",),
        core=False,
    ),
    "rst_rastertoworldcoordy": FnSpec(
        "rst_rastertoworldcoordy",
        "gbx_rst_rastertoworldcoordy",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        core_fn=lambda ds, a: coords.raster_to_world_y(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_rastertoworldcoordy(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_RasterToWorldCoordY.scala",),
        core=False,
    ),
    "rst_rastertoworldcoord": FnSpec(
        "rst_rastertoworldcoord",
        "gbx_rst_rastertoworldcoord",
        "accessor",
        _BOTH,
        {"x": 64, "y": 64},
        # Return the pair as a LIST [x, y] (not the {x, y} dict the binding emits)
        # so fingerprint_output yields a scalar_list, matching the heavy side's
        # ofArray(Array(pair._1, pair._2)). Order: x (easting) then y (northing).
        core_fn=lambda ds, a: [
            coords.raster_to_world_x(ds, a["x"], a["y"]),
            coords.raster_to_world_y(ds, a["x"], a["y"]),
        ],
        col_fn=lambda t, a: prx.rst_rastertoworldcoord(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_RasterToWorldCoord.scala",),
        core=False,
    ),
    # world->raster is the INVERSE geotransform. It is exact per-CRS, but a single
    # fixed world literal is in-extent for only one of the corpus CRSs; for the
    # others the index is huge/negative (the EPSG:4326 0.0001-deg grid overflows
    # int32, where rasterio.index floor-casts and GDAL .toInt truncate differently).
    # So these run pure-core-only and their fingerprints are suppressed in the
    # scorecard, exactly like rst_memsize / rst_type. The world point (-73.985,
    # 40.745) is in-extent for the EPSG:4326 (NYC) corpus tiles.
    "rst_worldtorastercoordx": FnSpec(
        "rst_worldtorastercoordx",
        "gbx_rst_worldtorastercoordx",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        core_fn=lambda ds, a: coords.world_to_raster_x(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_worldtorastercoordx(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_WorldToRasterCoordX.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_worldtorastercoordy": FnSpec(
        "rst_worldtorastercoordy",
        "gbx_rst_worldtorastercoordy",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        core_fn=lambda ds, a: coords.world_to_raster_y(ds, a["x"], a["y"]),
        col_fn=lambda t, a: prx.rst_worldtorastercoordy(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_WorldToRasterCoordY.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_worldtorastercoord": FnSpec(
        "rst_worldtorastercoord",
        "gbx_rst_worldtorastercoord",
        "accessor",
        ("pure-core",),
        {"x": -73.985, "y": 40.745},
        # Pair as a LIST [col, row] to mirror the heavy ofArray(pair._1, pair._2).
        core_fn=lambda ds, a: [
            coords.world_to_raster_x(ds, a["x"], a["y"]),
            coords.world_to_raster_y(ds, a["x"], a["y"]),
        ],
        col_fn=lambda t, a: prx.rst_worldtorastercoord(t, a["x"], a["y"]),
        sources=_COORDS_LIGHT + (_HEAVY + "RST_WorldToRasterCoord.scala",),
        core=False,
        fingerprint=False,
    ),
    # rst_tilexyz renders a warped+encoded slippy-map tile. The output bytes depend
    # on the warp/encode stack (GDAL vs rasterio/PIL) and the source CRS, so it is
    # pure-core-only with its fingerprint suppressed in the scorecard. (z, x, y)
    # cover NYC at zoom 12 so the EPSG:4326 corpus tiles intersect the tile bbox.
    "rst_tilexyz": FnSpec(
        "rst_tilexyz",
        "gbx_rst_tilexyz",
        "accessor",
        ("pure-core",),
        {"z": 12, "x": 1205, "y": 1539},
        core_fn=lambda ds, a: xyz.render_tile(
            ds, a["z"], a["x"], a["y"], "PNG", 256, "bilinear"
        ),
        col_fn=lambda t, a: prx.rst_tilexyz(t, a["z"], a["x"], a["y"]),
        sources=_XYZ_LIGHT + (_HEAVY + "web/RST_TileXYZ.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- map / struct accessors (Task 4): timing-only ---------------------------
    # These return maps (metadata, bandmetadata, histogram), structs/dicts
    # (georeference), CRS/encoding-dependent bytes (boundingbox WKB) or
    # gdalinfo-style JSON (summary). None can be made byte- or value-identical
    # cross-engine, so they are TIMED but not compared: fingerprint=False emits an
    # empty fingerprint on both engines and the comparator marks the cell `na`.
    # WHY pure-core-only (no spark-path): a spark-path cell is a noop-write timing
    # pass that emits no fingerprint, so for a fingerprint-suppressed function it
    # would be an uncomparable `na` cell with no parity value. Scoping these to
    # pure-core captures the per-op timing without spending the spark-path/cluster
    # budget on a cell that can never be compared. (Each has a working tile-input
    # col_fn and a heavy column() form, so the restriction is a deliberate
    # coverage choice, not a missing adapter.)
    "rst_metadata": FnSpec(
        "rst_metadata",
        "gbx_rst_metadata",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.metadata(ds),
        col_fn=lambda t, a: prx.rst_metadata(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_MetaData.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_bandmetadata": FnSpec(
        "rst_bandmetadata",
        "gbx_rst_bandmetadata",
        "accessor",
        ("pure-core",),
        {},
        # bandmetadata needs a 1-based band index; band 1 exists in every tile.
        core_fn=lambda ds, a: accessors.bandmetadata(ds, 1),
        col_fn=lambda t, a: prx.rst_bandmetadata(t, 1),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_BandMetaData.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_georeference": FnSpec(
        "rst_georeference",
        "gbx_rst_georeference",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.georeference(ds),
        col_fn=lambda t, a: prx.rst_georeference(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_GeoReference.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_boundingbox": FnSpec(
        "rst_boundingbox",
        "gbx_rst_boundingbox",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.boundingbox(ds),
        col_fn=lambda t, a: prx.rst_boundingbox(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_BoundingBox.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_summary": FnSpec(
        "rst_summary",
        "gbx_rst_summary",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.summary(ds),
        col_fn=lambda t, a: prx.rst_summary(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Summary.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_histogram": FnSpec(
        "rst_histogram",
        "gbx_rst_histogram",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.histogram(ds),
        col_fn=lambda t, a: prx.rst_histogram(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "pixel/RST_Histogram.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- Task 5: tile-out transforms with scalar / fixed args (13) -----------
    # Each returns a raster tile, so the runner fingerprints the output as a
    # raster (same path as terrain). All run both modes and are compared, EXCEPT
    # rst_resample_to_res (see below). Fixed scalar args are identical across the
    # core_fn, col_fn, and the Scala BenchDispatch case.
    # --- edit (edit.py) ---
    "rst_band": FnSpec(
        "rst_band",
        "gbx_rst_band",
        "edit",
        _BOTH,
        {"band_index": 2},
        min_bands=2,
        core_fn=lambda ds, a: edit.band(ds, a["band_index"]),
        col_fn=lambda t, a: prx.rst_band(t, a["band_index"]),
        sources=_EDIT_LIGHT + (_HEAVY + "pixel/RST_Band.scala",),
        core=False,
    ),
    # rst_threshold (timing-only): the two tiers implement DIFFERENT, documented
    # output contracts (see the lightweight-tier note in raster-functions.mdx).
    # The heavyweight binarises via gdal_calc — a SINGLE-band 0/1 mask over band 1.
    # The lightweight KEEPS each passing pixel's original value and sets failing
    # pixels to NoData, preserving every input band. On a multi-band tile that
    # surfaces as a band-count divergence (heavy 1 vs light N) on top of the
    # value-semantics difference. This is a by-design contract difference, not a
    # bug, so the cell is TIMED but never fingerprint-compared.
    "rst_threshold": FnSpec(
        "rst_threshold",
        "gbx_rst_threshold",
        "edit",
        _BOTH,
        {"op": ">", "value": 0.5},
        core_fn=lambda ds, a: edit.threshold(ds, a["op"], a["value"]),
        col_fn=lambda t, a: prx.rst_threshold(t, a["op"], a["value"]),
        sources=_EDIT_LIGHT + (_HEAVY + "pixel/RST_Threshold.scala",),
        core=False,
        fingerprint=False,
    ),
    "rst_initnodata": FnSpec(
        "rst_initnodata",
        "gbx_rst_initnodata",
        "edit",
        _BOTH,
        {},
        core_fn=lambda ds, a: edit.init_nodata(ds),
        col_fn=lambda t, a: prx.rst_initnodata(t),
        sources=_EDIT_LIGHT + (_HEAVY + "RST_InitNoData.scala",),
        core=False,
    ),
    "rst_setsrid": FnSpec(
        "rst_setsrid",
        "gbx_rst_setsrid",
        "edit",
        _BOTH,
        {"srid": 4326},
        core_fn=lambda ds, a: edit.set_srid(ds, a["srid"]),
        col_fn=lambda t, a: prx.rst_setsrid(t, a["srid"]),
        sources=_EDIT_LIGHT + (_HEAVY + "pixel/RST_SetSrid.scala",),
        core=False,
    ),
    "rst_updatetype": FnSpec(
        "rst_updatetype",
        "gbx_rst_updatetype",
        "edit",
        _BOTH,
        {"new_type": "Float64"},
        core_fn=lambda ds, a: edit.update_type(ds, a["new_type"]),
        # F.lit the STRING arg: prx _col passes bare str through as a column NAME
        # (auto-lit covers only numeric scalars), so an unwrapped "Float64" resolves
        # as a column in spark-path. Numeric args may stay raw.
        col_fn=lambda t, a: prx.rst_updatetype(t, F.lit(a["new_type"])),
        sources=_EDIT_LIGHT + (_HEAVY + "RST_UpdateType.scala",),
        core=False,
    ),
    # --- features (features.py) ---
    "rst_fillnodata": FnSpec(
        "rst_fillnodata",
        "gbx_rst_fillnodata",
        "features",
        _BOTH,
        {"max_search_dist": 10.0, "smoothing_iter": 0},
        core_fn=lambda ds, a: features.fill_nodata(
            ds, a["max_search_dist"], a["smoothing_iter"]
        ),
        col_fn=lambda t, a: prx.rst_fillnodata(
            t, a["max_search_dist"], a["smoothing_iter"]
        ),
        sources=_FEATURES_LIGHT + (_HEAVY + "pixel/RST_FillNodata.scala",),
        core=False,
    ),
    # --- focal (focal.py) ---
    # operation="median" is valid on BOTH engines: the heavy KernelFilter accepts
    # {avg, min, max, median, mode} (NOT "mean" -> "Invalid operation"), and the
    # light focal.filt accepts {min, max, median, mean}. "median" is the common
    # value, so the bench args carry it for both. (Timing-only, not compared.)
    "rst_filter": FnSpec(
        "rst_filter",
        "gbx_rst_filter",
        "focal",
        _BOTH,
        {"kernel_size": 3, "operation": "median"},
        core_fn=lambda ds, a: focal.filt(ds, a["kernel_size"], a["operation"]),
        # F.lit the STRING operation arg (kernel_size is numeric -> auto-lit).
        col_fn=lambda t, a: prx.rst_filter(t, a["kernel_size"], F.lit(a["operation"])),
        sources=_FOCAL_LIGHT
        + (_HEAVY + "RST_Filter.scala", _OPS + "KernelFilter.scala", _GDAL_BLOCK),
        core=False,
    ),
    "rst_convolve": FnSpec(
        "rst_convolve",
        "gbx_rst_convolve",
        "focal",
        _BOTH,
        # kernel_size documents the fixed 3x3 mean kernel (_CONVOLVE_KERNEL); the
        # actual coefficients are hardcoded identically on both engines.
        {"kernel_size": 3},
        core_fn=lambda ds, a: focal.convolve(ds, _CONVOLVE_KERNEL),
        col_fn=lambda t, a: prx.rst_convolve(
            t, F.array(*[F.array(*[F.lit(c) for c in row]) for row in _CONVOLVE_KERNEL])
        ),
        sources=_FOCAL_LIGHT
        + (_HEAVY + "RST_Convolve.scala", _OPS + "Convolve.scala", _GDAL_BLOCK),
        core=False,
    ),
    # --- format (ops.py / analysis.py) ---
    "rst_asformat": FnSpec(
        "rst_asformat",
        "gbx_rst_asformat",
        "format",
        _BOTH,
        {"new_format": "GTiff"},
        core_fn=lambda ds, a: ops.as_format(ds, a["new_format"]),
        col_fn=lambda t, a: prx.rst_asformat(t, a["new_format"]),
        sources=_OPS_LIGHT + (_HEAVY + "RST_AsFormat.scala",),
        core=False,
    ),
    "rst_cog_convert": FnSpec(
        "rst_cog_convert",
        "gbx_rst_cog_convert",
        "format",
        _BOTH,
        {
            "compression": "DEFLATE",
            "blocksize": 512,
            "overview_resampling": "AVERAGE",
        },
        core_fn=lambda ds, a: analysis_core.cog_convert(
            ds, a["compression"], a["blocksize"], a["overview_resampling"]
        ),
        col_fn=lambda t, a: prx.rst_cog_convert(
            t, a["compression"], a["blocksize"], a["overview_resampling"]
        ),
        sources=_ANALYSIS_LIGHT + (_HEAVY + "analysis/RST_CogConvert.scala",),
        core=False,
    ),
    # --- resample (resample.py) ---
    "rst_resample": FnSpec(
        "rst_resample",
        "gbx_rst_resample",
        "resample",
        _BOTH,
        {"factor": 2.0, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_by_factor(
            ds, a["factor"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample(t, a["factor"], a["algorithm"]),
        sources=_RESAMPLE_LIGHT + (_HEAVY + "resample/RST_Resample.scala",),
        core=False,
    ),
    "rst_resample_to_size": FnSpec(
        "rst_resample_to_size",
        "gbx_rst_resample_to_size",
        "resample",
        _BOTH,
        {"width_px": 128, "height_px": 128, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_to_size(
            ds, a["width_px"], a["height_px"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample_to_size(
            t, a["width_px"], a["height_px"], a["algorithm"]
        ),
        sources=_RESAMPLE_LIGHT + (_HEAVY + "resample/RST_ResampleToSize.scala",),
        core=False,
    ),
    # rst_resample_to_res takes an absolute ground resolution in CRS units. A
    # single fixed res cannot be sane across the multi-CRS corpus (a 0.0001-deg
    # grid and a 10-m grid share no common absolute resolution; one CRS would
    # produce a degenerate 1x1 raster while another blows up), exactly like the
    # world->raster coord functions. So it runs pure-core-only and its
    # fingerprint is suppressed in the scorecard.
    "rst_resample_to_res": FnSpec(
        "rst_resample_to_res",
        "gbx_rst_resample_to_res",
        "resample",
        ("pure-core",),
        {"x_res": 5.0, "y_res": 5.0, "algorithm": "bilinear"},
        core_fn=lambda ds, a: resample.resample_to_res(
            ds, a["x_res"], a["y_res"], a["algorithm"]
        ),
        col_fn=lambda t, a: prx.rst_resample_to_res(
            t, a["x_res"], a["y_res"], a["algorithm"]
        ),
        sources=_RESAMPLE_LIGHT + (_HEAVY + "resample/RST_ResampleToRes.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- Task 6: tile-out transforms with geometry / expression / band-map /
    # function args (10) --------------------------------------------------------
    # SIX are full raster comparisons (both modes): the arguments are CRS- and
    # band-count-independent and the two engines run the *same* algorithm.
    #   evi, savi      -- band indices + coefficients (same numexpr formula)
    #   index          -- formula name + band-map (hardcoded identically; "ndvi")
    #   mapalgebra     -- single-input expression "A*2" (same numexpr/gdal_calc)
    #   derivedband    -- same Python VRT pixel-function body, exec'd in-process
    #   proximity      -- value-set + distunits (no geometry; CRS-independent)
    # FOUR are timing-only (pure-core-only, fingerprint suppressed). Reason per
    # function in the constant's comment above:
    #   clip, sample, viewshed -- need an in-extent geometry; no single literal
    #                             is in-extent across the multi-CRS corpus
    #   viewshed (also)        -- documented xrspatial-vs-GDAL parity divergence
    #   color_relief           -- needs a color-table file; GDAL DEMProcessing
    #                             interpolation vs pyrx np.interp diverge
    # --- band-index (indices.py): full comparison ---
    "rst_evi": FnSpec(
        "rst_evi",
        "gbx_rst_evi",
        "band-math",
        _BOTH,
        # 2-band corpus: reuse band 1 as the blue band. Coefficients are the GDAL
        # / binding defaults, hardcoded identically on both engines.
        {
            "red_idx": 1,
            "nir_idx": 2,
            "blue_idx": 1,
            "l": 1.0,
            "c1": 6.0,
            "c2": 7.5,
            "g": 2.5,
        },
        min_bands=2,
        core_fn=lambda ds, a: indices.evi(
            ds,
            a["red_idx"],
            a["nir_idx"],
            a["blue_idx"],
            l=a["l"],
            c1=a["c1"],
            c2=a["c2"],
            g=a["g"],
        ),
        col_fn=lambda t, a: prx.rst_evi(
            t,
            a["red_idx"],
            a["nir_idx"],
            a["blue_idx"],
            a["l"],
            a["c1"],
            a["c2"],
            a["g"],
        ),
        sources=_INDICES_LIGHT + (_HEAVY + "spectral/RST_EVI.scala",),
        core=False,
    ),
    "rst_savi": FnSpec(
        "rst_savi",
        "gbx_rst_savi",
        "band-math",
        _BOTH,
        {"red_idx": 1, "nir_idx": 2, "l": 0.5},
        min_bands=2,
        core_fn=lambda ds, a: indices.savi(ds, a["red_idx"], a["nir_idx"], l=a["l"]),
        col_fn=lambda t, a: prx.rst_savi(t, a["red_idx"], a["nir_idx"], a["l"]),
        sources=_INDICES_LIGHT + (_HEAVY + "spectral/RST_SAVI.scala",),
        core=False,
    ),
    # --- generic named-index dispatcher (indices.py): full comparison ---
    # The band-map cannot ride the stringly args map, so index_name + band_map are
    # hardcoded identically here and in the Scala BenchDispatch case (the args
    # dict carries them only for documentation / the language-neutral dump).
    "rst_index": FnSpec(
        "rst_index",
        "gbx_rst_index",
        "band-math",
        _BOTH,
        {"index_name": _INDEX_NAME, "band_map": _INDEX_BAND_MAP},
        min_bands=2,
        core_fn=lambda ds, a: indices.index(ds, _INDEX_NAME, _INDEX_BAND_MAP),
        col_fn=lambda t, a: prx.rst_index(
            t,
            _INDEX_NAME,
            F.create_map(
                *[x for k, v in _INDEX_BAND_MAP.items() for x in (F.lit(k), F.lit(v))]
            ),
        ),
        sources=_INDICES_LIGHT + (_HEAVY + "spectral/RST_Index.scala",),
        core=False,
    ),
    # --- map algebra (mapalgebra.py): full comparison ---
    # Single-input "A*2": CRS- and band-count-independent. core consumes bytes
    # (round-trip the open ds); the column form wraps the single tile in an array.
    "rst_mapalgebra": FnSpec(
        "rst_mapalgebra",
        "gbx_rst_mapalgebra",
        "format",
        _BOTH,
        {"expr": _MAPALGEBRA_EXPR},
        core_fn=lambda ds, a: mapalgebra.mapalgebra(
            [_ds_to_gtiff_bytes(ds)], a["expr"]
        ),
        col_fn=lambda t, a: prx.rst_mapalgebra(F.array(t), a["expr"]),
        sources=_MAPALGEBRA_LIGHT + (_HEAVY + "RST_MapAlgebra.scala",),
        core=False,
    ),
    # --- derived band (derivedband.py): full comparison ---
    # Both engines exec the SAME Python VRT pixel-function source in-process and
    # fill out_ar identically, so this is a genuine algorithmic match.
    "rst_derivedband": FnSpec(
        "rst_derivedband",
        "gbx_rst_derivedband",
        "format",
        _BOTH,
        {"func_name": _DERIVEDBAND_FUNC_NAME},
        core_fn=lambda ds, a: derivedband.derivedband(
            ds, _DERIVEDBAND_PYFUNC, _DERIVEDBAND_FUNC_NAME
        ),
        col_fn=lambda t, a: prx.rst_derivedband(
            t, _DERIVEDBAND_PYFUNC, _DERIVEDBAND_FUNC_NAME
        ),
        sources=_DERIVEDBAND_LIGHT + (_HEAVY + "RST_DerivedBand.scala", _PIXEL_COMBINE),
        core=False,
    ),
    # --- proximity (analysis.py): full comparison ---
    # No geometry; value-set + distunits are CRS-independent. GDAL ComputeProximity
    # vs scipy distance_transform_edt may diverge numerically — that divergence is
    # exactly what the scorecard is meant to surface (same policy as terrain
    # NoData-edge divergences), so it stays a full comparison.
    "rst_proximity": FnSpec(
        "rst_proximity",
        "gbx_rst_proximity",
        "analysis",
        _BOTH,
        {"target_values": "1", "distunits": "GEO"},
        core_fn=lambda ds, a: analysis_core.proximity(
            ds, a["target_values"], a["distunits"], None
        ),
        col_fn=lambda t, a: prx.rst_proximity(t, a["target_values"], a["distunits"]),
        sources=_ANALYSIS_LIGHT + (_HEAVY + "analysis/RST_Proximity.scala",),
        core=False,
    ),
    # --- clip (edit.py): timing-only ---
    # No single literal geometry is in-extent across the multi-CRS corpus; the
    # global-cover polygon (_CLIP_GEOM_WKB) lets the timing call run on every tile
    # without erroring, but the clipped output is not compared.
    "rst_clip": FnSpec(
        "rst_clip",
        "gbx_rst_clip",
        "edit",
        ("pure-core",),
        {"cutline_all_touched": False},
        # Derive the cutline per-tile from ds.bounds (shrunk 50%) so it is
        # in-extent on every CRS; the static _CLIP_GEOM_WKB arg is ignored here.
        core_fn=lambda ds, a: edit.clip_to_geom(
            ds, _shrunk_bounds_box_wkb(ds), a["cutline_all_touched"]
        ),
        col_fn=lambda t, a: prx.rst_clip(
            t, F.lit(_CLIP_GEOM_WKB), F.lit(a["cutline_all_touched"])
        ),
        sources=_EDIT_LIGHT + (_HEAVY + "RST_Clip.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- color relief (terrain.py): timing-only ---
    # Needs a color-table file path (synthesized on demand) and the heavy GDAL
    # DEMProcessing interpolation diverges from pyrx np.interp, so not compared.
    "rst_color_relief": FnSpec(
        "rst_color_relief",
        "gbx_rst_color_relief",
        "terrain",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: terrain.color_relief(ds, _color_table_path()),
        col_fn=lambda t, a: prx.rst_color_relief(t, _color_table_path()),
        sources=_TERRAIN_LIGHT + (_HEAVY + "dem/RST_ColorRelief.scala", _DEM_HELPER),
        core=False,
        fingerprint=False,
    ),
    # --- viewshed (analysis.py): timing-only ---
    # Needs an in-extent observer point (none works across the multi-CRS corpus)
    # AND the binding documents an xrspatial-vs-GDAL parity divergence in the
    # binary visibility mask. Timing-only on both counts.
    "rst_viewshed": FnSpec(
        "rst_viewshed",
        "gbx_rst_viewshed",
        "analysis",
        ("pure-core",),
        {"observer_height": 2.0, "target_height": 1.6},
        # Observer = tile center (in the tile's own CRS) so it is in-extent on
        # every CRS; a fixed (0,0) falls outside the UTM/3857 tile ranges.
        core_fn=lambda ds, a: analysis_core.viewshed(
            ds, *_tile_center_xy(ds), a["observer_height"], a["target_height"], None
        ),
        col_fn=lambda t, a: prx.rst_viewshed(
            t, F.lit(_VIEWSHED_OBSERVER_WKB), a["observer_height"], a["target_height"]
        ),
        sources=_ANALYSIS_LIGHT + (_HEAVY + "analysis/RST_Viewshed.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- sample (ops.py): timing-only ---
    # Needs an in-extent POINT; no single world point is in-extent across the
    # multi-CRS corpus. Out-of-extent points return null, fine for timing.
    "rst_sample": FnSpec(
        "rst_sample",
        "gbx_rst_sample",
        "format",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: ops.sample(ds, _SAMPLE_POINT_WKB),
        col_fn=lambda t, a: prx.rst_sample(t, F.lit(_SAMPLE_POINT_WKB)),
        sources=_OPS_LIGHT + (_HEAVY + "pixel/RST_Sample.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- bucket C, group C1: readers + buildoverviews -------------------------
    # The reader/constructor fns take CONTENT or a PATH, not an open dataset, so
    # they use the `input_kind` adapter: the runner hands core_fn the raw bytes
    # ("bytes") or the corpus file path ("path") instead of an opened ds.
    #
    # rst_tryopen (bytes): "do the bytes open?" -> bool, coerced to 1.0/0.0 to
    # match the heavy scalar fingerprint (same convention as rst_isempty). Both
    # engines just open + release, so the scalar (1.0 on a valid corpus tile)
    # compares exactly.
    "rst_tryopen": FnSpec(
        "rst_tryopen",
        "gbx_rst_tryopen",
        "accessor",
        _BOTH,
        {},
        core_fn=lambda b, a: 1.0 if ops.try_open(b) else 0.0,
        col_fn=lambda t, a: prx.rst_tryopen(t),
        sources=_OPS_LIGHT + (_HEAVY + "RST_TryOpen.scala",),
        core=False,
        input_kind="bytes",
    ),
    # rst_fromcontent (bytes): build a tile from raster bytes + driver. The
    # comparable output is the decoded raster grid, so core_fn returns the bytes
    # themselves (already GTiff in the corpus) -> raster fingerprint. The heavy
    # side opens the same bytes (vsimem) and fingerprints the dataset, so the two
    # grids match. The spark-path column reads the tile's raster (binary content)
    # column, mirroring gbx_rst_fromcontent(content, "GTiff").
    "rst_fromcontent": FnSpec(
        "rst_fromcontent",
        "gbx_rst_fromcontent",
        "format",
        _BOTH,
        {"driver": "GTiff"},
        core_fn=lambda b, a: bytes(b),
        # F.lit the STRING driver arg (else prx _col reads "GTiff" as a column name).
        col_fn=lambda t, a: prx.rst_fromcontent(t["raster"], F.lit(a["driver"])),
        sources=(_PYRX_SERDE, _HEAVY + "constructor/RST_FromContent.scala"),
        core=False,
        input_kind="bytes",
    ),
    # rst_fromfile (path): read the raster at a filesystem path into bytes. core
    # opens the path and returns its bytes -> raster fingerprint; the heavy side
    # opens the same path. pure-core only: the spark-path tile DataFrame carries
    # no file-path column (tiles are materialized from bytes), so there is no
    # path column to feed gbx_rst_fromfile in the column form.
    "rst_fromfile": FnSpec(
        "rst_fromfile",
        "gbx_rst_fromfile",
        "format",
        ("pure-core",),
        {"driver": "GTiff"},
        core_fn=lambda p, a: Path(p).read_bytes(),
        # pure-core only: the spark-path runner filters this out by modes, so the
        # column form is unused at runtime and kept only for a callable col_fn.
        col_fn=lambda t, a: prx.rst_fromfile(F.lit("__unused__"), a["driver"]),
        # rst_fromfile is LIGHTWEIGHT-ONLY (issue #34): the heavy JVM tier cannot
        # read a FUSE/UC path from an executor, so there is NO heavy RST_FromFile
        # expression (the old constructor/RST_FromFile.scala was removed in
        # e9fc03e9). Its behavior is the light path serde round-trip; functions.scala
        # documents the light-only registration decision.
        sources=(
            _PYRX_SERDE,
            "src/main/scala/com/databricks/labs/gbx/rasterx/functions.scala",
        ),
        core=False,
        input_kind="path",
    ),
    # rst_buildoverviews (tile): internal pyramid overviews leave the base band
    # unchanged, so the raster fingerprint (computed over the full-resolution
    # band) is identical pre/post -> a full comparison. Args are CRS-independent.
    "rst_buildoverviews": FnSpec(
        "rst_buildoverviews",
        "gbx_rst_buildoverviews",
        "format",
        _BOTH,
        {"levels": [2, 4], "resampling": "average"},
        core_fn=lambda ds, a: ops.build_overviews(ds, a["levels"], a["resampling"]),
        col_fn=lambda t, a: prx.rst_buildoverviews(
            t, F.array(*[F.lit(int(x)) for x in a["levels"]]), a["resampling"]
        ),
        sources=_OPS_LIGHT + (_HEAVY + "pixel/RST_BuildOverviews.scala",),
        core=False,
        input_kind="tile",
    ),
    # --- bucket C, group C2: subdataset fns (timing-only) ---------------------
    # A plain GTiff corpus tile has no subdatasets, so neither fn produces a
    # comparable output: rst_subdatasets returns an empty map; rst_getsubdataset
    # finds no match. Both are TIMED (real work: metadata scan / open attempt)
    # but fingerprint-suppressed (fingerprint=False -> empty on both engines).
    # WHY pure-core-only (no spark-path): same as the map/struct accessors above —
    # a fingerprint-suppressed function's spark-path cell would be an uncomparable
    # timing-only `na`, so it is scoped to pure-core to capture timing without
    # spending the spark-path budget on a cell that can never be compared.
    "rst_subdatasets": FnSpec(
        "rst_subdatasets",
        "gbx_rst_subdatasets",
        "accessor",
        ("pure-core",),
        {},
        core_fn=lambda ds, a: accessors.subdatasets(ds),
        col_fn=lambda t, a: prx.rst_subdatasets(t),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_Subdatasets.scala",),
        core=False,
        fingerprint=False,
    ),
    # rst_getsubdataset: on a plain GTiff there is no subdataset, so the pyrx
    # core RAISES ValueError ("no subdataset named '0'"). That would surface as a
    # status=error row from the runner's per-fn try/catch. To time it WITHOUT a
    # noisy error, the bench core_fn swallows the expected "no subdataset" raise
    # and returns the empty map (timing-only, fingerprint suppressed). The heavy
    # RST_GetSubdataset.execute likewise returns null on GTiff; the dispatch
    # guards the null and emits an empty fingerprint.
    "rst_getsubdataset": FnSpec(
        "rst_getsubdataset",
        "gbx_rst_getsubdataset",
        "accessor",
        ("pure-core",),
        {"name": "0"},
        core_fn=lambda ds, a: _getsubdataset_timing(ds, a["name"]),
        col_fn=lambda t, a: prx.rst_getsubdataset(t, a["name"]),
        sources=_ACCESSORS_LIGHT + (_HEAVY + "accessors/RST_GetSubdataset.scala",),
        core=False,
        fingerprint=False,
    ),
    # --- bucket C, group C3: multi-tile-input functions (3) -------------------
    # rst_frombands / rst_combineavg / rst_merge each consume an ARRAY of tiles.
    # The corpus row gives ONE tile, so the runner SYNTHESIZES the multi-tile
    # input from it (bench.synth) and writes it to disk ONCE -> both engines read
    # byte-identical files (input_kind == "tile_array"). The pyrx runner opens the
    # synthesized files into a ds list and hands it to core_fn; the col_fn receives
    # the ARRAY<tile> column the runner built from the same synthesized tiles.
    #
    # All produce a raster tile -> raster fingerprint, full comparison, both modes:
    #   frombands  -> stack N single-band tiles into one N-band tile (band order =
    #                 array order); deterministic stack, byte-comparable grid.
    #   combineavg -> NoData-aware per-pixel mean of 2 ALIGNED copies; on aligned
    #                 inputs the mean is deterministic and the grid is comparable.
    #   merge      -> mosaic 2 OFFSET-origin copies into their union extent; the
    #                 union grid + placement is deterministic across engines.
    "rst_frombands": FnSpec(
        "rst_frombands",
        "gbx_rst_frombands",
        "format",
        _BOTH,
        {},
        # core_fn is fed a LIST of open datasets (the synthesized single-band
        # tiles, in band order). Pair each with its 0-based position as the band
        # index so the reducer's ascending sort preserves the array/band order.
        core_fn=lambda dss, a: agg_core.frombands_tiles(
            [(i, _ds_to_gtiff_bytes(ds)) for i, ds in enumerate(dss)]
        ),
        col_fn=lambda arr, a: prx.rst_frombands(arr),
        sources=_AGG_LIGHT
        + (
            _HEAVY + "constructor/RST_FromBands.scala",
            _OPS + "MergeBands.scala",
        ),
        core=False,
        input_kind="tile_array",
    ),
    "rst_combineavg": FnSpec(
        "rst_combineavg",
        "gbx_rst_combineavg",
        "format",
        _BOTH,
        {},
        core_fn=lambda dss, a: agg_core.combineavg_tiles(
            [_ds_to_gtiff_bytes(ds) for ds in dss]
        ),
        col_fn=lambda arr, a: prx.rst_combineavg(arr),
        sources=_AGG_LIGHT
        + (
            _HEAVY + "RST_CombineAvg.scala",
            _OPS + "CombineAVG.scala",
        ),
        core=False,
        input_kind="tile_array",
    ),
    "rst_merge": FnSpec(
        "rst_merge",
        "gbx_rst_merge",
        "format",
        _BOTH,
        {},
        core_fn=lambda dss, a: agg_core.merge_tiles(
            [_ds_to_gtiff_bytes(ds) for ds in dss]
        ),
        col_fn=lambda arr, a: prx.rst_merge(arr),
        sources=_AGG_LIGHT
        + (
            _HEAVY + "RST_Merge.scala",
            _OPS + "MergeRasters.scala",
        ),
        core=False,
        input_kind="tile_array",
    ),
    # --- bucket C, group C4: tiling fns -> a COLLECTION of tiles (5) ----------
    # rst_maketiles / rst_retile / rst_tooverlappingtiles / rst_separatebands /
    # rst_xyzpyramid each take ONE tile and emit MANY. They ride the default
    # input_kind == "tile" (a single open dataset), but the core_fn returns a
    # LIST of tile bytes, which the runner fingerprints with the new
    # `raster_collection` kind: tile COUNT (compared exactly) plus the pooled,
    # ORDER-INDEPENDENT agg stats over all output tiles' pixels. The col_fn
    # yields an ARRAY column the spark-path runner writes via noop. Args are
    # sized for the 256/512-px corpus (e.g. retile 128x128 -> 4 tiles on 256).
    "rst_maketiles": FnSpec(
        "rst_maketiles",
        "gbx_rst_maketiles",
        "format",
        _BOTH,
        {"size_in_mb": 1},
        core_fn=lambda ds, a: tiling.make_tiles(ds, float(a["size_in_mb"])),
        col_fn=lambda t, a: prx.rst_maketiles(t, a["size_in_mb"]),
        sources=_TILING_LIGHT
        + (
            _HEAVY + "generators/RST_MakeTiles.scala",
            _OPS + "BalancedSubdivision.scala",
            _OPS + "ReTile.scala",
        ),
        core=False,
    ),
    "rst_retile": FnSpec(
        "rst_retile",
        "gbx_rst_retile",
        "format",
        _BOTH,
        {"tile_width": 128, "tile_height": 128},
        core_fn=lambda ds, a: tiling.retile(ds, a["tile_width"], a["tile_height"]),
        col_fn=lambda t, a: prx.rst_retile(t, a["tile_width"], a["tile_height"]),
        sources=_TILING_LIGHT
        + (_HEAVY + "generators/RST_ReTile.scala", _OPS + "ReTile.scala"),
        core=False,
    ),
    "rst_tooverlappingtiles": FnSpec(
        "rst_tooverlappingtiles",
        "gbx_rst_tooverlappingtiles",
        "format",
        _BOTH,
        # overlap is a percentage (matches heavy); 25% -> step 96 on 128px tiles
        {"tile_width": 128, "tile_height": 128, "overlap": 25},
        core_fn=lambda ds, a: tiling.to_overlapping_tiles(
            ds, a["tile_width"], a["tile_height"], a["overlap"]
        ),
        col_fn=lambda t, a: prx.rst_tooverlappingtiles(
            t, a["tile_width"], a["tile_height"], a["overlap"]
        ),
        sources=_TILING_LIGHT
        + (
            _HEAVY + "generators/RST_ToOverlappingTiles.scala",
            _OPS + "OverlappingTiles.scala",
            _OPS + "ReTile.scala",
        ),
        core=False,
    ),
    "rst_separatebands": FnSpec(
        "rst_separatebands",
        "gbx_rst_separatebands",
        "format",
        _BOTH,
        {},
        core_fn=lambda ds, a: tiling.separate_bands(ds),
        col_fn=lambda t, a: prx.rst_separatebands(t),
        sources=_TILING_LIGHT
        + (
            _HEAVY + "generators/RST_SeparateBands.scala",
            _OPS + "SeparateBands.scala",
        ),
        core=False,
    ),
    # rst_xyzpyramid is a tile-in / collection-out generator, but every emitted
    # tile is one rst_tilexyz render — the pyramid just loops RST_TileXYZ.execute
    # over the intersecting (z,x,y) set. Like rst_tilexyz, the rendered bytes are
    # render-engine-specific: the heavyweight pipes raw source values through
    # gdal_translate -of PNG, while the lightweight tier emits a rescaled RGBA
    # web-map tile via rio-tiler/PIL. The slippy-map / web-mercator convention
    # (this whole section is "Web-Mercator Tile Output") makes the RGBA display
    # tile the canonical output, so the lightweight render is the correct one;
    # the heavy gdal_translate path is non-canonical (deferred Scala follow-up:
    # heavy RST_TileXYZ should rescale to RGBA before encoding). Either way the
    # two stacks cannot be made pooled-pixel-identical, so — exactly as
    # rst_tilexyz already is — this runs pure-core-only with its fingerprint
    # suppressed. The intersecting tile COUNT already agrees across tiers; the
    # render bytes are timed but never compared.
    "rst_xyzpyramid": FnSpec(
        "rst_xyzpyramid",
        "gbx_rst_xyzpyramid",
        "format",
        ("pure-core",),
        # zoom 10-11 (not 0-1): the corpus tiles are a small NYC extent, so at low
        # zoom the single whole-world (0,0,0) tile forces an enormous warp that
        # stalls in native GDAL. At z10-11 the extent maps to a handful of small,
        # cheap tiles (matches rst_tilexyz's high-zoom bench args).
        {"min_z": 10, "max_z": 11},
        # pyramid returns [{"z","x","y","bytes"}, ...]; project to the tile bytes
        # (raster_collection shape). Timing-only: fingerprint suppressed, so the
        # runner emits an empty fingerprint on both engines and the cell is `na`.
        core_fn=lambda ds, a: [
            d["bytes"] for d in xyz.pyramid(ds, a["min_z"], a["max_z"])
        ],
        col_fn=lambda t, a: prx.rst_xyzpyramid(t, a["min_z"], a["max_z"]),
        sources=_XYZ_LIGHT
        + (
            _HEAVY + "web/RST_XYZPyramid.scala",
            _HEAVY + "web/RST_TileXYZ.scala",
            "src/main/scala/com/databricks/labs/gbx/rasterx/tile/TileMath.scala",
        ),
        core=False,
        fingerprint=False,
    ),
    # --- bucket B, group B-grid: DGGS functions (11) --------------------------
    # rst_h3_tessellate + the 10 rst_{h3,quadbin}_rastertogrid{avg,count,max,
    # median,min} functions map a raster into discrete-global-grid cells. They
    # ride the default input_kind == "tile" (a single open dataset) but their
    # core_fn returns a per-band list of cell records (not bytes, not scalars),
    # so each declares fingerprint_kind == "dggs_grid" to route the output through
    # fingerprint_dggs_grid (cell count + sorted signed-int64 cell-id hash +
    # order-independent agg over the per-cell measures). H3/quadbin cell ids are
    # PARITY-comparable across tiers (confirmed by the test_fingerprint parity
    # gate), so an identical cell-set hashes identically on both engines.
    #
    # Resolutions are sized for the small-extent corpus tiles (EPSG:4326 NYC,
    # 256/512 px at 0.0001 deg ~= 0.0256-deg extent on a 256px tile): H3 res 7
    # (~1.2 km hex edge) -> ~5 cells/band; quadbin res 15 (~0.011-deg cell) ->
    # ~12 cells/band. Both land a sane handful so the cell-set + agg compare
    # meaningfully. The raster is interpreted as EPSG:4326 lon/lat with NO
    # reprojection on BOTH tiers, so non-4326 corpus tiles feed their projected
    # origins as raw lon/lat identically across engines (consistent, by design).
    # --- H3 tessellation (tessellate.py): one tile per overlapping H3 cell ---
    # tessellate_h3 returns a FLAT list of (cellid, bytes); wrap it as a single
    # "band" ([result]) so _grid_records' per-band iteration sees the tuples.
    # The bytes carry no measure, so the dggs_grid fingerprint is count + hash
    # only (empty agg) -- still a full cell-set comparison.
    "rst_h3_tessellate": FnSpec(
        "rst_h3_tessellate",
        "gbx_rst_h3_tessellate",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: [tessellate.tessellate_h3(ds, a["resolution"])],
        col_fn=lambda t, a: prx.rst_h3_tessellate(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_TESSELLATE_LIGHT
        + (
            _HEAVY + "generators/RST_H3_Tessellate.scala",
            _OPS + "RasterTessellate.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    # --- H3 raster->grid aggregates (gridagg.py): {avg,count,max,median,min} ---
    "rst_h3_rastertogridavg": FnSpec(
        "rst_h3_rastertogridavg",
        "gbx_rst_h3_rastertogridavg",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "h3", "avg"),
        col_fn=lambda t, a: prx.rst_h3_rastertogridavg(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridAvg.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    "rst_h3_rastertogridcount": FnSpec(
        "rst_h3_rastertogridcount",
        "gbx_rst_h3_rastertogridcount",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "h3", "count"
        ),
        col_fn=lambda t, a: prx.rst_h3_rastertogridcount(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridCount.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    "rst_h3_rastertogridmax": FnSpec(
        "rst_h3_rastertogridmax",
        "gbx_rst_h3_rastertogridmax",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "h3", "max"),
        col_fn=lambda t, a: prx.rst_h3_rastertogridmax(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridMax.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    "rst_h3_rastertogridmedian": FnSpec(
        "rst_h3_rastertogridmedian",
        "gbx_rst_h3_rastertogridmedian",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "h3", "median"
        ),
        col_fn=lambda t, a: prx.rst_h3_rastertogridmedian(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridMedian.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    "rst_h3_rastertogridmin": FnSpec(
        "rst_h3_rastertogridmin",
        "gbx_rst_h3_rastertogridmin",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "h3", "min"),
        col_fn=lambda t, a: prx.rst_h3_rastertogridmin(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridMin.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    "rst_h3_rastertogridsum": FnSpec(
        "rst_h3_rastertogridsum",
        "gbx_rst_h3_rastertogridsum",
        "dggs",
        _BOTH,
        {"resolution": 7},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "h3", "sum"),
        col_fn=lambda t, a: prx.rst_h3_rastertogridsum(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_H3_RasterToGridSum.scala",
            _HEAVY + "grid/RST_H3_RasterToGrid.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    # --- quadbin raster->grid aggregates (gridagg.py) -------------------------
    "rst_quadbin_rastertogridavg": FnSpec(
        "rst_quadbin_rastertogridavg",
        "gbx_rst_quadbin_rastertogridavg",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "avg"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridavg(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridAvg.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_quadbin_rastertogridcount": FnSpec(
        "rst_quadbin_rastertogridcount",
        "gbx_rst_quadbin_rastertogridcount",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "count"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridcount(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridCount.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_quadbin_rastertogridmax": FnSpec(
        "rst_quadbin_rastertogridmax",
        "gbx_rst_quadbin_rastertogridmax",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "max"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridmax(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridMax.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_quadbin_rastertogridmedian": FnSpec(
        "rst_quadbin_rastertogridmedian",
        "gbx_rst_quadbin_rastertogridmedian",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "median"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridmedian(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridMedian.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_quadbin_rastertogridmin": FnSpec(
        "rst_quadbin_rastertogridmin",
        "gbx_rst_quadbin_rastertogridmin",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "min"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridmin(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridMin.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_quadbin_rastertogridsum": FnSpec(
        "rst_quadbin_rastertogridsum",
        "gbx_rst_quadbin_rastertogridsum",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "quadbin", "sum"
        ),
        col_fn=lambda t, a: prx.rst_quadbin_rastertogridsum(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_GRIDAGG_LIGHT
        + (
            _HEAVY + "grid/RST_Quadbin_RasterToGridSum.scala",
            _HEAVY + "grid/RST_Quadbin_RasterToGrid.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    # --- BNG raster->grid aggregates (gridagg.py): {avg,count,max,median,min} --
    # BNG cell ids are OS grid reference STRINGS (e.g. "TQ3080"), NOT Longs, so
    # these declare fingerprint_kind == "dggs_grid_str" -- the runner routes the
    # per-band grid output through fingerprint_dggs_grid_str (count + sorted STRING
    # id hash + agg), the string analogue of the H3/quadbin dggs_grid path. BNG is
    # 27700-native: raster_to_grid reprojects the tile to EPSG:27700 internally
    # before binning (unlike the 4326-native h3/quadbin), so the timing includes
    # that warp -- exactly as the heavy RST_BNG_RasterToGrid does.
    # Resolution 3 == "1km" (BNG.resolutionMap index 3); NEVER metres-as-Int. On
    # the small-extent corpus tiles a 1km cell lands a sane handful. col_fn mirrors
    # the h3/quadbin siblings (the light rst_bng_rastertogrid* wrappers are UDTFs
    # invoked via SQL LATERAL, so the col_fn is spark-path-filtered like its peers).
    "rst_bng_rastertogridavg": FnSpec(
        "rst_bng_rastertogridavg",
        "gbx_rst_bng_rastertogridavg",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "bng", "avg"),
        col_fn=lambda t, a: prx.rst_bng_rastertogridavg(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridAvg.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    "rst_bng_rastertogridcount": FnSpec(
        "rst_bng_rastertogridcount",
        "gbx_rst_bng_rastertogridcount",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "bng", "count"
        ),
        col_fn=lambda t, a: prx.rst_bng_rastertogridcount(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridCount.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    "rst_bng_rastertogridmax": FnSpec(
        "rst_bng_rastertogridmax",
        "gbx_rst_bng_rastertogridmax",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "bng", "max"),
        col_fn=lambda t, a: prx.rst_bng_rastertogridmax(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridMax.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    "rst_bng_rastertogridmedian": FnSpec(
        "rst_bng_rastertogridmedian",
        "gbx_rst_bng_rastertogridmedian",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(
            ds, a["resolution"], "bng", "median"
        ),
        col_fn=lambda t, a: prx.rst_bng_rastertogridmedian(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridMedian.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    "rst_bng_rastertogridmin": FnSpec(
        "rst_bng_rastertogridmin",
        "gbx_rst_bng_rastertogridmin",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "bng", "min"),
        col_fn=lambda t, a: prx.rst_bng_rastertogridmin(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridMin.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    "rst_bng_rastertogridsum": FnSpec(
        "rst_bng_rastertogridsum",
        "gbx_rst_bng_rastertogridsum",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: gridagg.raster_to_grid(ds, a["resolution"], "bng", "sum"),
        col_fn=lambda t, a: prx.rst_bng_rastertogridsum(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_GRIDAGG_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "grid/RST_BNG_RasterToGridSum.scala",
            _HEAVY + "grid/RST_BNG_RasterToGrid.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    # --- quadbin + BNG tessellation (tessellate.py): one tile per overlapping cell
    # Mirror rst_h3_tessellate: the core_fn wraps the FLAT list of (cellid, bytes)
    # tuples as a single "band" ([...]) so _grid_records' per-band iteration sees
    # the tuples; the bytes carry no measure, so the fingerprint is count + hash
    # only (empty agg). quadbin ids are Longs (dggs_grid); BNG ids are STRINGS
    # (dggs_grid_str). BNG tessellate reprojects the tile to EPSG:27700 internally
    # (timing includes that warp), like the heavy tessellateBngIter.
    "rst_quadbin_tessellate": FnSpec(
        "rst_quadbin_tessellate",
        "gbx_rst_quadbin_tessellate",
        "dggs",
        _BOTH,
        {"resolution": 15},
        core_fn=lambda ds, a: [
            list(tessellate.iter_tessellate_quadbin(ds, a["resolution"]))
        ],
        col_fn=lambda t, a: prx.rst_quadbin_tessellate(t, a["resolution"]),
        fingerprint_kind="dggs_grid",
        sources=_TESSELLATE_LIGHT
        + (
            _QUADBIN_LIB,
            _HEAVY + "generators/RST_Quadbin_Tessellate.scala",
            _OPS + "RasterTessellate.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    "rst_bng_tessellate": FnSpec(
        "rst_bng_tessellate",
        "gbx_rst_bng_tessellate",
        "dggs",
        _BOTH,
        {"resolution": 3},
        core_fn=lambda ds, a: [
            list(tessellate.iter_tessellate_bng(ds, a["resolution"]))
        ],
        col_fn=lambda t, a: prx.rst_bng_tessellate(t, a["resolution"]),
        fingerprint_kind="dggs_grid_str",
        sources=_TESSELLATE_LIGHT
        + (
            _BNG_LIB,
            _HEAVY + "generators/RST_BNG_Tessellate.scala",
            _OPS + "RasterTessellate.scala",
            _GRIDX + "BNG.scala",
        ),
        gb_tile=True,
        core=False,
    ),
    # --- bucket B, group B-vec: vector-out functions (2) ----------------------
    # rst_contour (contour LINES) + rst_polygonize (POLYGONS) emit a set of
    # vector features (geometry + per-feature value). Their core_fn returns a
    # list of {"geom_wkb", "value"} (contour) / (geom_wkb, value) tuples
    # (polygonize) -- neither bytes nor scalars -- so each declares
    # fingerprint_kind == "vector" to route the output through fingerprint_vector
    # (feature COUNT + total measure [line length for lines, polygon area for
    # polygons, by geometry type] + order-independent agg over the attributes).
    #
    # Heavy arg defaults matched exactly: contour rides FIXED_LEVELS (the heavy
    # ContourGenerateEx FIXED_LEVELS path), so the bench uses explicit fixed
    # levels [0.2, 0.4, 0.6, 0.8] -- the float32 corpus band is ~[0, 1], so
    # these span its value range and trace a handful of contour LineStrings.
    # polygonize rides band 1 + connectedness 4 (the heavy RST_Polygonize
    # builder's defaults: case 1 => Literal(1), Literal(4)). The contour
    # `interval`/`base`/`attr_field` ride the binding defaults (0.0/0.0/"elev");
    # with non-empty levels `interval`/`base` are ignored on both engines.
    "rst_contour": FnSpec(
        "rst_contour",
        "gbx_rst_contour",
        "vector",
        _BOTH,
        {"levels": [0.2, 0.4, 0.6, 0.8]},
        core_fn=lambda ds, a: analysis_core.contour(ds, a["levels"], 0.0, 0.0, "elev"),
        col_fn=lambda t, a: prx.rst_contour(
            t, F.array(*[F.lit(float(v)) for v in a["levels"]])
        ),
        fingerprint_kind="vector",
        # GDAL's contour generator and the lightweight marching-squares segmenter
        # produce the same iso-lines but split them into segments differently, so
        # per-segment measures/attrs spread ~1.5%. That is an inherent algorithm
        # difference, not a parity bug -- a 2% per-fn tol absorbs it without
        # loosening the strict global tol for any other function.
        rel_tol=0.02,
        sources=_ANALYSIS_LIGHT + (_HEAVY + "analysis/RST_Contour.scala",),
        core=False,
    ),
    "rst_polygonize": FnSpec(
        "rst_polygonize",
        "gbx_rst_polygonize",
        "vector",
        _BOTH,
        {"band": 1, "connectedness": 4},
        core_fn=lambda ds, a: features.polygonize(ds, a["band"], a["connectedness"]),
        col_fn=lambda t, a: prx.rst_polygonize(
            t, F.lit(a["band"]), F.lit(a["connectedness"])
        ),
        fingerprint_kind="vector",
        sources=_FEATURES_LIGHT + (_HEAVY + "vector/RST_Polygonize.scala",),
        core=False,
    ),
    # --- bucket D: geometry-in constructors (3) -------------------------------
    # rst_rasterize / rst_gridfrompoints / rst_dtmfromgeoms take GEOMETRY input and
    # PRODUCE a raster. No single literal geometry is in-extent across the multi-CRS
    # corpus, so they ride input_kind == "geometry": the runner hands core_fn(ds,
    # args, geom) the tile's GeometrySet (boxes / points / zpoints as WKB + burn
    # values, in the tile CRS, deterministic + identical across both engines via
    # geometry.json). The extent / size / srid come from the SAME tile ds the
    # geometry was derived from, so the burn grid aligns with the source tile on
    # every CRS and the heavy tier (reading identical geometry + the same extent
    # from the same tile) produces a pixel-comparable raster. Pure-core-only: the
    # spark-path tile DataFrame carries no geometry column.
    #
    # rst_rasterize is SINGLE-geometry (the binding + heavy execute both take ONE
    # geom_wkb + value), so the bench burns the FIRST corpus box. gridfrompoints /
    # dtmfromgeoms take an ARRAY of points, so the bench feeds the whole point set.
    "rst_rasterize": FnSpec(
        "rst_rasterize",
        "gbx_rst_rasterize",
        "vector",
        ("pure-core",),
        {},
        core_fn=lambda ds, a, g: features.rasterize_geom(
            g.boxes[0][0], g.boxes[0][1], *_tile_extent_size_srid(ds)
        ),
        # spark-path is mode-filtered out (geometry-in, pure-core only); the col_fn
        # is here only for the FnSpec contract (callable). The static box / value
        # are unused at runtime.
        col_fn=lambda t, a: prx.rst_rasterize(
            F.lit(b""),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(1.0),
            F.lit(1.0),
            F.lit(1),
            F.lit(1),
            F.lit(4326),
        ),
        input_kind="geometry",
        sources=_FEATURES_LIGHT + (_HEAVY + "vector/RST_Rasterize.scala",),
        core=False,
    ),
    # max_pts is a LARGE sentinel (>= any corpus point count) so the lightweight
    # IDW uses ALL points -- NOT a nearest-k subset. This is required for parity:
    # the heavy tier runs ``gdal_grid invdist:power=p:max_points=m`` with NO search
    # radius, and gdal_grid's plain ``invdist`` ignores ``max_points`` unless a
    # search radius is set (radius1=radius2=0 => interpolate from every point). The
    # lightweight ``idw_grid`` instead does a true nearest-``max_pts`` cKDTree
    # selection. Feeding max_pts=12 to both therefore fed DIFFERENT effective point
    # sets (heavy: all 64; light: nearest 12) -> a ~13-27% grid divergence that is
    # an artifact of mismatched neighbor selection, not an algorithm bug. Clamping
    # max_pts to the full point set makes both tiers IDW over the same points with
    # the identical Sum(v_i/d_i^p)/Sum(1/d_i^p) formula, so the grids agree.
    "rst_gridfrompoints": FnSpec(
        "rst_gridfrompoints",
        "gbx_rst_gridfrompoints",
        "vector",
        ("pure-core",),
        {"power": 2.0, "max_pts": 1000000},
        core_fn=lambda ds, a, g: tin.idw_grid(
            tin.points_xy_from_wkb([wkb for wkb, _ in g.points]),
            [v for _, v in g.points],
            *_tile_extent_size_srid(ds),
            power=a["power"],
            max_pts=a["max_pts"],
        ),
        col_fn=lambda t, a: prx.rst_gridfrompoints(
            F.array(),
            F.array(),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(1.0),
            F.lit(1.0),
            F.lit(1),
            F.lit(1),
            F.lit(4326),
            a["power"],
            a["max_pts"],
        ),
        input_kind="geometry",
        sources=_TIN_LIGHT + (_HEAVY + "grid/RST_GridFromPoints.scala",),
        core=False,
    ),
    "rst_dtmfromgeoms": FnSpec(
        "rst_dtmfromgeoms",
        "gbx_rst_dtmfromgeoms",
        "vector",
        ("pure-core",),
        # breaklines=None + tolerances=0.0 (scipy Delaunay is unconstrained, so
        # tolerances have no analogue; mirrored on the heavy side as 0.0).
        {"no_data": -9999.0},
        core_fn=lambda ds, a, g: tin.delaunay_dtm(
            tin.points_xyz_from_wkb(g.zpoints),
            None,
            *_tile_extent_size_srid(ds),
            no_data=a["no_data"],
        ),
        col_fn=lambda t, a: prx.rst_dtmfromgeoms(
            F.array(),
            F.array(),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(0.0),
            F.lit(1.0),
            F.lit(1.0),
            F.lit(1),
            F.lit(1),
            F.lit(4326),
            a["no_data"],
        ),
        input_kind="geometry",
        sources=_TIN_LIGHT + (_HEAVY + "RST_DTMFromGeoms.scala",),
        core=False,
    ),
    # --- bucket A: the 7 *_agg aggregators (Spark groupBy aggregate harness) ----
    # Each reduces a GROUP of rows to ONE output tile via a real Spark
    # df.groupBy(key).agg(col_fn(...)). There is no single-row pure-core analogue of
    # a UDAF, so these are spark-path ONLY. The runner's aggregate branch builds the
    # input DataFrame + a `key`, runs the grouped aggregate, and yields TWO signals:
    #   CONSISTENCY: a FIXED deterministic group (single key, small N) aggregates to
    #     ONE out tile -> raster fingerprint (heavy vs light compared in P4.4). The
    #     group is byte-identical across tiers (the tile aggregators reuse the SAME
    #     synthesized tiles the C3 fns read; the geometry aggregators reuse the SAME
    #     per-tile GeometrySet -- write-once-read-both via _synth / geometry.json).
    #   PERF: the scaled groupBy timed via the existing noop-write timing.
    #
    # The four TILE aggregators ride input_kind == "tile_aggregate": the harness
    # builds a (tile[, band_index]) DataFrame from the synth recipe's tiles, keys
    # them into one group, and aggregates. combineavg over ALIGNED copies (synth
    # combineavg), merge over OFFSET copies (synth merge), frombands/derivedband
    # over the per-band split (synth frombands; each band tile is one group row).
    # frombands_agg REQUIRES a per-row band_index INT (0,1,...) so both tiers' agg
    # sorts ascending; derivedband_agg rides the same fixed mean-bands pyfunc as the
    # non-agg rst_derivedband (_DERIVEDBAND_PYFUNC / _DERIVEDBAND_FUNC_NAME).
    #
    # The three GEOMETRY aggregators ride input_kind == "geometry_aggregate": the
    # harness builds rows of (geom_wkb, value[, ...]) from the tile's GeometrySet
    # (boxes for rasterize_agg, points for gridfrompoints_agg, zpoints + NULL
    # breaklines for dtmfromgeoms_agg), keys them into one group, and supplies the
    # extent/size/srid as per-group constants read from the source tile. gridfrom-
    # points_agg power=2.0/max_pts=12; dtmfromgeoms_agg tolerances=0.0 (unconstrained
    # Delaunay; breaklines NULL on both tiers).
    "rst_combineavg_agg": FnSpec(
        "rst_combineavg_agg",
        "gbx_rst_combineavg_agg",
        "format",
        ("spark-path",),
        {},
        # col_fn receives the tile struct column; the harness wires the groupBy.
        col_fn=lambda t, a: prx.rst_combineavg_agg(t),
        core_fn=lambda t, a: t,  # spark-path-only; no pure-core analogue
        input_kind="tile_aggregate",
        sources=_AGG_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "RST_CombineAvg.scala",
            _HEAVY + "agg/RST_CombineAvgAgg.scala",
            _OPS + "CombineAVG.scala",
        ),
        core=False,
    ),
    "rst_merge_agg": FnSpec(
        "rst_merge_agg",
        "gbx_rst_merge_agg",
        "format",
        ("spark-path",),
        {},
        col_fn=lambda t, a: prx.rst_merge_agg(t),
        core_fn=lambda t, a: t,
        input_kind="tile_aggregate",
        sources=_AGG_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "RST_Merge.scala",
            _HEAVY + "agg/RST_MergeAgg.scala",
            _OPS + "MergeRasters.scala",
        ),
        core=False,
    ),
    "rst_frombands_agg": FnSpec(
        "rst_frombands_agg",
        "gbx_rst_frombands_agg",
        "format",
        ("spark-path",),
        {},
        # col_fn takes (tile, band_index); the harness adds the band_index column.
        col_fn=lambda t, a, bi: prx.rst_frombands_agg(t, bi),
        core_fn=lambda t, a: t,
        input_kind="tile_aggregate",
        sources=_AGG_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "constructor/RST_FromBands.scala",
            _HEAVY + "agg/RST_FromBandsAgg.scala",
            _OPS + "MergeBands.scala",
        ),
        core=False,
    ),
    "rst_derivedband_agg": FnSpec(
        "rst_derivedband_agg",
        "gbx_rst_derivedband_agg",
        "format",
        ("spark-path",),
        # the fixed mean-bands pyfunc (same body the non-agg rst_derivedband rides),
        # hardcoded identically here and in the Scala dispatch.
        {"python_func": _DERIVEDBAND_PYFUNC, "func_name": _DERIVEDBAND_FUNC_NAME},
        col_fn=lambda t, a: prx.rst_derivedband_agg(
            t, a["python_func"], a["func_name"]
        ),
        core_fn=lambda t, a: t,
        input_kind="tile_aggregate",
        sources=_DERIVEDBAND_LIGHT
        + _AGG_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "RST_DerivedBand.scala",
            _HEAVY + "agg/RST_DerivedBandAgg.scala",
        ),
        core=False,
    ),
    "rst_rasterize_agg": FnSpec(
        "rst_rasterize_agg",
        "gbx_rst_rasterize_agg",
        "vector",
        ("spark-path",),
        {},
        # col_fn takes (geom_wkb, value, xmin, ymin, xmax, ymax, w, h, srid); the
        # harness supplies the geometry columns + the per-group extent constants.
        col_fn=lambda g, v, ext, a: prx.rst_rasterize_agg(
            g, v, ext[0], ext[1], ext[2], ext[3], ext[4], ext[5], ext[6]
        ),
        core_fn=lambda t, a: t,
        input_kind="geometry_aggregate",
        sources=_AGG_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "vector/RST_Rasterize.scala",
            _HEAVY + "agg/RST_RasterizeAgg.scala",
        ),
        core=False,
    ),
    # max_pts is the same large sentinel as the non-agg rst_gridfrompoints (see the
    # comment there): heavy gdal_grid ``invdist`` with no radius uses ALL points, so
    # the lightweight side must too, or the grids diverge by the neighbor-selection
    # artifact rather than agreeing under identical IDW math.
    "rst_gridfrompoints_agg": FnSpec(
        "rst_gridfrompoints_agg",
        "gbx_rst_gridfrompoints_agg",
        "vector",
        ("spark-path",),
        {"power": 2.0, "max_pts": 1000000},
        col_fn=lambda g, v, ext, a: prx.rst_gridfrompoints_agg(
            g,
            v,
            ext[0],
            ext[1],
            ext[2],
            ext[3],
            ext[4],
            ext[5],
            ext[6],
            a["power"],
            a["max_pts"],
        ),
        core_fn=lambda t, a: t,
        input_kind="geometry_aggregate",
        sources=_TIN_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "grid/RST_GridFromPoints.scala",
            _HEAVY + "grid/RST_GridFromPointsAgg.scala",
        ),
        core=False,
    ),
    "rst_dtmfromgeoms_agg": FnSpec(
        "rst_dtmfromgeoms_agg",
        "gbx_rst_dtmfromgeoms_agg",
        "vector",
        ("spark-path",),
        # breaklines NULL on both tiers; tolerances 0.0 (scipy Delaunay is
        # unconstrained, so tolerances have no analogue -- mirrored heavy as 0.0).
        {"merge_tolerance": 0.0, "snap_tolerance": 0.0, "no_data": -9999.0},
        col_fn=lambda g, v, ext, a: prx.rst_dtmfromgeoms_agg(
            g,
            F.lit(None).cast("array<binary>"),
            a["merge_tolerance"],
            a["snap_tolerance"],
            ext[0],
            ext[1],
            ext[2],
            ext[3],
            ext[4],
            ext[5],
            ext[6],
            a["no_data"],
        ),
        core_fn=lambda t, a: t,
        input_kind="geometry_aggregate",
        sources=_TIN_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "RST_DTMFromGeoms.scala",
            _HEAVY + "RST_DTMFromGeomsAgg.scala",
        ),
        core=False,
    ),
    # rst_h3_rasterize_agg: a GRID aggregator -- rows of (cellid LONG, value
    # DOUBLE) grouped -> one GTiff tile per group, burning each H3 cell's centroid
    # pixel onto an EXPLICIT, hardcoded grid. Rides input_kind == "h3_aggregate":
    # the harness builds rows of (cellid, value) from the fixed deterministic cell
    # set (h3_rasterize_cells) and supplies the explicit grid constants
    # (_H3RAGG_GRID) baked into args. The light + heavy tiers burn the SAME cell
    # set onto the SAME canvas, so the consistency fingerprint is cross-tier
    # comparable (the masks match exactly -- see test_h3_rasterize_parity.py). The
    # cell recipe + grid constants are mirrored EXACTLY in BenchDispatch.scala.
    # value=None -> presence mask (burns 1.0); mode "centroids"; kring_pad 1 (the
    # grid was snapped with kring_pad=1, so the explicit extent already covers it).
    "rst_h3_rasterize_agg": FnSpec(
        "rst_h3_rasterize_agg",
        "gbx_rst_h3_rasterize_agg",
        "dggs",
        ("spark-path",),
        {
            "grid": _H3RAGG_GRID,  # (xmin, ymin, xmax, ymax, width, height, srid)
            "pixel_size": _H3RAGG_PIXEL_SIZE,
            "mode": _H3RAGG_MODE,
            "kring_pad": _H3RAGG_KRING_PAD,
        },
        # col_fn takes (cellid_col, value_col, args); the harness streams the
        # (cellid, value) rows and the explicit grid rides in args. value=None ->
        # presence mask (1.0). Grid passed explicitly (NOT auto-derived per tier).
        col_fn=lambda cid, v, a: prx.rst_h3_rasterize_agg(
            cid,
            value=v,
            srid=F.lit(a["grid"][6]),
            pixel_size=F.lit(a["pixel_size"]),
            xmin=F.lit(a["grid"][0]),
            ymin=F.lit(a["grid"][1]),
            xmax=F.lit(a["grid"][2]),
            ymax=F.lit(a["grid"][3]),
            width=F.lit(a["grid"][4]),
            height=F.lit(a["grid"][5]),
            mode=F.lit(a["mode"]),
            kring_pad=F.lit(a["kring_pad"]),
        ),
        core_fn=lambda t, a: t,  # spark-path-only; no pure-core analogue
        input_kind="h3_aggregate",
        # Float burn order can differ across tiers, but the burn is a discrete LUT
        # lookup (last-wins on a canonical (cellId, value) sort, identical both
        # tiers), so the masks/values are exact -- keep the global tol. Loosen only
        # if the cluster run shows a near-miss.
        sources=_CELLRASTER_LIGHT
        + (
            _PYRX_SERDE,
            _HEAVY + "agg/RST_H3_RasterizeAgg.scala",
            _GRIDX + "H3.scala",
        ),
        core=False,
    ),
    # rst_quadbin_rasterize_agg: a GRID aggregator like rst_h3_rasterize_agg, but
    # unlike H3 (which passes an EXPLICIT hardcoded grid) the quadbin aggregator
    # streams (cellid LONG, value DOUBLE) rows from the fixed deterministic cell
    # set (quadbin_rasterize_cells) and uses the 2-arg overload that AUTO-DERIVES
    # the 4326 grid from that cell set -- deterministic given the fixed set, so the
    # burn is reproducible and cross-tier comparable. value=None -> presence mask
    # (1.0). Rides input_kind == "grid_aggregate" (the harness streams the LONG
    # cell rows). Cell recipe mirrored EXACTLY in BenchDispatch.scala.
    "rst_quadbin_rasterize_agg": FnSpec(
        "rst_quadbin_rasterize_agg",
        "gbx_rst_quadbin_rasterize_agg",
        "dggs",
        ("spark-path",),
        {},
        # col_fn takes (cellid_col, value_col, args); 2-arg overload auto-derives
        # the grid from the streamed cell set (NOT an explicit grid like H3).
        col_fn=lambda cid, v, a: prx.rst_quadbin_rasterize_agg(cid, value=v),
        core_fn=lambda t, a: t,  # spark-path-only; no pure-core analogue
        input_kind="grid_aggregate",
        sources=_CELLRASTER_LIGHT
        + (
            _QUADBIN_LIB,
            _PYRX_SERDE,
            _HEAVY + "agg/RST_Quadbin_RasterizeAgg.scala",
            _GRIDX + "Quadbin.scala",
        ),
        core=False,
    ),
    # rst_bng_rasterize_agg: a GRID aggregator streaming (cellid STRING, value
    # DOUBLE) rows from the fixed deterministic BNG cell set (bng_rasterize_cells);
    # cellid is an OS grid reference STRING (not a Long). The 2-arg overload
    # auto-derives the 27700-native grid from the cell set (srid forced to 27700).
    # value=None -> presence mask (1.0). Rides input_kind == "grid_aggregate" (the
    # harness streams STRING cell rows for BNG). The consistency fingerprint is a
    # raster (the burned tile), so no dggs_grid_str is needed here -- only the
    # rastertogrid/tessellate fns emit string cell-id fingerprints. Cell recipe
    # mirrored EXACTLY in BenchDispatch.scala (res-3 London, kRing k=3).
    "rst_bng_rasterize_agg": FnSpec(
        "rst_bng_rasterize_agg",
        "gbx_rst_bng_rasterize_agg",
        "dggs",
        ("spark-path",),
        {},
        col_fn=lambda cid, v, a: prx.rst_bng_rasterize_agg(cid, value=v),
        core_fn=lambda t, a: t,  # spark-path-only; no pure-core analogue
        input_kind="grid_aggregate",
        sources=_CELLRASTER_LIGHT
        + (
            _BNG_LIB,
            _PYRX_SERDE,
            _HEAVY + "agg/RST_BNG_RasterizeAgg.scala",
            _GRIDX + "BNG.scala",
        ),
        core=False,
    ),
}

# Maps each tile-aggregator FnSpec to the bench.synth recipe whose tiles form its
# fixed CONSISTENCY group (write-once-read-both, identical bytes across tiers).
# combineavg over aligned copies; merge over offset copies; frombands/derivedband
# over the per-band split (each split band tile is one group row / one input band).
_AGG_SYNTH_RECIPE: Dict[str, str] = {
    "rst_combineavg_agg": "combineavg",
    "rst_merge_agg": "merge",
    "rst_frombands_agg": "frombands",
    "rst_derivedband_agg": "frombands",
}


def agg_synth_recipe(fn: str) -> str:
    """The bench.synth recipe for a tile aggregator's fixed group (KeyError if none)."""
    return _AGG_SYNTH_RECIPE[fn]


# Maps each tile_array FnSpec to its bench.synth recipe name. The runner uses this
# to synthesize the multi-tile input deterministically (write-once-read-both).
_SYNTH_RECIPE: Dict[str, str] = {
    "rst_frombands": "frombands",
    "rst_combineavg": "combineavg",
    "rst_merge": "merge",
}


def synth_recipe(fn: str) -> str:
    """The bench.synth recipe name for a `tile_array` function (KeyError if none)."""
    return _SYNTH_RECIPE[fn]


def _getsubdataset_timing(ds, name):
    """Time rst_getsubdataset without a noisy error row on a no-subdataset tile.

    On a plain GTiff corpus tile there is no subdataset, so accessors.getsubdataset
    raises ValueError. This is timing-only (fingerprint suppressed), and we prefer
    a clean ``ok`` row over an ``error`` row, so the expected "no subdataset" raise
    is swallowed and an empty map returned; any other failure still propagates.
    """
    try:
        return accessors.getsubdataset(ds, name)
    except ValueError:
        return {}


def select(
    functions: List[str] = None,
    categories: List[str] = None,
    set: str = "core",
) -> List[FnSpec]:
    """Select FnSpecs from the registry.

    ``set`` chooses the tier: ``"core"`` (the fast default, only ``core=True``
    entries) or ``"full"`` (every registered FnSpec). An explicit ``functions``
    list selects by name and ignores the tier. Note: the ``set`` parameter
    shadows the builtin within this function, so use ``frozenset(...)`` for the
    membership sets below.
    """
    out = list(REGISTRY.values())
    # An explicit ``functions`` list selects by name and IGNORES the tier, so the
    # core filter must be skipped when names are given — otherwise requesting a
    # core=False function (e.g. rst_threshold / rst_derivedband) by name while the
    # default tier is "core" silently yields zero specs. Apply the core filter
    # only when selecting by tier (no explicit functions).
    if set == "core" and not functions:
        out = [f for f in out if f.core]
    # set == "full": no core filter
    if functions:
        names = frozenset(functions)  # frozenset, NOT set(...) -- `set` is a param name
        out = [f for f in out if f.name in names]
    if categories:
        cats = frozenset(categories)
        out = [f for f in out if f.category in cats]
    return out


def registered_rst() -> "frozenset[str]":
    """Canonical registered rst_* function names (the 107) from registered_functions.txt."""
    from pathlib import Path

    # resolve repo root robustly from this file's location
    root = Path(__file__).resolve()
    cand = None
    for _ in range(12):
        c = root / "docs" / "tests-function-info" / "registered_functions.txt"
        if c.exists():
            cand = c
            break
        root = root.parent
    if cand is not None:
        text = cand.read_text()
    else:
        # Installed without the repo tree above this file (e.g. the wheel on a cluster):
        # read the copy packaged alongside this module. Kept byte-identical to the
        # canonical docs/tests-function-info/registered_functions.txt by the
        # binding-parity check (check-binding-parity.py), so there is no drift.
        from importlib.resources import files

        text = (
            files("databricks.labs.gbx.bench")
            .joinpath("registered_functions.txt")
            .read_text()
        )
    names = set()
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s[4:] if s.startswith("gbx_") else s
        if s.startswith("rst_"):
            names.add(s)
    return frozenset(names)


def dump_functions_json(path) -> None:
    """Write the language-neutral function list (no callables) for the Scala runner."""
    data = [
        {
            "name": f.name,
            "sql_name": f.sql_name,
            "category": f.category,
            "modes": list(f.modes),
            "args": f.args,
        }
        for f in REGISTRY.values()
    ]
    Path(path).write_text(json.dumps(data, indent=2))
