"""Family oracle tables: inputs -> expected, run at every implementation level and surface.

One table per function family. Host tests, container parity runs and the cluster and
Unity Catalog passes all read these rows, so a level is "correct" exactly when its rows pass.
"""

from __future__ import annotations

import base64
import json
import math
import os
import tempfile
from dataclasses import dataclass
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
# pass must be dropped from here.
_INF = "inf → NULL awaits the BNG.scala isfinite guard + a JAR rebuild"
HEAVY_PENDING: Dict[str, Dict[str, str]] = {"bng_inf_e": {"L1": _INF, "L2": _INF}}

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
    # A non-finite coordinate is NULL like NaN, on every level (AP-21): there is no right cell for it.
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


# MVP-H: the DataFrame helpers in pyrx/helpers.py (a case's `fn` is the helper's name, not a SQL name).
# The first arg builds the input rows; the rest are the helper's arguments after the DataFrame and,
# for the by-key helpers, the method / stat.
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


def _build(name: str, tmpdir: str):
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
    raise ValueError(case.kind)


def check_error(case: Case, exc: BaseException) -> Optional[str]:
    if case.kind == "raises":
        if str(case.expect) in str(exc):
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
        if (got.shape, got.transform) != (want.shape, want.transform):
            return f"{case.cid}: {got.shape}@{got.transform} vs {want.shape}@{want.transform}"
        return (
            None
            if np.array_equal(got.read(), want.read())
            else f"{case.cid}: pixels differ"
        )


def _check_helper(case: Case, rows, tiles) -> Optional[str]:
    """MVP-H: judge the collected rows of one helper call (`tiles` are the input tiles)."""
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


def run_helpers(spark, cases, tmpdir: Optional[str] = None):
    """MVP-H: build a (key, tile) DataFrame from the case, call the helper by name, check the rows."""
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.pyrx import helpers

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    out = []
    for case in cases:
        if "session" not in case.surfaces:
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


_SQL = {"DOUBLE": "double", "INT": "int", "STRING": "string", "BINARY": "binary"}


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
    """T0 for L4: call the UC handlers with Python values. GEOMETRY params get EWKB bytes."""
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
