"""Family oracle tables: inputs -> expected, run at every implementation level and surface.

One table per function family. Host tests, container parity runs and the cluster and
Unity Catalog passes all read these rows, so a level is "correct" exactly when its rows pass.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Sequence, Tuple

MERC_PROJ = (
    "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 "
    "+units=m +nadgrids=@null +wktext +no_defs"
)
# An error tile (a swallowed failure) carries its diagnostic under this one metadata key on
# every level. The light tier defines it (`_serde.build_error_tile`); the JVM tier conforms.
ERROR_TILE_KEY = "last_error"
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
    expect: Any = None
    surfaces: Tuple[str, ...] = ("session", "uc")
    geom_srid: int = (
        4326  # the integer SRID UC GEOMETRY args are built with (never a CRS string)
    )


# Rows a JVM level is known to fail until it conforms to the light contract, as {row: {level: reason}}.
# The JVM parity runs expect exactly these (level, row) pairs to fail, strictly, and drop an entry
# when the row passes.
HEAVY_PENDING = {
    "bng_digit_res": {
        "L1": "heavy refuses a digit-string BNG resolution",
        "L2": "the Java UDF refuses a digit-string BNG resolution",
    },
    "bng_digit_res_quad": {
        "L1": "heavy refuses a digit-string BNG resolution",
        "L2": "the Java UDF refuses a digit-string BNG resolution",
    },
    "bng_bad_res_int": {
        "L1": "heavy answers a wrong cell for an integer resolution BNG does not define"
    },
    "bng_bad_easting": {
        "L1": "heavy fails with an INTERNAL_ERROR that does not name the value"
    },
    "tcrs_corrupt": {"L1": "heavy's error tile carries error_message, not last_error"},
}

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
    Case("bng_null_res", B, (530000.0, 180000.0, None), "null"),
    # A NULL in an INT column makes pandas hand the whole batch over as floats (3.0): the
    # valid rows around the NULL must still answer, row by row.
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
    ),
    Case("bng_bad_res", B, (530000.0, 180000.0, "3km"), "raises", "3km"),
    # A resolution BNG does not define is a parameter error on every level, never a clamped cell.
    Case("bng_bad_res_int", B, (530000.0, 180000.0, 1000), "raises", "1000"),
    Case("bng_bad_res_digits", B, (530000.0, 180000.0, "1000"), "raises", "1000"),
    # A non-number easting is a parameter error too (only NaN and out-of-range values are data errors).
    Case("bng_bad_easting", B, ("abc", 180000.0, 3), "raises", "abc"),
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
    if name == "tile_virtual":
        path = os.path.join(tmpdir, "oracle_4326.tif")
        if not os.path.exists(path):
            with open(path, "wb") as fh:
                fh.write(_tile_bytes("EPSG:4326"))
        return _tile(
            None, path, {"col_off": 0, "row_off": 0, "width": 64, "height": 48}
        )
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
        if (
            g.shape == w.shape
        ):  # same grid: nearest-neighbour pixels must agree, not just their mean
            same = (g.filled(-1) == w.filled(-1)).mean()
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
    raw = case.args if case.kind == "exact_rows" else (case.args,)
    return [[resolve_arg(a, tmpdir) for a in row] for row in raw]


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
            results.append(
                (
                    case,
                    check(case, outs if case.kind == "exact_rows" else outs[0], args),
                )
            )
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            results.append((case, check_error(case, exc)))
    return results
