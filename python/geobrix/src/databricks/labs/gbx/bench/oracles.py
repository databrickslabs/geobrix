"""Family oracle tables (rule 15): inputs -> expected, run at every level and tier.

One table per MVP family. Host tests (T1), container parity runs and the T2 cluster/UC
passes all read these rows, so a level is "correct" exactly when its rows pass.
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
CUSTOM_TMERC = "+proj=tmerc +lat_0=0 +lon_0=1.5 +k=0.9996 +x_0=500000 +y_0=0 +ellps=WGS84 +units=m +no_defs"


@dataclass(frozen=True)
class Build:
    name: str  # a fixture built by _build(name)


@dataclass(frozen=True)
class Case:
    cid: str
    fn: str
    args: Tuple[Any, ...]
    kind: (
        str  # exact | null | raises | uc_refused | error_tile | tile_props | geom_props
    )
    expect: Any = None
    surfaces: Tuple[str, ...] = ("session", "uc")
    geom_srid: int = (
        4326  # the integer SRID UC GEOMETRY args are built with (never a CRS string)
    )


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
    Case("bng_bad_res", B, (530000.0, 180000.0, "3km"), "raises"),
    Case("bng_uc_digit_res", B, (530000.0, 180000.0, "3"), "exact", "TQ3080", ("uc",)),
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
    # The three UC CRS rows the user's final decision requires (int SRID or EPSG:/ESRI:<n>; the rest refused):
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
    Case("mv_bad_level", M, (SQUARE, "bogus"), "raises"),
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


def _check_tile(case: Case, out, args) -> Optional[str]:
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.core.crs import crs_equal
    from databricks.labs.gbx.pyrx.core import warp

    raster = _field(out, "raster")
    if raster is None:
        return f"{case.cid}: no raster in the result (metadata={_field(out, 'metadata')!r})"
    with MemoryFile(_raster_of(args[0])) as src_mf, src_mf.open() as src:
        ref = warp.reproject_to_crs(src, str(args[1]))
    want_crs, want_srid = (
        case.expect if isinstance(case.expect, tuple) else (case.expect, None)
    )
    with (
        MemoryFile(bytes(raster)) as a,
        a.open() as got,
        MemoryFile(ref) as b,
        b.open() as want,
    ):
        if not crs_equal(got.crs, want_crs):
            return f"{case.cid}: CRS {got.crs} is not {want_crs}"
        from databricks.labs.gbx.core.crs import authority_srid_of

        if want_srid is not None and authority_srid_of(got.crs) != want_srid:
            return f"{case.cid}: SRID {authority_srid_of(got.crs)} is not {want_srid}"
        if abs(got.width - want.width) > 1 or abs(got.height - want.height) > 1:
            return f"{case.cid}: {got.width}x{got.height} vs reference {want.width}x{want.height}"
        g, w = got.read(1, masked=True), want.read(1, masked=True)
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
    if case.kind == "null":
        return None if out is None else f"{case.cid}: got {out!r}, want NULL"
    if case.kind in ("raises", "uc_refused"):
        return f"{case.cid}: returned {out!r}, want an error"
    if case.kind == "error_tile":
        ok = (
            out is not None
            and _field(out, "raster") is None
            and _field(out, "path") is None
        )
        return (
            None
            if ok
            else f"{case.cid}: got {out!r}, want an error tile (no raster, no path)"
        )
    if case.kind == "tile_props":
        return _check_tile(case, out, args)
    if case.kind == "geom_props":
        return _check_geom(case, out)
    raise ValueError(case.kind)


def check_error(case: Case, exc: BaseException) -> Optional[str]:
    if case.kind == "raises":
        return None
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


def run_session(
    spark,
    cases: Sequence[Case],
    name_of: Callable[[str], str] = lambda f: f,
    tmpdir: Optional[str] = None,
    surface: str = "session",
) -> List[Tuple[Case, Optional[str]]]:
    """Run each case through SQL (`name_of(fn)(cols...)`) and check it. One row per case."""
    from databricks.labs.gbx import manifest

    tmpdir = tmpdir or tempfile.mkdtemp(prefix="gbx-oracle-")
    man = manifest.load()
    results = []
    for case in cases:
        if surface not in case.surfaces:
            continue
        args = [resolve_arg(a, tmpdir) for a in case.args]
        fn = man.functions[case.fn]
        params = fn.params
        schema = ", ".join(
            f"c{i} {_ddl_type(v, params[i])}" for i, v in enumerate(args)
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
            spark.createDataFrame([tuple(args)], schema).createOrReplaceTempView(
                "_gbx_oracle"
            )
            out = spark.sql(f"SELECT {call} AS out FROM _gbx_oracle").first()["out"]
            results.append((case, check(case, out, args)))
        except (
            Exception
        ) as exc:  # noqa: BLE001 - the case decides whether an error is expected
            results.append((case, check_error(case, exc)))
    return results
