"""MVP probes as plain functions: each returns a JSON-able dict that the notebook prints."""

from __future__ import annotations

import json
import os
import re
import time
from collections import Counter
from typing import List, Optional, Sequence, Tuple


def write_notebook(
    path: str, call: str, kwargs: dict, wheel: str, extras: str = ""
) -> None:
    """Three cells: %pip the staged env wheel (the interactive two-step install), restart, print(<call>(spark, **kwargs)).

    The env wheel is the light install, so it takes only feature extras (``extras``, e.g. ``"stac"``), never a
    ``light_*`` name. The Serverless runner strips the two ``%pip`` cells and installs the wheel itself.
    """
    spec = f"geobrix[{extras}]" if extras else "geobrix"
    pip = (
        f'%pip install --force-reinstall --no-deps "geobrix @ file://{wheel}"\n'
        f'%pip install "{spec} @ file://{wheel}"'
    )
    run = (
        "import json\nfrom databricks.labs.gbx.bench import probes\n"
        f"print(json.dumps(probes.{call}(spark, **{kwargs!r}), default=str, indent=1))"
    )
    cells = [pip, "dbutils.library.restartPython()", run]
    nb = {
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "source": c,
                "outputs": [],
                "execution_count": None,
            }
            for c in cells
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    with open(path, "w") as fh:
        json.dump(nb, fh, indent=1)


def _sha256(path: str) -> str:
    import hashlib

    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def run_detect(spark) -> dict:
    """The capabilities, the level each name registers at, and the installed wheel's identity."""
    import importlib.metadata as md

    from databricks.labs.gbx import register
    from databricks.labs.gbx.manager import capabilities

    try:
        from databricks.labs.gbx import _build_info  # only in an env wheel
    except ImportError:
        _build_info = None
    caps = capabilities.detect(spark, need_jvm=True)
    choices = register(spark)
    dist = md.distribution("geobrix")
    return {
        "caps": {**caps.__dict__, "jar": caps.jar and caps.jar.__dict__},
        "choices": {k: v.__dict__ for k, v in choices.items()},
        "wheel": {
            "version": dist.version,
            "git_sha": getattr(_build_info, "GIT_SHA", None),
        },
    }


def run_ek1(spark, n: int = 1_000_000) -> dict:
    """E-K1: registerJavaFunction from Python on standard; called from SQL and call_function; vs L3."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx import register

    name = "gbx_bng_eastnorthasbng"
    out = {}
    pts = spark.range(n).select(
        (F.rand(7) * 700000).alias("e"), (F.rand(8) * 1300000).alias("n")
    )
    pts.createOrReplaceTempView("ek1_pts")
    for lv in (("L2", "-"), ("L3", "c0")):
        try:
            register(spark, only=[name], _levels={name: lv})
            t = time.time()
            # the call in a subquery (a Java UDF is not deterministic, so it cannot sit inside an aggregate) and
            # the hash reduced before the sum (a sum of raw 64-bit hashes overflows under ANSI): the leg's own shape
            spark.sql(
                f"SELECT sum(pmod(xxhash64(c), 2147483647)) FROM (SELECT {name}(e, n, 3) AS c FROM ek1_pts)"
            ).collect()
            out[f"{lv[0]}_sql_s"] = time.time() - t
            out[f"{lv[0]}_call_function"] = (
                pts.limit(1)
                .select(F.call_function(name, "e", "n", F.lit(3)))
                .first()[0]
            )
        except Exception as exc:  # noqa: BLE001 - a refusal is a measured outcome
            out[f"{lv[0]}_error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
    try:
        register(
            spark,
            only=["gbx_rst_transformcrs"],
            _levels={"gbx_rst_transformcrs": ("L2", "-")},
        )
        out["L2_raster"] = "registered"
    except (
        Exception
    ) as exc:  # noqa: BLE001 - expected on plain standard (no GDAL in the sandbox)
        out["L2_raster_error"] = f"{type(exc).__name__}: {str(exc)[:500]}"
    return out


def run_etvf(spark, src: str) -> dict:
    """E-TVF: a JVM generator installed as a table function, called with a TABLE argument (expected refused)."""
    from databricks.labs.gbx import register

    name = "gbx_bng_tessellateexplode"
    choice = register(spark, only=[name])
    out = {"choice": {k: v.__dict__ for k, v in choice.items()}}
    try:
        n = spark.sql(
            f"SELECT count(*) FROM {name}(TABLE(SELECT wkt AS geom, 3 AS resolution FROM {src}))"
        ).first()[0]
        out["table_arg"] = f"WORKS: {n} rows"
    except Exception as exc:  # noqa: BLE001 - the refusal is the measured outcome
        out["table_arg_error"] = f"{type(exc).__name__}: {str(exc)[:600]}"
    return out


_ECHO = (
    "attrs = sorted(a for a in dir(g) if not a.startswith('_'))[:40]\n"
    "return f'{type(g).__module__}.{type(g).__name__}|{repr(g)[:160]}|{attrs}'"
)
_OUT = "import shapely\ng = shapely.set_srid(shapely.from_wkt(w), 4326)\nreturn shapely.to_wkb(g, include_srid=True)"


def run_pgeom(spark, catalog: str, schema: str) -> dict:
    """P-GEOM (OD-15): what a UC Python function receives for GEOMETRY, and whether it can return one."""
    q = f"`{catalog}`.`{schema}`"
    out = {}
    for decl in ("GEOMETRY(ANY)", "GEOMETRY(4326)", "GEOMETRY"):
        name = "gbx_mvp_pgeom_in_" + "".join(ch for ch in decl.lower() if ch.isalnum())
        try:
            spark.sql(
                f"CREATE OR REPLACE FUNCTION {q}.{name}(g {decl}) RETURNS STRING LANGUAGE PYTHON "
                f"ENVIRONMENT (environment_version = '6') AS $$\n{_ECHO}\n$$"
            )
            out[decl] = spark.sql(
                f"SELECT {q}.{name}(st_geomfromwkt('POINT(1 2)', 4326))"
            ).first()[0]
        except Exception as exc:  # noqa: BLE001
            out[decl] = f"ERROR {type(exc).__name__}: {str(exc)[:400]}"
    try:
        spark.sql(
            f"CREATE OR REPLACE FUNCTION {q}.gbx_mvp_pgeom_out(w STRING) RETURNS GEOMETRY LANGUAGE PYTHON "
            "ENVIRONMENT (dependencies = '[\"shapely==2.1.2\"]', environment_version = '6') "
            f"AS $$\n{_OUT}\n$$"
        )
        out["return"] = (
            spark.sql(
                f"SELECT st_srid({q}.gbx_mvp_pgeom_out('POINT(1 2)')) AS srid, "
                f"st_astext({q}.gbx_mvp_pgeom_out('POINT(1 2)')) AS wkt"
            )
            .first()
            .asDict()
        )
    except Exception as exc:  # noqa: BLE001
        out["return"] = f"ERROR {type(exc).__name__}: {str(exc)[:400]}"
    return out


def run_uc_s6(spark, catalog: str, schema: str, volume_dir: str, tmpdir: str) -> dict:
    """gbx.uc.install from env 6, the probe batch form, then the MVP-S and MVP-G oracles through 3-level names."""
    from databricks.labs.gbx import __version__ as ver
    from databricks.labs.gbx import manifest, register, uc
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.uc import ddl

    t = time.time()
    stmts = uc.install(catalog, schema, volume_dir, spark=spark)
    out = {"install_s": time.time() - t, "statements": len(stmts)}
    man = manifest.load()
    wheel = uc._single_wheel(volume_dir)
    out["wheel"] = wheel
    out["wheel_sha256"] = _sha256(wheel)
    for s in ddl.function_ddl(
        man.functions["gbx_bng_eastnorthasbng"],
        catalog,
        schema,
        "file://" + wheel,
        man.uc_dependencies,
        ver.split("+", 1)[0],
        style="batch",
        name="gbx_mvp_bng_eastnorthasbng_batch",
    ):
        spark.sql(s)
    register(spark, target="uc", catalog=catalog, schema=schema)
    res = oracles.run_session(
        spark,
        oracles.MVP_S,
        name_of=lambda f: f"`{catalog}`.`{schema}`.{f}",
        tmpdir=tmpdir,
        surface="uc",
    )
    out["failures"] = [m for _, m in res if m]
    out["cases"] = len(res)
    return out


# The grid and vector tables of the first slice, and the tables of the raster, cell, aggregate and vector forms that
# came after it. Each table here has UC rows; the MVP tables and SPEC_SE run from their own files.
UC_CORE_TABLES = ("GRID_SCALAR", "GRID_ARRAY", "GRID_GENERATOR", "VECTOR_SCALAR")
UC_TIER_B_TABLES = (
    "RASTER_ACCESSOR",
    "RASTER_STAT",
    "SPEC_ANALYSIS",
    "RASTER_TILE_OP",
    "SPEC_WARP",
    "SPEC_CALC",
    "SPEC_DEM",
    "RASTER_ARRAY_OP",
    "RASTER_RASTERIZE",
    "RASTER_VECTORIZE",
    "RASTER_CONSTRUCTOR",
    "WEB_TILES",
    "RASTER_AGGREGATE",
    "VECTOR_GENERATOR",
    "CELL_AGGREGATE",
    "RASTER_TO_GRID",
    "RASTER_GENERATOR",
    "VECTOR_AGGREGATE",
    "RASTER_FILE_OP",
)
UC_SLICE_TABLES = UC_CORE_TABLES + UC_TIER_B_TABLES


def uc_slice_cases(tables: Sequence[str] = UC_SLICE_TABLES) -> list:
    """Every UC row of the slice tables: a row of a function that has an L4 level, and a recipe row (a statement over
    UC functions, such as the aggregate recipes, whose function has no L4 level of its own).
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench import oracles

    man = manifest.load()
    return [
        c
        for tbl in tables
        for c in oracles.TABLES[tbl]
        if "uc" in c.surfaces
        and (c.sql is not None or "L4" in man.functions[c.fn].levels)
    ]


def run_uc_slice(
    spark,
    catalog: str,
    schema: str,
    volume_dir: str,
    tmpdir: str,
    jar: Optional[str] = None,
    tables: Sequence[str] = UC_SLICE_TABLES,
) -> dict:
    """The Unity Catalog slice on a warehouse or Serverless env 6: install, every UC row of the slice tables through
    3-level names, then the same SQL text with unqualified names (S3).

    1. times ``gbx.uc.install`` (a JAR in ``volume_dir``, or ``jar``, makes the JVM-only BNG functions Scala
       functions; ``jar`` names a Volume JAR, so a folder that holds only the wheel serves the Python form and the
       Scala form from the same install);
    2. runs every UC row of the slice tables (scalars and recipes through ``run_session``, the table functions through
       ``run_rows``) against ``catalog.schema.<name>``;
    3. ``register(spark, target='uc', ...)`` (the schema is at this wheel's version), drops the session's temporary
       ``gbx_`` functions (a session function outranks a schema function of the same bare name, and the pass would
       then never reach UC), then ``USE catalog.schema`` and the same rows with the bare names: the same text must
       give the same answers. Run it in a session that needs no session ``gbx_`` functions afterwards.

    Prints ``[T28 slice] <step> START`` and ``DONE <s>s ...`` per step and a RUNNING line every 60 s.

    Returns ``{"install_s", "statements", "scala", "failures", "cases", "unqualified_failures", "unqualified_cases"}``;
    ``scala`` counts the functions created on the JAR.
    """
    from databricks.labs.gbx import manifest, register, uc
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.uc_perf import _Run

    man = manifest.load()
    cases = uc_slice_cases(tables)
    run_ = _Run("T28 slice", "t28-slice", 4, 7200.0)
    t = time.time()
    stmts, err, _ = run_.step(
        "install",
        lambda: uc.install(catalog, schema, volume_dir, spark=spark, jar=jar),
    )
    if err:
        run_.stop()
        raise RuntimeError(f"gbx.uc.install failed: {err}")
    out = {
        "install_s": time.time() - t,
        "statements": len(stmts),
        "scala": sum("LANGUAGE SCALA" in s for s in stmts),
    }

    def is_rows(c):
        return man.functions[c.fn].kind == "generator" and c.sql is None

    def run(name_of):
        scalar = [c for c in cases if not is_rows(c)]
        rows = [c for c in cases if is_rows(c)]
        res = oracles.run_session(
            spark, scalar, name_of=name_of, tmpdir=tmpdir, surface="uc"
        )
        res += oracles.run_rows(
            spark, rows, name_of=name_of, tmpdir=tmpdir, surface="uc"
        )
        return res

    try:
        res, err, _ = run_.step(
            "qualified", lambda: run(lambda f: f"`{catalog}`.`{schema}`.{f}")
        )
        out["failures"] = [err] if err else [m for _, m in res if m]
        out["cases"] = 0 if err else len(res)

        def unqualified():
            register(spark, target="uc", catalog=catalog, schema=schema)
            for fn in (n for n, f in man.functions.items() if "L4" in f.levels):
                spark.sql(f"DROP TEMPORARY FUNCTION IF EXISTS {fn}")
            spark.sql(f"USE `{catalog}`.`{schema}`")
            return run(lambda f: f)

        res, err, _ = run_.step("unqualified", unqualified)
        out["unqualified_failures"] = [err] if err else [m for _, m in res if m]
        out["unqualified_cases"] = 0 if err else len(res)
    finally:
        run_.stop()
    return out


def uc_probe_sql(
    catalog: str, schema: str, jar_path: str, n: int = 1_000_000
) -> List[Tuple[str, str]]:
    """E-UCScala + the OD-18 batch form + per-statement timing at n rows, for gbx-uc-sql on the warehouse."""
    q = f"`{catalog}`.`{schema}`"
    gen = f"(SELECT rand(7) * 700000 AS e, rand(8) * 1300000 AS n FROM range({n}))"
    env = f"ENVIRONMENT (java_dependencies = '[\"{jar_path}\"]', environment_version = '6')"
    handler = "HANDLER 'com.databricks.labs.gbx.kernels.uc.BngUc"
    return [
        (
            "ucscala_create",
            f"CREATE OR REPLACE FUNCTION {q}.gbx_mvp_bng_eastnorthasbng_jvm(easting DOUBLE, "
            f"northing DOUBLE, resolution STRING) RETURNS STRING LANGUAGE SCALA {env} "
            f"{handler}.eastNorthAsBng'",
        ),
        (
            "ucscala_gdal_create",
            f"CREATE OR REPLACE FUNCTION {q}.gbx_mvp_gdal_version_jvm() RETURNS STRING "
            f"LANGUAGE SCALA {env} {handler}.gdalVersion'",
        ),
        (
            "ucscala_oracle",
            f"SELECT {q}.gbx_mvp_bng_eastnorthasbng_jvm(530000d, 180000d, '3')",
        ),
        ("ucscala_gdal", f"SELECT {q}.gbx_mvp_gdal_version_jvm()"),
        (
            "t_ucpython",
            f"SELECT count(*), sum(xxhash64({q}.gbx_bng_eastnorthasbng(e, n, '3'))) FROM {gen}",
        ),
        (
            "t_ucpython_batch",
            f"SELECT count(*), sum(xxhash64({q}.gbx_mvp_bng_eastnorthasbng_batch(e, n, '3'))) FROM {gen}",
        ),
        (
            "t_ucscala",
            f"SELECT count(*), sum(xxhash64({q}.gbx_mvp_bng_eastnorthasbng_jvm(e, n, '3'))) FROM {gen}",
        ),
        ("t_select1", "SELECT 1"),
    ]


def run_smallagg_input(spark, corpus: str, table: str, n: int = 1000) -> dict:
    """The SMALL-AGG input: `n` corpus row tiles (512 x 512, 4 bands) of ONE CRS as `(i, raster)` rows in a Delta
    table, so a warehouse statement can feed groups of 10..n of them to a UC Python finalize.
    """
    import os

    from databricks.labs.gbx._gdal_policy import open_data
    from databricks.labs.gbx.bench.levels_forms import _corpus_rows

    paths, crs0 = [], None
    for p in _corpus_rows(corpus) or []:
        with open_data(p) as ds:
            crs = ds.crs.to_epsg() if ds.crs else None
        crs0 = crs if crs0 is None else crs0
        if crs == crs0:
            paths.append(p)
        if len(paths) >= n:
            break
    # The rows go through the client: refuse before reading the files if they would be too many bytes.
    from databricks.labs.gbx.bench.clientcap import check_estimate, client_dataframe

    check_estimate("small-agg input", 1, sum(os.path.getsize(p) for p in paths))
    rows = []
    for i, p in enumerate(paths):
        with open(p, "rb") as fh:
            rows.append((i, os.path.basename(p), bytearray(fh.read())))
    client_dataframe(
        spark, rows, "i int, name string, raster binary", what="small-agg input"
    ).write.mode("overwrite").saveAsTable(table)
    return {"table": table, "tiles": len(rows), "epsg": crs0}


def uc_smallagg_sql(
    catalog: str,
    schema: str,
    wheel_path: str,
    deps: dict,
    table: str,
    sizes: Sequence[int] = (10, 50, 200, 1000),
) -> List[Tuple[str, str]]:
    """SMALL-AGG (OD-20): collect_list of a group's rasters into a UC Python finalize (the light merge), per group size."""
    q = f"`{catalog}`.`{schema}`"
    env = json.dumps(
        [f"geobrix @ file://{wheel_path}"]
        + [f"{k}=={deps[k]}" for k in ("rasterio", "pyproj", "shapely")]
    )
    body = "from databricks.labs.gbx.pyrx.core.agg import merge_tiles\nreturn merge_tiles([bytes(r) for r in rasters])"
    stmts = [
        (
            "smallagg_create",
            f"CREATE OR REPLACE FUNCTION {q}.gbx_mvp_rst_merge_uc(rasters ARRAY<BINARY>) RETURNS BINARY "
            f"LANGUAGE PYTHON ENVIRONMENT (dependencies = '{env}', environment_version = '6') AS $$\n{body}\n$$",
        )
    ]
    for s in sizes:
        stmts.append(
            (
                f"t_smallagg_{s}",
                f"SELECT count(*), sum(length(m)) FROM (SELECT {q}.gbx_mvp_rst_merge_uc(collect_list(raster)) AS m "
                f"FROM (SELECT raster FROM {table} WHERE i < {s}))",
            )
        )
    return stmts


# ---- P2: the tile sizes a Unity Catalog Python function accepts (Task 23, D-P2) ----

P2_FN = "gbx_mvp_p2_tile_echo"
P2_TMP_FN = "gbx_mvp_p2_tmp_write"
P2_PYSPARK_FN = "gbx_mvp_p2_import_pyspark"
P2_SIZES = (
    ("64KiB", 64 << 10),
    ("1MiB", 1 << 20),  # the reference tile: a 512 x 512 Float32 ZSTD GeoTIFF
    ("16MiB", 16 << 20),
    ("64MiB", 64 << 20),
    ("256MiB", 256 << 20),
)

# The handler returns the tile it got with one metadata key set; the key's value says what arrived.
_P2_ECHO = (
    "d = t.asDict(recursive=True) if hasattr(t, 'asDict') else dict(t)\n"
    "raster = d.get('raster')\n"
    "md = dict(d.get('metadata') or {})\n"
    "md['p2'] = (f'{type(t).__module__}.{type(t).__name__}|raster={type(raster).__name__}:'\n"
    "            f'{-1 if raster is None else len(raster)}|metadata={type(d.get(\"metadata\")).__name__}')\n"
    "d['metadata'] = md\n"
    "return d"
)
_P2_TMP = (
    "import os\n"
    "try:\n"
    "    p = '/tmp/gbx_p2_probe.bin'\n"
    "    data = os.urandom(n)\n"
    "    with open(p, 'wb') as fh:\n"
    "        fh.write(data)\n"
    "    with open(p, 'rb') as fh:\n"
    "        back = fh.read()\n"
    "    os.remove(p)\n"
    "    return f'ok {len(back)} {back == data}'\n"
    "except Exception as exc:\n"
    "    return f'ERROR {type(exc).__name__}: {str(exc)[:200]}'"
)
_P2_PYSPARK = (
    "try:\n"
    "    import pyspark\n"
    "    return f'importable {pyspark.__version__}'\n"
    "except ImportError as exc:\n"
    "    return f'not importable: {str(exc)[:200]}'"
)


def p2_error_class(message: str) -> str:
    """The class of a failed P2 call: ``row_size``, ``memory``, ``timeout``, ``type`` or ``other``."""
    m = (message or "").lower()
    if any(
        k in m
        for k in (
            "row size",
            "too large",
            "exceeds the limit",
            "2147483647",
            "message_too_large",
            "payload",
            "size limit",
            "maximum size",
        )
    ):
        return "row_size"
    if any(
        k in m for k in ("outofmemory", "out of memory", "memoryerror")
    ) or re.search(r"\boom", m):
        return "memory"
    if any(k in m for k in ("timeout", "timed out", "deadline", "exceeded the time")):
        return "timeout"
    if any(
        k in m
        for k in ("unsupported", "not supported", "datatype", "return type", "struct")
    ):
        return "type"
    return "other"


def p2_parse_call(value: str, want_bytes: int) -> dict:
    """The cell a P2 tile call answers (``<out bytes>|<what the handler received>|<in == out>``) as fields.

    What the handler received has ``|`` in it, so the first and last parts are the byte count and the equality.
    """
    parts = str(value).split("|")
    out_bytes, same = parts[0], parts[-1]
    return {
        "received": "|".join(parts[1:-1]),
        "out_bytes": int(out_bytes) if out_bytes.isdigit() else out_bytes,
        "roundtrip": same == "true" and out_bytes == str(want_bytes),
        "raw": str(value)[:300],
    }


def p2_tile_bytes(label: str, n: int) -> bytes:
    """The raster bytes of one P2 tile: the 1 MiB reference is a 512 x 512 Float32 ZSTD GeoTIFF, the rest are opaque bytes."""
    import numpy as np

    rng = np.random.default_rng(2)
    if label != "1MiB":
        return rng.bytes(n)
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    with MemoryFile() as mem:
        with mem.open(
            driver="GTiff",
            width=512,
            height=512,
            count=1,
            dtype="float32",
            crs="EPSG:32633",
            transform=from_origin(500000.0, 5000000.0, 10.0, 10.0),
            compress="zstd",
        ) as dst:
            dst.write(rng.random((512, 512), dtype=np.float32), 1)
        return mem.read()


def p2_stage_tiles(tile_dir: str, sizes=P2_SIZES) -> dict:
    """Write each tile's raster bytes to ``<tile_dir>/p2_<label>.bin`` (kept when it already holds the same bytes)."""
    import os

    os.makedirs(tile_dir, exist_ok=True)
    out = {}
    for label, n in sizes:
        path = f"{tile_dir}/p2_{label}.bin"
        data = p2_tile_bytes(label, n)
        have = os.path.getsize(path) if os.path.exists(path) else -1
        if have != len(data):
            with open(path + ".part", "wb") as fh:
                fh.write(data)
            os.replace(path + ".part", path)
        out[label] = {"path": path, "bytes": len(data)}
    return out


def p2_statements(
    catalog: str,
    schema: str,
    tile_dir: str,
    wheel_uri: str,
    deps: dict,
    sizes=P2_SIZES,
) -> List[Tuple[str, str]]:
    """The P2 statements as ``(cid, sql)``: the three CREATEs, one call per tile size (the tile is read from its
    file with ``read_files`` and wrapped in the v2 tile struct), the ``/tmp`` call, the ``import pyspark`` call
    and the DROPs. The Serverless notebook and the warehouse run the same text.
    """
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import quote_ident, quote_str

    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    env = quote_str(
        json.dumps(
            [f"geobrix @ {wheel_uri}"]
            + [f"{k}=={deps[k]}" for k in ("rasterio", "pyproj", "shapely")]
        )
    )
    spec = f"ENVIRONMENT (dependencies = {env}, environment_version = '6')"

    def create(name, params, returns, body):
        return (
            f"CREATE OR REPLACE FUNCTION {q}.{name}({params}) RETURNS {returns} LANGUAGE PYTHON\n"
            f"{spec}\nAS $$\n{body}\n$$"
        )

    out = [
        ("p2_create_echo", create(P2_FN, f"t {TILE_SQL}", TILE_SQL, _P2_ECHO)),
        ("p2_create_tmp", create(P2_TMP_FN, "n INT", "STRING", _P2_TMP)),
        ("p2_create_pyspark", create(P2_PYSPARK_FN, "d INT", "STRING", _P2_PYSPARK)),
    ]
    for label, _n in sizes:
        tile = (
            "CAST(named_struct('cellid', CAST(0 AS BIGINT), 'raster', content, 'path', NULL, 'path_mode', NULL, "
            "'window', NULL, 'clip_polygon', NULL, 'clip_crs', NULL, 'crs', 'EPSG:32633', "
            f"'metadata', map('src', 'p2')) AS {TILE_SQL})"
        )
        src = f"read_files({quote_str(f'{tile_dir}/p2_{label}.bin')}, format => 'binaryFile')"
        out.append(
            (
                f"p2_call_{label}",
                "SELECT concat_ws('|', CAST(length(r.t.raster) AS STRING), r.t.metadata['p2'], "
                "CAST(r.t.raster = r.content AS STRING)) AS out "
                f"FROM (SELECT content, {q}.{P2_FN}({tile}) AS t FROM {src}) r",
            )
        )
    out += [
        ("p2_call_tmp", f"SELECT {q}.{P2_TMP_FN}(1048576) AS out"),
        ("p2_call_pyspark", f"SELECT {q}.{P2_PYSPARK_FN}(0) AS out"),
    ]
    out += [
        (f"p2_drop_{n}", f"DROP FUNCTION IF EXISTS {q}.{n}")
        for n in (P2_FN, P2_TMP_FN, P2_PYSPARK_FN)
    ]
    return out


def run_p2_tiles(spark, catalog: str, schema: str, volume_dir: str) -> dict:
    """P2 (D-P2): which tile sizes a UC Python function takes and returns, whether it can write ``/tmp``, and
    whether ``pyspark`` imports there. Run it on Serverless env 6; the warehouse runs the same statements
    (``p2_statements``) through ``gbx-uc-sql``. ``volume_dir`` holds the one env 6 wheel (the functions'
    dependency) and gets a ``p2_tiles/`` folder with the tile files. The probe functions are dropped at the end.

    Prints ``[P2] <step> START`` and ``[P2] <step> DONE <s>s ok|<error class>`` per step, and a RUNNING line every
    60 s through ``liveness.Heartbeat``.

    Returns ``{"wheel", "sizes": {label: {"bytes", "create_ok", "call_ok", "received", "out_bytes", "roundtrip",
    "wall_s", "error_class", "error"}}, "tmp_writable", "tmp", "pyspark_importable", "pyspark", "dropped"}``.
    """
    import os
    import tempfile

    from databricks.labs.gbx import manifest, uc
    from databricks.labs.gbx.bench import liveness

    wheel = uc._single_wheel(volume_dir)
    stmts = dict(
        p2_statements(
            catalog,
            schema,
            f"{volume_dir.rstrip('/')}/p2_tiles",
            "file://" + wheel,
            manifest.load().uc_dependencies,
        )
    )
    hb = liveness.Heartbeat(
        os.path.join(tempfile.gettempdir(), "p2-heartbeat.json"),
        interval=30,
        phase="p2",
        print_every=60,
    ).start()
    steps = (
        ["stage"]
        + ["create"]
        + [lab for lab, _ in P2_SIZES]
        + ["tmp", "pyspark", "drop"]
    )

    def step(name, fn):
        hb.begin(name, steps.index(name), len(steps), set_name="P2")
        print(f"[P2] {name} START", flush=True)
        t = time.time()
        err = None
        try:
            val = fn()
        except Exception as exc:  # noqa: BLE001 - the error is the measurement
            val, err = None, f"{type(exc).__name__}: {str(exc)[:600]}"
        wall = time.time() - t
        cls = p2_error_class(err) if err else None
        print(
            f"[P2] {name} DONE {wall:.1f}s {'ok' if err is None else cls}", flush=True
        )
        hb.end(status=None if err is None else cls)
        return val, err, wall, cls

    out = {"wheel": wheel, "sizes": {}}
    try:
        staged, err, _, _ = step(
            "stage",
            lambda: p2_stage_tiles(f"{volume_dir.rstrip('/')}/p2_tiles"),
        )
        out["staged"] = staged if err is None else err

        def create_echo():
            spark.sql(stmts["p2_create_echo"])

        _, create_err, _, _ = step("create", create_echo)
        for label, n in P2_SIZES:
            if create_err:
                out["sizes"][label] = {
                    "bytes": n,
                    "create_ok": False,
                    "call_ok": False,
                    "error": create_err,
                    "error_class": p2_error_class(create_err),
                }
                continue
            val, err, wall, cls = step(
                label, lambda: spark.sql(stmts[f"p2_call_{label}"]).first()[0]
            )
            rec = {
                "bytes": (
                    (staged.get(label) or {}).get("bytes", n)
                    if isinstance(staged, dict)
                    else n
                ),
                "create_ok": True,
                "call_ok": err is None,
                "wall_s": round(wall, 2),
                "error_class": cls,
                "error": err,
            }
            if err is None:
                rec.update(p2_parse_call(val, rec["bytes"]))
            out["sizes"][label] = rec
        for key, name in (("tmp", "p2_create_tmp"), ("pyspark", "p2_create_pyspark")):

            def probe(key=key, name=name):
                spark.sql(stmts[name])
                return spark.sql(stmts[f"p2_call_{key}"]).first()[0]

            val, err, _, _ = step(key, probe)
            out[key] = val if err is None else f"ERROR {err}"
        out["tmp_writable"] = str(out.get("tmp", "")).startswith("ok 1048576 True")
        out["pyspark_importable"] = str(out.get("pyspark", "")).startswith("importable")
    finally:
        dropped = []

        def drop():
            for n in (P2_FN, P2_TMP_FN, P2_PYSPARK_FN):
                spark.sql(stmts[f"p2_drop_{n}"])
                dropped.append(n)

        step("drop", drop)
        out["dropped"] = dropped
        hb.stop()
    return out


# --- the governed ETL procedures (uc.procedures) --------------------------------------------------------------------
PROC_PREFIX = "gbxproc_"


def _proc_norm(value):
    """A cell as a comparable value: bytes by digest, floats rounded, structs and arrays by content."""
    import hashlib

    if isinstance(value, (bytes, bytearray)):
        return "sha:" + hashlib.sha256(bytes(value)).hexdigest()[:16]
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, (list, tuple)):
        return [_proc_norm(v) for v in value]
    if isinstance(value, dict):
        return {k: _proc_norm(v) for k, v in sorted(value.items())}
    if hasattr(value, "asDict"):
        return _proc_norm(value.asDict(recursive=True))
    return value


def _proc_pixels(blob, decimals: int = 9) -> list:
    """The pixels of a GeoTIFF as nested lists (None for no bytes): two tiles are the same when their pixels are.

    ``decimals`` rounds the values: a tile that went through a Float32 step (the average and the standard deviation of
    ``gbx_combine_by_key``) equals the session's Float64 value to about seven digits, not nine. A pixel with no data
    is None, whatever the NoData value of the tile.
    """
    if blob is None:
        return None
    from databricks.labs.gbx._gdal_policy import open_bytes

    with open_bytes(bytes(blob)) as ds:
        masks = ds.read_masks(1).tolist()
        return [
            [round(float(x), decimals) if m else None for x, m in zip(row, mrow)]
            for row, mrow in zip(ds.read(1).tolist(), masks)
        ]


def _proc_mvt_features(blob) -> list:
    """The features of an MVT layer as sorted (WKT-free) (geometry, properties) pairs, for a tile compare."""
    import json as _json

    import mapbox_vector_tile as mvt

    out = []
    for layer in mvt.decode(
        bytes(blob), default_options={"y_coord_down": True}
    ).values():
        for f in layer["features"]:
            out.append(_json.dumps([f["geometry"], f["properties"]], sort_keys=True))
    return sorted(out)


def proc_cases(catalog: str, schema: str) -> list:
    """The small input of each probed recipe and the session-SQL flow its output is compared with.

    Each case is ``{"name", "tables", "args", "setup": [SQL], "out", "ref", "compare"}``: ``setup`` creates the inputs
    (SQL statements), ``args`` are the named arguments of the ``CALL``, ``out`` reads the table the call wrote, ``ref`` is the same flow in session SQL (unqualified ``gbx_`` names), and ``compare`` is ``rows``,
    ``pixels`` (a column of GeoTIFF bytes) or ``mvt`` (the features of a layer).
    """
    import h3

    q = f"`{catalog}`.`{schema}`"

    def t(name):
        return f"{q}.{PROC_PREFIX}{name}"

    def lit(name):
        return (
            "'" + t(name).replace("`", "") + "'"
        )  # a table name as the STRING a procedure takes

    square = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
    bowtie = "POLYGON((0 0, 2 2, 2 0, 0 2, 0 0))"
    cases = []

    # polygons -> BNG
    cases.append(
        {
            "name": "gbx_polygons_to_bng",
            "tables": ["bng_src", "bng_tgt"],
            "setup": [
                f"CREATE OR REPLACE TABLE {t('bng_src')} (id INT, geom GEOMETRY(27700))",
                f"INSERT INTO {t('bng_src')} VALUES (1, st_geomfromwkt('POLYGON((530000 180000, 530300 180000, "
                f"530300 180300, 530000 180300, 530000 180000))', 27700))",
                f"DROP TABLE IF EXISTS {t('bng_tgt')}",
            ],
            "args": {
                "source_table": lit("bng_src"),
                "target_table": lit("bng_tgt"),
                "bng_resolution": "'100m'",
            },
            "out": f"SELECT id, cellid, core, chip FROM {t('bng_tgt')} ORDER BY cellid, core",
            "ref": f"SELECT s.id, c.cellid, c.core, c.chip FROM {t('bng_src')} AS s, "
            f"LATERAL gbx_bng_tessellateexplode(st_asewkb(s.geom), '100m', false) AS c ORDER BY cellid, core",
            "compare": "rows",
            "geometries": ["chip"],
        }
    )
    # clean geometries
    cases.append(
        {
            "name": "gbx_clean_geometries",
            "tables": ["cg_src", "cg_tgt", "cg_rej"],
            "args": {
                "source_table": lit("cg_src"),
                "target_table": lit("cg_tgt"),
                "rejects_table": lit("cg_rej"),
            },
            "setup": [
                f"CREATE OR REPLACE TABLE {t('cg_src')} (id INT, geom GEOMETRY(4326))",
                f"INSERT INTO {t('cg_src')} VALUES (1, st_geomfromwkt('{bowtie}', 4326)), "
                f"(2, st_geomfromwkt('{square}', 4326)), (3, NULL)",
                f"DROP TABLE IF EXISTS {t('cg_tgt')}",
                f"DROP TABLE IF EXISTS {t('cg_rej')}",
            ],
            "out": f"SELECT id, st_astext(geom) AS wkt FROM {t('cg_tgt')} ORDER BY id",
            "ref": "SELECT id, st_astext(st_geomfromwkb(g)) AS wkt FROM (SELECT id, "
            f"gbx_st_makevalid(st_asbinary(geom), 'linework') AS g FROM {t('cg_src')} WHERE geom IS NOT NULL) "
            "WHERE g IS NOT NULL AND st_isvalid(st_geomfromwkb(g)) ORDER BY id",
            "compare": "rows",
            "also": (
                f"SELECT id, reason FROM {t('cg_rej')} ORDER BY id",
                f"SELECT id, 'the geometry is NULL' AS reason FROM {t('cg_src')} WHERE geom IS NULL ORDER BY id",
            ),
        }
    )
    # migrate Mosaic geometry
    legacy = (
        "named_struct('typeId', 5, 'srid', 4326, "
        "'boundaries', array(array(array(0D, 0D), array(10D, 0D), array(10D, 10D), array(0D, 10D), array(0D, 0D))), "
        "'holes', CAST(array() AS array<array<array<array<double>>>>))"
    )
    cases.append(
        {
            "name": "gbx_migrate_mosaic_geometry",
            "tables": ["mig_src", "mig_tgt"],
            "args": {"source_table": lit("mig_src"), "target_table": lit("mig_tgt")},
            "setup": [
                f"CREATE OR REPLACE TABLE {t('mig_src')} AS SELECT 1 AS id, {legacy} AS geom",
                f"DROP TABLE IF EXISTS {t('mig_tgt')}",
            ],
            "out": f"SELECT id, st_astext(geom) AS wkt FROM {t('mig_tgt')} ORDER BY id",
            "ref": f"SELECT id, st_astext(st_geomfromwkb(gbx_st_legacyaswkb(geom))) AS wkt FROM {t('mig_src')} ORDER BY id",
            "compare": "rows",
        }
    )
    # points -> H3 surface: a cell with no value among cells that have one, and cells no point fell in. The ring around
    # the centre in order: every other cell has a value, so each absent one sits between two that do and is filled
    # only when the procedure adds the absent cells (fill_rings).
    centre = h3.latlng_to_cell(37.77, -122.42, 8)
    around = [sorted(h3.grid_ring(centre, 1))[0]]
    while len(around) < 6:
        around.append(
            next(
                c
                for c in sorted(h3.grid_ring(centre, 1))
                if c not in around and h3.are_neighbor_cells(c, around[-1])
            )
        )
    pts = [(centre, None)] + [(c, float(i + 1)) for i, c in enumerate(around[::2])]
    values = ", ".join(
        f"({h3.cell_to_latlng(c)[1]!r}, {h3.cell_to_latlng(c)[0]!r}, "
        f"{'NULL' if v is None else repr(v)})"
        for c, v in pts
    )
    cases.append(
        {
            "name": "gbx_points_to_h3_surface",
            "tables": ["h3_src", "h3_tgt"],
            "args": {
                "source_table": lit("h3_src"),
                "target_table": lit("h3_tgt"),
                "surface_name": "'probe'",
                "h3_resolution": "8",
            },
            "setup": [
                f"CREATE OR REPLACE TABLE {t('h3_src')} (longitude DOUBLE, latitude DOUBLE, value DOUBLE)",
                f"INSERT INTO {t('h3_src')} VALUES {values}",
                f"DROP TABLE IF EXISTS {t('h3_tgt')}",
            ],
            "out": f"SELECT surface, tile FROM {t('h3_tgt')}",
            "ref": None,  # built in Python: the fill payload of the session aggregate, then the session rasterizer
            "compare": "pixels",
            "column": "tile",
        }
    )
    # features -> MVT: two points in one tile of each zoom
    cases.append(
        {
            "name": "gbx_features_to_mvt",
            "tables": ["mvt_src", "mvt_tgt"],
            "args": {
                "source_table": lit("mvt_src"),
                "target_table": lit("mvt_tgt"),
                "mvt_layer": "'layer'",
                "min_zoom": "0",
                "max_zoom": "1",
            },
            "setup": [
                f"CREATE OR REPLACE TABLE {t('mvt_src')} (geom GEOMETRY(4326), attrs STRUCT<n: INT>)",
                f"INSERT INTO {t('mvt_src')} VALUES (st_geomfromwkt('POINT (100 45)', 4326), named_struct('n', 1)), "
                f"(st_geomfromwkt('POINT (101 46)', 4326), named_struct('n', 2))",
                f"DROP TABLE IF EXISTS {t('mvt_tgt')}",
            ],
            "out": f"SELECT z, x, y, mvt FROM {t('mvt_tgt')} ORDER BY z, x, y",
            "ref": f"SELECT p.z, p.x, p.y, p.mvt_bytes AS mvt FROM {t('mvt_src')} AS s, "
            "LATERAL gbx_st_asmvt_pyramid(st_asbinary(s.geom), s.attrs, 0, 1, 'layer', 4096) AS p ORDER BY z, x, y",
            "compare": "mvt",
        }
    )
    return cases


def _proc_call(name: str, args: dict) -> str:
    return f"CALL {name}(" + ", ".join(f"{k} => {v}" for k, v in args.items()) + ")"


# The keys of the tile inputs. A key of more than 8 tiles needs the levels of the tree procedures: "wide" has 12 (two
# levels), "deep" has 70 (three levels).
_TREE_WIDE, _TREE_DEEP = 12, 70
# The ops of gbx_combine_by_key the probe runs, and the decimals the compare keeps (the average and the standard
# deviation come back Float32 from the map algebra, the session's Float64 rounded).
_COMBINE_PROBE = (
    ("avg", 3), ("sum", 9), ("count", 9), ("min", 9), ("max", 9), ("stddev", 3),
)  # fmt: skip


# The key of the combine input that holds more tiles than one UC function call takes (12 > 8).
_COMBINE_WIDE_KEY = "many"


def _wide_stats_expr(prefix: str, tile: str) -> str:
    """One string of a tile's size and band statistics (rounded to 3 digits): the cell both surfaces write for the key
    that the UC reference cannot combine in one call. ``prefix`` is the schema qualifier ("" in a session).
    """
    parts = [
        f"CAST({prefix}gbx_rst_width({tile}) AS STRING)",
        f"CAST({prefix}gbx_rst_height({tile}) AS STRING)",
    ]
    parts += [
        f"to_json(transform({prefix}gbx_rst_{fn}({tile}), x -> round(x, 3)))"
        for fn in ("avg", "min", "max")
    ]
    return "concat_ws('|', " + ", ".join(parts) + ")"


def _wide_session_stats(spark, catalog: str, schema: str, label: str) -> dict:
    """``{"wide_stats": ..}`` (or ``{"wide_error": ..}``): the band statistics of the wide key combined in one session
    call, which is what the warehouse leg cannot compute and ``compare_combine_many`` holds its target to.
    """
    op = label.split(":", 1)[1]
    src = f"`{catalog}`.`{schema}`.{PROC_PREFIX}cmb_src"
    try:
        value = spark.sql(
            f"SELECT {_wide_stats_expr('', 'c')} FROM (SELECT gbx_rst_combine{op}(collect_list(tile)) AS c "
            f"FROM {src} WHERE tile_key = '{_COMBINE_WIDE_KEY}')"
        ).first()[0]
        return {"wide_stats": value}
    except Exception as exc:  # noqa: BLE001 - recorded, the run goes on
        return {"wide_error": f"{type(exc).__name__}: {str(exc)[:300]}"}


def compare_combine_many(warehouse: dict, serverless: dict) -> List[str]:
    """The warehouse's band statistics of the wide combine key against the Serverless run's session result, per op.

    ``warehouse`` is the results file of the procedure statements (``gbx-uc-sql --out``), ``serverless`` the dict
    ``run_procedures`` returned. One line per op that differs or is missing; an empty list means they agree.
    """
    bad = []
    for op, _decimals in _COMBINE_PROBE:
        got = (
            warehouse.get(f"gbx_combine_by_key:{op}:{_COMBINE_WIDE_KEY}_stats") or {}
        ).get("value")
        want = (
            (serverless.get("recipes") or {}).get(f"gbx_combine_by_key:{op}") or {}
        ).get("wide_stats")
        if got is None or want is None:
            bad.append(
                f"{op}: missing ({'warehouse' if got is None else 'serverless'} result)"
            )
        elif str(got) != str(want):
            bad.append(f"{op}: warehouse {got} != session {want}")
    return bad


def _proc_tile_inputs():
    """The tile inputs of the tile cases: ``([(tile_key, tile)], zone tile)``, each tile the v2 tile dict of a 4 x 4
    GeoTIFF. Session and warehouse build the same bytes. The mosaic keys: ``a`` (two tiles side by side), ``b`` (one),
    ``wide`` and ``deep`` (more than the 8 a function call takes, side by side without overlap).
    """
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff, _tile

    def tile(origin_x, origin_y, base):
        arr = (np.arange(16, dtype="float64").reshape(1, 4, 4)) + base
        return _tile(_gtiff(arr, "EPSG:3857", origin=(origin_x, origin_y), px=10.0))

    mosaic = [
        ("a", tile(0.0, 40.0, 0)),
        ("a", tile(40.0, 40.0, 100)),
        ("b", tile(0.0, 40.0, 200)),
    ]
    mosaic += [("wide", tile(40.0 * i, 40.0, 16 * i)) for i in range(_TREE_WIDE)]
    mosaic += [("deep", tile(40.0 * i, 40.0, i)) for i in range(_TREE_DEEP)]
    return mosaic, tile(0.0, 100.0, 0)


def _proc_combine_inputs():
    """The input of ``gbx_combine_by_key``: ``[(tile_key, tile)]`` of aligned 4 x 4 tiles with holes. ``one`` has a
    single tile (kept as it is), ``few`` three (one function call), ``many`` twelve (two levels). The pixel (0, 0) has
    no data in any member; every odd member has no data at (1, 1)."""
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff, _tile

    def tile(base, i):
        arr = (
            np.arange(16, dtype="float64").reshape(1, 4, 4) * (1 + 0.25 * (i % 3))
            + base
        )
        arr[0, 0, 0] = -9999.0
        if i % 2:
            arr[0, 1, 1] = -9999.0
        return _tile(_gtiff(arr, "EPSG:3857", origin=(0.0, 40.0), px=10.0))

    return (
        [("one", tile(7, 0))]
        + [("few", tile(10 * i, i)) for i in range(3)]
        + [("many", tile(3 * i, i)) for i in range(12)]
    )


def _proc_tile_case_defs(t, lit) -> list:
    """The tile cases as dicts (no session needed): mosaic by key, one case per op of combine by key, zonal stats.

    ``label`` is the key of the case's record in the run's result (the procedure name, and the op for a combine).
    The reference of a case is the same flow over the session functions on the same input.
    """
    cases = [
        {
            "name": "gbx_mosaic_by_key",
            "tables": ["mos_src", "mos_tgt"],
            "args": {"source_table": lit("mos_src"), "target_table": lit("mos_tgt")},
            "out": f"SELECT tile_key, tile FROM {t('mos_tgt')} ORDER BY tile_key",
            "ref": f"SELECT tile_key, gbx_rst_merge(collect_list(tile)) AS tile FROM {t('mos_src')} "
            "GROUP BY tile_key ORDER BY tile_key",
            "compare": "pixels",
            "column": "tile",
        }
    ]
    for op, decimals in _COMBINE_PROBE:
        tgt = f"cmb_{op}"
        cases.append(
            {
                "name": "gbx_combine_by_key",
                "label": f"gbx_combine_by_key:{op}",
                "tables": ["cmb_src", tgt],
                "args": {
                    "source_table": lit("cmb_src"),
                    "target_table": lit(tgt),
                    "op": f"'{op}'",
                },
                "out": f"SELECT tile_key, tile FROM {t(tgt)} ORDER BY tile_key",
                "ref": f"SELECT tile_key, gbx_rst_combine{op}(collect_list(tile)) AS tile FROM {t('cmb_src')} "
                "GROUP BY tile_key ORDER BY tile_key",
                "compare": "pixels",
                "column": "tile",
                "decimals": decimals,
            }
        )
    cases.append(
        {
            "name": "gbx_zonal_stats",
            "tables": ["zs_tiles", "zs_zones", "zs_tgt"],
            "args": {
                "zones_table": lit("zs_zones"),
                "source_table": lit("zs_tiles"),
                "target_table": lit("zs_tgt"),
            },
            "out": f"SELECT zone_id, tile_id, summary FROM {t('zs_tgt')} ORDER BY zone_id, tile_id",
            "ref": "SELECT z.zone_id, t.tile_id, gbx_rst_summary(gbx_rst_clip(t.tile, st_asewkb(z.geom), true)) "
            f"AS summary FROM {t('zs_zones')} AS z JOIN {t('zs_tiles')} AS t ON st_intersects(z.geom, t.bbox) "
            "ORDER BY zone_id, tile_id",
            "compare": "rows",
        }
    )
    return cases


def _proc_tile_cases(spark, catalog: str, schema: str, tables: list = None) -> list:
    """The tile-input cases (mosaic by key, combine by key, zonal statistics): their input tables are built from
    GeoTIFF bytes, through ``client_dataframe`` (the guarded builder of Spark Connect inputs).

    Each table is added to ``tables`` (when given) before it is created, so a failure part-way still drops it.
    """
    from databricks.labs.gbx.bench.clientcap import client_dataframe
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    tiles, zone_bytes = _proc_tile_inputs()
    zone_tile = [(1, zone_bytes, "POLYGON((0 0,100 0,100 100,0 100,0 0))")]
    q = f"`{catalog}`.`{schema}`"

    def t(name):
        return f"{q}.{PROC_PREFIX}{name}"

    def lit(name):
        return "'" + t(name).replace("`", "") + "'"

    def made(*names):
        for name in names:
            if tables is not None and t(name) not in tables:
                tables.append(t(name))

    made(
        "mos_src", "mos_tgt", "cmb_src", "zs_tiles", "zs_zones", "zs_tgt",
        *(f"cmb_{op}" for op, _d in _COMBINE_PROBE),
    )  # fmt: skip
    client_dataframe(
        spark,
        tiles,
        "tile_key string, tile " + V2_TILE_SCHEMA.simpleString(),
        what="mosaic tile input",
    ).write.mode("overwrite").saveAsTable(t("mos_src").replace("`", ""))
    spark.sql(f"DROP TABLE IF EXISTS {t('mos_tgt')}")
    client_dataframe(
        spark,
        _proc_combine_inputs(),
        "tile_key string, tile " + V2_TILE_SCHEMA.simpleString(),
        what="combine tile input",
    ).write.mode("overwrite").saveAsTable(t("cmb_src").replace("`", ""))
    for op, _d in _COMBINE_PROBE:
        spark.sql(f"DROP TABLE IF EXISTS {t('cmb_' + op)}")
    client_dataframe(
        spark,
        zone_tile,
        "tile_id int, tile " + V2_TILE_SCHEMA.simpleString() + ", bbox_wkt string",
        what="zonal tile input",
    ).createOrReplaceTempView("gbxproc_tiles_tmp")
    spark.sql(
        f"CREATE OR REPLACE TABLE {t('zs_tiles')} AS SELECT tile_id, tile, st_geomfromwkt(bbox_wkt, 3857) AS bbox "
        "FROM gbxproc_tiles_tmp"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE {t('zs_zones')} (zone_id INT, geom GEOMETRY(3857))"
    )
    spark.sql(
        f"INSERT INTO {t('zs_zones')} VALUES (1, st_geomfromwkt('POLYGON((0 0,50 0,50 50,0 50,0 0))', 3857)), "
        "(2, st_geomfromwkt('POLYGON((900 900,950 900,950 950,900 950,900 900))', 3857))"
    )
    spark.sql(f"DROP TABLE IF EXISTS {t('zs_tgt')}")
    return _proc_tile_case_defs(t, lit)


def _proc_h3_reference(spark, source: str) -> list:
    """The session flow of the H3 surface: the cells within one ring of a cell added with no value (the h3 library, not
    the product function the procedure calls), the session cell-fill aggregate, then the session rasterize aggregate.
    """
    import h3

    from databricks.labs.gbx.bench.clientcap import client_dataframe
    from databricks.labs.gbx.pygx import _cellfill

    base = spark.sql(
        f"SELECT h3_longlatash3(longitude, latitude, 8) AS cellid, avg(value) AS value FROM {source} GROUP BY 1"
    ).collect()
    cells = {r["cellid"]: r["value"] for r in base}
    padded = dict(cells)
    for cell in cells:
        for nb in h3.grid_disk(h3.int_to_str(cell), 1):
            padded.setdefault(h3.str_to_int(nb), None)
    client_dataframe(
        spark,
        sorted(padded.items()),
        "cellid bigint, value double",
        what="H3 padded cells",
    ).createOrReplaceTempView("gbxproc_h3_padded")
    payload = spark.sql(
        "SELECT gbx_h3_cellfill(cellid, value, 1, 'mean', 2.0) FROM gbxproc_h3_padded"
    ).first()[0]
    filled = _cellfill.decode(bytes(payload))
    client_dataframe(
        spark, filled, "cellid bigint, value double", what="H3 filled cells"
    ).createOrReplaceTempView("gbxproc_h3_filled")
    nulls = (
        ", ".join(["CAST(NULL AS DOUBLE)"] * 5)
        + ", CAST(NULL AS INT), CAST(NULL AS INT)"
    )
    return spark.sql(
        f"SELECT 'probe' AS surface, gbx_rst_h3_rasterize_agg(cellid, value, 4326, {nulls}, 'centroids', 0) AS tile "
        "FROM gbxproc_h3_filled"
    ).collect()


def _diff_sql(out: str, ref: str) -> str:
    """One number: the rows in ``out`` or in ``ref`` but not in both (0: the same rows)."""
    return (
        f"SELECT (SELECT count(*) FROM (({out}) EXCEPT ({ref}))) + "
        f"(SELECT count(*) FROM (({ref}) EXCEPT ({out}))) AS diff"
    )


def _merge_tree_sql(q: str, source: str, largest_key: int) -> str:
    """The merged tile of each key of ``source`` (columns tile_key, tile) from the UC ``gbx_rst_merge``, which takes at
    most TREE_FAN_IN tiles per call: the tiles of a key are merged in buckets of that size, level by level, until a key
    has no more than one call's worth, then once more. ``largest_key`` is the most tiles any key holds. The merge of
    tiles that do not overlap does not depend on their order, so the reference of the procedure's own tree is exact.
    """
    from databricks.labs.gbx.uc.procedures import TREE_FAN_IN

    level = f"SELECT tile_key, tile FROM {source}"
    n = largest_key
    while n > TREE_FAN_IN:
        level = (
            f"SELECT tile_key, {q}.gbx_rst_merge(collect_list(tile)) AS tile FROM ("
            "SELECT tile_key, tile, (row_number() OVER (PARTITION BY tile_key "
            f"ORDER BY xxhash64(tile.raster, tile.path)) - 1) DIV {TREE_FAN_IN} AS bucket FROM ({level})"
            ") GROUP BY tile_key, bucket"
        )
        n = -(-n // TREE_FAN_IN)
    return f"SELECT tile_key, {q}.gbx_rst_merge(collect_list(tile)) AS c FROM ({level}) GROUP BY tile_key"


def proc_warehouse_statements(
    catalog: str, schema: str, tmpdir: str
) -> List[Tuple[str, str]]:
    """The procedure probe as ``(cid, sql)`` for ``gbx-uc-sql``: per recipe the input tables, the ``CALL``, the rows
    the call wrote, and the ``diff`` against the same flow as plain SQL over the UC functions (0: equal). Then the
    MVT tiles themselves, a struct ``attrs`` call, the DDL types and routine comments the Serverless leg cannot read
    after its cleanup, the Zarr tag, history and untag on a synthetic manifest, and the DROPs.

    The functions and procedures are installed in ``catalog.schema`` already; run the file with
    ``--catalog``/``--schema`` set to them.
    """
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import quote_str
    from databricks.labs.gbx.zarr import _manifest as M

    q = f"`{catalog}`.`{schema}`"
    out: List[Tuple[str, str]] = []

    def t(name):
        return f"{q}.{PROC_PREFIX}{name}"

    def lit(name):
        return "'" + t(name).replace("`", "") + "'"

    def call(name, args):
        return (
            f"CALL {q}.{name}("
            + ", ".join(f"{k} => {v}" for k, v in args.items())
            + ")"
        )

    def case(name, setup, args, out_sql, ref_sql, also=None, label=None):
        label = (
            label or name
        )  # the statement ids; a procedure with several cases (combine, per op) has a label each
        for i, stmt in enumerate(setup):
            out.append((f"{label}:setup{i}", stmt))
        out.append((f"{label}:call", call(name, args)))
        out.append((f"{label}:rows", f"SELECT count(*) AS n FROM ({out_sql})"))
        out.append((f"{label}:diff", _diff_sql(out_sql, ref_sql)))
        if also:
            out.append((f"{label}:diff_also", _diff_sql(*also)))

    by_name = {c["name"]: c for c in proc_cases(catalog, schema)}
    # polygons -> BNG
    c = by_name["gbx_polygons_to_bng"]
    case(
        c["name"], c["setup"], c["args"],
        f"SELECT id, cellid, core, chip FROM {t('bng_tgt')}",
        f"SELECT s.id, c.cellid, c.core, c.chip FROM {t('bng_src')} AS s, "
        f"LATERAL {q}.gbx_bng_tessellateexplode(CAST(s.geom AS GEOMETRY(ANY)), '100m', false) AS c",
    )  # fmt: skip
    # clean geometries: the repaired rows and the rejects
    c = by_name["gbx_clean_geometries"]
    case(
        c["name"], c["setup"], c["args"],
        f"SELECT id, st_astext(geom) AS wkt FROM {t('cg_tgt')}",
        f"SELECT id, st_astext(g) AS wkt FROM (SELECT id, {q}.gbx_st_makevalid(geom, 'linework') AS g "
        f"FROM {t('cg_src')} WHERE geom IS NOT NULL) WHERE g IS NOT NULL AND st_isvalid(g)",
        also=(
            f"SELECT id, reason FROM {t('cg_rej')}",
            f"SELECT id, 'the geometry is NULL' AS reason FROM {t('cg_src')} WHERE geom IS NULL",
        ),
    )  # fmt: skip
    out.append(
        (
            "gbx_clean_geometries:types",
            "SELECT concat_ws('|', column_name, data_type) FROM "
            f"`{catalog}`.information_schema.columns WHERE table_schema = {quote_str(schema)} "
            f"AND table_name = '{PROC_PREFIX}cg_tgt' AND column_name = 'geom'",
        )
    )
    # migrate Mosaic geometry
    c = by_name["gbx_migrate_mosaic_geometry"]
    case(
        c["name"], c["setup"], c["args"],
        f"SELECT id, st_astext(geom) AS wkt FROM {t('mig_tgt')}",
        f"SELECT id, st_astext({q}.gbx_st_legacyaswkb(geom)) AS wkt FROM {t('mig_src')}",
    )  # fmt: skip
    # points -> H3 surface (the tile is BINARY on UC): the plain SQL flow pads the cells with the product h3_kring too
    c = by_name["gbx_points_to_h3_surface"]
    cells = f"SELECT h3_longlatash3(longitude, latitude, 8) AS cellid, avg(value) AS value FROM {t('h3_src')} GROUP BY 1"
    padded = (
        f"WITH c AS ({cells}) SELECT cellid, max(value) AS value FROM (SELECT cellid, value FROM c UNION ALL "
        "SELECT explode(h3_kring(cellid, 1)) AS cellid, CAST(NULL AS DOUBLE) AS value FROM c) GROUP BY cellid"
    )
    case(
        c["name"], c["setup"], c["args"],
        f"SELECT surface, sha2(tile, 256) AS h FROM {t('h3_tgt')}",
        f"SELECT 'probe' AS surface, sha2({q}.gbx_rst_h3_rasterize({q}.gbx__h3_cellfill_cells(g.cells, 1, 'mean', 2.0), "
        "4326, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), "
        "CAST(NULL AS DOUBLE), CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 0, CAST(NULL AS STRING)), 256) AS h "
        f"FROM (SELECT collect_list(struct(cellid, value)) AS cells FROM ({padded})) AS g",
    )  # fmt: skip
    # features -> MVT
    c = by_name["gbx_features_to_mvt"]
    case(
        c["name"], c["setup"], c["args"],
        f"SELECT z, x, y, sha2(mvt, 256) AS h FROM {t('mvt_tgt')}",
        f"SELECT p.z, p.x, p.y, sha2({q}.gbx__st_asmvt_features(collect_list(p.mvt_bytes), 'layer', 4096), 256) AS h "
        f"FROM {t('mvt_src')} AS s, LATERAL {q}.gbx_st_asmvt_pyramid(CAST(s.geom AS GEOMETRY(ANY)), to_json(s.attrs), "
        "0, 1, 'layer', 4096) AS p GROUP BY p.z, p.x, p.y",
    )  # fmt: skip
    for z, x, y in ((0, 0, 0), (1, 1, 0)):
        out.append(
            (
                f"gbx_features_to_mvt:tile_{z}_{x}_{y}",
                f"SELECT mvt FROM {t('mvt_tgt')} WHERE z = {z} AND x = {x} AND y = {y}",
            )
        )
    out.append(
        (
            "gbx_st_asmvt_pyramid:struct_attrs",
            f"SELECT count(*) FROM {q}.gbx_st_asmvt_pyramid(st_geomfromwkt('POINT (100 45)', 4326), "
            "named_struct('n', 1), 0, 1, 'layer', 4096)",
        )
    )
    # tile inputs: the bytes of the session probe, as hex literals
    mosaic, zone_bytes = _proc_tile_inputs()

    def tile_sql(tile):
        return (
            "CAST(named_struct('cellid', CAST(0 AS BIGINT), "
            f"'raster', X'{bytes(tile['raster']).hex()}', 'path', CAST(NULL AS STRING), 'path_mode', CAST(NULL AS STRING), "
            "'window', NULL, 'clip_polygon', NULL, 'clip_crs', CAST(NULL AS STRING), 'crs', CAST(NULL AS STRING), "
            f"'metadata', CAST(map() AS MAP<STRING, STRING>)) AS {TILE_SQL})"
        )

    values = ", ".join(f"('{k}', {tile_sql(v)})" for k, v in mosaic)
    # the bytes of a mosaic of mosaics can differ from the flat mosaic's (the encoding follows the size), the pixels
    # do not: the compare is the size and the band statistics
    stat = lambda tile: ", ".join(  # noqa: E731
        [f"{q}.gbx_rst_width({tile}) AS w", f"{q}.gbx_rst_height({tile}) AS h"]
        + [
            f"transform({q}.gbx_rst_{fn}({tile}), x -> round(x, 3)) AS {fn}"
            for fn in ("avg", "min", "max")
        ]
    )
    case(
        "gbx_mosaic_by_key",
        [
            f"CREATE OR REPLACE TABLE {t('mos_src')} (tile_key STRING, tile {TILE_SQL})",
            f"INSERT INTO {t('mos_src')} VALUES {values}",
            f"DROP TABLE IF EXISTS {t('mos_tgt')}",
        ],
        {"source_table": lit("mos_src"), "target_table": lit("mos_tgt")},
        f"SELECT tile_key, {stat('tile')} FROM {t('mos_tgt')}",
        f"SELECT tile_key, {stat('c')} FROM ({_merge_tree_sql(q, t('mos_src'), max(Counter(k for k, _ in mosaic).values()))})",
    )  # fmt: skip
    # combine by key: one source table, one CALL and one stats compare per op (the bytes of a tile that went through the
    # map algebra differ from the session's, the band statistics do not)
    for i, (op, _decimals) in enumerate(_COMBINE_PROBE):
        setup = []
        if i == 0:
            cvalues = ", ".join(
                f"('{k}', {tile_sql(v)})" for k, v in _proc_combine_inputs()
            )
            setup = [
                f"CREATE OR REPLACE TABLE {t('cmb_src')} (tile_key STRING, tile {TILE_SQL})",
                f"INSERT INTO {t('cmb_src')} VALUES {cvalues}",
            ]
        setup.append(f"DROP TABLE IF EXISTS {t('cmb_' + op)}")
        # The reference is one UC combine call per key, which takes at most 8 tiles: the key past that (``many``, 12
        # tiles, two levels of the procedure's tree) is left out of the diff, and its band statistics are written as
        # one cell for `compare_combine_many` to hold against the session result of the Serverless run.
        case(
            "gbx_combine_by_key",
            setup,
            {"source_table": lit("cmb_src"), "target_table": lit("cmb_" + op), "op": f"'{op}'"},
            f"SELECT tile_key, {stat('tile')} FROM {t('cmb_' + op)} WHERE tile_key <> '{_COMBINE_WIDE_KEY}'",
            f"SELECT tile_key, {stat('c')} FROM (SELECT tile_key, {q}.gbx_rst_combine{op}(collect_list(tile)) AS c "
            f"FROM {t('cmb_src')} WHERE tile_key <> '{_COMBINE_WIDE_KEY}' GROUP BY tile_key)",
            label=f"gbx_combine_by_key:{op}",
        )  # fmt: skip
        out.append(
            (
                f"gbx_combine_by_key:{op}:{_COMBINE_WIDE_KEY}_stats",
                f"SELECT {_wide_stats_expr(q + '.', 'tile')} AS stats FROM {t('cmb_' + op)} "
                f"WHERE tile_key = '{_COMBINE_WIDE_KEY}'",
            )
        )
    case(
        "gbx_zonal_stats",
        [
            f"CREATE OR REPLACE TABLE {t('zs_tiles')} (tile_id INT, tile {TILE_SQL}, bbox GEOMETRY(3857))",
            f"INSERT INTO {t('zs_tiles')} VALUES (1, {tile_sql(zone_bytes)}, "
            "st_geomfromwkt('POLYGON((0 0,100 0,100 100,0 100,0 0))', 3857))",
            f"CREATE OR REPLACE TABLE {t('zs_zones')} (zone_id INT, geom GEOMETRY(3857))",
            f"INSERT INTO {t('zs_zones')} VALUES (1, st_geomfromwkt('POLYGON((0 0,50 0,50 50,0 50,0 0))', 3857)), "
            "(2, st_geomfromwkt('POLYGON((900 900,950 900,950 950,900 950,900 900))', 3857))",
            f"DROP TABLE IF EXISTS {t('zs_tgt')}",
        ],
        {"zones_table": lit("zs_zones"), "source_table": lit("zs_tiles"), "target_table": lit("zs_tgt")},
        f"SELECT zone_id, tile_id, summary FROM {t('zs_tgt')}",
        f"SELECT z.zone_id, t.tile_id, {q}.gbx_rst_summary({q}.gbx_rst_clip(t.tile, CAST(z.geom AS GEOMETRY(ANY)), true, "
        f"CAST(NULL AS STRING))) AS summary FROM {t('zs_zones')} AS z JOIN {t('zs_tiles')} AS t "
        "ON st_intersects(z.geom, t.bbox)",
    )  # fmt: skip
    # the catalog's view of the procedures: name, type and comment
    out.append(
        (
            "information_schema:procedures",
            f"SELECT count(*) AS n FROM `{catalog}`.information_schema.routines WHERE routine_schema = "
            f"{quote_str(schema)} AND routine_type = 'PROCEDURE' AND comment LIKE 'geobrix %'",
        )
    )
    out.append(
        (
            "information_schema:one",
            f"SELECT concat_ws('|', routine_name, routine_type, comment) FROM `{catalog}`.information_schema.routines "
            f"WHERE routine_schema = {quote_str(schema)} AND routine_name = 'gbx_zonal_stats'",
        )
    )
    # Zarr: a synthetic manifest of one group, one array and one commit

    class _Recorder:
        def __init__(self):
            self.stmts = []

        def sql(self, text):
            self.stmts.append(text)

    zm = f"{catalog}.{schema}.{PROC_PREFIX}zm_wh"
    rec = _Recorder()
    M.create_manifest(
        rec, zm, M.StoreProps("managed", tmpdir, None, True, "fuse", "7 days")
    )
    zq = f"`{catalog}`.`{schema}`.{PROC_PREFIX}zm_wh"
    out += [
        ("zarr:drop_first", f"DROP TABLE IF EXISTS {zq}"),
        ("zarr:create", rec.stmts[0].strip()),
        (
            "zarr:insert",
            f"INSERT INTO {zq} (kind, array, key, write_id, meta) VALUES "
            "('group', NULL, '.', 'w1', NULL), ('array', 'v', 'v/.zarray', 'w1', NULL), "
            "('commit', NULL, 'w1', 'w1', '{\"mode\": \"create\"}')",
        ),
        (
            "zarr:tag",
            f"CALL {q}.gbx_zarr_tag_version('{zq.replace(chr(96), '')}', 'probe')",
        ),
        (
            "zarr:tagged",
            f"SELECT concat_ws(',', collect_list(concat(kind, ':', coalesce(array, '-'), ':', key, ':', write_id))) "
            f"FROM (SELECT * FROM {zq} WHERE tag = 'probe' ORDER BY kind, array, key)",
        ),
        (
            "zarr:tag_again",
            f"CALL {q}.gbx_zarr_tag_version('{zq.replace(chr(96), '')}', 'probe')",
        ),
        (
            "zarr:history",
            f"CALL {q}.gbx_zarr_version_history('{zq.replace(chr(96), '')}')",
        ),
        (
            "zarr:untag",
            f"CALL {q}.gbx_zarr_untag_version('{zq.replace(chr(96), '')}', 'probe')",
        ),
        ("zarr:left", f"SELECT count(*) FROM {zq} WHERE tag = 'probe'"),
    ]
    tables = [
        f"{PROC_PREFIX}{x}"
        for x in (
            "bng_src bng_tgt cg_src cg_tgt cg_rej mig_src mig_tgt h3_src h3_tgt mvt_src mvt_tgt "
            "mos_src mos_tgt cmb_src zs_tiles zs_zones zs_tgt zm_wh"
        ).split()
    ] + [f"{PROC_PREFIX}cmb_{op}" for op, _d in _COMBINE_PROBE]
    out += [(f"drop:{n}", f"DROP TABLE IF EXISTS {q}.{n}") for n in tables]
    return out


def _proc_geometry(blob):
    """A WKB or EWKB geometry as normalised text (None for no bytes): the UC functions return EWKB with the SRID
    where the session functions return WKB, and the geometry is what has to agree."""
    if blob is None:
        return None
    import shapely

    return shapely.to_wkt(
        shapely.normalize(shapely.from_wkb(bytes(blob))), rounding_precision=6
    )


def _proc_rows(rows, case) -> list:
    """The rows of a result in a comparable form, by the case's ``compare``.

    ``geometries`` names the WKB columns compared as geometries. The ``mvt`` rows are merged per tile (z, x, y): the
    session function returns one tile per feature, the procedure one tile with the features of all of them.
    """
    kind = case["compare"]
    out = []
    for r in rows:
        d = r.asDict(recursive=True)
        if kind == "pixels":
            col = d[case["column"]]
            blob = col["raster"] if isinstance(col, dict) else col
            d[case["column"]] = _proc_pixels(blob, case.get("decimals", 9))
        elif kind == "mvt":
            d["mvt"] = _proc_mvt_features(d["mvt"])
        for col in case.get("geometries", ()):
            d[col] = _proc_geometry(d[col])
        out.append(_proc_norm(d))
    if kind == "mvt":
        merged = {}
        for d in out:
            merged.setdefault((d["z"], d["x"], d["y"]), []).extend(d["mvt"])
        out = [
            {"z": z, "x": x, "y": y, "mvt": sorted(f)}
            for (z, x, y), f in sorted(merged.items())
        ]
    return out


def _proc_status(got, want) -> str:
    """``match``, ``mismatch``, or ``empty`` when the two sides agree only because both are empty (nothing was
    compared, so nothing was proven)."""
    if got != want:
        return "mismatch"
    return "match" if want else "empty"


def run_procedures(
    spark,
    catalog: str,
    schema: str,
    tmpdir: str,
    volume_dir: str = None,
    install_procedures: bool = False,
) -> dict:
    """CALL each governed ETL procedure on a small input table and compare its output with the same flow in session SQL.

    ``volume_dir`` (the Volume folder with the one env 6 wheel) makes the run install the functions and the
    procedures first, with ``gbx.uc.install(..., procedures=True)``; without it they must be installed already. The
    ``install_procedures`` (without ``volume_dir``) replaces only the procedures, from this wheel's text, and leaves
    the installed functions as they are. The session functions the flows need are registered in this session. Tables are ``<catalog>.<schema>.gbxproc_*``,
    dropped at the end. ``tmpdir`` is a scratch folder (a Zarr store root).

    Prints ``[PROC] <procedure> <surface> START`` and ``[PROC] <procedure> <surface> DONE <s>s rows=<n>`` per procedure
    and per surface (``call``: the procedure; ``session``: the session flow), and a RUNNING line every 60 s through
    ``liveness.Heartbeat`` (begun with ``set_name="PROC"``).

    Returns ``{"installed", "skipped", "recipes": {procedure: {"call_s", "session_s", "rows", "match", "status",
    "diff", "error"}}, "dropped"}``. ``match`` is True only for a ``status`` of ``match``: equal and not empty; two
    empty results give ``empty``, and a rejects table compared the same way gives ``also_status``.
    """
    import os
    import tempfile

    from databricks.labs.gbx import register, uc
    from databricks.labs.gbx.bench import liveness
    from databricks.labs.gbx.uc import procedures

    hb = liveness.Heartbeat(
        os.path.join(tempfile.gettempdir(), "proc-heartbeat.json"),
        interval=30,
        phase="proc",
        print_every=60,
    ).start()
    out = {"installed": None, "skipped": {}, "recipes": {}, "dropped": []}
    counter = {"n": 0}
    total = (len(procedures.RECIPES) + len(_COMBINE_PROBE) - 1) * 2 + 2

    def step(label, surface, fn):
        """One timed step; the error is the measurement, so it is recorded, never raised."""
        hb.begin(f"{label} {surface}", counter["n"], total, set_name="PROC")
        counter["n"] += 1
        print(f"[PROC] {label} {surface} START", flush=True)
        t0 = time.time()
        val, err = None, None
        try:
            val = fn()
        except Exception as exc:  # noqa: BLE001
            err = f"{type(exc).__name__}: {str(exc)[:600]}"
        wall = time.time() - t0
        rows = len(val) if isinstance(val, list) else None
        print(
            f"[PROC] {label} {surface} DONE {wall:.1f}s rows={rows if err is None else 'ERROR'}"
            + (f" {err}" if err else ""),
            flush=True,
        )
        hb.end(status=None if err is None else "error")
        return val, err, wall

    tables = []
    try:
        if volume_dir is not None:
            stmts, err, _ = step(
                "install", "functions+procedures",
                lambda: uc.install(catalog, schema, volume_dir, spark=spark, procedures=True),
            )  # fmt: skip
            out["installed"] = len(stmts) if err is None else err
        built, built_skipped = procedures.build_all(catalog, schema)
        out["skipped"] = built_skipped
        if install_procedures and volume_dir is None:
            _, err, _ = step(
                "install", "procedures",
                lambda: [spark.sql(text) for _name, text in built],
            )  # fmt: skip
            out["installed"] = len(built) if err is None else err
        register(
            spark,
            only=[
                "gbx_bng_tessellateexplode", "gbx_st_makevalid", "gbx_st_legacyaswkb", "gbx_st_asmvt_pyramid",
                "gbx_rst_merge", "gbx_rst_clip", "gbx_rst_summary", "gbx_h3_cellfill", "gbx_rst_h3_rasterize_agg",
                *(f"gbx_rst_combine{op}" for op, _d in _COMBINE_PROBE),
            ],
        )  # fmt: skip
        cases = proc_cases(catalog, schema) + _proc_tile_cases(
            spark, catalog, schema, tables
        )
        for case in cases:
            name = case["name"]
            label = case.get(
                "label", name
            )  # a procedure with several cases (combine, one per op) has a record each
            rec = out["recipes"].setdefault(label, {})
            tables += [
                tbl
                for tbl in (
                    f"`{catalog}`.`{schema}`.{PROC_PREFIX}{x}" for x in case["tables"]
                )
                if tbl not in tables
            ]
            for stmt in case.get("setup", ()):
                spark.sql(stmt)
            _, err, wall = step(
                label, "call",
                lambda: spark.sql(
                    f"CALL `{catalog}`.`{schema}`.{name}("
                    + ", ".join(f"{k} => {v}" for k, v in case["args"].items())
                    + ")"
                ).collect(),
            )  # fmt: skip
            rec["call_s"], rec["error"] = round(wall, 2), err
            if err:
                continue
            got = _proc_rows(spark.sql(case["out"]).collect(), case)
            ref_rows, rerr, rwall = step(
                label, "session",
                lambda: (
                    _proc_h3_reference(spark, f"`{catalog}`.`{schema}`.{PROC_PREFIX}h3_src")
                    if case["ref"] is None
                    else spark.sql(case["ref"]).collect()
                ),
            )  # fmt: skip
            rec["session_s"], rec["rows"] = round(rwall, 2), len(got)
            if name == "gbx_combine_by_key":
                rec.update(_wide_session_stats(spark, catalog, schema, label))
            if rerr:
                rec["error"] = rerr
                continue
            want = _proc_rows(ref_rows, case)
            rec["status"] = _proc_status(got, want)
            rec["match"] = rec["status"] == "match"
            if rec["status"] == "mismatch":
                rec["diff"] = {"procedure": got[:3], "session": want[:3]}
            if "also" in case:  # a second output table of the same call (the rejects)
                extra_got = _proc_rows(
                    spark.sql(case["also"][0]).collect(), {"compare": "rows"}
                )
                extra_want = _proc_rows(
                    spark.sql(case["also"][1]).collect(), {"compare": "rows"}
                )
                rec["also_status"] = _proc_status(extra_got, extra_want)
                rec["match_also"] = rec["also_status"] == "match"
        out["recipes"]["zarr"] = _proc_zarr(
            spark, catalog, schema, tmpdir, step, tables
        )
    finally:

        def drop():
            for tbl in tables:
                spark.sql(f"DROP TABLE IF EXISTS {tbl}")
                out["dropped"].append(tbl)
            return out["dropped"]

        step("cleanup", "drop", drop)
        hb.stop()
    return out


def _proc_zarr(
    spark, catalog: str, schema: str, tmpdir: str, step, tables: list
) -> dict:
    """Tag, history and untag through the procedures on one manifest table, against the Python functions on a twin."""
    base = f"`{catalog}`.`{schema}`.{PROC_PREFIX}"
    a, b = base + "zm_proc", base + "zm_py"
    rec = {}
    try:
        from databricks.labs.gbx.zarr import _maint
        from databricks.labs.gbx.zarr import _manifest as M

        props = M.StoreProps("managed", tmpdir, None, True, "fuse", "7 days")
        for tbl in (a, b):
            tables.append(tbl)
            spark.sql(f"DROP TABLE IF EXISTS {tbl}")
            M.create_manifest(spark, tbl.replace("`", ""), props)
            spark.sql(
                f"INSERT INTO {tbl} (kind, array, key, write_id, meta) VALUES "
                "('group', NULL, '.', 'w1', NULL), ('array', 'v', 'v/.zarray', 'w1', NULL), "
                "('commit', NULL, 'w1', 'w1', '{\"mode\": \"create\"}')"
            )

        def tag_rows(tbl):
            return [
                tuple(r)
                for r in spark.sql(
                    f"SELECT kind, array, key, write_id FROM {tbl} WHERE tag = 'probe' ORDER BY kind, array, key"
                ).collect()
            ]

        _, err, _ = step(
            "gbx_zarr_tag_version", "call",
            lambda: spark.sql(f"CALL `{catalog}`.`{schema}`.gbx_zarr_tag_version('{a.replace(chr(96), '')}', 'probe')").collect(),
        )  # fmt: skip
        _maint.gbx_zarr_tag(spark, b.replace("`", ""), "probe")
        tagged, twin = tag_rows(a), tag_rows(b)
        rec["tag_status"] = "error" if err else _proc_status(tagged, twin)
        rec["tag_match"] = rec["tag_status"] == "match"
        rec["error"] = err
        _, err, _ = step(
            "gbx_zarr_tag_version", "call (existing tag)",
            lambda: spark.sql(f"CALL `{catalog}`.`{schema}`.gbx_zarr_tag_version('{a.replace(chr(96), '')}', 'probe')").collect(),
        )  # fmt: skip
        rec["second_tag_refused"] = err is not None and "already exists" in err
        hist, herr, _ = step(
            "gbx_zarr_version_history", "call",
            lambda: spark.sql(f"CALL `{catalog}`.`{schema}`.gbx_zarr_version_history('{a.replace(chr(96), '')}')").collect(),
        )  # fmt: skip
        rec["history_rows"] = None if herr else len(hist)
        rec["history_error"] = herr
        _, err, _ = step(
            "gbx_zarr_untag_version", "call",
            lambda: spark.sql(f"CALL `{catalog}`.`{schema}`.gbx_zarr_untag_version('{a.replace(chr(96), '')}', 'probe')").collect(),
        )  # fmt: skip
        rec["untag_ok"] = err is None and tag_rows(a) == []
        rec["untag_error"] = err
    except Exception as exc:  # noqa: BLE001 - the error is the measurement
        rec["error"] = f"{type(exc).__name__}: {str(exc)[:600]}"
    return rec


# --- the Unity Catalog performance measurements (bench.uc_perf) -------------------------------------------------------


def uc_perf_sql(
    catalog: str, schema: str, jar_path, wheel_path: str, **kw
) -> List[Tuple[str, str]]:
    """The warehouse statements of the UC performance measurements as ``(cid, sql)`` for ``gbx-uc-sql``
    (``bench.uc_perf.uc_perf_sql``): floors, SQL inlining, SQL vs Scala vs Python vs batch, sandbox sharing, raster to
    grid, cell fill, points to raster, tree merge, decode cap, zonal, chain, MVT. ``oracles.uc_sql_text`` writes the file.
    """
    from databricks.labs.gbx.bench import uc_perf

    return uc_perf.uc_perf_sql(catalog, schema, jar_path, wheel_path, **kw)


def run_uc_perf(spark, catalog: str, schema: str, volume_dir: str, **kw) -> dict:
    """The same measurements on Serverless env 6 (``bench.uc_perf.run_uc_perf``), with START / RUNNING / DONE lines."""
    from databricks.labs.gbx.bench import uc_perf

    return uc_perf.run_uc_perf(spark, catalog, schema, volume_dir, **kw)


def run_proc_warm(spark, catalog: str, schema: str, **kw) -> dict:
    """The procedure CALL cold, warm and after an idle gap, and the two target-creation forms
    (``bench.uc_perf.run_proc_warm``)."""
    from databricks.labs.gbx.bench import uc_perf

    return uc_perf.run_proc_warm(spark, catalog, schema, **kw)


# --- the batched workspace pass: register, family oracles, the JAR install route (Task 28) --------------------------


class _Tee:
    """A stdout that keeps what is written and passes it on, so a notebook shows the lines as they come."""

    def __init__(self, real):
        self.real, self.lines, self._part = real, [], ""

    def write(self, text):
        self.real.write(text)
        self._part += text
        *done, self._part = self._part.split("\n")
        self.lines += done
        return len(text)

    def flush(self):
        self.real.flush()

    def finish(self):
        if self._part:
            self.lines.append(self._part)
            self._part = ""
        return self.lines


REGISTER_DOCS_LIMIT_S = 30.0  # past this the docs recommend register(spark, only=[...])
FROMFILE_MIB = (
    128  # decoded size of the fromfile cap fixture: between the 64 MiB and 256 MiB caps
)


def run_register_all(
    spark,
    only: Optional[Sequence[str]] = None,
    fromfile_dir: Optional[str] = None,
    fromfile_mib: int = FROMFILE_MIB,
) -> dict:
    """``register(spark)`` over every name, timed: the wall time, the lines it printed, the level chosen per name and
    the names no level served here. With ``fromfile_dir`` it also reads a raster decoded to ``fromfile_mib`` MiB (between
    the 64 MiB Serverless cap and the 256 MiB classic one) through ``gbx_rst_fromfile`` in SQL and records whether the
    call was refused: the driver-side ``read_cap`` carry has no local test.

    Prints ``[T28 register] <step> START`` and ``DONE <s>s ...`` per step, and a RUNNING line every 60 s.

    Returns ``{"wall_s", "over_docs_limit", "lines", "names", "by_level": {level: [names]}, "variants",
    "reasons": {reason: count}, "skipped": [names], "fromfile": {...}}``. ``over_docs_limit`` is True when the wall
    time is over :data:`REGISTER_DOCS_LIMIT_S`: the docs then recommend ``register(spark, only=[...])``.
    """
    import contextlib
    import sys

    from databricks.labs.gbx import manifest, register
    from databricks.labs.gbx.bench.uc_perf import _Run

    run_ = _Run("T28 register", "t28-register", 2 if fromfile_dir else 1, 3600.0)
    tee = _Tee(sys.stdout)

    def do_register():
        with contextlib.redirect_stdout(tee):
            t0 = time.time()
            got = register(spark, only=list(only) if only else None)
            return got, time.time() - t0

    out: dict = {}
    try:
        res, err, wall = run_.step("register", do_register)
        out["lines"] = tee.finish()
        if err:
            out.update(error=err, wall_s=round(wall, 2))
            return out
        choices, reg_wall = res
        man = manifest.load()
        wanted = (
            [n for n, f in man.functions.items() if f.status != "mvp"]
            if not only
            else [n if n.startswith("gbx_") else f"gbx_{n}" for n in only]
        )
        by_level: dict = {}
        reasons: Counter = Counter()
        for name, ch in sorted(choices.items()):
            by_level.setdefault(ch.level, []).append(name)
            reasons.update(ch.reasons)
        out.update(
            wall_s=round(reg_wall, 2),
            over_docs_limit=reg_wall > REGISTER_DOCS_LIMIT_S,
            names=len(choices),
            by_level=by_level,
            variants={n: ch.variant for n, ch in sorted(choices.items())},
            reasons=dict(reasons),
            skipped=sorted(set(wanted) - set(choices)),
        )
        if fromfile_dir:
            out["fromfile"], _, _ = run_.step(
                "fromfile_cap", lambda: _fromfile_cap(spark, fromfile_dir, fromfile_mib)
            )
    finally:
        run_.stop()
    return out


def _fromfile_cap(spark, tile_dir: str, mib: int) -> dict:
    """Read a raster decoded to ``mib`` MiB through ``gbx_rst_fromfile`` in SQL; whether the call was refused and how."""
    from databricks.labs.gbx.bench import uc_perf

    staged = uc_perf.perf_stage_tiles(tile_dir, (mib,))[f"perf_{mib}MiB.tif"]
    rec = {"path": staged["path"], "file_bytes": staged["bytes"], "decoded_mib": mib}
    try:
        spark.sql(f"SELECT gbx_rst_fromfile('{staged['path']}') AS t").collect()
        rec.update(refused=False, error=None)
    except Exception as exc:  # noqa: BLE001 - the refusal is the measured outcome
        msg = " ".join(str(exc).split())
        rec.update(refused=True, error=msg[:400], names_cap="64" in msg)
    return rec


_OWN_FILE_TABLES = {"MVP_S", "MVP_G", "MVP_A", "MVP_H", "MVP_IO", "SPEC_SE"}
_IMPL_KEYS = ("variants", "factory", "callable", "udtf")


def _forms_at(fn, level: str) -> list:
    """The variants of ``fn`` at ``level`` that a forced-level run can register (first one is the one it uses)."""
    spec = fn.levels.get(level)
    if spec is None:
        return []
    if level == "L3":
        forms = list(spec.get("variants") or ())
        if not forms and any(k in spec for k in _IMPL_KEYS):
            forms = ["-"]
        if spec.get("engine"):
            forms += ["engine-c0", "engine-c1"]
        return forms
    if level == "L1":
        return (["-"] if "companion" in spec else []) + (
            ["engine"] if spec.get("engine") else []
        )
    return ["-"]


def _oracle_register(spark, fn, level: Optional[str], rec: dict):
    """Register ``fn`` for an oracle run: ``(level, variant, pin)`` or None (the reason is in ``rec['skipped']``)."""
    from databricks.labs.gbx import register

    name = fn.sql_name
    if level is None:
        choice = register(spark, only=[name]).get(name)
        if choice is None:
            rec["skipped"][name] = "no level can serve it here"
            return None
        return choice.level, choice.variant, {}
    forms = _forms_at(fn, level)
    if not forms:
        rec["skipped"][name] = f"no {level} form"
        return None
    pin = {"_levels": {name: (level, forms[0])}}
    try:
        register(spark, only=[name], **pin)
    except Exception as exc:  # noqa: BLE001 - recorded, the others still run
        rec["skipped"][name] = f"{level} not registrable: {str(exc)[:200]}"
        return None
    return level, forms[0], pin


def _oracle_run_cases(spark, fn, cases, tmpdir: str):
    from databricks.labs.gbx.bench import oracles

    if oracles.has_groups(fn):
        return oracles.run_groups(spark, cases, tmpdir=tmpdir)
    if fn.kind == "generator":
        return oracles.run_rows(spark, cases, tmpdir=tmpdir)
    return oracles.run_session(spark, cases, tmpdir=tmpdir)


def _oracle_capped(spark, fn, capped, pin: dict, tmpdir: str, rec: dict) -> list:
    """The rows that run with the session's read cap lowered (a Connect session cannot set it: recorded as skipped)."""
    from databricks.labs.gbx import register
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.ds.file_gbx import READ_CAP_KEY

    ran = []
    for c in capped:
        try:
            spark.conf.set(READ_CAP_KEY, str(oracles.READ_CAP_ROWS[c.cid]))
        except Exception as exc:  # noqa: BLE001
            rec["skipped"][c.cid] = f"cannot lower the read cap: {str(exc)[:120]}"
            continue
        try:
            register(spark, only=[fn.sql_name], **pin)
            ran += _oracle_run_cases(spark, fn, [c], tmpdir)
        finally:
            spark.conf.unset(READ_CAP_KEY)
            register(spark, only=[fn.sql_name], **pin)
    return ran


def _oracle_function(spark, fn, table: str, level, tmpdir: str, rec: dict, out: dict):
    """Run the cases of one function; update ``rec`` (the table's record) and ``out``."""
    from databricks.labs.gbx.bench import oracles

    cases = [
        c
        for c in oracles.TABLES[table]
        if c.fn == fn.sql_name and ("session" in c.surfaces or "python" in c.surfaces)
    ]
    chosen = _oracle_register(spark, fn, level, rec) if cases else None
    if chosen is None:
        return
    at, variant, pin = chosen
    out["levels"][fn.sql_name] = f"{at}/{variant}"
    capped = [c for c in cases if c.cid in oracles.READ_CAP_ROWS]
    ran = _oracle_run_cases(spark, fn, [c for c in cases if c not in capped], tmpdir)
    ran += _oracle_capped(spark, fn, capped, pin, tmpdir, rec)
    pending = {
        cid
        for cid, lv in oracles.HEAVY_PENDING.items()
        if at in lv and cid in {c.cid for c in cases}
    }
    red = {c.cid: m for c, m in ran if m}
    rec["functions"] += 1
    rec["cases"] += len(ran)
    for cid, m in red.items():
        if cid in pending:
            out["pending_red"].append(cid)
        else:
            out["red"][cid] = m[-500:]
            rec["failures"] += 1
    out["pending_now_green"] += sorted(pending - set(red))


def run_family_oracles(
    spark,
    tmpdir: str,
    level: Optional[str] = None,
    tables: Optional[Sequence[str]] = None,
    only: Optional[Sequence[str]] = None,
    cap_s: float = 3600.0,
) -> dict:
    """Every family oracle table through SQL on this session, one function at a time.

    ``level=None`` runs each function at the level the manager chooses here; ``level='L1'``/``'L2'``/``'L3'`` forces
    it for every function that has that level (the others are recorded as skipped, never run at another level).
    ``tables`` limits the oracle tables (``['GRID_SCALAR']``), ``only`` the function names. A row in
    ``oracles.HEAVY_PENDING`` for the level is expected to be red and is reported apart.

    Prints ``[T28 oracles] <TABLE> START`` and ``DONE <s>s ...`` per table, and a RUNNING line every 60 s.

    Returns ``{"level", "tables": {table: {"functions", "cases", "failures": n, "skipped": {fn: why}}}, "levels":
    {fn: "L3/c0"}, "red": {cid: message}, "pending_red": [cid], "pending_now_green": [cid], "failures": n}``.
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench.uc_perf import _Run

    man = manifest.load()
    wanted_only = {n if n.startswith("gbx_") else f"gbx_{n}" for n in (only or ())}
    by_table: dict = {}
    for fn in sorted(man.functions.values(), key=lambda f: f.sql_name):
        if (
            fn.status != "stable"
            or fn.oracle in _OWN_FILE_TABLES
            or (tables and fn.oracle not in tables)
            or (wanted_only and fn.sql_name not in wanted_only)
        ):
            continue
        by_table.setdefault(fn.oracle, []).append(fn)
    run_ = _Run("T28 oracles", "t28-oracles", len(by_table), cap_s)
    out: dict = {
        "level": level or "manager",
        "tables": {},
        "levels": {},
        "red": {},
        "pending_red": [],
        "pending_now_green": [],
    }

    def one_table(table, fns):
        rec = out["tables"][table] = {
            "functions": 0,
            "cases": 0,
            "failures": 0,
            "skipped": {},
        }
        for fn in fns:
            _oracle_function(spark, fn, table, level, tmpdir, rec, out)
        return dict(rec)

    try:
        for table, fns in by_table.items():
            run_.step(f"{table}", lambda t=table, f=fns: one_table(t, f))
    finally:
        run_.stop()
    out["failures"] = len(out["red"])
    return out


P1_STATES_DONE = ("INSTALLED", "FAILED", "SKIPPED", "UNINSTALL_ON_RESTART")


def _p1_library(jar_path: str):
    from databricks.sdk.service.compute import Library

    return Library(jar=jar_path)


def _p1_caps(spark) -> dict:
    from databricks.labs.gbx.manager import capabilities

    capabilities.forget(spark)
    caps = capabilities.detect(spark, need_jvm=True)
    return {
        **caps.__dict__,
        "jar": caps.jar and caps.jar.__dict__,
        "jar_usable": caps.jar is not None,
    }


def _p1_status(client, cluster_id: str, jar_path: str) -> dict:
    for st in client.libraries.cluster_status(cluster_id):
        lib = getattr(st, "library", None)
        if getattr(lib, "jar", None) == jar_path:
            status = getattr(st.status, "value", st.status)
            return {"status": str(status), "messages": list(st.messages or [])}
    return {"status": "NOT_LISTED", "messages": []}


def run_p1(
    spark,
    cluster_id: str,
    jar_path: str,
    phase: str = "install",
    client=None,
    wait_s: float = 900.0,
    poll_s: float = 10.0,
    restart: bool = True,
) -> dict:
    """P1 (D-P1): can a JAR installed as a library through the Libraries API on a RUNNING cluster be used at L1?

    A restart ends the session this runs in, so the probe has two phases, each its own notebook run on the cluster:

    * ``phase='install'``: ``capabilities.detect`` before; ``libraries.install`` of ``jar_path`` on ``cluster_id``;
      wait for the library to reach INSTALLED or FAILED; ``detect`` again with no restart (``at_once``). It prints
      ``[T28 P1] RESULT <json>``. When the JAR is not usable yet and ``restart`` is True it then restarts the cluster
      (the run ends with it; the line above is the record).
    * ``phase='after'``: on the restarted cluster, the library status and ``detect`` (``after_restart``).

    D-P1 is the Libraries API route iff ``at_once`` or ``after_restart`` shows a usable JAR. ``client`` is a
    ``WorkspaceClient`` (default: the notebook's own). Prints ``[T28 P1] <step> START`` / ``DONE`` lines.
    """
    if phase not in ("install", "after"):
        raise ValueError(f"phase must be 'install' or 'after' (got {phase!r})")
    from databricks.labs.gbx.bench.uc_perf import _Run

    if client is None:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
    run_ = _Run("T28 P1", "t28-p1", 3 if phase == "install" else 1, wait_s + 600)
    out: dict = {"phase": phase, "cluster_id": cluster_id, "jar": jar_path}
    try:
        if phase == "after":
            out["library"], _, _ = run_.step(
                "status", lambda: _p1_status(client, cluster_id, jar_path)
            )
            out["after_restart"], err, _ = run_.step("detect", lambda: _p1_caps(spark))
            if err:
                out["after_restart"] = {"error": err}
            out["usable_after_restart"] = bool(
                isinstance(out["after_restart"], dict)
                and out["after_restart"].get("jar_usable")
            )
            return out
        out["before"], err, _ = run_.step("detect_before", lambda: _p1_caps(spark))
        t0 = time.time()

        def install():
            client.libraries.install(cluster_id, [_p1_library(jar_path)])
            while True:
                st = _p1_status(client, cluster_id, jar_path)
                if st["status"] in P1_STATES_DONE or time.time() - t0 > wait_s:
                    return st
                time.sleep(poll_s)

        out["library"], err, wall = run_.step("install", install)
        if err:
            out["library"] = {"status": "ERROR", "messages": [err]}
        out["install_s"] = round(wall, 1)
        out["at_once"], err, _ = run_.step(
            "detect_after_install", lambda: _p1_caps(spark)
        )
        out["usable_at_once"] = bool(
            isinstance(out["at_once"], dict) and out["at_once"].get("jar_usable")
        )
        out["restart_requested"] = bool(
            restart
            and not out["usable_at_once"]
            and out["library"].get("status") == "INSTALLED"
        )
        print(f"[T28 P1] RESULT {json.dumps(out, default=str)}", flush=True)
        if out["restart_requested"]:
            print(
                "[T28 P1] RESTART requested: this run ends with the cluster. Run phase='after' on the restarted cluster.",
                flush=True,
            )
            client.clusters.restart(cluster_id)
        return out
    finally:
        run_.stop()


# --- the workspace mechanism checks (Task 28 run F) and the engine L4 probe (M-6) ---------------------------------------

MECH = "gbx_mvp_t28_"
MECH_MIB = (1, 16, 64)
MECH_ENGINE_DEPS = ("rasterio", "pyproj", "shapely")  # the pins of the probe sandboxes
M6_FN = "gbx_mvp_se_l4_probe"
M6_TILE = ("1MiB", 1 << 20)  # the reference tile: a 512 x 512 Float32 ZSTD GeoTIFF

_M6_BODY = (
    "import sys\n"
    "from databricks.labs.gbx import manifest\n"
    "from databricks.labs.gbx._gdal_policy import open_bytes\n"
    "from databricks.labs.gbx.engine import run as engine_run\n"
    "d = t.asDict(recursive=True) if hasattr(t, 'asDict') else dict(t)\n"
    "md = dict(d.get('metadata') or {})\n"
    "res = engine_run.run(manifest.load().functions['gbx_rst_slope'], [bytes(d['raster'])], {'unit': 'degrees'})\n"
    "if res is None or res.raster is None:\n"
    "    md['m6'] = 'error|' + str(None if res is None else res.error)[:200]\n"
    "else:\n"
    "    with open_bytes(bytes(res.raster)) as ds:\n"
    "        a = ds.read(1, masked=True)\n"
    "        stats = f'{int(a.count())}:{float(a.min()):.3f}:{float(a.max()):.3f}'\n"
    "    md['m6'] = f\"ok|valid:min:max={stats}|pyspark_imported={'pyspark' in sys.modules}\"\n"
    "    d['raster'] = res.raster\n"
    "d['metadata'] = md\n"
    "return d"
)


def _env(wheel_uri: str, deps: dict, names: Sequence[str] = MECH_ENGINE_DEPS) -> str:
    from databricks.labs.gbx.uc.ddl import quote_str

    return (
        "ENVIRONMENT (dependencies = "
        + quote_str(
            json.dumps([f"geobrix @ {wheel_uri}"] + [f"{k}=={deps[k]}" for k in names])
        )
        + ", environment_version = '6')"
    )


def _tile_from_content() -> str:
    """The v2 tile struct built from the ``content`` column of ``read_files(..., format => 'binaryFile')``."""
    from databricks.labs.gbx.manifest import TILE_SQL

    return (
        "CAST(named_struct('cellid', CAST(0 AS BIGINT), 'raster', content, 'path', NULL, 'path_mode', NULL, "
        "'window', NULL, 'clip_polygon', NULL, 'clip_crs', NULL, 'crs', 'EPSG:32633', "
        f"'metadata', map('src', 'm6')) AS {TILE_SQL})"
    )


def m6_statements(
    catalog: str, schema: str, tile_dir: str, wheel_uri: str, deps: dict
) -> List[Tuple[str, str]]:
    """M-6 (engine L4): a UC Python function ``gbx_mvp_se_l4_probe(t TILE) RETURNS TILE`` whose handler runs the GDAL spec
    engine for ``gbx_rst_slope`` on the 1 MiB reference tile as ``(cid, sql)``: the CREATE, the call and the DROP. Pass: the
    call answers ``ok|valid:min:max=...|pyspark_imported=False``. The Serverless notebook and the warehouse run the same text.
    """
    from databricks.labs.gbx.manifest import TILE_SQL
    from databricks.labs.gbx.uc.ddl import quote_ident, quote_str

    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    src = quote_str(f"{tile_dir}/p2_{M6_TILE[0]}.bin")
    return [
        (
            "m6:create",
            f"CREATE OR REPLACE FUNCTION {q}.{M6_FN}(t {TILE_SQL}) RETURNS {TILE_SQL} LANGUAGE PYTHON\n"
            f"{_env(wheel_uri, deps)}\nAS $$\n{_M6_BODY}\n$$",
        ),
        (
            "m6:call",
            "SELECT concat_ws('|', CAST(length(r.t.raster) AS STRING), r.t.metadata['m6']) AS out FROM "
            f"(SELECT {q}.{M6_FN}({_tile_from_content()}) AS t FROM read_files({src}, format => 'binaryFile')) r",
        ),
        ("m6:drop", f"DROP FUNCTION IF EXISTS {q}.{M6_FN}"),
    ]


def m6_ok(value) -> bool:
    """The M-6 pass: a slope tile came back and ``pyspark`` was never imported by the handler."""
    text = str(value)
    return "|ok|valid:min:max=" in "|" + text and "pyspark_imported=False" in text


def run_m6(spark, catalog: str, schema: str, volume_dir: str) -> dict:
    """M-6, Serverless leg: create the probe, call it on the 1 MiB reference tile, drop it. The tile is written to
    ``<volume_dir>/m6_tiles``; ``volume_dir`` holds the one env 6 wheel. Prints ``[T28 M6] <step> START`` / ``DONE`` lines.

    Returns ``{"wheel", "call": value, "pass": bool, "errors": {step: message}, "dropped"}``.
    """
    from databricks.labs.gbx import manifest, uc
    from databricks.labs.gbx.bench.uc_perf import _Run

    wheel = uc._single_wheel(volume_dir)
    tile_dir = f"{volume_dir.rstrip('/')}/m6_tiles"
    stmts = dict(
        m6_statements(
            catalog,
            schema,
            tile_dir,
            "file://" + wheel,
            manifest.load().uc_dependencies,
        )
    )
    run_ = _Run("T28 M6", "t28-m6", 4, 1800.0)
    out: dict = {"wheel": wheel, "errors": {}, "dropped": False}
    try:
        _, err, _ = run_.step("stage", lambda: p2_stage_tiles(tile_dir, (M6_TILE,)))
        if err:
            out["errors"]["stage"] = err
        _, err, _ = run_.step("create", lambda: spark.sql(stmts["m6:create"]))
        if err:
            out["errors"]["create"] = err
        else:
            val, err, _ = run_.step(
                "call", lambda: spark.sql(stmts["m6:call"]).first()[0]
            )
            out["call"] = val
            if err:
                out["errors"]["call"] = err
    finally:
        _, err, _ = run_.step("drop", lambda: spark.sql(stmts["m6:drop"]))
        out["dropped"] = err is None
        run_.stop()
    out["pass"] = not out["errors"] and m6_ok(out.get("call"))
    return out


def _write_raw_tiff(path: str, mib: int) -> int:
    """An uncompressed single-band Float32 GeoTIFF of about ``mib`` MiB on disk (a gradient): the file's size."""
    import math as _math

    import numpy as np
    from rasterio.transform import from_origin
    from rasterio.windows import Window

    from databricks.labs.gbx._gdal_policy import open_data

    side = _math.isqrt(mib * 1024 * 1024 // 4)
    with open_data(
        path + ".part", "w", driver="GTiff", width=side, height=side, count=1, dtype="float32",
        crs="EPSG:32633", transform=from_origin(500000.0, 5000000.0, 10.0, 10.0),
    ) as dst:  # fmt: skip
        cols = np.arange(side, dtype="float32")
        for row in range(0, side, 256):
            h = min(256, side - row)
            rows = np.arange(row, row + h, dtype="float32")[:, None]
            dst.write(
                rows * 0.001 + cols[None, :] * 0.0001, 1, window=Window(0, row, side, h)
            )
    os.replace(path + ".part", path)
    return os.path.getsize(path)


def stage_mech_tiles(tile_dir: str, mibs: Sequence[int] = MECH_MIB) -> dict:
    """Write what the mechanism checks read into ``tile_dir``: ``mech_<n>MiB.tif`` (uncompressed, about ``n`` MiB ON DISK,
    for the ``read_files`` route) and ``perf_<n>MiB.tif`` (decoded ``n`` MiB, small on disk, for the merge at size). A
    file that is already there is kept."""
    from databricks.labs.gbx.bench import uc_perf

    os.makedirs(tile_dir, exist_ok=True)
    out = dict(uc_perf.perf_stage_tiles(tile_dir, tuple(mibs)))
    for mib in mibs:
        path = f"{tile_dir}/mech_{mib}MiB.tif"
        size = (
            os.path.getsize(path)
            if os.path.exists(path)
            else _write_raw_tiff(path, mib)
        )
        out[f"mech_{mib}MiB.tif"] = {"path": path, "bytes": size}
    return out


def _mech_python_fn(
    q: str, name: str, params: str, returns: str, body: str, env: str
) -> str:
    return (
        f"CREATE OR REPLACE FUNCTION {q}.{name}({params}) RETURNS {returns} LANGUAGE PYTHON\n"
        f"{env}\nAS $$\n{body}\n$$"
    )


_MECH_GEOM_BODY = (
    "import shapely\n"
    "out = []\n"
    "for g in gs:\n"
    "    kind = type(g).__module__ + '.' + type(g).__name__\n"
    "    z = 'unknown'\n"
    "    try:\n"
    "        raw = g if isinstance(g, (bytes, bytearray)) else getattr(g, 'wkb', None)\n"
    "        if raw is not None:\n"
    "            z = str(bool(shapely.has_z(shapely.from_wkb(bytes(raw)))))\n"
    "    except Exception as exc:\n"
    "        z = 'error:' + type(exc).__name__\n"
    "    out.append(kind + '|' + z)\n"
    "return '~'.join(out)"
)
_MECH_PARTIAL_BODY = (
    "return [{'band': 1, 'cellid': 7, 'count': 4.0, 'sum': 10.0}, "
    "{'band': 1, 'cellid': 8, 'count': 2.0, 'sum': 3.0}]"
)
# (cid suffix, statement, expectation): expectation is ("any",), ("eq", text), ("contains", text) or ("error", text)
Stmt = Tuple[str, str, tuple]


def t28_mech_units(
    catalog: str, schema: str, tile_dir: str, wheel_uri: str, deps: dict
) -> List[dict]:
    """The workspace mechanism checks of the Task 28 pass, as units of statements with the result each one expects.

    A unit is ``{"id", "title", "surfaces", "statements": [(cid, sql, expect)], "drops": [(cid, sql)]}``. ``surfaces``
    names where it runs: ``s6`` (a Serverless notebook) and ``w`` (the warehouse). The statements of a unit run in order;
    a unit is ``ok`` when every expectation holds. Every routine is named ``gbx_mvp_t28_*`` and dropped at the end.
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx import uc as _uc
    from databricks.labs.gbx.uc import ddl
    from databricks.labs.gbx.uc.ddl import quote_ident, quote_str

    q = f"{quote_ident('catalog', catalog)}.{quote_ident('schema', schema)}"
    man = manifest.load()
    bare = ddl.sql_type("ARRAY<STRUCT<band:INT,cellid:BIGINT,count:DOUBLE,sum:DOUBLE>>")
    lean_env = "ENVIRONMENT (environment_version = '6')"
    units: List[dict] = []

    def unit(uid, title, surfaces, statements, drops=()):
        units.append(
            {"id": uid, "title": title, "surfaces": tuple(surfaces), "statements": [
                (f"{uid}:{c}", s, e) for c, s, e in statements
            ], "drops": [(f"{uid}:{c}", s) for c, s in drops]}
        )  # fmt: skip

    t_add, t_shadow = f"{MECH}tmp_add", f"{MECH}shadow"
    unit(
        "f1", "a bodied CREATE TEMPORARY FUNCTION under Spark Connect", ("s6",),
        [
            ("create", f"CREATE OR REPLACE TEMPORARY FUNCTION {t_add}(x INT) RETURNS INT RETURN x + 1", ("any",)),
            ("call", f"SELECT {t_add}(41) AS out", ("eq", "42")),
        ],
        [("drop", f"DROP TEMPORARY FUNCTION IF EXISTS {t_add}")],
    )  # fmt: skip
    unit(
        "f2", "an unqualified name: the session temporary function, not the schema function", ("s6",),
        [
            ("create_schema", f"CREATE OR REPLACE FUNCTION {q}.{t_shadow}(x INT) RETURNS STRING RETURN 'schema'", ("any",)),
            ("create_temp", f"CREATE OR REPLACE TEMPORARY FUNCTION {t_shadow}(x INT) RETURNS STRING RETURN 'temp'", ("any",)),
            ("use", f"USE {q}", ("any",)),
            ("call_unqualified", f"SELECT {t_shadow}(1) AS out", ("eq", "temp")),
            ("call_qualified", f"SELECT {q}.{t_shadow}(1) AS out", ("eq", "schema")),
        ],
        [("drop_temp", f"DROP TEMPORARY FUNCTION IF EXISTS {t_shadow}"), ("drop_schema", f"DROP FUNCTION IF EXISTS {q}.{t_shadow}")],
    )  # fmt: skip
    stub = f"{MECH}tvf_stub"
    unit(
        "f3", "a SQL table function whose body raises (a TVF stub)", ("s6", "w"),
        [
            ("create", f"CREATE OR REPLACE FUNCTION {q}.{stub}(x INT) RETURNS TABLE (a INT) RETURN SELECT CAST(raise_error('gbx_mvp_t28 stub: runs in a session') AS INT) AS a", ("any",)),
            ("call", f"SELECT * FROM {q}.{stub}(1)", ("error", "stub: runs in a session")),
        ],
        [("drop", f"DROP FUNCTION IF EXISTS {q}.{stub}")],
    )  # fmt: skip
    part, proj = f"{MECH}partial", f"{MECH}proj"
    unit(
        "f4", "a projection over a generator over a Python function, as a table function", ("s6", "w"),
        [
            ("create_python", _mech_python_fn(q, part, "n INT", bare, _MECH_PARTIAL_BODY, lean_env), ("any",)),
            ("create_tvf", f"CREATE OR REPLACE FUNCTION {q}.{proj}(n INT) RETURNS TABLE (band INT, cellID BIGINT, measure DOUBLE) RETURN SELECT band, cellid AS cellID, try_divide(sum, count) AS measure FROM (SELECT inline({q}.{part}(n)))", ("any",)),
            ("call", f"SELECT concat_ws('|', CAST(count(*) AS STRING), CAST(sum(measure) AS STRING)) AS out FROM {q}.{proj}(3)", ("eq", "2|4.0")),
        ],
        [("drop_tvf", f"DROP FUNCTION IF EXISTS {q}.{proj}"), ("drop_python", f"DROP FUNCTION IF EXISTS {q}.{part}")],
    )  # fmt: skip
    geom = f"{MECH}geom_array"
    pt = "CAST(st_geomfromewkt('SRID=4326;POINT Z (1 2 3)') AS GEOMETRY(ANY))"
    unit(
        "f5", "an ARRAY<GEOMETRY(ANY)> parameter: what the handler gets, and whether it keeps Z", ("s6", "w"),
        [
            ("create", _mech_python_fn(q, geom, f"gs {ddl.sql_type('ARRAY<GEOMETRY(ANY)>')}", "STRING", _MECH_GEOM_BODY, _env(wheel_uri, deps, ("shapely",))), ("any",)),
            ("call", f"SELECT {q}.{geom}(array({pt}, {pt})) AS out", ("contains", "|True")),
        ],
        [("drop", f"DROP FUNCTION IF EXISTS {q}.{geom}")],
    )  # fmt: skip
    proc = f"{MECH}proc_temp"
    unit(
        "f6", "a temporary table inside a procedure", ("s6", "w"),
        [
            ("create", f"CREATE OR REPLACE PROCEDURE {q}.{proc}()\nLANGUAGE SQL\nSQL SECURITY INVOKER\nAS BEGIN\nCREATE TEMPORARY TABLE {MECH}tmp_t AS SELECT 1 AS a;\nSELECT count(*) FROM {MECH}tmp_t;\nEND", ("any",)),
            ("call", f"CALL {q}.{proc}()", ("any",)),
        ],
        [("drop", f"DROP PROCEDURE IF EXISTS {q}.{proc}")],
    )  # fmt: skip
    width, fromc = f"{q}.gbx_rst_width", f"{q}.gbx_rst_fromcontent"
    unit(
        "f7",
        "read_files(binaryFile) into gbx_rst_fromcontent at 1, 16 and 64 MiB",
        ("s6", "w"),
        [
            (
                f"{mib}MiB",
                "SELECT concat_ws('|', CAST(length(content) AS STRING), CAST("
                f"{width}({fromc}(content, 'GTiff')) AS STRING)) AS out FROM read_files("
                f"{quote_str(f'{tile_dir}/mech_{mib}MiB.tif')}, format => 'binaryFile')",
                ("any",),
            )
            for mib in MECH_MIB
        ],
    )
    lean, allp = f"{MECH}deps_lean", f"{MECH}deps_all"
    width_fn = man.functions["gbx_rst_width"]
    ver = _uc._public_version()

    def probe_copy(name, shared):
        (create,) = ddl.function_ddl(
            width_fn, catalog, schema, wheel_uri, man.uc_dependencies, ver,
            name=name, shared_deps=shared, helpers=False,
        )  # fmt: skip
        return create

    tile = f"(SELECT {_tile_from_content()} AS t FROM read_files({quote_str(f'{tile_dir}/mech_1MiB.tif')}, format => 'binaryFile'))"
    unit(
        "f8", "install and first-call time: one function's own packages vs every package of the manifest", ("s6", "w"),
        [
            ("create_lean", probe_copy(lean, False), ("any",)),
            ("call_lean", f"SELECT {q}.{lean}(t) AS out FROM {tile}", ("any",)),
            ("create_all", probe_copy(allp, True), ("any",)),
            ("call_all", f"SELECT {q}.{allp}(t) AS out FROM {tile}", ("any",)),
        ],
        [("drop_lean", f"DROP FUNCTION IF EXISTS {q}.{lean}"), ("drop_all", f"DROP FUNCTION IF EXISTS {q}.{allp}")],
    )  # fmt: skip
    unit(
        "f9",
        "ESCAPE '!' in the version-check LIKE",
        ("s6", "w"),
        [
            (
                "like",
                f"SELECT count(*) AS out FROM {quote_ident('catalog', catalog)}.information_schema.routines "
                f"WHERE lower(routine_schema) = {quote_str(schema.lower())} AND routine_name LIKE 'gbx!_%' ESCAPE '!'",
                ("any",),
            )
        ],
    )
    merge_src = (
        lambda mib: f"read_files({quote_str(f'{tile_dir}/perf_{mib}MiB.tif')}, format => 'binaryFile')"
    )  # noqa: E731
    merge = f"{q}.gbx_rst_merge"
    unit(
        "f10",
        "one merge of two tiles at size (decoded 16 and 64 MiB each)",
        ("s6", "w"),
        [
            (
                f"merge_{mib}MiB",
                f"SELECT {width}({merge}(array(t, t))) AS out FROM (SELECT {fromc}(content, 'GTiff') AS t FROM {merge_src(mib)})",
                ("any",),
            )
            for mib in (16, 64)
        ],
    )
    return units


def mech_expectation(expect: tuple, entry: Optional[dict]) -> Optional[str]:
    """None when a statement's result ``entry`` (``{"value": ..}`` or ``{"error": ..}``, as ``gbx-uc-sql`` writes it) meets
    its expectation; else one line saying how it did not."""
    if entry is None:
        return "no result"
    kind = expect[0]
    if kind == "error":
        if "error" not in entry:
            return f"expected an error with {expect[1]!r}, got a value {str(entry.get('value'))[:80]!r}"
        return (
            None
            if expect[1] in entry["error"]
            else f"error without {expect[1]!r}: {entry['error'][:160]}"
        )
    if "error" in entry:
        return f"error: {entry['error'][:200]}"
    value = str(entry.get("value"))
    if kind == "eq" and value != expect[1]:
        return f"got {value[:80]!r}, want {expect[1]!r}"
    if kind == "contains" and expect[1] not in value:
        return f"got {value[:120]!r}, want it to contain {expect[1]!r}"
    return None


def mech_statements(
    catalog: str,
    schema: str,
    tile_dir: str,
    wheel_uri: str,
    deps: dict,
    surface: str = "w",
) -> List[Tuple[str, str]]:
    """The mechanism checks of one surface (``w``: the warehouse, ``s6``) as ``(cid, sql)`` for ``gbx-uc-sql``: each unit's
    statements in order, then every DROP. The product functions the f7, f8 and f10 statements call must be installed, and
    the tiles of :func:`stage_mech_tiles` must be in ``tile_dir`` (the Serverless run stages them).
    """
    units = [
        u
        for u in t28_mech_units(catalog, schema, tile_dir, wheel_uri, deps)
        if surface in u["surfaces"]
    ]
    out = [(cid, sql) for u in units for cid, sql, _ in u["statements"]]
    return out + [d for u in units for d in u["drops"]]


def check_mech_results(
    results: dict,
    catalog: str,
    schema: str,
    tile_dir: str,
    wheel_uri: str,
    deps: dict,
    surface: str = "w",
) -> dict:
    """``{unit id: {"title", "ok", "failures": {cid: why}}}`` for a results file of :func:`mech_statements`."""
    out = {}
    for u in t28_mech_units(catalog, schema, tile_dir, wheel_uri, deps):
        if surface not in u["surfaces"]:
            continue
        bad = {}
        for cid, _sql, expect in u["statements"]:
            why = mech_expectation(expect, results.get(cid))
            if why:
                bad[cid] = why
        out[u["id"]] = {"title": u["title"], "ok": not bad, "failures": bad}
    return out


def run_t28_mechanisms(spark, catalog: str, schema: str, volume_dir: str) -> dict:
    """The workspace mechanism checks on Serverless env 6 (Task 28 run F): each unit of :func:`t28_mech_units` in order, the
    error of a statement recorded as the measurement, every probe dropped at the end. ``volume_dir`` holds the one env 6
    wheel and gets a ``mech_tiles/`` folder. Needs the product UC functions installed in ``catalog.schema``.

    Prints ``[T28 mech] <unit> START`` and ``DONE <s>s ...`` per unit and a RUNNING line every 60 s.

    Returns ``{"wheel", "staged", "units": {id: {"title", "ok", "statements": {cid: {"value"|"error", "wall_s"}},
    "failures"}}, "dropped", "ok"}``.
    """
    from databricks.labs.gbx import manifest, uc
    from databricks.labs.gbx.bench.uc_perf import _Run

    wheel = uc._single_wheel(volume_dir)
    tile_dir = f"{volume_dir.rstrip('/')}/mech_tiles"
    units = [
        u
        for u in t28_mech_units(
            catalog,
            schema,
            tile_dir,
            "file://" + wheel,
            manifest.load().uc_dependencies,
        )
        if "s6" in u["surfaces"]
    ]
    run_ = _Run("T28 mech", "t28-mech", len(units) + 3, 1800.0)
    out: dict = {"wheel": wheel, "units": {}, "dropped": []}
    prev, _, _ = run_.step(
        "session",
        lambda: spark.sql("SELECT current_catalog(), current_schema()").first(),
    )

    def first_cell(sql):
        rows = spark.sql(sql).collect()
        return str(rows[0][0]) if rows and len(rows[0]) else None

    def run_unit(u):
        rec = {"title": u["title"], "statements": {}, "failures": {}}
        for cid, sql, expect in u["statements"]:
            t0 = time.time()
            try:
                entry = {"value": first_cell(sql)}
            except Exception as exc:  # noqa: BLE001 - the error is the measurement
                entry = {"error": " ".join(str(exc).split())[:600]}
            entry["wall_s"] = [round(time.time() - t0, 2)]
            rec["statements"][cid] = entry
            why = mech_expectation(expect, entry)
            if why:
                rec["failures"][cid] = why
        rec["ok"] = not rec["failures"]
        return rec

    try:
        out["staged"], err, _ = run_.step("stage", lambda: stage_mech_tiles(tile_dir))
        if err:
            out["staged"] = {"error": err}
        for u in units:
            rec, err, _ = run_.step(u["id"], lambda u=u: run_unit(u))
            out["units"][u["id"]] = (
                rec if err is None else {"title": u["title"], "ok": False, "error": err}
            )
    finally:

        def drop():
            if prev is not None:
                spark.sql(f"USE `{prev[0]}`.`{prev[1]}`")
            for u in units:
                for cid, sql in u["drops"]:
                    try:
                        spark.sql(sql)
                        out["dropped"].append(cid)
                    except Exception as exc:  # noqa: BLE001 - keep dropping the rest
                        out["dropped"].append(f"{cid}: ERROR {type(exc).__name__}")

        run_.step("cleanup", drop)
        run_.stop()
    out["ok"] = all(r.get("ok") for r in out["units"].values())
    return out
