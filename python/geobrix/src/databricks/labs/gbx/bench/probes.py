"""MVP probes as plain functions: each returns a JSON-able dict that the notebook prints."""

from __future__ import annotations

import json
import time
from typing import List, Sequence, Tuple


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


def uc_probe_sql(
    catalog: str, schema: str, jar_path: str, n: int = 1_000_000
) -> List[Tuple[str, str]]:
    """E-UCScala + the OD-18 batch form + per-statement timing at n rows, for gbx-uc-sql on the warehouse."""
    q = f"`{catalog}`.`{schema}`"
    gen = f"(SELECT rand(7) * 700000 AS e, rand(8) * 1300000 AS n FROM range({n}))"
    env = f"ENVIRONMENT (java_dependencies = '[\"{jar_path}\"]', environment_version = '4')"
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
    rows = []
    for i, p in enumerate(paths):
        with open(p, "rb") as fh:
            rows.append((i, os.path.basename(p), bytearray(fh.read())))
    spark.createDataFrame(rows, "i int, name string, raster binary").write.mode(
        "overwrite"
    ).saveAsTable(table)
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
