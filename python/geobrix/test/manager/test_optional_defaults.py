"""A row's optional parameters have their manifest defaults on every level the manager can pick: a call with the
required arguments only must resolve, SQL and Python, with or without the GDAL engine.
"""

import pytest

from databricks.labs.gbx import manifest, register
from databricks.labs.gbx.bench import oracles
from databricks.labs.gbx.engine import run
from databricks.labs.gbx.manager import levels
from databricks.labs.gbx.manifest import sql_type

MAN = manifest.load()
WITH_OPTIONALS = sorted(
    f.sql_name for f in MAN.functions.values() if f.max_args > f.min_args
)


def _forms(fn):
    """The L3 forms a session can land on: the light kernel, and the GDAL engine's two UDFs (a row whose only light
    form is the engine has no light kernel)."""
    row = manifest.load().functions[fn]
    forms = [] if levels.engine_only(row, "L3") else [("L3", "-")]
    if row.gdal is not None:
        forms += [("L3", "engine-c0"), ("L3", "engine-c1")]
    return forms


CASES = [(fn, lv, var) for fn in WITH_OPTIONALS for lv, var in _forms(fn)]


def _required_args_only(spark, fn):
    """The row called with its required arguments, each NULL of its own type (the call must resolve and run)."""
    args = ", ".join(
        f"CAST(NULL AS {sql_type(p.session[0])})" for p in fn.params[: fn.min_args]
    )
    if fn.kind == "generator":
        return spark.sql(
            f"SELECT * FROM VALUES (1) AS gbx_t(x), LATERAL {fn.sql_name}({args})"
        )
    if fn.kind == "aggregate":
        return spark.sql(f"SELECT {fn.sql_name}({args}) FROM range(1)")
    return spark.sql(f"SELECT {fn.sql_name}({args})")


@pytest.mark.parametrize("sql_name,level,variant", CASES, ids=lambda v: str(v))
def test_a_row_is_callable_with_its_required_arguments_only(
    spark, sql_name, level, variant
):
    if variant.startswith("engine") and run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    register(spark, only=[sql_name], _levels={sql_name: (level, variant)})
    _required_args_only(spark, MAN.functions[sql_name]).collect()


@pytest.mark.parametrize("variant", ["engine-c0", "engine-c1"])
def test_slope_with_only_a_tile_runs_on_the_engine(spark, tmp_path, variant):
    """The reported defect: `SELECT gbx_rst_slope(tile)` raised TypeError (unit, xscale, yscale missing)."""
    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    register(spark, only=["gbx_rst_slope"], _levels={"gbx_rst_slope": ("L3", variant)})
    tile = oracles._build("dem_3857", str(tmp_path))
    spark.createDataFrame(
        [(tile,)], f"tile {manifest.TILE_SQL}"
    ).createOrReplaceTempView("_gbx_dem")
    from databricks.labs.gbx.rasterx import functions as rx

    (r,) = spark.sql("SELECT gbx_rst_slope(tile) AS s FROM _gbx_dem").collect()
    assert r["s"]["raster"] is not None, dict(r["s"]["metadata"])
    (r,) = spark.table("_gbx_dem").select(rx.rst_slope("tile").alias("s")).collect()
    assert r["s"]["raster"] is not None, dict(r["s"]["metadata"])


SELF_DEFAULTING = [
    "gbx_h3_cell_bbox",
    "gbx_rst_evi",
    "gbx_rst_fillnodata",
    "gbx_rst_fromfile",
    "gbx_rst_land_cover",
    "gbx_rst_savi",
]


class _Recording:
    """A real session whose SQL statements are kept."""

    def __init__(self, spark):
        self._spark, self.statements = spark, []

    def sql(self, text, *a, **k):
        self.statements.append(text)
        return self._spark.sql(text, *a, **k)

    def __getattr__(self, name):
        return getattr(self._spark, name)


def test_kernels_with_their_own_defaults_register_without_a_sql_wrapper(spark):
    """These six registered behind a SQL wrapper only because their kernels had no defaults; a SQL function cannot sit
    inside a LATERAL call on Serverless. Now the kernel is the registered function and takes a call without its
    optional arguments.
    """
    rec = _Recording(spark)
    out = register(
        rec, only=SELF_DEFAULTING, _prefer={n: "L3" for n in SELF_DEFAULTING}
    )
    assert {n: out[n].level for n in SELF_DEFAULTING} == {
        n: "L3" for n in SELF_DEFAULTING
    }
    made = [s for s in rec.statements if s.startswith("CREATE")]
    assert not [s for s in made if "gbx__probe_sqlfn" not in s], made
    for n in SELF_DEFAULTING:
        _required_args_only(spark, MAN.functions[n]).collect()
    (got,) = spark.sql(
        "SELECT gbx_h3_cell_bbox(CAST(617700169958293503 AS BIGINT)) AS b"
    ).collect()
    (want,) = spark.sql(
        "SELECT gbx_h3_cell_bbox(CAST(617700169958293503 AS BIGINT), 4326, 'centroids', 0, NULL) AS b"
    ).collect()
    assert got["b"] is not None and got["b"] == want["b"]
