"""A virtual tile (a path, no raster) never reaches a JVM function.

The JVM level reads tile bytes only: handed a virtual tile it raises ``VirtualTileException``. A virtual tile is the
light readers' default output, so a function the manager serves from the JVM (L1) must either put a dispatch wrapper in
front of it that sends the reference to light, or not be served from the JVM by default. This holds that for every
manifest function, not one by one.
"""

import pytest

from databricks.labs.gbx import manifest
from databricks.labs.gbx.manager import jvm, levels

MAN = manifest.load()
FNS = sorted(MAN.functions.values(), key=lambda f: f.sql_name)
_TILE_TYPES = ("TILE", "ARRAY<TILE>")


def _takes_tile(fn) -> bool:
    return any(t in _TILE_TYPES for p in fn.params for t in p.session)


def _first_available(fn) -> str:
    return next(lv for lv in levels.order(fn) if lv in fn.levels)


# The functions whose best level is the JVM and that take a tile: the ones a virtual tile can reach it through.
L1_TILE_FNS = [f for f in FNS if _takes_tile(f) and _first_available(f) == "L1"]


def test_there_are_jvm_first_tile_functions_to_hold():
    """The set is derived, not listed; this keeps the checks below from passing on an empty one."""
    # color_relief, frombands, and the 21 accessors that return one value
    assert len(L1_TILE_FNS) >= 23, [f.sql_name for f in L1_TILE_FNS]


@pytest.mark.parametrize("fn", L1_TILE_FNS, ids=lambda f: f.sql_name)
def test_a_tile_function_served_from_the_jvm_has_a_virtual_dispatch(fn):
    """The manifest row says the function takes a virtual tile, so the manager installs the dispatch wrapper under its
    SQL name (`jvm.install_l1` builds it only for such a row; any other is a bare companion).
    """
    assert fn.accepts_virtual_tile, (
        f"{fn.sql_name} is served from the JVM first and has no virtual dispatch: a virtual tile raises "
        "VirtualTileException. Fix: set accepts_virtual_tile, or prefer L3 in manager/levels.py PREFERENCE"
    )


@pytest.mark.parametrize(
    "name", ["gbx_rst_contour", "gbx_rst_isoband", "gbx_rst_merge"]
)
def test_the_functions_the_jvm_cannot_serve_by_default_are_light_first(name):
    """contour and isoband return an array a dispatch wrapper cannot sit in front of; the array merge is refused by the
    JVM over the memory budget, where light streams it."""
    assert _first_available(MAN.functions[name]) == "L3"


# --- the wrapper, run: a stand-in for each level, in a local session ------------------------------------------------


def _literal(p, tile_expr, tiles_expr):
    t = p.session[0]
    if t == "TILE":
        return tile_expr
    if t == "ARRAY<TILE>":
        return tiles_expr
    return {"STRING": "'x'", "INT": "1", "DOUBLE": "1.0", "BOOLEAN": "true"}.get(t, "1")


# What a function that returns one value answers for 'light': a value of its return type that no other level gives.
_LIGHT_VALUE = {
    "STRING": "light",
    "INT": 7,
    "BIGINT": 7,
    "DOUBLE": 7.5,
    "BOOLEAN": True,
    "BINARY": bytearray(b"light"),
}


def _returns_tile(fn) -> bool:
    return fn.returns_session == "TILE"


def _who(fn, out):
    """'light' for what a light stand-in answered: its tile's metadata, or its value for a function that returns one."""
    return out["metadata"]["who"] if _returns_tile(fn) else out


@pytest.fixture(scope="module")
def stand_ins(spark):
    """Per function: ``gbx__l1_*`` raises (a JVM that is handed a reference), ``gbx__l3_*`` answers 'light'."""
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    def refused(first, *_):
        if (
            first is None
        ):  # the branch the wrapper hands NULL: the JVM function answers NULL for it
            return None
        raise RuntimeError(
            "VirtualTileException: Heavyweight rst_* received a virtual tile"
        )

    def light(*_):
        return {
            "cellid": 0,
            "raster": b"light",
            "path": None,
            "path_mode": None,
            "window": None,
            "clip_polygon": None,
            "clip_crs": None,
            "crs": None,
            "metadata": {"who": "light"},
        }

    virt = dict(light(), raster=None, path="/tmp/not-read.tif", metadata={})
    mat = dict(light(), metadata={})
    schema = StructType(
        [StructField("v", V2_TILE_SCHEMA), StructField("m", V2_TILE_SCHEMA)]
    )
    spark.createDataFrame([(virt, mat)], schema).createOrReplaceTempView("_gbx_vd")
    for fn in L1_TILE_FNS:
        if _returns_tile(fn):
            answer, rtype = light, V2_TILE_SCHEMA
        else:
            value = _LIGHT_VALUE[fn.returns_session]
            answer, rtype = (lambda *_, v=value: v), fn.returns_session
        spark.udf.register(jvm._priv(fn, "L1"), refused, rtype)
        spark.udf.register(jvm._priv(fn, "L3"), answer, rtype)
    return spark


@pytest.mark.parametrize("fn", L1_TILE_FNS, ids=lambda f: f.sql_name)
def test_the_dispatch_sends_a_reference_to_light_and_never_to_the_jvm(stand_ins, fn):
    """A virtual tile (alone, or in an array beside a materialized one) is answered by the light function; a materialized
    tile reaches the JVM one. A wrapper that sent the reference to the JVM would raise here.
    """
    spark = stand_ins
    # The manager's own install step, not a bare CREATE OR REPLACE: the session is shared across this directory, and an
    # earlier test may have left the light UDF under this SQL name, which only a drop-then-create can replace.
    jvm._create_wrapper(
        spark, fn, jvm.dispatch_ddl(fn, jvm._priv(fn, "L1"), jvm._priv(fn, "L3"))
    )
    required = fn.params[: fn.min_args]

    def call(tile_expr, tiles_expr):
        args = ", ".join(_literal(p, tile_expr, tiles_expr) for p in required)
        return spark.sql(f"SELECT {fn.sql_name}({args}) AS out FROM _gbx_vd").collect()[
            0
        ]["out"]

    light = "light" if _returns_tile(fn) else _LIGHT_VALUE[fn.returns_session]
    assert _who(fn, call("v", "array(v, v)")) == light
    assert _who(fn, call("v", "array(m, v)")) == light
    with pytest.raises(Exception, match="VirtualTileException"):
        call(
            "m", "array(m, m)"
        )  # the control: a materialized tile does reach the JVM function


# --- the contract rows: a reference in, a correct result out, at every level -----------------------------------------


def _has_virtual(arg) -> bool:
    if isinstance(arg, (list, tuple)):
        return any(_has_virtual(a) for a in arg)
    return (
        isinstance(arg, dict)
        and arg.get("raster") is None
        and bool(arg.get("path"))
        and "last_error" not in (arg.get("metadata") or {})
    )


@pytest.mark.parametrize("fn", L1_TILE_FNS, ids=lambda f: f.sql_name)
def test_every_jvm_first_tile_function_has_an_oracle_row_for_a_reference(fn, tmp_path):
    """The family harness runs every session row at each level the manifest declares, L1 included, and holds the output
    to the row's expectation: so a reference row that expects a result (not an error) is what proves the function
    answers one at the JVM level."""
    from databricks.labs.gbx.bench import oracles

    rows = [
        c
        for table in oracles.TABLES.values()  # a function's rows may sit in a table of another family (SPEC_SE)
        for c in table
        if c.fn == fn.sql_name
        and "session" in c.surfaces
        and c.kind
        not in ("raises", "error_tile", "error_tile_has", "error_fix", "null")
        and _has_virtual([oracles.resolve_arg(a, str(tmp_path)) for a in c.args])
    ]
    assert rows, (
        f"{fn.sql_name} is served from the JVM first, and no session oracle row gives it a virtual tile and expects a "
        "result. Fix: add a row in its oracle table that builds the input as paths"
    )


@pytest.mark.integration
def test_a_scalar_accessor_answers_a_reference_from_light_and_bytes_from_the_jvm(
    jvm_spark, tmp_path
):
    """With the JAR the width is JVM first: the SQL name is the dispatch wrapper over both private functions, a bytes
    tile and the reference to the same file give the same width, and the JVM function alone refuses the reference.
    """
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx import register
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    name = "gbx_rst_width"
    chosen = register(jvm_spark, only=[name])
    assert chosen[name].level == "L1", chosen[name]
    body = " ".join(
        str(r[0]) for r in jvm_spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").collect()
    )
    assert "gbx__l1_rst_width" in body and "gbx__l3_rst_width" in body
    mat, virt = (
        oracles.resolve_arg(oracles.Build(b), str(tmp_path))
        for b in ("tile_4326", "tile_virtual")
    )
    schema = StructType(
        [StructField("m", V2_TILE_SCHEMA), StructField("v", V2_TILE_SCHEMA)]
    )
    jvm_spark.createDataFrame([(mat, virt)], schema).createOrReplaceTempView("_gbx_w")
    row = jvm_spark.sql(
        f"SELECT {name}(m) AS m, {name}(v) AS v, gbx__l3_rst_width(v) AS light FROM _gbx_w"
    ).collect()[0]
    assert (row["m"], row["v"], row["light"]) == (64, 64, 64)
    with pytest.raises(Exception, match="VirtualTileException"):
        jvm_spark.sql("SELECT gbx__l1_rst_width(v) FROM _gbx_w").collect()


ENGINE_ONLY_L1 = [
    f
    for f in FNS
    if "L1" in f.levels and f.accepts_virtual_tile and levels.engine_only(f, "L3")
]


def test_there_are_engine_only_rows_with_a_jvm_companion():
    """The set is derived: the DEM rows whose numpy light form was removed keep their JVM companion."""
    assert {"gbx_rst_aspect", "gbx_rst_tri", "gbx_rst_tpi", "gbx_rst_roughness"} <= {
        f.sql_name for f in ENGINE_ONLY_L1
    }


@pytest.mark.parametrize("fn", ENGINE_ONLY_L1, ids=lambda f: f.sql_name)
def test_the_light_side_of_the_jvm_dispatch_of_an_engine_only_row_is_the_engine(fn):
    """A row whose only light form is the GDAL spec engine has no numpy kernel for the wrapper to send a virtual tile
    to: its light side is the engine's row-wise UDF, registered without importing osgeo (a session that lacks the
    engine still installs the wrapper, and a virtual tile then refuses at the call, naming the env wheel).
    """
    from unittest import mock

    spark = mock.MagicMock()
    with mock.patch.object(
        jvm.l3, "register_l3", side_effect=AssertionError("no numpy light form")
    ):
        name = jvm._install_light_side(spark, fn)
    assert name == jvm._priv(fn, "L3")
    assert spark.udf.register.call_args.args[0] == name
