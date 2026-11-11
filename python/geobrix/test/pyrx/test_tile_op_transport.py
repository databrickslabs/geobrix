"""The light tile functions choose how their rows cross the JVM <-> Python boundary.

A pickled row UDF groups up to 100 tiles into ONE pickle built on the executor JVM heap, so with multi-MB tiles the
whole partition is one heap buffer: on dedicated DBR 19 a 64-tile partition spent a third to a half of each task in
garbage collection and 4x the rows took 8x the time. The Arrow form keeps the rows off the heap in batches bounded by
bytes. It is an ``arrow_udf`` (pyarrow arrays in and out, pyspark 4.1+), which hands INT, BIGINT and NULL over exactly
on every runtime, the legacy pandas conversion of classic DBR 19 included; Spark 4.0 keeps the pickles. The tile table
functions take the Arrow-optimized row UDTF where the session hands it its arguments exactly (``arrow_tile_udtf_transport``).

These tests pin (a) that every light row UDF and UDTF over tiles or large BINARY has its forms, and that only
small-scalar ones stay pickled; (b) the session's choice; (c) that the Arrow forms return what the pickled forms return,
for materialized and virtual tiles, single tiles, pairs and arrays, tile and scalar outputs, NULL arguments and table
functions; and (d) that a cell id past 2**53 comes through exactly.
"""

import math

import pytest
from pyspark.util import PythonEvalType

from databricks.labs.gbx import _transport
from databricks.labs.gbx.manager import capabilities as caps
from databricks.labs.gbx.manifest import TILE_SQL
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyvx import functions as pvx

BIG_CELL = 2**62 + 1  # past 2**53: an H3-sized id that a float cannot hold
HAS_ARROW_UDF = _transport.has_arrow_udf()
needs_arrow_udf = pytest.mark.skipif(
    not HAS_ARROW_UDF,
    reason="this pyspark has no arrow_udf (Spark 4.0): the session keeps the pickled rows",
)

# Row UDFs of the light modules that stay pickled: small scalars in and out, where a pickle is cheap.
STAY_PICKLED = {
    (prx, "_binpoints_partial_udf"),  # (x, y, extent, size) -> (col, row)
    (prx, "_h3_cell_bbox_udf"),  # (cellid, srid, mode, ...) -> bbox doubles
}
# Row UDTFs over tiles that stay pickled: TABLE(...) PARTITION BY table functions, whose Serverless failure (an
# IndexError once a task holds two rows) every UDTF form shares; their form stays as it is.
STAY_PICKLED_UDTF = {
    (prx, "_RstMergeTvf"),
    (prx, "_RstBngRasterizeTvf"),
}


def _unwrap(obj):
    """The module's UDF object (the Column API wraps it to carry the session's read cap)."""
    return getattr(obj, "_f", obj)


def _row_udfs(mod):
    """(name, object) of every module-level row UDF of *mod*: a SessionTileUdf or a plain (pickled) UDF."""
    out = []
    for name, obj in sorted(vars(mod).items()):
        obj = _unwrap(obj)
        if isinstance(obj, _transport.SessionTileUdf) or (
            getattr(obj, "evalType", None) == PythonEvalType.SQL_BATCHED_UDF
            and callable(getattr(obj, "func", None))
            and not isinstance(getattr(obj, "func", None), type)
        ):
            out.append((name, obj))
    return out


def _row_udtfs(mod):
    """(name, object) of every module-level UDTF of *mod* that takes rows: a SessionTileUdtf or a plain one."""
    out = []
    for name, obj in sorted(vars(mod).items()):
        obj = _unwrap(obj)
        if isinstance(obj, _transport.SessionTileUdtf) or isinstance(
            getattr(obj, "func", None), type
        ):
            out.append((name, obj))
    return out


SESSION = [
    (mod.__name__.rsplit(".", 2)[-2], name, obj)
    for mod in (prx, pvx)
    for name, obj in _row_udfs(mod)
    if isinstance(obj, _transport.SessionTileUdf)
]


def _factory_built():
    from databricks.labs.gbx.engine import l3 as engine_l3

    return {
        "clip_sql": prx._build_clip_sql_udf(()),
        "clip_v2": prx._build_clip_v2_udf(()),
        "uf_transformcrs": prx._build_uf_transformcrs(()),
        "transformcrs_sql": prx._build_transformcrs_sql_udf(()),
        "fromfile_materialized": prx._build_fromfile_sql_materialized_udf(),
        "engine_c0": engine_l3.c0("gbx_rst_slope"),
        "engine_c0_rasters": engine_l3.c0("gbx_rst_ndvi"),
        "engine_forced": engine_l3.forced_c0("gbx_rst_slope"),
    }


class _FakeSpark:
    """A session as the capability registry reads it: the server's `.version` and `.conf`."""

    def __init__(self, version, conf=None):
        self.version = version
        values = dict(conf or {})
        self.conf = type("C", (), {"get": lambda self, k, d=None: values.get(k, d)})()


@pytest.fixture(autouse=True)
def _no_override(monkeypatch):
    for cap in ("ARROW_TILE_TRANSPORT", "ARROW_TILE_UDTF_TRANSPORT"):
        monkeypatch.delenv(caps.CAP_ENV_PREFIX + cap, raising=False)


# ---------------------------------------------------------------------------
# (a) the forms
# ---------------------------------------------------------------------------


def test_the_moved_set_is_the_whole_tile_surface():
    assert (
        len(SESSION) >= 90
    )  # the tile ops, accessors, constructors and rasterize-from-cells rows; three vector arrays
    pickled = {
        (mod, name)
        for mod in (prx, pvx)
        for name, obj in _row_udfs(mod)
        if not isinstance(obj, _transport.SessionTileUdf)
    }
    # a row UDF over tiles or large BINARY that is still pickled is the GC cost this module removes
    assert pickled - STAY_PICKLED == set()


def test_every_table_function_over_tiles_takes_the_session_transport():
    """A UDTF of pyrx or pyvx that is not a SessionTileUdtf pickles its tiles (or geometries) on the executor heap."""
    plain = {
        (mod, name)
        for mod in (prx, pvx)
        for name, obj in _row_udtfs(mod)
        if not isinstance(obj, _transport.SessionTileUdtf)
    }
    assert plain - STAY_PICKLED_UDTF == set()
    moved = [
        o
        for mod in (prx, pvx)
        for _, o in _row_udtfs(mod)
        if isinstance(o, _transport.SessionTileUdtf)
    ]
    assert (
        len(moved) >= 45
    )  # nine raster generators, 32 raster-to-grid reducers, four vector table functions


def test_the_manifest_arrow_flag_marks_every_tile_table_function():
    """Every row whose table function takes a TILE or an ARRAY<BINARY> carries `arrow`, and its UDTF is a session one;
    the raster-to-grid factories' classes are reached through their rows."""
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manifest import resolve_ref

    flagged, missing = 0, []
    for fn in manifest.load().functions.values():
        spec = fn.levels.get("L3") or {}
        if "udtf" not in spec:
            continue
        types = {t.upper() for p in fn.params for t in p.session}
        takes_tiles = (
            "TILE" in types
            or "ARRAY<BINARY>" in types
            or fn.sql_name == "gbx_st_asmvt_pyramid"
        )
        obj = _unwrap(resolve_ref(spec["udtf"]))
        if takes_tiles:
            if not spec.get("arrow") or not isinstance(obj, _transport.SessionTileUdtf):
                missing.append(fn.sql_name)
            flagged += 1
        else:
            assert not spec.get("arrow"), fn.sql_name
    assert missing == []
    assert (
        flagged >= 44
    )  # 41 raster generators and raster-to-grid rows, three TIN rows, the MVT pyramid


@pytest.mark.parametrize(
    "udf",
    [pytest.param(o, id=f"{m}.{n}") for m, n, o in SESSION]
    + [pytest.param(o, id=k) for k, o in _factory_built().items()],
)
def test_each_form_has_its_three_transports(udf):
    assert isinstance(udf, _transport.SessionTileUdf)
    assert udf.form("pickle").evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.form("arrow_row").evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
    assert udf.form("arrow_row").returnType == udf.form("pickle").returnType
    if HAS_ARROW_UDF:
        assert udf.form("arrow").evalType == PythonEvalType.SQL_SCALAR_ARROW_UDF
        assert udf.form("arrow").returnType == udf.form("pickle").returnType
    # what reads a UDF object (is_light_udf, carrying, the oracles' plain-function leg) sees the pickled form
    assert udf.evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.func is udf.form("pickle").func


@pytest.mark.parametrize(
    "udtf",
    [
        pytest.param(o, id=f"{m.__name__.rsplit('.', 2)[-2]}.{n}")
        for m in (prx, pvx)
        for n, o in _row_udtfs(m)
        if isinstance(o, _transport.SessionTileUdtf)
    ],
)
def test_each_table_function_has_a_pickled_and_an_arrow_transport(udtf):
    assert udtf.form("pickle").evalType == PythonEvalType.SQL_TABLE_UDF
    assert udtf.form("arrow").evalType == PythonEvalType.SQL_ARROW_TABLE_UDF
    assert udtf.form("arrow").returnType == udtf.form("pickle").returnType
    assert udtf.form("arrow").func is udtf.form("pickle").func is udtf.func


# ---------------------------------------------------------------------------
# (b) the session's choice
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("server", ["4.0.0", "4.1.0", "4.2.0"])
def test_the_session_server_and_client_choose_the_transport(server):
    spark = _FakeSpark(server)
    label = "arrow" if server >= "4.1" and HAS_ARROW_UDF else "pickle"
    assert caps.path(spark, "tile_udf") == f"tile_udf:{label}"
    for udf in (_unwrap(prx._init_nodata_udf), _unwrap(prx._u_width)):
        assert udf.for_session(spark) is udf.form(label)


@pytest.mark.parametrize("legacy", ["true", "false"])
def test_the_legacy_pandas_conversion_no_longer_keeps_the_pickles(legacy):
    """Classic DBR 19 (Spark 4.2.0) turns spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled on, which made
    an Arrow-optimized row UDF get a NULL INT as NaN. The arrow_udf form converts nothing through pandas, so the conf
    does not change the choice: classic DBR 19 moves its tile rows as Arrow batches too.
    """
    spark = _FakeSpark(
        "4.2.0",
        {"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": legacy},
    )
    udf = _unwrap(prx._band_udf)
    assert udf.for_session(spark) is udf.form("arrow" if HAS_ARROW_UDF else "pickle")


def test_a_group_udf_falls_back_to_the_arrow_row_form_not_the_pickles():
    """combine* hands Python a whole group in one row: Spark 4.0 keeps it off the heap with the Arrow-optimized row
    UDF (its cell ids guarded), never the pickles."""
    udf = _unwrap(prx._combineavg_udf)
    assert udf.fallback == "arrow_row"
    assert udf.for_session(_FakeSpark("4.0.0")) is udf.form("arrow_row")


def test_the_override_turns_arrow_off_on_a_server_that_has_it(monkeypatch):
    monkeypatch.setenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_TRANSPORT", "0")
    spark = _FakeSpark("4.2.0")
    udf = _unwrap(prx._convolve_udf)
    assert udf.for_session(spark) is udf.form("pickle")


@pytest.mark.parametrize(
    "server, conf, label",
    [
        ("4.0.0", {}, "pickle"),  # Spark 4.0's Arrow UDTF converts through pandas
        ("4.1.0", {}, "arrow"),
        (
            "4.2.0",
            {caps.LEGACY_PANDAS_UDTF_CONVERSION: "false"},
            "arrow",
        ),  # classic DBR 19's default
        ("4.2.0", {caps.LEGACY_PANDAS_UDTF_CONVERSION: "true"}, "pickle"),
    ],
)
def test_the_session_chooses_the_table_function_transport(server, conf, label):
    spark = _FakeSpark(server, conf)
    assert caps.path(spark, "tile_udtf") == f"tile_udtf:{label}"
    udtf = _unwrap(prx._RstRetileUDTF)
    assert udtf.for_session(spark) is udtf.form(label)


def test_the_udtf_override_turns_arrow_off(monkeypatch):
    monkeypatch.setenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_UDTF_TRANSPORT", "0")
    udtf = _unwrap(prx._RstH3TessellateUDTF)
    assert udtf.for_session(_FakeSpark("4.2.0")) is udtf.form("pickle")


class _Reg:
    def __init__(self, out):
        self.out = out

    def register(self, name, f, *a, **k):
        self.out[name] = f


@needs_arrow_udf
def test_a_registration_takes_the_session_form_of_a_factory_row(monkeypatch):
    """A factory row (gbx_rst_clip) registers the form its session chooses, as a variants row does."""
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manager import l3

    registered = {}

    class _Sess:
        udf = _Reg(registered)

    monkeypatch.setattr(
        "databricks.labs.gbx._gdal_policy.carrying_session", lambda spark: _Sess()
    )
    monkeypatch.setattr(l3, "_needs_default_wrapper", lambda *a, **k: False)
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.capabilities.path",
        lambda spark, op: f"{op}:arrow",
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.capabilities._sql_functions", lambda spark: False
    )
    fn = manifest.load().functions["gbx_rst_clip"]
    assert "factory" in fn.levels["L3"]
    l3.register_l3(object(), fn, "-")
    assert registered["gbx_rst_clip"].evalType == PythonEvalType.SQL_SCALAR_ARROW_UDF


@pytest.mark.parametrize(
    "label, eval_type",
    [
        ("arrow", PythonEvalType.SQL_ARROW_TABLE_UDF),
        ("pickle", PythonEvalType.SQL_TABLE_UDF),
    ],
)
def test_a_registration_takes_the_session_form_of_a_table_function(
    monkeypatch, label, eval_type
):
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manager import l3

    registered = {}

    class _Sess:
        udtf = _Reg(registered)

    monkeypatch.setattr(
        "databricks.labs.gbx._gdal_policy.carrying_session", lambda spark: _Sess()
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.capabilities.path",
        lambda spark, op: f"{op}:{label}",
    )
    for name in ("gbx_rst_retile", "gbx_rst_h3_rastertogridavg", "gbx_st_triangulate"):
        l3.register_l3(object(), manifest.load().functions[name], "-")
        assert registered[name].evalType == eval_type


# ---------------------------------------------------------------------------
# (c) the pyarrow form, piece by piece (no Spark)
# ---------------------------------------------------------------------------


def _tile_array():
    import pyarrow as pa

    window = pa.struct([("col_off", pa.int32()), ("row_off", pa.int32())])
    t = pa.struct(
        [
            ("cellid", pa.int64()),
            ("raster", pa.binary()),
            ("window", window),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    rows = [
        {
            "cellid": BIG_CELL,
            "raster": b"x" * 5,
            "window": {"col_off": 1, "row_off": None},
            "metadata": [("a", "b")],
        },
        None,
        {"cellid": None, "raster": None, "window": None, "metadata": None},
    ]
    return pa.array(rows, type=t)


@needs_arrow_udf  # pyspark 4.1+'s converter, the one the arrow form prefers
def test_the_own_converter_gives_what_pyspark_gives():
    """The fallback (a pyspark without its converter) hands a row function the same values: Rows, dicts, ints."""
    from pyspark.sql.conversion import ArrowTableToRowsConversion
    from pyspark.sql.pandas.types import from_arrow_type

    col = _tile_array()
    own = _transport._own_input_converter(col.type)
    theirs = ArrowTableToRowsConversion._create_converter(
        from_arrow_type(col.type), none_on_identity=True, binary_as_bytes=True
    )
    a = [own(v) for v in col.to_pylist()]
    b = [theirs(v) for v in col.to_pylist()]
    assert [_plain(x) for x in a] == [_plain(x) for x in b]
    assert a[0]["cellid"] == BIG_CELL and isinstance(a[0]["cellid"], int)
    assert a[0]["window"]["row_off"] is None and a[0]["metadata"] == {"a": "b"}
    assert a[1] is None and a[0].raster == b"xxxxx"


@needs_arrow_udf
def test_a_batch_hands_each_row_its_exact_values_and_returns_one_array():
    import pyarrow as pa
    from pyspark.sql.types import LongType

    seen = []

    def row(tile, n):
        seen.append((None if tile is None else tile["cellid"], n))
        return None if tile is None else tile["cellid"]

    out = _transport._arrow_batch(row, LongType())(
        _tile_array(), pa.array([None, 7, 2**40], pa.int64())
    )
    assert seen == [(BIG_CELL, None), (None, 7), (None, 2**40)]
    assert out.type == pa.int64() and out.to_pylist() == [BIG_CELL, None, None]


@needs_arrow_udf
def test_a_batch_returns_a_tile_struct_from_dicts_and_rows(tiles):
    import pyarrow as pa
    from pyspark.sql import Row

    from databricks.labs.gbx.pyrx._serde import V2_TILE_SCHEMA

    first = dict(tiles["dem"][0], cellid=BIG_CELL)
    outputs = [first, None, Row(**dict(tiles["dem"][1], cellid=3))]

    out = _transport._arrow_batch(lambda i: outputs[i], V2_TILE_SCHEMA)(
        pa.array([0, 1, 2], pa.int32())
    )
    got = out.to_pylist()
    assert got[0]["cellid"] == BIG_CELL and got[0]["raster"] == first["raster"]
    assert got[1] is None and got[2]["cellid"] == 3


# ---------------------------------------------------------------------------
# (d) the guard of the arrow_row form (Spark 4.0's Arrow)
# ---------------------------------------------------------------------------


def test_an_exact_struct_passes_through_as_it_came():
    row = {"cellid": 7, "raster": b"x", "window": {"col_off": 0, "row_off": 0}}
    assert _transport._exact_member(row) is row


def test_floats_come_back_as_integers_and_a_null_as_none():
    got = _transport._exact_member(
        {"cellid": 7.0, "window": {"col_off": 2.0, "row_off": math.nan}}
    )
    assert got == {"cellid": 7, "window": {"col_off": 2, "row_off": None}}
    assert isinstance(got["cellid"], int)
    assert _transport._exact_member({"cellid": math.nan})["cellid"] is None


def test_a_cell_id_past_2_53_is_refused():
    with pytest.raises(_transport._LossyCellId, match="Fix:"):
        _transport._exact_member({"cellid": float(2**62)})


def test_a_tile_output_refuses_as_an_error_tile_and_a_scalar_output_raises():
    lossy = {"cellid": float(2**62), "raster": b"x"}
    tile_out = _unwrap(prx._init_nodata_udf).form("arrow_row").func(lossy)
    assert tile_out["raster"] is None
    assert "Fix: filter the NULL tiles out first" in tile_out["metadata"]["last_error"]
    with pytest.raises(_transport._LossyCellId):
        _unwrap(prx._u_width).form("arrow_row").func(lossy)
    merged = _unwrap(prx._merge_udf).form("arrow_row").func([lossy])
    assert (
        "Fix: take the NULL tiles out of the array" in merged["metadata"]["last_error"]
    )


# ---------------------------------------------------------------------------
# (e) the Arrow forms return what the pickled forms return
# ---------------------------------------------------------------------------


def _nc_tile(size, seed):
    """A NetCDF file whose variables `Band1` to `Band3` are three DEM surfaces (a materialized tile)."""
    import os
    import shutil
    import tempfile

    import numpy as np

    from databricks.labs.gbx._gdal_policy import open_bytes
    from databricks.labs.gbx.bench import oracles
    from databricks.labs.gbx.bench.levels_forms import _se_dem_tile

    def surface(k):
        with open_bytes(_se_dem_tile(size, seed + k)["raster"]) as ds:
            return ds.read(1)

    bands = np.stack([surface(k) for k in range(3)])
    d = tempfile.mkdtemp(prefix="gbx-transport-nc-")
    try:
        tif, nc = os.path.join(d, "b.tif"), os.path.join(d, "b.nc")
        with open(tif, "wb") as fh:
            fh.write(oracles._gtiff(bands, "EPSG:3857"))
        oracles.to_netcdf(tif, nc)
        with open(nc, "rb") as fh:
            return oracles._tile(fh.read())
    finally:
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="module")
def tiles():
    from databricks.labs.gbx.bench.levels_forms import _se_dem_tile

    return {"dem": [_se_dem_tile(64, s) for s in range(3)], "nc": [_nc_tile(32, 0)]}


# fn -> (input kind, UDF name, SQL argument text)
CALLS = {
    "band": ("dem", "_band_udf", "(tile, 1)"),
    "setsrid": ("dem", "_setsrid_udf", "(tile, 32633)"),
    "setcrs": ("dem", "_setcrs_udf", "(tile, 'EPSG:32633')"),
    "getsubdataset": ("nc", "_getsubdataset_udf", "(tile, 'Band2')"),
    "initnodata": ("dem", "_init_nodata_udf", "(tile)"),
    "updatetype": ("dem", "_update_type_udf", "(tile, 'Float32')"),
    "convolve": (
        "dem",
        "_convolve_udf",
        "(tile, array(array(0D, 1D, 0D), array(1D, 1D, 1D), array(0D, 1D, 0D)))",
    ),
    "align_to": ("dem", "_align_to_udf", "(tile, tile)"),
    "merge": ("dem", "_merge_udf", "(array(tile, tile))"),
    "fromcontent": ("dem", "_fromcontent_udf", "(tile.raster, 'GTiff')"),
    "width": ("dem", "_u_width", "(tile)"),
    "metadata": ("dem", "_metadata_udf", "(tile)"),
    "histogram": ("dem", "_histogram_udf", "(tile, 16, 0D, 400D, false)"),
    "avg": ("dem", "_u_avg", "(tile)"),
    "memsize": ("dem", "_memsize_struct_udf", "(tile)"),
    "tilexyz": (
        "dem",
        "_tilexyz_udf",
        "(tile, 12, 2088, 1384, 'PNG', 256, 'bilinear', 'auto')",
    ),
    # a NULL argument is NULL out (the oracle rows band_null_index, setsrid_null_srid): the transports must agree
    "worldtorastercoord_null_x": (
        "dem",
        "_worldtorastercoord_udf",
        "(tile, CAST(NULL AS DOUBLE), 0D)",
    ),
    "rastertoworldcoord_null_y": (
        "dem",
        "_rastertoworldcoord_udf",
        "(tile, 1, CAST(NULL AS INT))",
    ),
    "band_null_index": ("dem", "_band_udf", "(tile, CAST(NULL AS INT))"),
    "setsrid_null_srid": ("dem", "_setsrid_udf", "(tile, CAST(NULL AS INT))"),
    "setcrs_null_crs": ("dem", "_setcrs_udf", "(tile, CAST(NULL AS STRING))"),
    "updatetype_null_type": ("dem", "_update_type_udf", "(tile, CAST(NULL AS STRING))"),
    "convolve_null_kernel": (
        "dem",
        "_convolve_udf",
        "(tile, CAST(NULL AS ARRAY<ARRAY<DOUBLE>>))",
    ),
    "histogram_null_bounds": (
        "dem",
        "_histogram_udf",
        "(tile, 16, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), false)",
    ),
}


# the rows whose NULL argument gives NULL (the others take a default for it)
NULL_OUT = {
    "band_null_index", "setsrid_null_srid", "worldtorastercoord_null_x", "rastertoworldcoord_null_y",
}  # fmt: skip


@pytest.fixture(scope="module")
def both(spark):
    for _, name, _ in CALLS.values():
        udf = _unwrap(getattr(prx, name))
        spark.udf.register(f"t_{name}_pickle", udf.form("pickle"))
        if HAS_ARROW_UDF:
            spark.udf.register(f"t_{name}_arrow", udf.form("arrow"))
    return spark


def _sql(fn, transport):
    _, name, args = CALLS[fn]
    return f"t_{name}_{transport}{args}"


def _plain(v):
    """A collected value as plain Python, to compare across transports."""
    if v is None:
        return None
    if hasattr(v, "asDict"):
        return {k: _plain(x) for k, x in v.asDict().items()}
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    return v


@needs_arrow_udf
@pytest.mark.parametrize("fn", sorted(CALLS))
def test_arrow_returns_the_pickled_answer_for_materialized_tiles(both, tiles, fn):
    kind = CALLS[fn][0]
    rows = [(i, dict(t, cellid=i + 7)) for i, t in enumerate(tiles[kind])] + [
        (99, None)
    ]
    got = {}
    for transport in ("pickle", "arrow"):
        df = both.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(1)
        df.createOrReplaceTempView("tile_transport_src")
        got[transport] = {
            r["id"]: _plain(r["t"])
            for r in both.sql(
                f"SELECT id, {_sql(fn, transport)} AS t FROM tile_transport_src"
            ).collect()
        }
    assert got["arrow"] == got["pickle"]
    if "null" in fn:
        if fn in NULL_OUT:
            assert all(
                v is None for v in got["arrow"].values()
            )  # a NULL argument is NULL out
        return
    assert any(v is not None for k, v in got["arrow"].items() if k != 99)
    first = got["arrow"][0]
    if isinstance(first, dict) and "cellid" in first and "raster" in first:
        assert isinstance(first["cellid"], int)


@needs_arrow_udf
@pytest.mark.parametrize("fn", ["band", "setsrid", "setcrs", "initnodata", "width"])
def test_arrow_keeps_a_virtual_tile_virtual_as_the_pickled_form_does(
    both, tmp_path, tiles, fn
):
    from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile

    path = tmp_path / "dem.tif"
    path.write_bytes(tiles["dem"][0]["raster"])
    vt = VirtualTile(cellid=5, path=str(path)).to_row()
    out = {}
    for transport in ("pickle", "arrow"):
        df = both.createDataFrame([(1, vt), (2, None)], f"id int, tile {TILE_SQL}")
        df.coalesce(1).createOrReplaceTempView("tile_transport_virtual")
        out[transport] = _plain(
            both.sql(
                f"SELECT {_sql(fn, transport)} AS t FROM tile_transport_virtual WHERE id = 1"
            ).collect()[0]["t"]
        )
    assert out["arrow"] == out["pickle"] and out["arrow"] is not None
    if fn == "band":
        assert out["arrow"]["raster"] is None and out["arrow"]["path"] == str(path)


@needs_arrow_udf
def test_arrow_keeps_a_big_cell_id_exact_beside_a_null_tile(both, tiles):
    """A NULL tile in the batch: the arrow_udf form hands the id over exactly, so the output carries it unchanged."""
    rows = [(1, dict(tiles["dem"][0], cellid=BIG_CELL)), (2, None)]
    df = both.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(1)
    df.createOrReplaceTempView("tile_transport_big")
    for fn in ("setsrid", "initnodata", "band_null_index"):
        t = both.sql(
            f"SELECT {_sql(fn, 'arrow')} AS t FROM tile_transport_big WHERE id = 1"
        ).collect()[0]["t"]
        if fn == "band_null_index":
            assert t is None
        else:
            assert t["raster"] is not None and t["cellid"] == BIG_CELL


@needs_arrow_udf
def test_a_group_udf_returns_the_same_over_every_transport(spark, tiles):
    udf = _unwrap(prx._combineavg_udf)
    for label in ("pickle", "arrow", "arrow_row"):
        spark.udf.register(f"t_combine_{label}", udf.form(label))
    rows = [(1, [dict(t, cellid=BIG_CELL) for t in tiles["dem"][:2]]), (2, None)]
    spark.createDataFrame(rows, f"id int, tiles array<{TILE_SQL}>").coalesce(
        1
    ).createOrReplaceTempView("t_group")
    got = {
        label: [
            _plain(r["v"])
            for r in spark.sql(
                f"SELECT t_combine_{label}(tiles) AS v FROM t_group ORDER BY id"
            ).collect()
        ]
        for label in ("pickle", "arrow")
    }
    assert got["arrow"] == got["pickle"] and got["arrow"][0]["cellid"] == BIG_CELL


def test_the_vector_arrays_return_the_same_over_both_transports(spark):
    from shapely import wkb
    from shapely.geometry import box

    geoms = [wkb.dumps(box(0, 0, 1, 1)), wkb.dumps(box(1, 0, 2, 1))]
    udf = _unwrap(pvx._coverageinvalidedges_array_udf)
    labels = ("pickle", "arrow") if HAS_ARROW_UDF else ("pickle", "arrow_row")
    for label in labels:
        spark.udf.register(f"t_cov_{label}", udf.form(label))
    df = spark.createDataFrame([(1, geoms), (2, None)], "id int, g array<binary>")
    df.coalesce(1).createOrReplaceTempView("tile_transport_cov")
    got = {
        t: [
            _plain(r["v"])
            for r in spark.sql(
                f"SELECT t_cov_{t}(g, 0D) AS v FROM tile_transport_cov ORDER BY id"
            ).collect()
        ]
        for t in labels
    }
    assert got[labels[1]] == got["pickle"]


@needs_arrow_udf
def test_the_engine_c0_returns_the_same_over_both_transports(spark, tiles):
    from databricks.labs.gbx.engine import l3 as engine_l3
    from databricks.labs.gbx.engine import run

    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    udf = engine_l3.c0("gbx_rst_slope")
    spark.udf.register("t_slope_pickle", udf.form("pickle"))
    spark.udf.register("t_slope_arrow", udf.form("arrow"))
    rows = [(i, dict(t, cellid=i + 3)) for i, t in enumerate(tiles["dem"])] + [
        (9, None)
    ]
    df = spark.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(1)
    df.createOrReplaceTempView("tile_transport_engine")
    got = {
        t: [
            _plain(r["v"])
            for r in spark.sql(
                f"SELECT t_slope_{t}(tile) AS v FROM tile_transport_engine ORDER BY id"
            ).collect()
        ]
        for t in ("pickle", "arrow")
    }
    assert got["arrow"] == got["pickle"] and got["arrow"][0]["raster"]


# table function -> (UDTF name, SQL argument text after the tile)
UDTF_CALLS = {
    "retile": ("_RstRetileUDTF", ", 32, 32"),
    "tooverlappingtiles": ("_RstToOverlappingTilesUDTF", ", 32, 32, 10"),
    "separatebands": ("_RstSeparateBandsUDTF", ""),
    "maketiles": ("_RstMakeTilesUDTF", ", 0"),
    "h3_tessellate": ("_RstH3TessellateUDTF", ", 6"),
    "h3_tessellate_null_resolution": ("_RstH3TessellateUDTF", ", CAST(NULL AS INT)"),
    "h3_rastertogridavg": ("_RstH3RasterToGridAvgUDTF", ", 6"),
}


@pytest.fixture(scope="module")
def both_udtf(spark):
    for name, _ in UDTF_CALLS.values():
        udtf = _unwrap(getattr(prx, name))
        spark.udtf.register(f"tt_{name}_pickle", udtf.form("pickle"))
        spark.udtf.register(f"tt_{name}_arrow", udtf.form("arrow"))
    return spark


def _arrow_udtf_exact(spark) -> bool:
    return caps.path(spark, "tile_udtf") == "tile_udtf:arrow"


@pytest.mark.parametrize("fn", sorted(UDTF_CALLS))
def test_an_arrow_table_function_returns_the_pickled_rows(both_udtf, tiles, fn):
    """LATERAL over materialized tiles, a big parent cell id and a NULL tile in the same batch: the same rows, in
    order, from both transports, wherever the session runs the Arrow UDTF."""
    if not _arrow_udtf_exact(both_udtf):
        pytest.skip(
            "this session keeps the pickled table functions: its Arrow UDTF converts through pandas"
        )
    name, args = UDTF_CALLS[fn]
    rows = (
        [(0, dict(tiles["dem"][0], cellid=BIG_CELL))]
        + [(i + 1, dict(t, cellid=i + 7)) for i, t in enumerate(tiles["dem"][1:])]
        + [(99, None)]
    )
    both_udtf.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(
        1
    ).createOrReplaceTempView("t_udtf_src")
    got = {}
    for transport in ("pickle", "arrow"):
        got[transport] = [
            _plain(r.asDict())
            for r in both_udtf.sql(
                f"SELECT s.id, u.* FROM t_udtf_src s, LATERAL tt_{name}_{transport}(s.tile{args}) u ORDER BY s.id"
            ).collect()
        ]
    assert got["arrow"] == got["pickle"]
    if "null" in fn:
        return
    assert got["arrow"]
    if fn in ("retile", "separatebands", "maketiles", "tooverlappingtiles"):
        assert {r["cellid"] for r in got["arrow"] if r["id"] == 0} == {BIG_CELL}
