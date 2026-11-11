"""The light tile functions choose how their rows cross the JVM <-> Python boundary.

A pickled row UDF groups up to 100 tiles into ONE pickle built on the executor JVM heap, so with multi-MB tiles the
whole partition is one heap buffer: on dedicated DBR 19 a 64-tile partition spent a third to a half of each task in
garbage collection and 4x the rows took 8x the time. The Arrow form keeps the rows off the heap in batches bounded by
bytes. Spark 4.0 hands an Arrow UDF an INT or BIGINT field as float64 when a NULL rides in the batch, so the Arrow form is
the session's choice where the server is Spark 4.1 or later (`arrow_tile_transport`), and the pickled form stays on 4.0.

These tests pin (a) that every light row UDF over tiles or large BINARY has the two forms, and that only small-scalar
UDFs stay pickled; (b) the session's choice; (c) that the Arrow form returns what the pickled form returns, for
materialized and virtual tiles, single tiles, pairs and arrays, tile and scalar outputs; and (d) that it keeps a cell
id exact or refuses it.
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

# Row UDFs of the light modules that stay pickled: small scalars in and out, where a pickle is cheap and a BIGINT cell id
# would come through Spark 4.0's Arrow as a float.
STAY_PICKLED = {
    (prx, "_binpoints_partial_udf"),  # (x, y, extent, size) -> (col, row)
    (prx, "_h3_cell_bbox_udf"),  # (cellid, srid, mode, ...) -> bbox doubles
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
    monkeypatch.delenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_TRANSPORT", raising=False)


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


@pytest.mark.parametrize(
    "udf",
    [pytest.param(o, id=f"{m}.{n}") for m, n, o in SESSION]
    + [pytest.param(o, id=k) for k, o in _factory_built().items()],
)
def test_each_form_has_a_pickled_and_an_arrow_transport(udf):
    assert isinstance(udf, _transport.SessionTileUdf)
    assert udf.form("pickle").evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.form("arrow").evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
    assert udf.form("arrow").returnType == udf.form("pickle").returnType
    # what reads a UDF object (is_light_udf, carrying, the oracles' plain-function leg) sees the pickled form
    assert udf.evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.func is udf.form("pickle").func


@pytest.mark.parametrize(
    "server, label", [("4.0.0", "pickle"), ("4.1.0", "arrow"), ("4.2.0", "arrow")]
)
def test_the_session_server_chooses_the_transport(server, label):
    spark = _FakeSpark(server)
    assert caps.path(spark, "tile_udf") == f"tile_udf:{label}"
    for udf in (_unwrap(prx._init_nodata_udf), _unwrap(prx._u_width)):
        assert udf.for_session(spark) is udf.form(label)


@pytest.mark.parametrize("legacy, label", [("true", "pickle"), ("false", "arrow")])
def test_a_session_whose_arrow_udfs_convert_through_pandas_keeps_the_pickles(
    legacy, label
):
    """Dedicated DBR 19 (Spark 4.2.0) turns the legacy pandas conversion on: an Arrow UDF there gets a NULL INT as NaN
    and a BIGINT as a float, so gbx_rst_band(tile, NULL) failed; such a session keeps the pickled rows.
    """
    spark = _FakeSpark("4.2.0", {caps.LEGACY_PANDAS_UDF_CONVERSION: legacy})
    assert caps.path(spark, "tile_udf") == f"tile_udf:{label}"
    udf = _unwrap(prx._band_udf)
    assert udf.for_session(spark) is udf.form(label)


def test_the_override_turns_arrow_off_on_a_server_that_has_it(monkeypatch):
    monkeypatch.setenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_TRANSPORT", "0")
    spark = _FakeSpark("4.2.0")
    udf = _unwrap(prx._convolve_udf)
    assert udf.for_session(spark) is udf.form("pickle")


def test_a_registration_takes_the_session_form_of_a_factory_row(monkeypatch):
    """A factory row (gbx_rst_clip) registers the form its session chooses, as a variants row does."""
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.manager import l3

    registered = {}

    class _Reg:
        def register(self, name, f, *a, **k):
            registered[name] = f

    class _Sess:
        udf = _Reg()

    monkeypatch.setattr(
        "databricks.labs.gbx._gdal_policy.carrying_session", lambda spark: _Sess()
    )
    monkeypatch.setattr(l3, "_needs_default_wrapper", lambda *a, **k: False)
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.capabilities.path",
        lambda spark, op: "tile_udf:arrow",
    )
    monkeypatch.setattr(
        "databricks.labs.gbx.manager.capabilities._sql_functions", lambda spark: False
    )
    fn = manifest.load().functions["gbx_rst_clip"]
    assert "factory" in fn.levels["L3"]
    l3.register_l3(object(), fn, "-")
    assert registered["gbx_rst_clip"].evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF


# ---------------------------------------------------------------------------
# (b) the integers of an Arrow-delivered struct
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
    tile_out = _unwrap(prx._init_nodata_udf).form("arrow").func(lossy)
    assert tile_out["raster"] is None
    assert "Fix: filter the NULL tiles out first" in tile_out["metadata"]["last_error"]
    with pytest.raises(_transport._LossyCellId):
        _unwrap(prx._u_width).form("arrow").func(lossy)
    merged = _unwrap(prx._merge_udf).form("arrow").func([lossy])
    assert (
        "Fix: take the NULL tiles out of the array" in merged["metadata"]["last_error"]
    )


# ---------------------------------------------------------------------------
# (c) the Arrow form returns what the pickled form returns
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


@pytest.fixture(scope="module")
def both(spark):
    for _, name, _ in CALLS.values():
        udf = _unwrap(getattr(prx, name))
        spark.udf.register(f"t_{name}_pickle", udf.form("pickle"))
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


@pytest.mark.parametrize("fn", sorted(CALLS))
def test_arrow_returns_the_pickled_answer_for_materialized_tiles(both, tiles, fn):
    if "null" in fn and caps.path(both, "tile_udf") != "tile_udf:arrow":
        # Spark 4.0 (and the legacy pandas conversion) hand a NULL argument over as NaN: the session never runs the
        # Arrow form there, so only a session that does compares NULL arguments
        pytest.skip(
            "this session keeps the pickled rows: an Arrow UDF gets a NULL argument as NaN here"
        )
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
        return
    assert any(v is not None for k, v in got["arrow"].items() if k != 99)
    first = got["arrow"][0]
    if isinstance(first, dict) and "cellid" in first and "raster" in first:
        assert isinstance(first["cellid"], int)


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


def test_arrow_keeps_a_big_cell_id_exact_or_refuses_it(both, tiles):
    """A NULL tile in the batch: Spark 4.1+ hands the id over exactly; Spark 4.0 turns it into a float, and the Arrow
    form then refuses it with the fix instead of returning another cell's id."""
    rows = [(1, dict(tiles["dem"][0], cellid=BIG_CELL)), (2, None)]
    df = both.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(1)
    df.createOrReplaceTempView("tile_transport_big")
    for fn in ("setsrid", "initnodata"):
        t = both.sql(
            f"SELECT {_sql(fn, 'arrow')} AS t FROM tile_transport_big WHERE id = 1"
        ).collect()[0]["t"]
        if t["raster"] is not None:
            assert t["cellid"] == BIG_CELL
        else:
            assert "Fix: filter the NULL tiles out first" in t["metadata"]["last_error"]


def test_the_vector_arrays_return_the_same_over_both_transports(spark):
    from shapely import wkb
    from shapely.geometry import box

    geoms = [wkb.dumps(box(0, 0, 1, 1)), wkb.dumps(box(1, 0, 2, 1))]
    udf = _unwrap(pvx._coverageinvalidedges_array_udf)
    spark.udf.register("t_cov_pickle", udf.form("pickle"))
    spark.udf.register("t_cov_arrow", udf.form("arrow"))
    df = spark.createDataFrame([(1, geoms), (2, None)], "id int, g array<binary>")
    df.coalesce(1).createOrReplaceTempView("tile_transport_cov")
    got = {
        t: [
            _plain(r["v"])
            for r in spark.sql(
                f"SELECT t_cov_{t}(g, 0D) AS v FROM tile_transport_cov ORDER BY id"
            ).collect()
        ]
        for t in ("pickle", "arrow")
    }
    assert got["arrow"] == got["pickle"]


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
