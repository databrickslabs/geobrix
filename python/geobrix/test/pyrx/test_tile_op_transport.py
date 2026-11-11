"""The light tile ops band, setsrid, setcrs and getsubdataset choose how their rows cross the JVM <-> Python boundary.

A pickled row UDF groups up to 100 tiles into ONE pickle built on the executor JVM heap, so with multi-MB tiles the
whole partition is one heap buffer: on dedicated DBR 19 a 64-tile partition spent a third to a half of each task in
garbage collection and 4x the rows took 8x the time. The Arrow form keeps the rows off the heap in batches bounded by
bytes. Spark 4.0 hands an Arrow UDF an INT or BIGINT field as float64 when a NULL rides in the batch, so the Arrow form is
the session's choice where the server is Spark 4.1 or later (`arrow_tile_transport`), and the pickled form stays on 4.0.

These tests pin (a) the two forms and the session's choice, and (b) that the Arrow form returns what the pickled form
returns, for materialized and virtual tiles, and keeps a cell id exact or refuses it.
"""

import pytest
from pyspark.util import PythonEvalType

from databricks.labs.gbx.manager import capabilities as caps
from databricks.labs.gbx.manifest import TILE_SQL
from databricks.labs.gbx.pyrx import functions as prx

FORMS = (
    "_band_udf",
    "_band_v2_udf",
    "_setsrid_udf",
    "_setsrid_v2_udf",
    "_uf_setsrid",
    "_setcrs_udf",
    "_uf_setcrs",
    "_getsubdataset_udf",
)
BIG_CELL = 2**62 + 1  # past 2**53: an H3-sized id that a float cannot hold


def _tile_udf(name):
    """The module's UDF object (the Column API wraps it to carry the session's read cap)."""
    udf = getattr(prx, name)
    return getattr(udf, "_f", udf)


class _FakeSpark:
    """A session as the capability registry reads it: the server's `.version` and `.conf`."""

    def __init__(self, version):
        self.version = version
        self.conf = type("C", (), {"get": lambda self, k, d=None: d})()


@pytest.fixture(autouse=True)
def _no_override(monkeypatch):
    monkeypatch.delenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_TRANSPORT", raising=False)


@pytest.mark.parametrize("name", FORMS)
def test_each_form_has_a_pickled_and_an_arrow_transport(name):
    udf = _tile_udf(name)
    assert isinstance(udf, prx.SessionTileUdf)
    assert udf.form("pickle").evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.form("arrow").evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
    # what reads a UDF object (is_light_udf, carrying, the oracles' plain-function leg) sees the pickled form
    assert udf.evalType == PythonEvalType.SQL_BATCHED_UDF
    assert udf.func is udf.form("pickle").func


@pytest.mark.parametrize(
    "server, label", [("4.0.0", "pickle"), ("4.1.0", "arrow"), ("4.2.0", "arrow")]
)
def test_the_session_server_chooses_the_transport(server, label):
    spark = _FakeSpark(server)
    assert caps.path(spark, "tile_udf") == f"tile_udf:{label}"
    assert _tile_udf("_band_udf").for_session(spark) is _tile_udf("_band_udf").form(
        label
    )


def test_the_override_turns_arrow_off_on_a_server_that_has_it(monkeypatch):
    monkeypatch.setenv(caps.CAP_ENV_PREFIX + "ARROW_TILE_TRANSPORT", "0")
    spark = _FakeSpark("4.2.0")
    assert _tile_udf("_setsrid_udf").for_session(spark) is _tile_udf(
        "_setsrid_udf"
    ).form("pickle")


# ---------------------------------------------------------------------------
# (b) the Arrow form returns what the pickled form returns
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def tiles():
    from databricks.labs.gbx.bench.levels_forms import _se_dem_tile
    from databricks.labs.gbx.bench.levels_jvm import _nc_tile

    return {"dem": [_se_dem_tile(64, s) for s in range(3)], "nc": [_nc_tile(32, 0)]}


@pytest.fixture(scope="module")
def both(spark):
    for name in ("_band_udf", "_setsrid_udf", "_setcrs_udf", "_getsubdataset_udf"):
        udf = _tile_udf(name)
        spark.udf.register(f"t_{name}_pickle", udf.form("pickle"))
        spark.udf.register(f"t_{name}_arrow", udf.form("arrow"))
    return spark


CALLS = {
    "band": ("dem", "_band_udf", "(tile, 1)"),
    "setsrid": ("dem", "_setsrid_udf", "(tile, 32633)"),
    "setcrs": ("dem", "_setcrs_udf", "(tile, 'EPSG:32633')"),
    "getsubdataset": ("nc", "_getsubdataset_udf", "(tile, 'Band2')"),
}


def _sql(fn, transport):
    _, name, args = CALLS[fn]
    return f"t_{name}_{transport}{args}"


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
            r["id"]: r["t"]
            for r in both.sql(
                f"SELECT id, {_sql(fn, transport)} AS t FROM tile_transport_src"
            ).collect()
        }
    assert got["arrow"][99] is None and got["pickle"][99] is None
    for i in range(len(tiles[kind])):
        a, p = got["arrow"][i], got["pickle"][i]
        assert bytes(a["raster"]) == bytes(p["raster"])
        assert a["cellid"] == p["cellid"] == i + 7 and isinstance(a["cellid"], int)
        assert dict(a["metadata"]) == dict(p["metadata"])


@pytest.mark.parametrize("fn", ["band", "setsrid", "setcrs"])
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
        out[transport] = (
            both.sql(
                f"SELECT {_sql(fn, transport)} AS t FROM tile_transport_virtual WHERE id = 1"
            )
            .collect()[0]["t"]
            .asDict(recursive=True)
        )
    assert out["arrow"] == out["pickle"]
    assert out["arrow"]["raster"] is None and out["arrow"]["path"] == str(path)


def test_arrow_keeps_a_big_cell_id_exact_or_refuses_it(both, tiles):
    """A NULL tile in the batch: Spark 4.1+ hands the id over exactly; Spark 4.0 turns it into a float, and the Arrow
    form then refuses it with the fix instead of returning another cell's id."""
    rows = [(1, dict(tiles["dem"][0], cellid=BIG_CELL)), (2, None)]
    df = both.createDataFrame(rows, f"id int, tile {TILE_SQL}").coalesce(1)
    df.createOrReplaceTempView("tile_transport_big")
    t = both.sql(
        "SELECT t__setsrid_udf_arrow(tile, 32633) AS t FROM tile_transport_big WHERE id = 1"
    ).collect()[0]["t"]
    if t["raster"] is not None:
        assert t["cellid"] == BIG_CELL
    else:
        assert "Fix: filter the NULL tiles out first" in t["metadata"]["last_error"]
