"""The light combine functions cross the JVM <-> Python boundary as Arrow batches, not pickled rows.

``gbx_rst_combine*(collect_list(tile))`` hands a whole group (N tiles of tens of MB) to Python as one row. A scalar
(pickled) UDF makes the executor JVM pickle each group's tiles on its heap, and at the default shuffle width that
exhausted the heap (JVM OutOfMemoryError, executors lost). An Arrow-optimized UDF moves the same rows as Arrow buffers
off the heap. These tests pin (a) that every combine UDF is Arrow-backed and (b) that going through Arrow changes no
output byte or edge-case answer.
"""

import numpy as np
import pytest
from pyspark.util import PythonEvalType
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx import manager as gbx_manager
from databricks.labs.gbx.manifest import TILE_SQL
from databricks.labs.gbx.pyrx import functions as prx
from databricks.labs.gbx.pyrx.core import agg

STATS = ("avg", "min", "max", "sum", "count", "median", "stddev")
ND = -9999.0


def _udf(stat: str, variant: str):
    return getattr(prx, f"_combine{stat}{variant}")


def _tile_bytes(values):
    arr = np.array(values, dtype="float64").reshape(1, 2, 2)
    profile = dict(
        driver="GTiff",
        width=2,
        height=2,
        count=1,
        dtype="float64",
        crs="EPSG:4326",
        transform=from_origin(0.0, 2.0, 1.0, 1.0),
        nodata=ND,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        return mf.read()


def _row(raster, cellid=0, metadata=None):
    return {
        "cellid": cellid,
        "raster": raster,
        "path": None,
        "path_mode": None,
        "window": None,
        "clip_polygon": None,
        "clip_crs": None,
        "crs": None,
        "metadata": metadata or {},
    }


def _core(stat: str):
    return (
        agg.combineavg_tiles if stat == "avg" else getattr(agg, f"combine_{stat}_tiles")
    )


T0 = _tile_bytes([1.0, 10.0, ND, 4.0])
T1 = _tile_bytes([2.0, ND, ND, 4.0])
T2 = _tile_bytes([3.0, 20.0, ND, 4.0])


# ---------------------------------------------------------------------------
# (a) the shared path: every combine UDF is Arrow-backed
# ---------------------------------------------------------------------------


def _all_combine_udfs():
    return [(f"{s}{v}", _udf(s, v)) for s in STATS for v in ("_udf", "_v2_udf")]


@pytest.mark.parametrize(
    "udf", [u for _, u in _all_combine_udfs()], ids=[n for n, _ in _all_combine_udfs()]
)
def test_combine_udfs_cross_as_arrow_batches(udf):
    """An Arrow-optimized UDF (SQL_ARROW_BATCHED_UDF) keeps a group's tiles off the executor JVM heap; the
    scalar (pickled) form is what ran executors out of memory."""
    assert udf.evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF


# ---------------------------------------------------------------------------
# (b) Arrow changes no answer: every stat, SQL form over collect_list
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sql_spark(spark):
    gbx_manager.register_family(
        spark,
        ("gbx_rst_",),
        only=[f"gbx_rst_combine{s}" for s in STATS],
        level="L3",
    )
    return spark


@pytest.mark.parametrize("stat", STATS)
def test_combine_sql_over_collect_list_is_the_core_answer(sql_spark, stat):
    """``gbx_rst_combine<stat>(collect_list(tile))`` returns the core reducer's bytes, per group."""
    df = sql_spark.createDataFrame(
        [
            ("a", _row(T0, cellid=7)),
            ("a", _row(T1, cellid=7)),
            ("a", _row(T2, cellid=7)),
            ("b", _row(T2, cellid=1)),
            ("b", _row(T0, cellid=2)),
        ],
        f"key string, tile {TILE_SQL}",
    )
    df.createOrReplaceTempView(f"combine_arrow_{stat}")
    rows = {
        r["key"]: r["r"]
        for r in sql_spark.sql(
            f"SELECT key, gbx_rst_combine{stat}(collect_list(tile)) AS r "
            f"FROM combine_arrow_{stat} GROUP BY key"
        ).collect()
    }
    # collect_list gives no member order; the reducers do not depend on it (checked here by comparing as sets of
    # orderings: the answer must equal the core over one of the member orders)
    for key, members, cellid in (
        ("a", (T0, T1, T2), 7),
        ("b", (T2, T0), -1),
    ):
        out = rows[key]
        assert out["cellid"] == cellid
        assert "last_error" not in out["metadata"]
        expected = {bytes(_core(stat)(list(order))) for order in _orders(members)}
        assert bytes(out["raster"]) in expected


def _orders(members):
    import itertools

    return itertools.permutations(members)


# ---------------------------------------------------------------------------
# (b) Arrow changes no edge-case answer (combineavg stands for the shared body)
# ---------------------------------------------------------------------------


def _edge_rows(sql_spark, rows):
    df = sql_spark.createDataFrame(rows, f"id int, tiles array<{TILE_SQL}>")
    df.createOrReplaceTempView("combine_arrow_edges")
    return {
        r["id"]: r["r"]
        for r in sql_spark.sql(
            "SELECT id, gbx_rst_combineavg(tiles) AS r FROM combine_arrow_edges"
        ).collect()
    }


def test_combineavg_edge_answers_survive_the_arrow_boundary(sql_spark):
    corrupt = _row(b"not a raster", cellid=0)
    error_tile = _row(None, cellid=5, metadata={"last_error": "upstream failed"})
    got = _edge_rows(
        sql_spark,
        [
            (1, [_row(T0, 7), None, _row(T1, 7)]),  # a NULL member is skipped
            (2, []),  # empty array
            (3, None),  # NULL array
            (4, [error_tile]),  # only error tiles: the first one, as it is
            (5, [corrupt, _row(T0, 9)]),  # a corrupt member is skipped and counted
        ],
    )
    assert bytes(got[1]["raster"]) == bytes(agg.combineavg_tiles([T0, T1]))
    assert (
        got[1]["cellid"] == 7
    )  # an int, as sent, though a NULL rode in the same batch
    assert "last_error" not in got[1]["metadata"]
    assert got[2] is None
    assert got[3] is None
    assert got[4]["raster"] is None
    assert got[4]["cellid"] == 5
    assert got[4]["metadata"] == {"last_error": "upstream failed"}
    assert bytes(got[5]["raster"]) == bytes(agg.combineavg_tiles([T0]))
    assert (
        got[5]["metadata"]["last_error"]
        == "RST_CombineAvg: skipped 1 corrupt input tile(s)"
    )


BIG_CELL = 2**62 + 1  # past 2**53: an H3-sized id that a float cannot hold


def test_combineavg_big_cell_ids_arrive_exact(sql_spark):
    got = _edge_rows(
        sql_spark,
        [(1, [_row(T0, BIG_CELL), _row(T1, BIG_CELL)]), (2, [_row(T0, BIG_CELL)])],
    )
    assert got[1]["cellid"] == BIG_CELL
    assert got[2]["cellid"] == BIG_CELL


def test_combineavg_big_cell_ids_beside_a_null_are_exact_or_refused(sql_spark):
    """A NULL member (or NULL cellid) can make an Arrow-optimized UDF (pyspark 4.0) read BIGINT ids as floats: the
    answer is the exact id (pyspark 4.1) or an error tile that says how to fix it, never a rounded id.
    """
    got = _edge_rows(
        sql_spark,
        [
            (
                1,
                [_row(T0, BIG_CELL), None, _row(T1, BIG_CELL)],
            ),  # a NULL member in the array
            (
                2,
                [_row(T0, BIG_CELL), _row(T1, BIG_CELL)],
            ),  # clean, but a NULL cellid rides in the batch
            (3, [_row(T0, None), _row(T1, 3)]),
        ],
    )
    for key in (1, 2):
        out = got[key]
        if out["raster"] is not None:
            assert out["cellid"] == BIG_CELL
        else:
            assert "Fix:" in out["metadata"]["last_error"]
    assert got[3]["cellid"] == -1  # a NULL id differs from 3: no shared cell


def test_exact_member_restores_floats_and_refuses_lossy_ids():
    """pyspark 4.0 hands integers beside a NULL as floats; a whole number below 2**53 is restored, a larger id refused."""
    fixed = prx._exact_member(
        {"cellid": 3.0, "raster": b"x", "window": {"col_off": 1.0, "row_off": 2.0, "width": 3.0, "height": 4.0}}
    )  # fmt: skip
    assert fixed["cellid"] == 3 and isinstance(fixed["cellid"], int)
    assert fixed["window"] == {"col_off": 1, "row_off": 2, "width": 3, "height": 4}
    assert all(isinstance(v, int) for v in fixed["window"].values())
    assert prx._exact_member(None) is None
    assert prx._exact_member({"cellid": BIG_CELL})["cellid"] == BIG_CELL
    with pytest.raises(prx._LossyCellId, match="Fix:"):
        prx._exact_member({"cellid": float(BIG_CELL)})


def test_combineavg_windowed_member_beside_a_materialized_one(sql_spark, tmp_path):
    """A virtual tile's window arrives as floats when another member has none (pyspark 4.0): the same pixels are read."""
    arr = np.full((1, 4, 4), 0.0)
    arr[0, 1:3, 1:3] = [[2.0, 20.0], [ND, ND]]
    path = tmp_path / "scene.tif"
    profile = dict(
        driver="GTiff", width=4, height=4, count=1, dtype="float64", nodata=ND,
        crs="EPSG:4326", transform=from_origin(0.0, 4.0, 1.0, 1.0),
    )  # fmt: skip
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        path.write_bytes(mf.read())
    windowed = _row(None, cellid=0)
    windowed.update(
        path=str(path),
        path_mode="external",
        window={"col_off": 1, "row_off": 1, "width": 2, "height": 2},
    )
    got = _edge_rows(sql_spark, [(1, [windowed, _row(T0, 0)])])
    with MemoryFile(bytes(got[1]["raster"])) as mf, mf.open() as ds:
        pixels = ds.read(1)
    np.testing.assert_allclose(pixels, [[1.5, 15.0], [ND, 4.0]])
