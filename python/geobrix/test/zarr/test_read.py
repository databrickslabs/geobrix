"""``gbx_zarr_read``: blocks and tables from registered, managed and plain stores."""

import ast
import json
import pathlib
import uuid
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")

from test.zarr import _helpers as H  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from databricks.labs.gbx.zarr import _read  # noqa: E402
from databricks.labs.gbx.zarr._manifest import (  # noqa: E402
    StoreProps,
    commit,
    create_manifest,
)
from databricks.labs.gbx.zarr._plan import Unit, plan_units  # noqa: E402
from databricks.labs.gbx.zarr._read import (  # noqa: E402
    BLOCKS_SCHEMA,
    block_to_numpy,
    gbx_zarr_read,
)
from databricks.labs.gbx.zarr._register import gbx_zarr_register  # noqa: E402
from databricks.labs.gbx.zarr._select import resolve  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import ZarrVersionUnavailable  # noqa: E402

SHAPE = (48, 73, 144)
SEL = {"latitude": slice(60, 50), "longitude": slice(-10, 10)}
ISEL = {"time": slice(0, 30)}


def _table(label):
    return f"gbx_zarr_read_{label}_{uuid.uuid4().hex[:8]}"


def _direct(store, name):
    return zarr.open_array(str(store / name), mode="r")[...]


def _assemble(rows, variable, shape, dtype="float32"):
    """The blocks of ``variable`` laid back into an array of ``shape`` (NaN where no block)."""
    out = np.zeros(shape, dtype=dtype)
    if out.dtype.kind == "f":
        out[...] = np.nan
    seen = np.zeros(shape, dtype=bool)
    for r in rows:
        if r["variable"] != variable:
            continue
        box = tuple(slice(a, b) for a, b in zip(r["start"], r["stop"]))
        block = block_to_numpy(r)
        assert (
            block.shape
            == tuple(r["shape"])
            == tuple(b - a for a, b in zip(r["start"], r["stop"]))
        )
        assert not seen[box].any(), "blocks overlap"
        out[box] = block
        seen[box] = True
    return out, seen


def _mask(shape, *index_lists):
    mask = np.zeros(shape, dtype=bool)
    mask[np.ix_(*index_lists)] = True
    return mask


@pytest.fixture(scope="module")
def v3(spark_delta, era5_v3_sharded):
    """The sharded v3 cube registered in place (read-only)."""
    table = _table("v3")
    gbx_zarr_register(spark_delta, str(era5_v3_sharded), table)
    yield table
    spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture
def utc(spark_delta):
    """Timestamps compare as wall-clock text only in a known session time zone."""
    old = spark_delta.conf.get("spark.sql.session.timeZone")
    spark_delta.conf.set("spark.sql.session.timeZone", "UTC")
    yield
    spark_delta.conf.set("spark.sql.session.timeZone", old)


# --- blocks ------------------------------------------------------------------------


def test_blocks_cover_selection_exactly(spark_delta, v3, era5_v3_sharded):
    df = gbx_zarr_read(spark_delta, v3, sel=SEL, isel=ISEL)
    assert df.schema == spark_delta.createDataFrame([], BLOCKS_SCHEMA).schema
    rows = df.collect()
    assert {r["variable"] for r in rows} == {"t2m", "u10"}  # not the coordinates
    time_idx, lat_idx = list(range(30)), list(range(12, 17))
    lon_idx = [*range(140, 144), *range(0, 5)]  # the seam: 350..357.5 then 0..10
    want = _mask(SHAPE, time_idx, lat_idx, lon_idx)
    for name in ("t2m", "u10"):
        got, seen = _assemble(rows, name, SHAPE)
        np.testing.assert_array_equal(seen, want)  # exactly the selection, no overlap
        np.testing.assert_array_equal(got[want], _direct(era5_v3_sharded, name)[want])
    first = next(r for r in rows if r["variable"] == "t2m" and r["start"][2] == 0)
    assert first["dims"] == ["time", "latitude", "longitude"]
    assert first["coord_start"][1] == "60.0" and first["coord_stop"][2] == "10.0"
    assert first["coord_start"][0].startswith("2023-01-01T00")
    assert first["dtype"] == "<f4" and first["fill_value"] == "NaN"
    attrs = json.loads(first["attrs"])
    assert attrs["units"] == "K" and "_FillValue" not in attrs  # applied by decoding


def test_blocks_respect_version_as_of(spark_delta, era5_v3_sharded, tmp_path):
    table, overlay = _table("ver"), tmp_path / "overlay"
    v1 = gbx_zarr_register(
        spark_delta,
        str(era5_v3_sharded),
        table,
        writable=True,
        overlay_root=str(overlay),
    )
    _, rows, arrays = plain_store_rows(era5_v3_sharded, origin="external")
    rows2, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], overlay, chunk=(1, 0, 1), value=7.0
    )
    wid = uuid.uuid4().hex
    new = {**rows2[-1], "write_id": wid, "op": "upsert", "expected_write_id": None}
    v2 = commit(
        spark_delta, table, pa.Table.from_pylist([new]), write_id=wid, meta={"m": "t"}
    )
    assert v2 > v1

    def read(**kw):
        df = gbx_zarr_read(spark_delta, table, variables=["t2m"], **kw)
        return _assemble(df.collect(), "t2m", SHAPE)

    old, seen = read(version_as_of=v1)
    assert seen.all()
    np.testing.assert_array_equal(old, _direct(era5_v3_sharded, "t2m"))
    new_values, _ = read()  # the latest
    np.testing.assert_array_equal(new_values, expected)
    assert (new_values[12:24, :37, 72:] == 7.0).all()
    pinned, _ = read(version_as_of=v2)
    np.testing.assert_array_equal(pinned, expected)
    with pytest.raises(ZarrVersionUnavailable):
        read(version_as_of=v2 + 10)
    latest, _ = read(timestamp_as_of="2999-01-01")  # the latest commit at or before it
    np.testing.assert_array_equal(latest, expected)
    with pytest.raises(ZarrVersionUnavailable):  # before the table existed
        read(timestamp_as_of="2000-01-01")


def test_plain_path_source(spark_delta, era5_v2):
    df = gbx_zarr_read(spark_delta, str(era5_v2), isel={"time": slice(0, 24)})
    rows = df.collect()
    assert {r["variable"] for r in rows} == {"t2m", "u10"}
    for name in ("t2m", "u10"):
        got, seen = _assemble(rows, name, SHAPE)
        assert seen[:24].all() and not seen[24:].any()
        np.testing.assert_array_equal(got[:24], _direct(era5_v2, name)[:24])
    # the v2 time coordinate is CF-decoded for the block labels
    r = next(x for x in rows if x["start"] == [0, 0, 0] and x["variable"] == "t2m")
    assert r["coord_start"][0] == "2023-01-01T00:00:00.000000000"


# --- decoding ----------------------------------------------------------------------
#
# The decoding matrix is Spark-free (test_read_unit.py), and it calls the planner. These two
# reads pass the arguments through gbx_zarr_read itself, which nothing else does: a wrapper
# that dropped either one would read as the default and pass every Spark-free test.


def test_decode_false_reaches_the_read_through_gbx_zarr_read(spark_delta, tmp_path):
    path = H.packed_store(tmp_path / "p.zarr")  # int16, scale 0.5, offset 10, fill -1
    raw = gbx_zarr_read(spark_delta, str(path), variables="p", decode=False).collect()
    stored, _ = _assemble(raw, "p", (6,), dtype="int16")
    np.testing.assert_array_equal(stored, [0, 2, -1, 4, 6, 0])  # not (v * 0.5) + 10
    assert {r["dtype"] for r in raw} == {"<i2"}


def test_use_zarr_fill_value_as_mask_reaches_the_read_through_gbx_zarr_read(
    spark_delta, tmp_path
):
    g = zarr.open_group(str(tmp_path / "z.zarr"), mode="w", zarr_format=2)
    a = g.create_array(
        "a",
        shape=(4,),
        chunks=(2,),
        dtype="i2",
        fill_value=0,
        attributes={"_ARRAY_DIMENSIONS": ["x"]},
    )
    a[:] = np.array([0, 3, 0, 5], dtype="i2")
    path = str(tmp_path / "z.zarr")
    rows = gbx_zarr_read(spark_delta, path, use_zarr_fill_value_as_mask=False).collect()
    kept, _ = _assemble(rows, "a", (4,), dtype="int16")
    np.testing.assert_array_equal(kept, [0, 3, 0, 5])  # the zeros are data
    assert {r["dtype"] for r in rows} == {"<i2"}
    masked = gbx_zarr_read(spark_delta, path).collect()  # the default masks a v2 fill
    got, _ = _assemble(masked, "a", (4,), dtype="float64")
    np.testing.assert_array_equal(got, [np.nan, 3, np.nan, 5])


# --- tables ------------------------------------------------------------------------


def test_table_output_time_column_is_timestamp(spark_delta, v3, era5_v3_sharded, utc):
    df = gbx_zarr_read(
        spark_delta,
        v3,
        output="table",
        isel={"time": slice(0, 2), "latitude": slice(0, 3), "longitude": slice(0, 4)},
    )
    assert df.columns == ["time", "latitude", "longitude", "t2m", "u10"]
    types = {f.name: f.dataType for f in df.schema.fields}
    assert isinstance(types["time"], T.TimestampType)
    assert isinstance(types["latitude"], T.DoubleType)
    assert isinstance(types["t2m"], T.FloatType)  # float32 stays FLOAT
    ordered = df.orderBy("time", F.col("latitude").desc(), "longitude")
    rows = ordered.collect()
    assert len(rows) == 2 * 3 * 4
    assert rows[0]["time"] == datetime(2023, 1, 1, 0, 0, 0)  # the session zone is UTC
    assert (rows[0]["latitude"], rows[0]["longitude"]) == (90.0, 0.0)
    assert rows[-1]["time"] == datetime(2023, 1, 1, 1, 0, 0)
    for name in ("t2m", "u10"):
        got = np.array([r[name] for r in rows], dtype="float32").reshape(2, 3, 4)
        np.testing.assert_array_equal(
            got, _direct(era5_v3_sharded, name)[0:2, 0:3, 0:4]
        )
    # the instant is exact whatever the session time zone
    epoch = ordered.select(F.unix_micros("time")).first()[0]
    assert epoch == int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()) * 10**6


def test_a_table_has_one_spark_type_per_kind_and_nulls_for_masked_cells(
    spark_delta, dtype_matrix
):
    from decimal import Decimal

    path, want = dtype_matrix
    df = gbx_zarr_read(spark_delta, str(path), output="table")
    assert {f.name: f.dataType.simpleString() for f in df.schema.fields} == {
        "lev": "int",
        "x": "bigint",
        "i1": "tinyint",
        "i2": "smallint",
        "i4": "int",
        "i8": "bigint",
        "u1": "smallint",
        "u2": "int",
        "u4": "bigint",
        "u8": "decimal(20,0)",
        "u8m": "decimal(20,0)",
        "b": "boolean",
        "f2": "float",
        "f4": "float",
        "f8": "double",
        "m2": "smallint",
        "mf": "double",
        "pk": "double",
    }
    rows = df.orderBy("lev", "x").collect()
    assert rows[0]["u8"] == Decimal(2**64 - 1) and rows[0]["i8"] == 2**53 + 1
    assert [r["m2"] for r in rows][:4] == [5, None, 7, None]
    assert [r["pk"] for r in rows][:3] == [11.0, None, 12.0]
    assert (
        rows[1]["mf"] != rows[1]["mf"] and rows[2]["mf"] is None
    )  # NaN data vs a masked cell
    assert [r["b"] for r in rows][:3] == [False, True, True]


def test_an_error_while_spark_runs_the_read_arrives_as_a_python_exception(
    spark_delta, tmp_path
):
    """Planning errors keep their classes; a task's error is Spark's PythonException."""
    import shutil

    from pyspark.errors import PythonException

    path = H.packed_store(tmp_path / "gone.zarr")
    df = gbx_zarr_read(spark_delta, str(path), variables="p")
    shutil.rmtree(path / "p")
    with pytest.raises(PythonException) as e:
        df.collect()
    assert "Fix:" in str(e.value) and "Zarr" in str(e.value)


def test_empty_selection_returns_empty_frame(spark_delta, v3):
    blocks = gbx_zarr_read(spark_delta, v3, sel={"latitude": slice(95, 91)})
    assert blocks.count() == 0
    assert blocks.schema == spark_delta.createDataFrame([], BLOCKS_SCHEMA).schema
    table = gbx_zarr_read(
        spark_delta, v3, output="table", sel={"latitude": slice(95, 91)}
    )
    assert table.count() == 0
    assert table.columns == ["time", "latitude", "longitude", "t2m", "u10"]
    assert isinstance(table.schema["time"].dataType, T.TimestampType)


def test_the_blocks_schema_is_the_documented_ddl():
    assert BLOCKS_SCHEMA == (
        "variable STRING, dims ARRAY<STRING>, start ARRAY<BIGINT>, stop ARRAY<BIGINT>, "
        "coord_start ARRAY<STRING>, coord_stop ARRAY<STRING>, dtype STRING, "
        "shape ARRAY<BIGINT>, data BINARY, fill_value STRING, attrs STRING"
    )


# --- managed stores and snapshots ----------------------------------------------------


_COMMIT_KEYS = (
    "kind",
    "array",
    "key",
    "shard_key",
    "file",
    "offset",
    "length",
    "checksum",
    "origin",
    "identity",
    "write_id",
    "tag",
    "meta",
    "op",
    "expected_write_id",
)


def _manage(spark, store, table):
    """A managed manifest over ``store``'s files (what gbx_zarr_write will make later)."""
    from databricks.labs.gbx.zarr._register import read_base_metadata

    metas, arrays, _ = read_base_metadata(str(store))
    _, rows, _ = plain_store_rows(store, origin="gbx")
    wid = uuid.uuid4().hex
    recs = []
    for key, raw in sorted(metas.items()):
        prefix, _, name = key.rpartition("/")
        recs.append(
            dict(
                kind="array" if prefix in arrays else "group",
                array=prefix or None,
                key=name,
                origin="gbx",
                meta=raw.decode(),
            )
        )
    recs += [{k: v for k, v in r.items() if k != "meta"} for r in rows]
    table_rows = pa.Table.from_pylist(
        [
            {
                k: {
                    **r,
                    "write_id": wid,
                    "op": "upsert",
                    "expected_write_id": None,
                }.get(k)
                for k in _COMMIT_KEYS
            }
            for r in recs
        ]
    )
    create_manifest(
        spark, table, StoreProps("managed", str(store), None, True, "fuse", "7 days")
    )
    return commit(spark, table, table_rows, write_id=wid, meta={"m": 1})


def test_a_managed_store_reads_from_its_data_root(spark_delta, era5_v3_unsharded):
    """One end-to-end managed read; v2's chunk keys are read the same way, without a table,
    in test_read_unit.py."""
    table = _table("managed")
    _manage(spark_delta, era5_v3_unsharded, table)
    try:
        rows = gbx_zarr_read(spark_delta, table, variables=["t2m"]).collect()
        got, seen = _assemble(rows, "t2m", SHAPE)
        assert seen.all()
        np.testing.assert_array_equal(got, _direct(era5_v3_unsharded, "t2m"))
    finally:
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


# --- planning ----------------------------------------------------------------------


def test_units_respect_budget(era5_v3_sharded):
    _, rows, arrays = plain_store_rows(era5_v3_sharded)
    meta = arrays["t2m"]
    per_chunk = int(np.prod(meta.chunks)) * meta.itemsize
    axes = resolve(meta.dims, meta.shape, {}, None, None)
    by_shard = {}
    for r in rows:
        if r["array"] == "t2m":
            by_shard.setdefault(r["shard_key"], []).append(r)
    grid = int(np.prod(meta.grid_shape))
    assert grid == 4 * 2 * 2

    units = plan_units(meta, axes, by_shard, 2 * per_chunk)
    assert len(units) == grid // 2 and all(len(u.chunks) == 2 for u in units)
    chunks = [c for u in units for c in u.chunks]
    assert len(chunks) == len(set(chunks)) == grid  # every chunk, once
    for u in units:  # a unit carries the rows of its own shards only
        keys = {
            meta.key_encoding.chunk_key(
                tuple(c // n for c, n in zip(ch, meta.shard.cps))
            )
            for ch in u.chunks
        }
        assert {r["shard_key"] for r in u.rows} == keys and u.array == "t2m"
    # a budget below one chunk is one chunk per unit, never zero units or empty ones
    assert {len(u.chunks) for u in plan_units(meta, axes, by_shard, 1)} == {1}
    assert len(plan_units(meta, axes, by_shard, 10**12)) == 1
    assert (
        plan_units(
            meta, resolve(meta.dims, meta.shape, {}, None, {"time": []}), by_shard, 1
        )
        == []
    )


def test_a_unit_round_trips_through_json():
    row = {"kind": "chunk", "array": "t2m", "key": "c/0/1/2", "file": None, "offset": 0}
    unit = Unit("t2m", ((0, 1, 2), (0, 1, 3)), (row,))
    again = Unit.from_json(unit.to_json())
    assert again == unit and isinstance(again.chunks[0], tuple)
    assert isinstance(again.rows, tuple)
    assert Unit.from_json(Unit("a", (), ()).to_json()) == Unit("a", (), ())
    # columns a task never reads stay on the driver, and no credential-like field travels
    heavy = Unit(
        "t2m", ((0,),), ({**row, "write_id": "f" * 32, "meta": "{}", "tag": None},)
    )
    assert set(json.loads(heavy.to_json())["rows"][0]) == set(row)


@pytest.mark.parametrize("n", [1, 8, 24, 37])
def test_every_unit_gets_a_partition_of_its_own(spark_delta, n):
    """Hashing the ids into n partitions leaves some empty and doubles up others; the
    Range scan joined to the units is exactly one unit per partition."""
    from pyspark.sql import functions as F

    units = [Unit("t2m", ((i,),), ()) for i in range(n)]
    work = _read._work_frame(spark_delta, units)
    assert work.columns == ["id", "unit"]
    by_partition = work.groupBy(F.spark_partition_id().alias("p")).count().collect()
    assert len(by_partition) == n and {r["count"] for r in by_partition} == {1}
    assert sorted(r["id"] for r in work.collect()) == list(range(n))
    assert {Unit.from_json(r["unit"]).chunks[0][0] for r in work.collect()} == set(
        range(n)
    )


# --- arguments ---------------------------------------------------------------------


def test_source_classification():
    assert _read.classify_source("/Volumes/c/s/v/x.zarr", "auto") == "path"
    assert _read.classify_source("s3://bucket/x.zarr", "auto") == "path"
    assert _read.classify_source("abfss://c@a.dfs.core.windows.net/x", "auto") == "path"
    assert _read.classify_source("cat.sch.tbl", "auto") == "table"
    assert _read.classify_source("tbl", "auto") == "table"
    assert _read.classify_source("cat.sch.tbl", "path") == "path"
    assert _read.classify_source("/x", "table") == "table"
    with pytest.raises(ValueError, match="source_type"):
        _read.classify_source("x", "location")


def test_the_zarr_package_uses_no_api_that_spark_connect_forbids():
    """Serverless and Connect have no sparkContext, JVM gateway, RDD or conf mutation."""
    package = pathlib.Path(_read.__file__).parent
    found = []
    for path in sorted(package.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute) and node.attr in (
                "sparkContext",
                "_jvm",
                "_jsc",
                "rdd",
            ):
                found.append(f"{path.name}:{node.lineno} .{node.attr}")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "set"
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == "conf"
            ):
                found.append(f"{path.name}:{node.lineno} conf.set")
    assert not found, found


# --- a table of variables chunked differently, from a registered store ----------------------


@pytest.fixture(scope="module")
def mixed(spark_delta, tmp_path_factory):
    """A registered v3 store whose two variables have different chunks, and ``b`` shards
    (see ``_helpers.make_mixed``)."""
    path = H.make_mixed(tmp_path_factory.mktemp("mixed") / "mixed.zarr")
    table = _table("mixed")
    gbx_zarr_register(spark_delta, str(path), table)
    yield path, table
    spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


def _xarray_dataset(path):
    import xarray as xr
    from xarray.backends.zarr import ZarrStore

    return xr.open_dataset(
        ZarrStore.open_group(str(path), mode="r", consolidated=False)
    )


def test_a_table_of_variables_chunked_differently_equals_xarray_from_a_registered_store(
    spark_delta, mixed, monkeypatch
):
    path, table = mixed
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)  # many units, merged rows
    df = gbx_zarr_read(spark_delta, table, output="table").orderBy("y", "x")
    rows = df.collect()
    theirs = _xarray_dataset(path)
    assert df.columns == ["y", "x", "a", "b"] and len(rows) == 96
    assert [(r["y"], r["x"]) for r in rows] == [
        (y, x) for y in range(8) for x in range(12)
    ]
    np.testing.assert_array_equal([r["a"] for r in rows], theirs.a.values.reshape(-1))
    np.testing.assert_array_equal([r["b"] for r in rows], theirs.b.values.reshape(-1))
    assert min(abs(r["b"]) for r in rows) >= 1  # no cell read as the fill value
