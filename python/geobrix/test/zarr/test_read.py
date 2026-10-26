"""``gbx_zarr_read``: blocks and tables from registered, managed and plain stores."""

import ast
import json
import logging
import pathlib
import uuid
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")

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
    block_to_xarray,
    gbx_zarr_read,
)
from databricks.labs.gbx.zarr._register import gbx_zarr_register  # noqa: E402
from databricks.labs.gbx.zarr._select import resolve  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    make_era5_like,
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrAccessUnavailable,
    ZarrGridMismatch,
    ZarrUnsupported,
    ZarrVersionUnavailable,
)

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


def test_block_helpers_round_trip_a_row(spark_delta, v3, era5_v3_sharded):
    row = gbx_zarr_read(
        spark_delta,
        v3,
        variables="t2m",
        isel={"time": slice(0, 4), "latitude": slice(0, 3), "longitude": slice(5, 9)},
    ).first()
    arr = block_to_numpy(row)
    np.testing.assert_array_equal(arr, _direct(era5_v3_sharded, "t2m")[0:4, 0:3, 5:9])
    assert arr.flags.writeable
    xa = block_to_xarray(row)
    assert xa.dims == ("time", "latitude", "longitude") and xa.name == "t2m"
    assert xa.attrs["units"] == "K"
    np.testing.assert_array_equal(xa.values, arr)
    asdict = row.asDict()  # a plain dict works the same
    np.testing.assert_array_equal(block_to_numpy(asdict), arr)


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


def test_plain_path_source_by_uri_and_registered_agree(
    spark_delta, v3, era5_v3_sharded
):
    a = gbx_zarr_read(
        spark_delta, f"file:{era5_v3_sharded}", variables=["u10"], access="fuse"
    )
    b = gbx_zarr_read(spark_delta, v3, variables=["u10"])
    ga, _ = _assemble(a.collect(), "u10", SHAPE)
    gb, _ = _assemble(b.collect(), "u10", SHAPE)
    np.testing.assert_array_equal(ga, gb)


def test_blocks_on_irregular_axis(spark_delta, tmp_path):
    # Gaussian-like: monotonic, no constant step
    lat = 90 * np.tanh(np.linspace(2.5, -2.5, 73)) / np.tanh(2.5)
    assert np.ptp(np.diff(lat)) > 1.0
    path = make_era5_like(tmp_path / "gauss.zarr", latitude=lat)
    lo, hi = lat[40], lat[20]
    df = gbx_zarr_read(
        spark_delta,
        str(path),
        variables=["t2m"],
        sel={"latitude": slice(hi, lo)},
        isel={"time": slice(0, 12)},
    )
    got, seen = _assemble(df.collect(), "t2m", SHAPE)
    want = _mask(SHAPE, range(12), range(20, 41), range(144))
    np.testing.assert_array_equal(seen, want)
    np.testing.assert_array_equal(got[want], _direct(path, "t2m")[want])


def test_a_dimension_the_attributes_call_a_longitude_wraps_at_the_seam(
    spark_delta, tmp_path
):
    """coord_attrs reach the selection planner: the name 'xi' says nothing, the units do."""
    path = tmp_path / "xi.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    xi = g.create_array(
        "xi", shape=(144,), chunks=(72,), dtype="f8", dimension_names=("xi",)
    )
    xi[:] = np.arange(144) * 2.5
    xi.attrs["units"] = "degrees_east"
    v = g.create_array(
        "v", shape=(144,), chunks=(72,), dtype="f4", dimension_names=("xi",)
    )
    v[:] = np.arange(144, dtype="f4")
    df = gbx_zarr_read(spark_delta, str(path), sel={"xi": slice(-10, 10)})
    got, seen = _assemble(df.collect(), "v", (144,))
    np.testing.assert_array_equal(
        np.flatnonzero(seen), [*range(0, 5), *range(140, 144)]
    )


def test_a_scalar_variable_reads_as_one_block(spark_delta, tmp_path):
    path = tmp_path / "scalar.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    crs = g.create_array("crs", shape=(), chunks=(), dtype="i4", fill_value=0)
    crs[...] = 4326
    v = g.create_array("v", shape=(4,), chunks=(2,), dtype="f4", dimension_names=("k",))
    v[:] = 1
    default = gbx_zarr_read(spark_delta, str(path)).collect()
    assert {r["variable"] for r in default} == {"v"}  # a scalar is not gridded data
    (row,) = gbx_zarr_read(spark_delta, str(path), variables="crs").collect()
    assert row["dims"] == [] and row["start"] == [] and row["shape"] == []
    assert int(block_to_numpy(row)) == 4326


# --- decoding ----------------------------------------------------------------------


def _packed_store(path, zarr_format=3):
    """int16 with scale_factor/add_offset and a _FillValue, and a v2-style zero fill_value."""
    g = zarr.open_group(str(path), mode="w", zarr_format=zarr_format)
    attrs = {"scale_factor": 0.5, "add_offset": 10.0, "_FillValue": -1}
    if zarr_format == 2:
        attrs["_ARRAY_DIMENSIONS"] = ["x"]
    a = g.create_array(
        "p",
        shape=(6,),
        chunks=(3,),
        dtype="i2",
        fill_value=0,
        attributes=attrs,
        **({"dimension_names": ("x",)} if zarr_format == 3 else {}),
    )
    a[:] = np.array([0, 2, -1, 4, 6, 0], dtype="i2")
    return path


def test_decode_false_returns_stored_values_and_attributes(spark_delta, tmp_path):
    path = _packed_store(tmp_path / "p.zarr")
    decoded = gbx_zarr_read(spark_delta, str(path), variables="p").collect()
    raw = gbx_zarr_read(spark_delta, str(path), variables="p", decode=False).collect()
    got, _ = _assemble(decoded, "p", (6,), dtype="float64")
    np.testing.assert_array_equal(
        got, [10.0, 11.0, np.nan, 12.0, 13.0, 10.0]
    )  # (v * 0.5) + 10, -1 masked
    stored, _ = _assemble(raw, "p", (6,), dtype="int16")
    np.testing.assert_array_equal(stored, [0, 2, -1, 4, 6, 0])
    assert {r["dtype"] for r in raw} == {"<i2"} and decoded[0]["dtype"].startswith("<f")
    # decoding consumed the packing attributes; the raw block still carries them
    assert "scale_factor" not in json.loads(decoded[0]["attrs"])
    assert json.loads(raw[0]["attrs"])["scale_factor"] == 0.5
    # a never-written cell holds, in the decoded block's units, the decoded fill value
    assert raw[0]["fill_value"] == "0" and decoded[0]["fill_value"] == "10.0"


def test_use_zarr_fill_value_as_mask_keeps_the_zeros_of_a_v2_integer_array(
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
    masked, _ = _assemble(
        gbx_zarr_read(spark_delta, path).collect(), "a", (4,), dtype="float64"
    )
    np.testing.assert_array_equal(masked, [np.nan, 3, np.nan, 5])  # xarray's default
    kept = gbx_zarr_read(spark_delta, path, use_zarr_fill_value_as_mask=False).collect()
    got, _ = _assemble(kept, "a", (4,), dtype="int16")
    np.testing.assert_array_equal(got, [0, 3, 0, 5])
    assert kept[0]["dtype"] == "<i2"


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


def test_table_by_label_selection_and_a_chosen_variable(spark_delta, v3, utc):
    df = gbx_zarr_read(
        spark_delta,
        v3,
        output="table",
        variables=["u10"],
        sel={"time": slice("2023-01-01T05", "2023-01-01T06"), "latitude": 0.0},
        isel={"longitude": [0, 1]},
    )
    assert df.columns == ["time", "latitude", "longitude", "u10"]
    rows = df.orderBy("time", "longitude").collect()
    assert [(r["time"].hour, r["latitude"], r["longitude"]) for r in rows] == [
        (5, 0.0, 0.0),
        (5, 0.0, 2.5),
        (6, 0.0, 0.0),
        (6, 0.0, 2.5),
    ]


def test_table_rejects_mixed_dims(spark_delta, v3):
    with pytest.raises(ZarrGridMismatch) as e:
        gbx_zarr_read(spark_delta, v3, output="table", variables=["t2m", "latitude"])
    assert "dimensions" in str(e.value) and "Fix:" in str(e.value)
    assert "same dimensions, or read them separately" in e.value.fix


def test_table_of_variables_with_different_chunking(spark_delta, tmp_path):
    path = tmp_path / "mix.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    for name, chunks in (("a", (2, 3)), ("b", (4, 2))):
        arr = g.create_array(
            name, shape=(4, 6), chunks=chunks, dtype="f8", dimension_names=("y", "x")
        )
        arr[:] = np.arange(24, dtype="f8").reshape(4, 6) * (1 if name == "a" else -1)
    rows = gbx_zarr_read(spark_delta, str(path), output="table").orderBy("y", "x")
    got = rows.collect()
    assert len(got) == 24 and rows.columns == [
        "y",
        "x",
        "a",
        "b",
    ]  # no coordinates: index
    assert isinstance(rows.schema["y"].dataType, T.LongType)
    np.testing.assert_array_equal([r["a"] for r in got], np.arange(24.0))
    np.testing.assert_array_equal([r["b"] for r in got], -np.arange(24.0))


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

    path = _packed_store(tmp_path / "gone.zarr")
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


def test_a_managed_store_reads_from_its_data_root(
    spark_delta, era5_v3_unsharded, era5_v2
):
    for store in (era5_v3_unsharded, era5_v2):  # chunk rows, v3 and v2
        table = _table("managed")
        _manage(spark_delta, store, table)
        rows = gbx_zarr_read(spark_delta, table, variables=["t2m"]).collect()
        got, seen = _assemble(rows, "t2m", SHAPE)
        assert seen.all()
        np.testing.assert_array_equal(got, _direct(store, "t2m"))
        spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.filterwarnings("ignore:Consolidated metadata")
def test_stale_consolidated_metadata_is_never_served(spark_delta, tmp_path):
    base = tmp_path / "base.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    z = g.create_array("z", shape=(4,), chunks=(2,), dtype="f4", dimension_names=("k",))
    z[:] = np.arange(4, dtype="f4")
    zarr.consolidate_metadata(str(base))
    z.resize((6,))  # the array's own document moves on; the consolidated one does not
    z[4:] = [10, 11]
    root = json.loads((base / "zarr.json").read_text())
    assert root["consolidated_metadata"]["metadata"]["z"]["shape"] == [4]  # stale

    table = _table("consol")
    gbx_zarr_register(spark_delta, str(base), table)
    rows = gbx_zarr_read(spark_delta, table).collect()
    got, seen = _assemble(rows, "z", (6,))
    assert seen.all() and list(got) == [0, 1, 2, 3, 10, 11]  # the per-array shape

    docs = _read.store_documents(
        {
            "zarr.json": json.dumps(root).encode(),
            "z/zarr.json": b"{}",
            ".zmetadata": b"{}",
        }
    )
    assert "consolidated_metadata" not in json.loads(docs["zarr.json"])
    assert ".zmetadata" not in docs and docs["z/zarr.json"] == b"{}"


# --- arrays GeoBrix cannot decode ----------------------------------------------------


def _store_with_a_string_coordinate(path):
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    v = g.create_array(
        "v", shape=(3,), chunks=(2,), dtype="f4", dimension_names=("station",)
    )
    v[:] = np.array([1, 2, 3], dtype="f4")
    s = g.create_array(
        "station", shape=(3,), chunks=(2,), dtype=str, dimension_names=("station",)
    )
    s[:] = np.array(["a", "bb", "ccc"], dtype=object)
    return path


def test_an_undecodable_array_raises_only_when_it_is_decoded(
    spark_delta, tmp_path, caplog
):
    path = _store_with_a_string_coordinate(tmp_path / "st.zarr")
    table = _table("strings")
    gbx_zarr_register(spark_delta, str(path), table)
    for source in (str(path), table):
        with caplog.at_level(logging.WARNING, logger=_read.__name__):
            rows = gbx_zarr_read(spark_delta, source).collect()  # the rest reads
        got, seen = _assemble(rows, "v", (3,))
        assert seen.all() and list(got) == [1, 2, 3]
        assert rows[0]["coord_start"] == [None]  # no label for a string coordinate
        assert any("station" in r.getMessage() for r in caplog.records)
        # asking for the array, or for labels from it, names it
        with pytest.raises(ZarrUnsupported) as e:
            gbx_zarr_read(spark_delta, source, variables=["station"])
        assert "'station'" in str(e.value) and "Fix:" in str(e.value)
        with pytest.raises(ZarrUnsupported) as e:
            gbx_zarr_read(spark_delta, source, sel={"station": "a"})
        assert "'station'" in str(e.value)
        with pytest.raises(ZarrUnsupported) as e:
            gbx_zarr_read(spark_delta, source, output="table")
        assert "'station'" in str(e.value)
        by_position = gbx_zarr_read(spark_delta, source, isel={"station": [0, 2]})
        _, seen = _assemble(by_position.collect(), "v", (3,))
        assert list(seen) == [True, False, True]
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


def test_small_budgets_split_the_read_into_many_tasks_without_losing_a_cell(
    spark_delta, v3, era5_v3_sharded, monkeypatch
):
    monkeypatch.setattr(_read, "_budget_bytes", lambda: 1)  # one chunk per unit
    rows = gbx_zarr_read(spark_delta, v3, variables=["t2m"]).collect()
    assert len(rows) == 16  # one block per (variable, chunk)
    got, seen = _assemble(rows, "t2m", SHAPE)
    assert seen.all()
    np.testing.assert_array_equal(got, _direct(era5_v3_sharded, "t2m"))


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


def test_tiles_output_arrives_in_a_later_task(spark_delta, v3):
    with pytest.raises(
        NotImplementedError, match="tiles output arrives in a later task"
    ):
        gbx_zarr_read(spark_delta, v3, output="tiles")


def test_arguments_are_checked_before_any_work(spark_delta, v3, era5_v2):
    with pytest.raises(ValueError, match="output"):
        gbx_zarr_read(spark_delta, v3, output="rows")
    with pytest.raises(ZarrAccessUnavailable) as e:
        gbx_zarr_read(spark_delta, v3, access="direct")
    assert 'access="auto"' in e.value.fix
    with pytest.raises(ValueError, match="access"):
        gbx_zarr_read(spark_delta, v3, access="turbo")
    with pytest.raises(ValueError, match="band_dim"):
        gbx_zarr_read(spark_delta, v3, band_dim="time")
    with pytest.raises(ValueError, match="tile_size"):
        gbx_zarr_read(spark_delta, v3, tile_size=(256, 256))
    with pytest.raises(ValueError, match="not both"):
        gbx_zarr_read(spark_delta, v3, version_as_of=1, timestamp_as_of="2020-01-01")
    with pytest.raises(ZarrVersionUnavailable) as e:  # a plain store has no versions
        gbx_zarr_read(spark_delta, str(era5_v2), version_as_of=1)
    assert "Fix:" in str(e.value) and "gbx_zarr_register" in e.value.fix
    with pytest.raises(ValueError, match="unknown variable"):
        gbx_zarr_read(spark_delta, v3, variables=["nope"])
    with pytest.raises(ValueError, match="unknown dimension"):
        gbx_zarr_read(spark_delta, v3, sel={"depth": 1})
    with pytest.raises(ValueError, match="both sel and isel"):
        gbx_zarr_read(spark_delta, v3, sel={"time": "2023-01-01"}, isel={"time": 0})
    with pytest.raises(ValueError, match="use_zarr_fill_value_as_mask"):
        gbx_zarr_read(spark_delta, v3, use_zarr_fill_value_as_mask="no")


def test_the_dependent_selection_is_per_variable(spark_delta, tmp_path):
    """sel on a dimension only some variables have reaches those variables and no error."""
    path = tmp_path / "two.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    a = g.create_array(
        "a", shape=(4, 6), chunks=(2, 3), dtype="f4", dimension_names=("y", "x")
    )
    a[:] = 1
    b = g.create_array("b", shape=(6,), chunks=(3,), dtype="f4", dimension_names=("x",))
    b[:] = 2
    rows = gbx_zarr_read(spark_delta, str(path), isel={"y": slice(0, 2)}).collect()
    _, seen_a = _assemble(rows, "a", (4, 6))
    _, seen_b = _assemble(rows, "b", (6,))
    assert seen_a[:2].all() and not seen_a[2:].any() and seen_b.all()


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
