"""``gbx_zarr_write``: pack files, modes, the two-phase commit and its rebase.

These run on local OSS Delta, where two MERGEs at the same moment always conflict, so
"concurrent" writers are interleaved through the ``_BEFORE_COMMIT`` hook: one write runs to
its commit while another sits between its files and its commit, from the same base.
Real row-level concurrency is exercised on Databricks.
"""

import hashlib
import json
import uuid

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

import pandas as pd  # noqa: E402

from databricks.labs.gbx.zarr import _write  # noqa: E402
from databricks.labs.gbx.zarr._manifest import find_commit_version  # noqa: E402
from databricks.labs.gbx.zarr._read import (  # noqa: E402
    BLOCKS_SCHEMA,
    block_to_numpy,
    gbx_zarr_read,
)
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_MARKER,
    gbx_zarr_register,
)
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrReadOnly,
    ZarrWriteConflict,
)

CHUNKS = {"time": 12, "latitude": 5, "longitude": 6}
DIMS = ("time", "latitude", "longitude")


def _table(label):
    return f"gbx_zarr_write_{label}_{uuid.uuid4().hex[:8]}"


def _dataset(hours=48, start="2023-01-01", ny=10, nx=12, seed=0):
    """A small cube: float32 ``t2m`` and ``u10`` on hourly time, descending latitude."""
    time = pd.date_range(start, periods=hours, freq="h")
    rng = np.random.default_rng(seed)
    shape = (hours, ny, nx)
    return xr.Dataset(
        {
            "t2m": (
                DIMS,
                (280 + rng.normal(size=shape)).astype("float32"),
                {"units": "K"},
            ),
            "u10": (DIMS, rng.normal(size=shape).astype("float32"), {"units": "m s-1"}),
        },
        coords={
            "time": ("time", time),
            "latitude": (
                "latitude",
                np.linspace(45.0, 36.0, ny),
                {"units": "degrees_north"},
            ),
            "longitude": ("longitude", np.arange(nx) * 1.0, {"units": "degrees_east"}),
        },
    )


@pytest.fixture
def drop(spark_delta):
    made = []
    yield made.append
    for t in made:
        spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


@pytest.fixture
def no_hooks(monkeypatch):
    monkeypatch.setattr(_write, "_FAULT", None)
    monkeypatch.setattr(_write, "_BEFORE_COMMIT", None)
    return monkeypatch


def _create(spark, drop, tmp_path, ds=None, label="s", **kw):
    table, root = _table(label), tmp_path / f"root_{label}"
    drop(table)
    kw.setdefault("chunks", CHUNKS)
    version = gbx_zarr_write(
        _dataset() if ds is None else ds, table, root=str(root), spark=spark, **kw
    )
    return table, root, version


def _read(spark, table, name, **kw):
    """Variable ``name`` of the store assembled from its blocks (NaN where none)."""
    rows = gbx_zarr_read(spark, table, variables=[name], **kw).collect()
    stops = [r["stop"] for r in rows]
    shape = tuple(max(s[i] for s in stops) for i in range(len(stops[0])))
    first = block_to_numpy(rows[0])
    out = np.full(shape, np.nan if first.dtype.kind == "f" else 0, dtype=first.dtype)
    if first.dtype.kind == "M":
        out = np.full(shape, np.datetime64("NaT"), dtype=first.dtype)
    for r in rows:
        out[tuple(slice(a, b) for a, b in zip(r["start"], r["stop"]))] = block_to_numpy(
            r
        )
    return out


def _blocks(spark, *blocks):
    """A blocks DataFrame of ``(variable, start, values)``; values are decoded values."""
    rows = []
    for name, start, values in blocks:
        values = np.ascontiguousarray(values)
        rows.append(
            (
                name,
                list(DIMS[-values.ndim :]) if name != "time" else ["time"],
                list(start),
                [a + n for a, n in zip(start, values.shape)],
                None,
                None,
                values.dtype.str,
                list(values.shape),
                values.tobytes(),
                None,
                None,
            )
        )
    return spark.createDataFrame(rows, BLOCKS_SCHEMA)


def _rows_of(spark, table, write_id, kind="chunk"):
    return spark.sql(
        f"SELECT * FROM {table} WHERE write_id = '{write_id}' AND kind = '{kind}' "
        "AND tag IS NULL"
    ).collect()


def _commit_meta(spark, table, write_id):
    row = spark.sql(
        f"SELECT meta FROM {table} WHERE kind = 'commit' AND key = '{write_id}'"
    ).first()
    return json.loads(row["meta"])


def _latest(spark, table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def _files(root):
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


# --- create ----------------------------------------------------------------------------


def test_create_then_read_round_trip(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset()
    wid = uuid.uuid4().hex
    table, root, version = _create(spark_delta, drop, tmp_path, ds, write_id=wid)
    assert version == find_commit_version(spark_delta, table, wid) == 1
    for name in ("t2m", "u10"):
        np.testing.assert_array_equal(_read(spark_delta, table, name), ds[name].values)
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    np.testing.assert_array_equal(
        _read(spark_delta, table, "latitude"), ds.latitude.values
    )
    marker = json.loads((root / OWNER_MARKER).read_text())
    assert marker["kind"] == "managed" and marker["table"].endswith(table)
    doc = json.loads(
        spark_delta.sql(
            f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 't2m'"
        ).first()["meta"]
    )
    sharding = doc["codecs"][0]
    assert sharding["name"] == "sharding_indexed"
    assert sharding["configuration"]["chunk_shape"] == [12, 5, 6]
    assert [c["name"] for c in sharding["configuration"]["codecs"]] == [
        "bytes",
        "zstd",
        "crc32c",
    ]
    assert sharding["configuration"]["codecs"][1]["configuration"]["level"] == 3
    time_rows = _rows_of(spark_delta, table, wid)
    assert [r["key"] for r in time_rows if r["array"] == "time"] == ["c/0"]  # one chunk
    t2m_rows = [r for r in time_rows if r["array"] == "t2m"]
    assert len(t2m_rows) == 4 * 2 * 2 and all(
        r["shard_key"] == "c/0/0/0" for r in t2m_rows
    )
    packs = list((root / "data" / "t2m" / wid).glob("part-*.zpack"))
    assert packs and all(r["file"].startswith(f"data/t2m/{wid}/") for r in t2m_rows)
    assert all(len(r["checksum"]) == 8 for r in t2m_rows)


def test_create_refuses_an_existing_table_and_a_missing_root(
    spark_delta, drop, tmp_path, no_hooks
):
    from databricks.labs.gbx.zarr.errors import ZarrOwnershipError

    table, root, _ = _create(spark_delta, drop, tmp_path)
    with pytest.raises(ZarrOwnershipError, match="already exists"):
        gbx_zarr_write(_dataset(), table, root=str(tmp_path / "x"), spark=spark_delta)
    with pytest.raises(ValueError, match="root") as e:
        gbx_zarr_write(_dataset(), _table("noroot"), spark=spark_delta)
    assert "Fix:" in str(e.value)
    with pytest.raises(ValueError, match=r"apply to mode='create' only"):
        gbx_zarr_write(
            _dataset(), table, mode="update", chunks="maps", spark=spark_delta
        )


def test_create_stores_no_row_for_a_chunk_of_fill(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset()
    ds["t2m"][0:12, 0:5, 0:6] = np.nan  # one whole inner chunk is the fill value
    wid = uuid.uuid4().hex
    table, _, _ = _create(spark_delta, drop, tmp_path, ds, write_id=wid)
    keys = {r["key"] for r in _rows_of(spark_delta, table, wid) if r["array"] == "t2m"}
    assert "c/0/0/0" not in keys and len(keys) == 15
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)


# --- append ----------------------------------------------------------------------------


def test_append_extends_time_and_coords(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset()
    table, _, v1 = _create(spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)))
    v2 = gbx_zarr_write(
        ds.isel(time=slice(24, 48)), table, mode="append", spark=spark_delta
    )
    assert v2 > v1
    for name in ("t2m", "u10"):
        np.testing.assert_array_equal(_read(spark_delta, table, name), ds[name].values)
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    with pytest.raises(ValueError, match='mode="update"'):
        gbx_zarr_write(
            ds.isel(time=slice(40, 48)), table, mode="append", spark=spark_delta
        )
    np.testing.assert_array_equal(  # a variable the append did not carry grows too
        _read(spark_delta, table, "u10").shape, (48, 10, 12)
    )


def test_append_of_blocks_needs_the_labels(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset()
    table, _, _ = _create(spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)))
    later = ds.isel(time=slice(24, 36))
    with pytest.raises(ValueError, match="'time' labels"):
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), later.t2m.values)),
            table,
            mode="append",
        )
    gbx_zarr_write(
        _blocks(
            spark_delta,
            ("t2m", (0, 0, 0), later.t2m.values),  # positions of the input, shifted
            ("time", (0,), later.time.values),
        ),
        table,
        mode="append",
    )
    got = _read(spark_delta, table, "t2m")
    np.testing.assert_array_equal(got[:36], ds.t2m.values[:36])
    np.testing.assert_array_equal(
        _read(spark_delta, table, "time"), ds.time.values[:36]
    )


# --- update ----------------------------------------------------------------------------


def test_update_aligned_writes_only_touched_chunks(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset()
    table, root, _ = _create(spark_delta, drop, tmp_path, ds)
    new = np.full((12, 5, 12), 1000.0, dtype="float32")  # two whole inner chunks
    wid = uuid.uuid4().hex
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (12, 5, 0), new)),
        table,
        mode="update",
        write_id=wid,
    )
    rows = _rows_of(spark_delta, table, wid)
    assert sorted(r["key"] for r in rows) == ["c/1/1/0", "c/1/1/1"]
    assert _commit_meta(spark_delta, table, wid)["rmw"] == 0  # nothing was read
    packs = list((root / "data" / "t2m" / wid).glob("*.zpack"))
    assert sum(p.stat().st_size for p in packs) == sum(r["length"] for r in rows)
    want = ds.t2m.values.copy()
    want[12:24, 5:10, :] = 1000.0
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), ds.u10.values)


def test_update_partial_edge_chunk_rmw(spark_delta, drop, tmp_path, no_hooks):
    """A box at latitude 60:73 lies inside the edge chunk 37:73 (36 of 37 rows): the chunk
    is read at the base, patched and re-encoded, so its other cells keep their values.
    """
    ds = era5_like_dataset()
    table, _, _ = _create(
        spark_delta,
        drop,
        tmp_path,
        ds,
        chunks={"time": 12, "latitude": 37, "longitude": 72},
    )
    wid = uuid.uuid4().hex
    box = np.full((12, 13, 72), 1000.0, dtype="float32")
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 60, 0), box)),
        table,
        mode="update",
        write_id=wid,
    )
    assert _commit_meta(spark_delta, table, wid)["rmw"] == 1
    want = ds.t2m.values.copy()
    want[0:12, 60:73, 0:72] = 1000.0
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)
    # the same through an xarray.Dataset, placed by its labels
    part = ds.isel(time=slice(12, 24), latitude=slice(60, 73), longitude=slice(72, 144))
    part = part[["t2m"]] * 0 + 7.0
    gbx_zarr_write(part, table, mode="update", spark=spark_delta)
    want[12:24, 60:73, 72:144] = 7.0
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)


# --- overwrite ---------------------------------------------------------------------------


def test_overwrite_replaces_variable(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset()
    table, _, _ = _create(spark_delta, drop, tmp_path, ds)
    wid = uuid.uuid4().hex
    fresh = ds.isel(time=slice(0, 12))[["t2m"]] * 0 + 5.0
    gbx_zarr_write(fresh, table, mode="overwrite", write_id=wid, spark=spark_delta)
    got = _read(spark_delta, table, "t2m")
    assert (got[:12] == 5.0).all() and np.isnan(got[12:]).all()
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), ds.u10.values)
    live = spark_delta.sql(
        f"SELECT DISTINCT write_id FROM {table} WHERE kind = 'chunk' AND array = 't2m' "
        "AND tag IS NULL"
    ).collect()
    assert [r["write_id"] for r in live] == [wid]  # no row of the first write is left


# --- idempotency and read-only stores ------------------------------------------------------


def test_idempotent_write_id(spark_delta, drop, tmp_path, no_hooks):
    ds = _dataset()
    wid = uuid.uuid4().hex
    table, root, v1 = _create(spark_delta, drop, tmp_path, ds, write_id=wid)
    before = _files(root)
    again = gbx_zarr_write(ds, table, root=str(root), write_id=wid, spark=spark_delta)
    assert again == v1 == _latest(spark_delta, table) and _files(root) == before
    uid = uuid.uuid4().hex
    blocks = _blocks(spark_delta, ("t2m", (0, 0, 0), np.zeros((12, 5, 6), "float32")))
    v2 = gbx_zarr_write(blocks, table, mode="update", write_id=uid)
    assert gbx_zarr_write(blocks, table, mode="update", write_id=uid) == v2
    assert _latest(spark_delta, table) == v2


def test_registered_read_only_refuses(spark_delta, drop, tmp_path, no_hooks):
    store = make_era5_like(tmp_path / "ro.zarr", zarr_format=3)
    table = _table("ro")
    drop(table)
    gbx_zarr_register(spark_delta, str(store), table)
    before = _files(store)
    with pytest.raises(ZarrReadOnly) as e:
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), np.zeros((12, 37, 72), "float32"))),
            table,
            mode="update",
        )
    assert e.value.fix == "register the store with writable=True and an overlay_root"
    assert _files(store) == before


@pytest.mark.parametrize("fmt", [3, 2])
def test_registered_writable_overlay_never_touches_base(
    spark_delta, drop, tmp_path, no_hooks, fmt
):
    store = make_era5_like(tmp_path / f"base{fmt}.zarr", zarr_format=fmt)
    overlay = tmp_path / f"overlay{fmt}"
    table = _table(f"ov{fmt}")
    drop(table)
    gbx_zarr_register(
        spark_delta, str(store), table, writable=True, overlay_root=str(overlay)
    )
    before = _files(store)
    original = zarr.open_array(str(store / "t2m"), mode="r")[...]
    wid = uuid.uuid4().hex
    whole = np.full((12, 37, 72), 3.0, dtype="float32")  # a whole inner chunk
    part = np.full((12, 13, 72), 9.0, dtype="float32")  # part of the edge chunk
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (12, 0, 72), whole), ("t2m", (0, 60, 0), part)),
        table,
        mode="update",
        write_id=wid,
    )
    want = original.copy()
    want[12:24, 0:37, 72:144] = 3.0
    want[0:12, 60:73, 0:72] = 9.0
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), want)
    assert _files(store) == before  # not a byte of the base changed
    rows = _rows_of(spark_delta, table, wid)
    if fmt == 3:  # the base's own key encoding, with the shard the chunk lies in
        assert {(r["key"], r["shard_key"]) for r in rows} == {
            ("c/1/0/1", "c/0/0/0"),
            ("c/0/1/0", "c/0/0/0"),
        }
    else:
        assert {(r["key"], r["shard_key"]) for r in rows} == {
            ("1.0.1", None),
            ("0.1.0", None),
        }
    assert all(
        r["origin"] == "gbx" and r["file"].startswith(f"data/t2m/{wid}/") for r in rows
    )
    assert (overlay / OWNER_MARKER).exists() and list(
        (overlay / "data" / "t2m").iterdir()
    )


# --- empty chunks: deletes and tombstones -----------------------------------------------------


def test_a_chunk_of_fill_under_a_live_shard_row_is_a_tombstone(
    spark_delta, drop, tmp_path, no_hooks
):
    store = make_era5_like(tmp_path / "tomb.zarr", zarr_format=3)
    table = _table("tomb")
    drop(table)
    gbx_zarr_register(
        spark_delta, str(store), table, writable=True, overlay_root=str(tmp_path / "ov")
    )
    meta = _write._docs_at(spark_delta, _write.M.snapshot(spark_delta, table)).arrays[
        "t2m"
    ]
    fill = np.full((12, 37, 72), meta.fill_value, dtype="float32")
    wid = uuid.uuid4().hex
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), fill)),
        table,
        mode="update",
        write_id=wid,
    )
    (row,) = _rows_of(spark_delta, table, wid)
    assert (row["key"], row["shard_key"]) == ("c/0/0/0", "c/0/0/0")
    assert (row["file"], row["offset"], row["length"], row["checksum"]) == (
        None,
        None,
        0,
        None,
    )
    got = _read(spark_delta, table, "t2m")
    np.testing.assert_array_equal(got[0:12, 0:37, 0:72], fill)
    np.testing.assert_array_equal(
        got[12:], zarr.open_array(str(store / "t2m"), mode="r")[12:]
    )


def test_a_chunk_of_fill_in_an_unsharded_array_deletes_its_row(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _create(spark_delta, drop, tmp_path, shards=False)
    wid = uuid.uuid4().hex
    gbx_zarr_write(
        _blocks(
            spark_delta, ("t2m", (0, 0, 0), np.full((12, 5, 6), np.nan, "float32"))
        ),
        table,
        mode="update",
        write_id=wid,
    )
    assert _rows_of(spark_delta, table, wid) == []
    live = spark_delta.sql(
        f"SELECT key FROM {table} WHERE kind = 'chunk' AND array = 't2m' AND tag IS NULL"
    ).collect()
    assert "c/0/0/0" not in {r["key"] for r in live} and len(live) == 15
    assert np.isnan(_read(spark_delta, table, "t2m")[0:12, 0:5, 0:6]).all()


# --- conflicts and rebase -----------------------------------------------------------------


def test_overlap_conflict_raises_and_commits_nothing(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _create(spark_delta, drop, tmp_path)
    a, b = uuid.uuid4().hex, uuid.uuid4().hex
    block_a = _blocks(
        spark_delta, ("t2m", (0, 0, 0), np.full((12, 5, 6), 1.0, "float32"))
    )
    # b writes part of the same chunk: a read-modify-write of the base, which a changed
    block_b = _blocks(
        spark_delta, ("t2m", (0, 0, 0), np.full((6, 5, 6), 2.0, "float32"))
    )
    ran = []

    def first_a_lands(write_id):
        if write_id == b and not ran:
            ran.append(gbx_zarr_write(block_a, table, mode="update", write_id=a))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", first_a_lands)
    with pytest.raises(ZarrWriteConflict) as e:
        gbx_zarr_write(block_b, table, mode="update", write_id=b)
    assert e.value.keys == ("t2m/c/0/0/0",) and e.value.winner == a
    assert "Fix:" in str(e.value) and ran
    assert _latest(spark_delta, table) == ran[0]  # nothing of b landed
    assert find_commit_version(spark_delta, table, b) is None
    assert _rows_of(spark_delta, table, b) == []
    assert (_read(spark_delta, table, "t2m")[0:12, 0:5, 0:6] == 1.0).all()


def test_out_of_order_appends_keep_max_extent(spark_delta, drop, tmp_path, no_hooks):
    """Day 3 lands from base v while day 2, from the same base, waits to commit; day 2's
    shape and the shared time chunk are metadata, so it rebases instead of failing."""
    ds = _dataset(hours=72)
    day = {d: ds.isel(time=slice(24 * (d - 1), 24 * d)) for d in (1, 2, 3)}
    chunks = {"time": 24, "latitude": 5, "longitude": 6}
    table, _, base = _create(spark_delta, drop, tmp_path, day[1], chunks=chunks)
    two, three = uuid.uuid4().hex, uuid.uuid4().hex
    ran = []

    def day_three_first(write_id):
        if write_id == two and not ran:
            ran.append(
                gbx_zarr_write(
                    day[3], table, mode="append", write_id=three, spark=spark_delta
                )
            )

    no_hooks.setattr(_write, "_BEFORE_COMMIT", day_three_first)
    v2 = gbx_zarr_write(day[2], table, mode="append", write_id=two, spark=spark_delta)
    assert ran and base < ran[0] < v2
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    for name in ("t2m", "u10"):
        np.testing.assert_array_equal(_read(spark_delta, table, name), ds[name].values)
    chunk_rows = spark_delta.sql(
        f"SELECT key, write_id FROM {table} WHERE kind = 'chunk' AND array = 'time' "
        "AND tag IS NULL"
    ).collect()
    assert [(r["key"], r["write_id"]) for r in chunk_rows] == [("c/0", two)]


def test_failed_task_leaves_no_rows(spark_delta, drop, tmp_path, no_hooks):
    table, _, version = _create(spark_delta, drop, tmp_path)
    no_hooks.setattr(_write, "_FAULT", ("t2m", "c/1/0/1"))
    wid = uuid.uuid4().hex
    with pytest.raises(Exception, match="test fault"):
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), np.ones((24, 10, 12), "float32"))),
            table,
            mode="update",
            write_id=wid,
        )
    assert _latest(spark_delta, table) == version
    assert find_commit_version(spark_delta, table, wid) is None
    assert _rows_of(spark_delta, table, wid) == []


def test_the_write_module_uses_no_api_that_spark_connect_forbids():
    import ast
    import pathlib

    found = []
    for name in ("_write.py", "_pack.py", "_layout.py"):
        path = pathlib.Path(_write.__file__).parent / name
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute) and node.attr in (
                "sparkContext",
                "_jvm",
                "_jsc",
                "rdd",
            ):
                found.append(f"{name}:{node.lineno} .{node.attr}")
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "set"
                and getattr(node.func.value, "attr", None) == "conf"
            ):
                found.append(f"{name}:{node.lineno} conf.set")
    assert not found, found


# --- planning, without a session ---------------------------------------------------------


def test_coverage_counts_aligned_and_spanning_blocks():
    spec = _write._NewArray(
        "v", ("y", "x"), (10, 12), np.dtype("float32"), {}, float("nan"), False
    )
    meta = _write._new_meta(
        spec, fmt=3, chunks={"y": 5, "x": 6}, shards=False, codecs=None, separator="/"
    )
    starts = np.array([[0, 0], [0, 6], [5, 3]])
    stops = np.array([[5, 6], [5, 12], [10, 9]])  # the third spans two chunks, in part
    touched, partial = _write._coverage(meta, starts, stops)
    assert touched == {(0, 0), (0, 1), (1, 0), (1, 1)}
    assert partial == {(1, 0), (1, 1)}


def test_append_positions_follow_the_axis_step():
    from databricks.labs.gbx.zarr.errors import ZarrGridMismatch

    hours = pd.date_range("2023-01-01", periods=24, freq="h").values
    day3 = pd.date_range("2023-01-03", periods=24, freq="h").values
    start = _write._derive_append_start
    assert start("time", hours, day3) == 48  # a day skipped: its steps stay unwritten
    assert start("time", hours, hours[-1:] + np.timedelta64(1, "h")) == 24
    with pytest.raises(ValueError, match='mode="update"'):
        start("time", hours, hours[5:])
    with pytest.raises(ZarrGridMismatch):
        start("time", hours, hours[-1:] + np.timedelta64(90, "m"))  # off the step
    assert start("x", np.array([0.0, 1.0, 3.0]), np.array([10.0])) == 3  # irregular
    gap = np.concatenate([hours, np.full(24, np.datetime64("NaT"), "M8[ns]"), day3])
    later = pd.date_range("2023-01-04", periods=2, freq="h").values
    assert start("time", gap, later) == 72  # missing labels are skipped


def test_a_new_variable_is_created_on_the_store_dimensions(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset()
    table, _, _ = _create(spark_delta, drop, tmp_path, ds)
    extra = ds[["t2m"]].rename({"t2m": "sp"}).isel(time=slice(0, 12)) * 0 + 101325.0
    gbx_zarr_write(extra, table, mode="update", spark=spark_delta)
    got = _read(spark_delta, table, "sp")
    assert got.shape == (48, 10, 12)  # the store's extent, not the input's
    assert (got[:12] == 101325.0).all() and np.isnan(got[12:]).all()
    doc = json.loads(
        spark_delta.sql(
            f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 'sp'"
        ).first()["meta"]
    )
    inner = doc["codecs"][0]["configuration"]["codecs"]  # the store's default layout
    assert doc["codecs"][0]["name"] == "sharding_indexed"
    assert [c["name"] for c in inner] == ["bytes", "zstd", "crc32c"]
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)
