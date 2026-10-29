"""``gbx_zarr_write``: pack files, modes, the two-phase commit and its rebase.

These run on local OSS Delta, where two MERGEs at the same moment always conflict, so
"concurrent" writers are interleaved through the ``_BEFORE_COMMIT`` hook: one write runs to
its commit while another sits between its files and its commit, from the same base.
Real row-level concurrency is exercised on Databricks.
"""

import hashlib
import json
import types
import uuid

import numpy as np
import pyarrow.compute as pa_compute
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

import pandas as pd  # noqa: E402

from databricks.labs.gbx.zarr import _write  # noqa: E402
from databricks.labs.gbx.zarr._manifest import find_commit_version  # noqa: E402
from databricks.labs.gbx.zarr._read import BLOCKS_SCHEMA  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_LEDGER,
    OWNER_MARKER,
    gbx_zarr_register,
    identity_of,
)
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrGridMismatch,
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


def _read(spark, table, name):
    """Variable ``name`` of the store assembled from its blocks (NaN where none)."""
    return H.read_variable(spark, table, name)


def _blocks(spark, *blocks):
    """A blocks DataFrame of ``(variable, start, values)``; values are decoded values."""
    return H.blocks_frame(spark, *blocks)


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


@pytest.mark.parametrize("mode", ["update", "overwrite"])
def test_a_one_chunk_write_reads_only_its_own_group_of_the_manifest(
    spark_delta, drop, tmp_path, no_hooks, mode
):
    """A one-chunk update or overwrite from blocks, with ``variables=None``: the assemble
    stage gets the base rows of that chunk's group only, not the manifest of every array
    it might have written, and the driver collects no delete row of an array the write
    does not replace. The overwrite still replaces the whole of the variable it writes.
    """
    ds = _dataset()
    table, _, _ = _create(spark_delta, drop, tmp_path, ds, shards=False)
    fed, collected = {}, {}
    frame_class = type(spark_delta.range(1))  # the session's DataFrame implementation
    real_map, real_rows = frame_class.mapInArrow, _write._commit_data_rows

    def counting_map(df, func, schema, *a, **k):
        if schema == _write.ROWS_DDL:  # the assemble stage's input
            fed.update(
                {r["kind"]: r["count"] for r in df.groupBy("kind").count().collect()}
            )
        return real_map(df, func, schema, *a, **k)

    def recording_rows(plan, rows, written):
        collected["deletes"] = (
            rows.filter(pa_compute.equal(rows.column("rtype"), "delete"))
            .column("array")
            .to_pylist()
        )
        return real_rows(plan, rows, written)

    no_hooks.setattr(frame_class, "mapInArrow", counting_map)
    no_hooks.setattr(_write, "_commit_data_rows", recording_rows)
    one = np.full((12, 5, 6), 7.0, dtype="float32")  # chunk c/0/0/0 of t2m, whole
    gbx_zarr_write(_blocks(spark_delta, ("t2m", (0, 0, 0), one)), table, mode=mode)

    assert fed.get("base") == 1  # the chunk's own row; t2m and u10 have 16 rows each
    assert "u10" not in collected["deletes"]
    got = _read(spark_delta, table, "t2m")
    assert (got[0:12, 0:5, 0:6] == 7.0).all()
    if mode == "update":
        want = ds.t2m.values.copy()
        want[0:12, 0:5, 0:6] = 7.0
        np.testing.assert_array_equal(got, want)
    else:  # the other 15 chunks of t2m are gone: the variable was replaced
        rest = got.copy()
        rest[0:12, 0:5, 0:6] = np.nan
        assert np.isnan(rest).all()
        assert sorted(collected["deletes"]) == ["t2m"] * 15
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), ds.u10.values)


@pytest.mark.parametrize("whole", [False, True])
def test_an_overwrite_collects_the_rest_of_its_variable_as_columns(
    spark_delta, drop, tmp_path, no_hooks, whole
):
    """The rows an overwrite deletes outside the groups its input touched (here up to 1,439
    of one variable) reach the driver as Arrow columns, never as a Python dict per row; an
    overwrite of the whole variable touches every group, so nothing is read or uploaded.
    """
    ds = _dataset(hours=12)
    one = {"time": 1, "latitude": 1, "longitude": 1}
    table, _, _ = _create(spark_delta, drop, tmp_path, ds, chunks=one, shards=False)
    uploads, collected = [], {}
    real_upload, real_rows = _write.to_spark_df, _write._commit_data_rows

    def per_row_dict(base):
        raise AssertionError("a delete row was built as a Python dict")

    def recording_rows(plan, rows, written):
        collected["deletes"] = (
            rows.filter(pa_compute.equal(rows.column("rtype"), "delete"))
            .column("array")
            .to_pylist()
        )
        return real_rows(plan, rows, written)

    no_hooks.setattr(_write, "delete_row", per_row_dict, raising=False)
    no_hooks.setattr(
        _write, "to_spark_df", lambda *a, **k: uploads.append(1) or real_upload(*a, **k)
    )
    no_hooks.setattr(_write, "_commit_data_rows", recording_rows)
    new = np.full((12, 10, 12) if whole else (1, 1, 1), 5.0, dtype="float32")
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), new)), table, mode="overwrite"
    )

    got = _read(spark_delta, table, "t2m")
    if whole:
        assert (got == 5.0).all()
        assert collected["deletes"] == [] and uploads == []
    else:
        assert got[0, 0, 0] == 5.0 and np.isnan(got.ravel()[1:]).all()
        assert collected["deletes"] == ["t2m"] * 1439 and len(uploads) == 1
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), ds.u10.values)


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
    for name in (
        "_write.py",
        "_write_tiles.py",
        "_write_tasks.py",
        "_pack.py",
        "_layout.py",
        "_maint.py",
    ):
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


_CHAIN = [
    {"name": "bytes", "configuration": {"endian": "little"}},
    {"name": "zstd", "configuration": {"level": 7, "checksum": False}},
]


def _codec_array_doc(monkeypatch, shards, codecs):
    """``(doc, meta)`` of ``t2m`` as a create makes it with this layout and codec chain.

    The document is the one ``_new_meta`` builds (taken from ``_v3_doc`` as it returns), so
    the layout asserted on is the layout the code chose for ``shards``.
    """
    built, real = [], _write._v3_doc

    def record(*args, **kw):
        built.append(real(*args, **kw))
        return built[-1]

    monkeypatch.setattr(_write, "_v3_doc", record)
    spec = _write._NewArray(
        "t2m", DIMS, (24, 10, 12), np.dtype("float32"), {}, float("nan"), False
    )
    kw = dict(fmt=3, chunks=CHUNKS, shards=shards, separator="/")
    meta = _write._new_meta(spec, codecs=_write._codec_chain(codecs), **kw)
    return built[-1], meta


@pytest.mark.parametrize(
    "shards, codecs, level",
    [
        (None, _CHAIN, 7),
        (False, _CHAIN, 7),
        (None, "objects", 5),
    ],
    ids=["sharded", "unsharded", "objects"],
)
def test_p3_explicit_codecs_make_the_array_document(monkeypatch, shards, codecs, level):
    from zarr.codecs import BytesCodec, ZstdCodec

    if codecs == "objects":
        codecs = [BytesCodec(), ZstdCodec(level=5)]
    doc, meta = _codec_array_doc(monkeypatch, shards, codecs)
    if shards is None:  # the default layout shards: the chain is each inner chunk's
        assert meta.shard is not None
        assert doc["codecs"][0]["name"] == "sharding_indexed"
        inner = doc["codecs"][0]["configuration"]["codecs"]
    else:
        assert meta.shard is None
        inner = doc["codecs"]
    assert [c["name"] for c in inner] == ["bytes", "zstd"]
    assert inner[1]["configuration"]["level"] == level


@pytest.mark.parametrize(
    "codecs",
    [
        [{"name": "zstd", "configuration": {"level": 3}}],  # no serializer
        [{"name": "bytes"}, {"name": "lz77"}],  # a codec zarr does not know
        [object()],  # not a codec at all
    ],
    ids=["noserializer", "unknown", "notacodec"],
)
def test_p3_bad_codec_chains_are_refused_with_a_fix_while_planning(monkeypatch, codecs):
    for shards in (None, False):  # each layout encodes a chunk with the chain
        with pytest.raises(ZarrUnsupported) as e:
            _codec_array_doc(monkeypatch, shards, codecs)
        assert "Fix:" in str(e.value)


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


# --- fix round 1: the reviewer's probes as tests ----------------------------------------------

from databricks.labs.gbx.zarr import _manifest as M  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
)


def _blocks_arrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            ("variable", pa.string()),
            ("dims", pa.list_(pa.string())),
            ("start", pa.list_(pa.int64())),
            ("stop", pa.list_(pa.int64())),
            ("coord_start", pa.list_(pa.string())),
            ("coord_stop", pa.list_(pa.string())),
            ("dtype", pa.string()),
            ("shape", pa.list_(pa.int64())),
            ("data", pa.binary()),
            ("fill_value", pa.string()),
            ("attrs", pa.string()),
        ]
    )


def _moving_source(spark, flag, first, second, extra=()):
    """A blocks DataFrame whose one t2m block is ``first`` (start, shape) until ``flag``
    exists and ``second`` after: an input that changes between two evaluations."""
    flag_s, schema = str(flag), _blocks_arrow_schema()
    extra = [dict(e) for e in extra]

    def source(batches):
        import os as _os

        import numpy as _np
        import pyarrow as _pa

        for _ in batches:
            pass
        start, shape = second if _os.path.exists(flag_s) else first
        rows = [
            {
                "variable": "t2m",
                "dims": list(DIMS),
                "start": list(start),
                "stop": [a + n for a, n in zip(start, shape)],
                "coord_start": None,
                "coord_stop": None,
                "dtype": "<f4",
                "shape": list(shape),
                "data": _np.full(shape, 2.0, "float32").tobytes(),
                "fill_value": None,
                "attrs": None,
            },
            *extra,
        ]
        yield _pa.RecordBatch.from_pylist(rows, schema=schema)

    return spark.range(1, numPartitions=1).mapInArrow(source, BLOCKS_SCHEMA)


def _flip_before_files(monkeypatch, flag):
    real = _write._write_files

    def flip_then_write(spark_, plan):
        flag.write_text("x")
        return real(spark_, plan)

    monkeypatch.setattr(_write, "_write_files", flip_then_write)


def _registered(spark, drop, tmp_path, label, fmt=3):
    store = make_era5_like(tmp_path / f"{label}.zarr", zarr_format=fmt)
    table = _table(label)
    drop(table)
    gbx_zarr_register(
        spark,
        str(store),
        table,
        writable=True,
        overlay_root=str(tmp_path / f"ov_{label}"),
    )
    return store, table


def test_p1_a_chunk_read_from_a_shard_conflicts_when_that_shard_changes(
    spark_delta, drop, tmp_path, no_hooks
):
    """I1: B patches half of chunk c/0/0/0, read from shard c/0/0/0. A's overwrite (which
    deletes every shard row) lands first. B's read guard sees it: B conflicts instead of
    landing the shard's pre-overwrite cells."""
    store, table = _registered(spark_delta, drop, tmp_path, "p1")
    a, b = uuid.uuid4().hex, uuid.uuid4().hex
    blocks_b = _blocks(
        spark_delta, ("t2m", (0, 0, 0), np.full((6, 37, 72), 2.0, "float32"))
    )
    blocks_a = _blocks(
        spark_delta, ("t2m", (36, 37, 72), np.full((12, 36, 72), 5.0, "float32"))
    )
    ran = []

    def overwrite_first(wid):
        if wid == b and not ran:
            ran.append(gbx_zarr_write(blocks_a, table, mode="overwrite", write_id=a))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", overwrite_first)
    with pytest.raises(ZarrWriteConflict) as e:
        gbx_zarr_write(blocks_b, table, mode="update", write_id=b)
    assert ran and "t2m/c/0/0/0" in e.value.keys
    assert any(c["kind"] == "shard" for c in e.value.conflicts)
    assert find_commit_version(spark_delta, table, b) is None
    got = _read(spark_delta, table, "t2m")
    assert np.isnan(got[0:12, 0:37, 0:72]).all()  # A's result alone: fill there


def test_an_overwrite_hides_the_shards_of_a_registered_store_with_tombstones(
    spark_delta, drop, tmp_path, no_hooks
):
    """Every shard row of the variable becomes a tombstone that keeps the
    identity of the base shard it hides (a deleted row would let a sync bring the shard
    back); only the rewritten chunk has data; the base files are untouched."""
    store, table = _registered(spark_delta, drop, tmp_path, "ow")
    before = _files(store)
    wid = uuid.uuid4().hex
    gbx_zarr_write(
        _blocks(
            spark_delta, ("t2m", (36, 37, 72), np.full((12, 36, 72), 5.0, "float32"))
        ),
        table,
        mode="overwrite",
        write_id=wid,
    )
    live = spark_delta.sql(
        f"SELECT kind, key, write_id, file, length, origin, identity FROM {table} "
        "WHERE array = 't2m' AND tag IS NULL AND kind IN ('shard', 'chunk')"
    ).collect()
    assert {(r["kind"], r["key"], r["write_id"]) for r in live} == {
        ("chunk", "c/3/1/1", wid),
        ("shard", "c/0/0/0", wid),
        ("shard", "c/1/0/0", wid),
    }
    for r in live:
        if r["kind"] == "shard":
            assert (r["file"], r["length"], r["origin"]) == (None, 0, "gbx")
            st = (store / "t2m" / r["key"]).stat()
            assert r["identity"] == identity_of(st.st_mtime_ns // 1_000_000, st.st_size)
    got = _read(spark_delta, table, "t2m")
    assert (got[36:48, 37:73, 72:144] == 5.0).all()
    assert np.isnan(got[:36]).all() and np.isnan(got[36:48, :37]).all()
    assert _files(store) == before
    u10 = zarr.open_array(str(store / "u10"), mode="r")[...]
    np.testing.assert_array_equal(_read(spark_delta, table, "u10"), u10)


@pytest.mark.parametrize(
    "label, first, second",
    [
        (
            "partial-to-unread-shard",
            ((0, 0, 0), (6, 37, 72)),
            ((24, 0, 0), (6, 37, 72)),
        ),
        ("whole-then-partial", ((0, 0, 0), (12, 37, 72)), ((24, 0, 0), (6, 37, 72))),
    ],
)
def test_p2_an_update_reads_its_input_once(
    spark_delta, drop, tmp_path, no_hooks, label, first, second
):
    """I4 + I5: an update makes one pass over its blocks, so an input that would differ
    on a second evaluation is written as that one pass saw it, base cells and all."""
    store, table = _registered(spark_delta, drop, tmp_path, "p2")
    base = zarr.open_array(str(store / "t2m"), mode="r")[...]
    flag = tmp_path / f"flag_{label}"
    df = _moving_source(spark_delta, flag, first, second)
    _flip_before_files(no_hooks, flag)
    gbx_zarr_write(df, table, mode="update")
    got = _read(spark_delta, table, "t2m")
    assert (got[24:30, 0:37, 0:72] == 2.0).all()
    np.testing.assert_array_equal(got[30:36, 0:37, 0:72], base[30:36, 0:37, 0:72])
    np.testing.assert_array_equal(got[0:24], base[0:24])  # the first pass never ran


def test_p2_an_append_refuses_an_input_that_changed_between_its_passes(
    spark_delta, drop, tmp_path, no_hooks
):
    """I4: create and append plan from a narrow first pass; the write pass's digest of the
    block positions must match it."""
    ds = _dataset()
    table, root, version = _create(
        spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24))
    )
    labels = ds.time.values[24:36]
    time_block = {
        "variable": "time",
        "dims": ["time"],
        "start": [0],
        "stop": [12],
        "coord_start": None,
        "coord_stop": None,
        "dtype": labels.dtype.str,
        "shape": [12],
        "data": labels.tobytes(),
        "fill_value": None,
        "attrs": None,
    }
    flag = tmp_path / "flag_append"
    df = _moving_source(
        spark_delta,
        flag,
        ((0, 0, 0), (6, 10, 12)),
        ((6, 0, 0), (6, 10, 12)),
        [time_block],
    )
    _flip_before_files(no_hooks, flag)
    with pytest.raises(ZarrError, match="materialize it as a table") as e:
        gbx_zarr_write(df, table, mode="append")
    assert "Fix:" in str(e.value) and _latest(spark_delta, table) == version


def test_the_position_pass_never_selects_the_data_column(spark_delta):
    """I5: pass 1 projects positions only, so a staged input is not read for its bytes."""
    df = _blocks(spark_delta, ("t2m", (0, 0, 0), np.zeros((2, 2, 2), "float32")))
    seen = []

    class Spy:
        columns = df.columns

        def select(self, *cols):
            seen.extend(str(c) for c in cols)
            return df.select(*cols)

    _write._position_pass(Spy())
    assert seen and not any("data" in c for c in seen)


def test_p3_explicit_codecs_round_trip_and_bad_chains_fail_while_planning(
    spark_delta, drop, tmp_path, no_hooks
):
    """One chain end to end (the chains' documents are test_p3_explicit_codecs_make_...), and
    a refused chain makes no table."""
    ds = _dataset(hours=24)
    table, _, _ = _create(
        spark_delta, drop, tmp_path, ds, label="p3sharded", codecs=_CHAIN
    )
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)
    doc = json.loads(
        spark_delta.sql(
            f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 't2m'"
        ).first()["meta"]
    )
    inner = doc["codecs"][0]["configuration"]["codecs"]
    assert [c["name"] for c in inner] == ["bytes", "zstd"]
    assert inner[1]["configuration"]["level"] == 7
    for label, codecs in (
        ("noserializer", [{"name": "zstd", "configuration": {"level": 3}}]),
        ("unknown", [{"name": "bytes"}, {"name": "lz77"}]),
        ("notacodec", [object()]),
    ):
        table = _table(f"p3{label}")
        with pytest.raises(ZarrUnsupported) as e:
            gbx_zarr_write(
                ds,
                table,
                root=str(tmp_path / label),
                chunks=CHUNKS,
                codecs=codecs,
                spark=spark_delta,
            )
        assert "Fix:" in str(e.value)
        assert not spark_delta.catalog.tableExists(table)  # refused before any table


def test_p4_a_new_variable_in_a_registered_v2_store(
    spark_delta, drop, tmp_path, no_hooks
):
    store, table = _registered(spark_delta, drop, tmp_path, "p4", fmt=2)
    full = xr.open_zarr(str(store), consolidated=False)
    extra = (
        full[["t2m"]].isel(time=slice(0, 12)).rename({"t2m": "sp"}).load() * 0 + 1013.0
    )
    gbx_zarr_write(extra, table, mode="update", spark=spark_delta)
    got = _read(spark_delta, table, "sp")
    assert got.shape == (48, 73, 144)
    assert (got[:12] == 1013.0).all() and np.isnan(got[12:]).all()
    zarray = json.loads(
        spark_delta.sql(
            f"SELECT meta FROM {table} WHERE kind = 'array' AND array = 'sp' "
            "AND key = '.zarray'"
        ).first()["meta"]
    )
    assert zarray["compressor"] == {"id": "zstd", "level": 3}
    keys = {
        r["key"]
        for r in spark_delta.sql(
            f"SELECT key FROM {table} WHERE kind = 'chunk' AND array = 'sp'"
        ).collect()
    }
    assert keys and all("." in k and "/" not in k for k in keys)  # v2 keys, like 0.0.0
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), full.t2m.values)


@pytest.mark.parametrize("how", ["concurrent", "cas"])
def test_p5_the_rebase_after_a_merge_level_conflict(
    spark_delta, drop, tmp_path, no_hooks, how
):
    """The two branches that react at the MERGE. Day 3 lands inside day 2's commit
    call: as a row-level conflict (ConcurrentCommit), or before the MERGE runs so it aborts
    on metadata only. Day 2 rebases and lands either way."""
    ds = _dataset(hours=72)
    day = {d: ds.isel(time=slice(24 * (d - 1), 24 * d)) for d in (1, 2, 3)}
    chunks = {"time": 24, "latitude": 5, "longitude": 6}
    table, _, _ = _create(spark_delta, drop, tmp_path, day[1], chunks=chunks)
    two, three = uuid.uuid4().hex, uuid.uuid4().hex
    real, calls = M.commit, []

    def racing(spark_, table_, rows, *, write_id, meta, sharded=None, **kw):
        calls.append(write_id)
        if write_id == two and calls.count(two) == 1:
            gbx_zarr_write(
                day[3], table, mode="append", write_id=three, spark=spark_delta
            )
            if how == "concurrent":
                raise M.ConcurrentCommit("simulated row-level conflict. Fix: retry.")
        return real(
            spark_, table_, rows, write_id=write_id, meta=meta, sharded=sharded, **kw
        )

    no_hooks.setattr(_write.M, "commit", racing)
    no_hooks.setattr(_write, "_pause", lambda s: None)
    gbx_zarr_write(day[2], table, mode="append", write_id=two, spark=spark_delta)
    assert calls.count(two) == 2
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)


def test_p7_an_update_fills_the_gap_an_out_of_order_append_left(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=72)
    day = {d: ds.isel(time=slice(24 * (d - 1), 24 * d)) for d in (1, 2, 3)}
    chunks = {"time": 24, "latitude": 5, "longitude": 6}
    table, _, _ = _create(spark_delta, drop, tmp_path, day[1], chunks=chunks)
    gbx_zarr_write(day[3], table, mode="append", spark=spark_delta)
    with pytest.raises(ValueError, match="fill a gap"):
        gbx_zarr_write(day[2], table, mode="append", spark=spark_delta)
    gbx_zarr_write(day[2], table, mode="update", spark=spark_delta)
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)


@pytest.mark.parametrize("how", ["once", "always"])
def test_p9_a_create_whose_commit_landed_keeps_its_marker(
    spark_delta, drop, tmp_path, no_hooks, how
):
    """I7: the lookup after the MERGE fails (a transient error once, or every time). The
    commit is in, so the marker must stay and the store stays writable."""
    root, table = tmp_path / f"r9{how}", _table(f"p9{how}")
    drop(table)
    real, calls = M.find_commit_version, []

    def flaky(spark_, table_, write_id, *, since=0):
        if table_.startswith("delta."):  # the root's ownership ledger, not the manifest
            return real(spark_, table_, write_id, since=since)
        calls.append(1)
        if len(calls) >= 2 and (how == "always" or len(calls) == 2):
            raise RuntimeError("transient lookup failure")
        return real(spark_, table_, write_id, since=since)

    no_hooks.setattr(M, "find_commit_version", flaky)
    wid = uuid.uuid4().hex
    if how == "once":
        assert (
            gbx_zarr_write(
                _dataset(),
                table,
                root=str(root),
                chunks=CHUNKS,
                write_id=wid,
                spark=spark_delta,
            )
            == 1
        )
    else:
        with pytest.raises(RuntimeError, match="transient"):
            gbx_zarr_write(
                _dataset(),
                table,
                root=str(root),
                chunks=CHUNKS,
                write_id=wid,
                spark=spark_delta,
            )
    no_hooks.setattr(M, "find_commit_version", real)
    assert real(spark_delta, table, wid) == 1
    assert (root / OWNER_MARKER).exists()
    gbx_zarr_write(
        _dataset(hours=24, start="2023-01-03"), table, mode="append", spark=spark_delta
    )


def test_p10_an_append_of_fill_stores_no_chunk(spark_delta, drop, tmp_path, no_hooks):
    """Chunks of fill are dropped in every mode."""
    table, _, _ = _create(spark_delta, drop, tmp_path)
    nxt = _dataset(hours=24, start="2023-01-03")
    nxt["t2m"][:] = np.nan
    wid = uuid.uuid4().hex
    gbx_zarr_write(nxt, table, mode="append", write_id=wid, spark=spark_delta)
    assert [r for r in _rows_of(spark_delta, table, wid) if r["array"] == "t2m"] == []
    got = _read(spark_delta, table, "t2m")
    assert got.shape == (72, 10, 12) and np.isnan(got[48:]).all()


def test_p11_an_update_with_coordinate_blocks_keeps_labels_appended_meanwhile(
    spark_delta, drop, tmp_path, no_hooks
):
    """I2: the update rewrites day 1's time labels; day 2 is appended before it commits.
    The rebase reads the latest time chunk, so day 2's labels survive."""
    ds = _dataset(hours=48)
    day1, day2 = ds.isel(time=slice(0, 24)), ds.isel(time=slice(24, 48))
    chunks = {"time": 24, "latitude": 5, "longitude": 6}
    table, _, _ = _create(spark_delta, drop, tmp_path, day1, chunks=chunks)
    upd, ran = uuid.uuid4().hex, []

    def append_first(wid):
        if wid == upd and not ran:
            ran.append(gbx_zarr_write(day2, table, mode="append", spark=spark_delta))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", append_first)
    blocks = _blocks(
        spark_delta,
        ("t2m", (0, 0, 0), np.full((24, 5, 6), 7.0, "float32")),
        ("time", (0,), day1.time.values),
    )
    gbx_zarr_write(blocks, table, mode="update", write_id=upd)
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    got = _read(spark_delta, table, "t2m")
    assert (got[0:24, 0:5, 0:6] == 7.0).all()
    np.testing.assert_array_equal(got[24:], day2.t2m.values)


def test_p12_a_new_variable_written_while_another_write_appends_takes_the_new_extent(
    spark_delta, drop, tmp_path, no_hooks
):
    """I3: the new array's write guards the documents it was sized from, so the append
    landing first forces a rebase that re-sizes it."""
    ds = _dataset(hours=48)
    day1, day2 = ds.isel(time=slice(0, 24)), ds.isel(time=slice(24, 48))
    table, _, _ = _create(spark_delta, drop, tmp_path, day1)
    new, ran = uuid.uuid4().hex, []

    def append_first(wid):
        if wid == new and not ran:
            ran.append(gbx_zarr_write(day2, table, mode="append", spark=spark_delta))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", append_first)
    extra = day1[["t2m"]].rename({"t2m": "sp"}).isel(time=slice(0, 12)) * 0 + 1.0
    gbx_zarr_write(extra, table, mode="update", write_id=new, spark=spark_delta)
    shapes = {
        r["array"]: json.loads(r["meta"])["shape"]
        for r in spark_delta.sql(
            f"SELECT array, meta FROM {table} WHERE kind = 'array' AND tag IS NULL"
        ).collect()
    }
    assert shapes["sp"] == shapes["t2m"] == [48, 10, 12] and shapes["time"] == [48]


def test_an_append_grows_an_array_created_after_its_base(
    spark_delta, drop, tmp_path, no_hooks
):
    """I3, the other order: the new array lands first; the append, whose base predates
    it, grows it at commit time."""
    ds = _dataset(hours=48)
    day1, day2 = ds.isel(time=slice(0, 24)), ds.isel(time=slice(24, 48))
    table, _, _ = _create(spark_delta, drop, tmp_path, day1)
    app, ran = uuid.uuid4().hex, []
    extra = day1[["t2m"]].rename({"t2m": "sp"}).isel(time=slice(0, 12)) * 0 + 1.0

    def new_variable_first(wid):
        if wid == app and not ran:
            ran.append(gbx_zarr_write(extra, table, mode="update", spark=spark_delta))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", new_variable_first)
    gbx_zarr_write(day2, table, mode="append", write_id=app, spark=spark_delta)
    got = _read(spark_delta, table, "sp")
    assert got.shape == (48, 10, 12) and (got[:12] == 1.0).all()


@pytest.mark.parametrize("same", [False, True])
def test_m11_two_writers_setting_one_label_differently_conflict(
    spark_delta, drop, tmp_path, no_hooks, same
):
    """A coordinate cell both writers set is data: different values
    conflict, equal values merge."""
    ds = _dataset(hours=24)
    table, _, _ = _create(spark_delta, drop, tmp_path, ds)
    lon = ds.longitude.values
    mine, theirs = lon + 0.5, lon + (0.5 if same else 0.25)
    b, ran = uuid.uuid4().hex, []

    def other_first(wid):
        if wid == b and not ran:
            ran.append(
                gbx_zarr_write(
                    _blocks(spark_delta, ("longitude", (0,), theirs)),
                    table,
                    mode="update",
                )
            )

    no_hooks.setattr(_write, "_BEFORE_COMMIT", other_first)
    blocks = _blocks(spark_delta, ("longitude", (0,), mine))
    if same:
        gbx_zarr_write(blocks, table, mode="update", write_id=b)
        np.testing.assert_array_equal(_read(spark_delta, table, "longitude"), mine)
    else:
        with pytest.raises(ZarrWriteConflict, match="different values") as e:
            gbx_zarr_write(blocks, table, mode="update", write_id=b)
        assert "longitude/c/0" in e.value.keys
        np.testing.assert_array_equal(_read(spark_delta, table, "longitude"), theirs)


# --- create edge cases ------------------------------------------------------------


def test_i6_create_refuses_an_existing_root_that_is_not_its_own(
    spark_delta, drop, tmp_path, no_hooks
):
    full = tmp_path / "vendor.zarr"
    full.mkdir()
    (full / "zarr.json").write_text("{}")
    with pytest.raises(ZarrOwnershipError, match="not empty") as e:
        gbx_zarr_write(_dataset(), _table("i6"), root=str(full), spark=spark_delta)
    assert "Fix:" in str(e.value) and not (full / OWNER_MARKER).exists()
    theirs, _, _ = _create(spark_delta, drop, tmp_path, label="theirs")
    with pytest.raises(ZarrOwnershipError, match="already owned"):
        gbx_zarr_write(
            _dataset(),
            _table("i6b"),
            root=str(tmp_path / "root_theirs"),
            spark=spark_delta,
        )
    empty = tmp_path / "empty"
    empty.mkdir()
    table = _table("i6c")
    drop(table)
    assert gbx_zarr_write(
        _dataset(), table, root=str(empty), chunks=CHUNKS, spark=spark_delta
    )


def test_m14_a_create_retried_after_a_crash_adopts_its_empty_manifest(
    spark_delta, drop, tmp_path, no_hooks
):
    from databricks.labs.gbx.zarr._register import _claim_marker, qualified_table_name

    table, root = _table("m14"), tmp_path / "m14"
    drop(table)
    owner = qualified_table_name(spark_delta, table)
    _claim_marker(str(root), owner, "managed")  # the crashed attempt got this far
    M.create_manifest(
        spark_delta,
        table,
        M.StoreProps("managed", str(root), None, True, "fuse", "7 days"),
    )
    (root / "data").mkdir()  # and wrote some packs
    v = gbx_zarr_write(
        _dataset(), table, root=str(root), chunks=CHUNKS, spark=spark_delta
    )
    assert v == 1
    np.testing.assert_array_equal(
        _read(spark_delta, table, "t2m"), _dataset().t2m.values
    )


def test_m14_a_losing_create_keeps_the_marker_the_winner_relies_on(
    spark_delta, drop, tmp_path, no_hooks
):
    table, root = _table("m14b"), tmp_path / "m14b"
    drop(table)
    real = M.create_manifest

    def the_other_wins(spark_, table_, props):
        real(spark_, table_, props)  # a concurrent create of the same table got there
        raise ZarrOwnershipError(f"{table_} already exists", fix="choose another")

    no_hooks.setattr(_write.M, "create_manifest", the_other_wins)
    with pytest.raises(ZarrOwnershipError):
        gbx_zarr_write(
            _dataset(), table, root=str(root), chunks=CHUNKS, spark=spark_delta
        )
    assert (root / OWNER_MARKER).exists()


# --- the rest of the rebase loop, and the marker wiring ---------------------------------


def _edit_array_doc(spark, table, array, edit):
    """Commit a change to the live ``zarr.json`` of ``array``, as another writer would."""
    import pyarrow as pa

    row = spark.sql(
        f"SELECT meta, write_id FROM {table} WHERE kind = 'array' AND array = '{array}' "
        "AND tag IS NULL"
    ).first()
    doc = json.loads(row["meta"])
    edit(doc)
    w = uuid.uuid4().hex
    return M.commit(
        spark,
        table,
        pa.Table.from_pylist(
            [
                dict(
                    kind="array",
                    array=array,
                    key="zarr.json",
                    write_id=w,
                    op="upsert",
                    expected_write_id=row["write_id"],
                    meta=json.dumps(doc),
                )
            ]
        ),
        write_id=w,
        meta={},
    )


def _live_doc(spark, table, array):
    row = spark.sql(
        f"SELECT meta FROM {table} WHERE kind = 'array' AND array = '{array}' "
        "AND tag IS NULL"
    ).first()
    return json.loads(row["meta"])


def test_a_rebase_refuses_a_layout_another_write_changed(
    spark_delta, drop, tmp_path, no_hooks
):
    ds = _dataset(hours=48)
    table, _, _ = _create(spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)))
    app, ran = uuid.uuid4().hex, []

    def relayout(wid):
        if wid == app and not ran:

            def edit(doc):
                doc["codecs"][0]["configuration"]["codecs"][1]["configuration"][
                    "level"
                ] = 9

            ran.append(_edit_array_doc(spark_delta, table, "t2m", edit))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", relayout)
    with pytest.raises(ZarrWriteConflict, match="layout"):
        gbx_zarr_write(
            ds.isel(time=slice(24, 48)),
            table,
            mode="append",
            write_id=app,
            spark=spark_delta,
        )


def test_m22_a_rebase_keeps_an_attribute_another_write_set(
    spark_delta, drop, tmp_path, no_hooks
):
    """The append's rebase builds the grown document over the live one, so an
    attribute set on an array meanwhile (a key the append never touches) survives."""
    ds = _dataset(hours=48)
    table, _, _ = _create(spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)))
    app, ran = uuid.uuid4().hex, []

    def annotate(wid):
        if wid == app and not ran:
            ran.append(
                _edit_array_doc(
                    spark_delta,
                    table,
                    "t2m",
                    lambda doc: doc["attributes"].update(source="reanalysis"),
                )
            )

    no_hooks.setattr(_write, "_BEFORE_COMMIT", annotate)
    gbx_zarr_write(
        ds.isel(time=slice(24, 48)),
        table,
        mode="append",
        write_id=app,
        spark=spark_delta,
    )
    doc = _live_doc(spark_delta, table, "t2m")
    assert doc["shape"][0] == 48
    assert doc["attributes"]["source"] == "reanalysis" and doc["attributes"]["units"]
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)


@pytest.mark.parametrize("same", [True, False])
def test_m22_two_writers_creating_one_array_clash_only_on_different_attributes(
    spark_delta, drop, tmp_path, no_hooks, same
):
    """Both writers create ``sp`` (the second only its document: its values are all
    fill). Equal documents merge; an attribute they set differently is a conflict."""
    ds = _dataset(hours=24)
    table, _, _ = _create(spark_delta, drop, tmp_path, ds)
    first = ds[["t2m"]].rename({"t2m": "sp"}).isel(time=slice(0, 12)) * 0 + 1.0
    first["sp"].attrs = {"units": "Pa"}
    second = first * np.nan
    second["sp"].attrs = {"units": "Pa" if same else "hPa"}
    b, ran = uuid.uuid4().hex, []

    def other_first(wid):
        if wid == b and not ran:
            ran.append(gbx_zarr_write(first, table, mode="update", spark=spark_delta))

    no_hooks.setattr(_write, "_BEFORE_COMMIT", other_first)
    if same:
        gbx_zarr_write(second, table, mode="update", write_id=b, spark=spark_delta)
        assert _live_doc(spark_delta, table, "sp")["attributes"]["units"] == "Pa"
    else:
        with pytest.raises(ZarrWriteConflict, match="units") as e:
            gbx_zarr_write(second, table, mode="update", write_id=b, spark=spark_delta)
        assert e.value.keys == ("sp",) and "Fix:" in str(e.value)
        assert _live_doc(spark_delta, table, "sp")["attributes"]["units"] == "Pa"


def test_the_rebase_loop_gives_up_after_its_attempt_bound(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _create(spark_delta, drop, tmp_path)
    calls = []

    def always_busy(*a, **k):
        calls.append(1)
        raise M.ConcurrentCommit("busy. Fix: retry.")

    no_hooks.setattr(_write.M, "commit", always_busy)
    no_hooks.setattr(_write, "_pause", lambda s: None)
    bound = _write._ATTEMPTS
    with pytest.raises(ZarrWriteConflict, match=f"gave up after {bound} attempts") as e:
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), np.ones((12, 5, 6), "float32"))),
            table,
            mode="update",
        )
    assert len(calls) == bound and "Fix:" in str(e.value)


def test_a_writer_that_loses_seven_rounds_still_lands(
    spark_delta, drop, tmp_path, no_hooks
):
    """Eight appenders that commit at the same moment all change the array documents and
    the time chunk, so Delta lets one of them land per round and the others rebase and try
    again: the last of eight needs eight attempts (measured on Databricks, UC managed
    tables). A bound of five gave up on three of the eight."""
    table, _, _ = _create(spark_delta, drop, tmp_path)
    real, calls, pauses = M.commit, [], []

    def lose_seven_rounds(spark_, table_, rows, *, write_id, meta, sharded=None, **kw):
        calls.append(write_id)
        if len(calls) <= 7:
            raise M.ConcurrentCommit("lost this round. Fix: retry.")
        return real(
            spark_, table_, rows, write_id=write_id, meta=meta, sharded=sharded, **kw
        )

    no_hooks.setattr(_write.M, "commit", lose_seven_rounds)
    no_hooks.setattr(_write, "_pause", pauses.append)
    wid = uuid.uuid4().hex
    new = np.full((12, 5, 6), 3.0, "float32")
    version = gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), new)),
        table,
        mode="update",
        write_id=wid,
    )
    assert len(calls) == 8 and len(pauses) == 7
    assert version == _latest(spark_delta, table)
    assert (_read(spark_delta, table, "t2m")[0:12, 0:5, 0:6] == 3.0).all()


def test_the_wait_between_attempts_is_capped(spark_delta, drop, tmp_path, no_hooks):
    """The wait doubles each attempt, up to a cap: with the bound above five, an uncapped
    wait would reach minutes by the last attempts."""
    table, _, _ = _create(spark_delta, drop, tmp_path)
    pauses = []

    def always_busy(*a, **k):
        raise M.ConcurrentCommit("busy. Fix: retry.")

    no_hooks.setattr(_write.M, "commit", always_busy)
    no_hooks.setattr(_write, "_pause", pauses.append)
    no_hooks.setattr(_write.random, "uniform", lambda lo, hi: hi)  # the longest wait
    with pytest.raises(ZarrWriteConflict):
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), np.ones((12, 5, 6), "float32"))),
            table,
            mode="update",
        )
    assert len(pauses) == _write._ATTEMPTS
    assert max(pauses) == _write._MAX_WAIT_SECONDS  # reached, and never passed
    assert pauses == sorted(pauses)  # and it never shrinks


def test_a_root_that_lost_its_marker_is_reclaimed_by_its_owner(
    spark_delta, drop, tmp_path, no_hooks
):
    """The marker is the note beside the claim. With the claim (the ledger) still naming the
    table, a write puts the note back. With neither left, the table's own properties still
    name the root and no other table holds it, so the write claims it again (the same
    compare-and-swap a new claim goes through) instead of leaving the table stuck."""
    import shutil
    import types

    from databricks.labs.gbx.core import _retry

    no_hooks.setattr(_retry, "time", types.SimpleNamespace(sleep=lambda s: None))
    table, root, version = _create(spark_delta, drop, tmp_path)
    block = _blocks(spark_delta, ("t2m", (0, 0, 0), np.ones((12, 5, 6), "float32")))
    (root / OWNER_MARKER).unlink()
    version = gbx_zarr_write(block, table, mode="update")
    assert json.loads((root / OWNER_MARKER).read_text())["table"].endswith(table)

    (root / OWNER_MARKER).unlink()
    shutil.rmtree(root / OWNER_LEDGER)
    assert gbx_zarr_write(block, table, mode="update") > version
    assert json.loads((root / OWNER_MARKER).read_text())["table"].endswith(table)
    assert (root / OWNER_LEDGER).is_dir()


# --- naming variables keeps what their dimensions need ------------------------------------------------------------------


def test_p13_naming_variables_keeps_the_coordinate_blocks_of_their_dimensions(
    spark_delta, drop, tmp_path, no_hooks
):
    """``variables=["t2m"]`` with blocks of t2m and time. An append finds its
    labels, a create makes the time array, an update patches the labels; a coordinate no
    named variable uses is left out."""
    ds = _dataset(hours=48)
    day1, day2 = ds.isel(time=slice(0, 24)), ds.isel(time=slice(24, 48))
    table, _, _ = _create(spark_delta, drop, tmp_path, day1)
    gbx_zarr_write(
        _blocks(
            spark_delta,
            ("t2m", (0, 0, 0), day2.t2m.values),
            ("time", (0,), day2.time.values),
        ),
        table,
        mode="append",
        variables=["t2m"],
    )
    np.testing.assert_array_equal(_read(spark_delta, table, "time"), ds.time.values)
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), ds.t2m.values)

    made = _table("p13c")
    drop(made)
    gbx_zarr_write(
        _blocks(
            spark_delta,
            ("t2m", (0, 0, 0), day1.t2m.values),
            ("time", (0,), day1.time.values),
        ),
        made,
        root=str(tmp_path / "p13c"),
        variables=["t2m"],
        chunks=CHUNKS,
    )
    np.testing.assert_array_equal(_read(spark_delta, made, "time"), day1.time.values)

    def relabel(shift):
        return gbx_zarr_write(
            _blocks(
                spark_delta,
                ("t2m", (0, 0, 0), np.full((24, 10, 12), 1.0, "float32")),
                ("time", (0,), day1.time.values + shift),
            ),
            made,
            mode="update",
            variables=["t2m"],
        )

    # labels the hourly int64 units cannot hold are refused, never stored in other units
    with pytest.raises(ZarrUnsupported, match="whole multiples"):
        relabel(np.timedelta64(30, "m"))
    relabel(np.timedelta64(1, "h"))  # relabel day 1 through an update
    np.testing.assert_array_equal(
        _read(spark_delta, made, "time"), day1.time.values + np.timedelta64(1, "h")
    )
    assert (_read(spark_delta, made, "t2m") == 1.0).all()


def test_rr1_m3_disjoint_read_modify_writers_of_one_shard_need_no_guard(
    spark_delta, drop, tmp_path, no_hooks
):
    """Chunks with rows of their own are guarded by those rows; the shard is not
    checked. Two partial writers of different chunks in one registered shard both land,
    each on its first commit, and neither commit carries a check row."""
    store, table = _registered(spark_delta, drop, tmp_path, "m3")
    whole = np.full((12, 37, 72), 1.0, "float32")
    gbx_zarr_write(  # give both chunks of shard c/0/0/0 a chunk row
        _blocks(spark_delta, ("t2m", (0, 0, 0), whole), ("t2m", (12, 0, 0), whole)),
        table,
        mode="update",
    )
    a, b = uuid.uuid4().hex, uuid.uuid4().hex
    part = np.full((6, 37, 72), 2.0, "float32")
    blocks_a = _blocks(spark_delta, ("t2m", (0, 0, 0), part))
    blocks_b = _blocks(spark_delta, ("t2m", (12, 0, 0), part))
    real, calls, ran = M.commit, [], []

    def recording(spark_, table_, rows, *, write_id, meta, sharded=None, **kw):
        calls.append((write_id, rows.column("op").to_pylist()))
        return real(
            spark_, table_, rows, write_id=write_id, meta=meta, sharded=sharded, **kw
        )

    def a_first(wid):
        if wid == b and not ran:
            ran.append(gbx_zarr_write(blocks_a, table, mode="update", write_id=a))

    no_hooks.setattr(_write.M, "commit", recording)
    no_hooks.setattr(_write, "_BEFORE_COMMIT", a_first)
    gbx_zarr_write(blocks_b, table, mode="update", write_id=b)
    for wid in (a, b):
        ops = [o for w, o in calls if w == wid]
        assert len(ops) == 1 and "check" not in ops[0]  # one commit, no guard
    got = _read(spark_delta, table, "t2m")
    assert (got[0:6, 0:37, 0:72] == 2.0).all() and (got[6:12, 0:37, 0:72] == 1.0).all()
    assert (got[12:18, 0:37, 0:72] == 2.0).all() and (
        got[18:24, 0:37, 0:72] == 1.0
    ).all()


def test_rr1_m4_an_overwrite_starts_a_partly_covered_chunk_from_fill(
    spark_delta, drop, tmp_path, no_hooks
):
    """An overwrite replaces the variable, so the cells of a chunk it covers
    only in part read as fill, not as the base, and nothing is read."""
    table, _, _ = _create(spark_delta, drop, tmp_path)
    wid = uuid.uuid4().hex
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), np.full((6, 5, 6), 3.0, "float32"))),
        table,
        mode="overwrite",
        write_id=wid,
    )
    got = _read(spark_delta, table, "t2m")
    assert (got[0:6, 0:5, 0:6] == 3.0).all()
    assert np.isnan(got[6:12, 0:5, 0:6]).all() and np.isnan(got[12:]).all()
    assert _commit_meta(spark_delta, table, wid)["rmw"] == 0


def test_rr1_m6_a_block_of_an_array_the_store_cannot_decode_says_so(
    spark_delta, drop, tmp_path, no_hooks
):
    path = tmp_path / "strings.zarr"
    g = zarr.open_group(str(path), mode="w", zarr_format=3)
    v = g.create_array("v", shape=(3,), chunks=(2,), dtype="f8", dimension_names=("x",))
    v[:] = [1.0, 2.0, 3.0]
    label = g.create_array(
        "label", shape=(3,), chunks=(2,), dtype=str, dimension_names=("x",)
    )
    label[:] = ["a", "b", "c"]
    table = _table("m6")
    drop(table)
    gbx_zarr_register(
        spark_delta, str(path), table, writable=True, overlay_root=str(tmp_path / "ov6")
    )
    row = ("label", ["x"], [0], [2], None, None, "<f8", [2], bytes(16), None, None)
    with pytest.raises(Exception, match="cannot be decoded") as e:
        gbx_zarr_write(
            spark_delta.createDataFrame([row], BLOCKS_SCHEMA), table, mode="update"
        )
    assert "variables=" not in str(e.value) and "Fix:" in str(e.value)


def test_rr1_m2_side_rows_are_routed_the_same_way_every_time():
    """A recomputed explode task must route its digest and coordinate rows as before."""
    import pyarrow as pa

    from databricks.labs.gbx.zarr._read import _BLOCKS_ARROW
    from databricks.labs.gbx.zarr._write_tasks import TaskConfig, explode_function

    cfg = TaskConfig(
        write_id="a" * 32,
        mode="update",
        root="/tmp",
        base_root=None,
        arrays={},
        offsets={},
        base_docs={},
        base_arrays={},
        unknown_fix="x",
        batch_rows=8,
        batch_bytes=1 << 20,
        roll_bytes=1 << 20,
    )
    coord = np.arange(3, dtype="f8")
    batch = pa.RecordBatch.from_pylist(
        [
            {
                "variable": "x",
                "dims": ["x"],
                "start": [0],
                "stop": [3],
                "coord_start": None,
                "coord_stop": None,
                "dtype": "<f8",
                "shape": [3],
                "data": coord.tobytes(),
                "fill_value": None,
                "attrs": None,
            }
        ],
        schema=_BLOCKS_ARROW,
    )

    def parts():
        out = list(explode_function(cfg)([batch]))
        return sorted(r["part"] for b in out for r in b.to_pylist())

    first = parts()
    assert first == parts() and len(set(first)) == 1


# --- carries from the reviews of the writer ----------------------------------------------------


def test_an_append_needs_the_coordinate_of_its_dimension(
    spark_delta, drop, tmp_path, no_hooks
):
    """Labels cannot be placed by value against a dimension that has none: the append is
    refused and nothing of it is visible."""
    from databricks.labs.gbx.zarr.errors import ZarrError

    ds = _dataset()
    table, _, v1 = _create(
        spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)).drop_vars("time")
    )
    with pytest.raises(ZarrError, match="no coordinate array for 'time'") as e:
        gbx_zarr_write(
            ds.isel(time=slice(24, 48)),
            table,
            mode="append",
            append_dim="time",
            spark=spark_delta,
        )
    assert "Fix:" in str(e.value) and _latest(spark_delta, table) == v1


def test_nat_into_an_integer_time_array_without_a_fill_is_refused(
    spark_delta, drop, tmp_path, no_hooks
):
    """xarray would store NaT as the smallest integer, which a read refuses as out of range."""
    from databricks.labs.gbx.zarr.errors import ZarrUnsupported

    store = make_era5_like(tmp_path / "nat.zarr", zarr_format=3)
    table = _table("nat")
    drop(table)
    gbx_zarr_register(
        spark_delta, str(store), table, writable=True, overlay_root=str(tmp_path / "ov")
    )
    times = np.array(["NaT", "2023-01-01T01:00"], dtype="datetime64[ns]")
    with pytest.raises(ZarrUnsupported, match="NaT") as e:
        gbx_zarr_write(
            _blocks(spark_delta, ("time", (0,), times)), table, mode="update"
        )
    assert "add a _FillValue to the time array" in e.value.fix


@pytest.mark.parametrize("start", [8, -1])
def test_m21_coordinate_blocks_outside_the_array_are_refused(
    spark_delta, drop, tmp_path, no_hooks, start
):
    table, _, v1 = _create(spark_delta, drop, tmp_path)
    labels = np.linspace(40.0, 38.0, 5)  # five of the store's ten latitudes
    with pytest.raises(ZarrGridMismatch, match="outside the array's 10 labels"):
        gbx_zarr_write(
            _blocks(spark_delta, ("latitude", (start,), labels)), table, mode="update"
        )
    assert _latest(spark_delta, table) == v1


def test_m14_a_new_nested_variable_gets_its_parent_group(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _create(spark_delta, drop, tmp_path)
    ones = np.ones((12, 5, 6), dtype="float32")
    gbx_zarr_write(
        _blocks(spark_delta, ("grp/sub/x", (0, 0, 0), ones)),
        table,
        mode="update",
        variables=["grp/sub/x"],
    )
    groups = {
        r["array"]
        for r in spark_delta.sql(
            f"SELECT array FROM {table} WHERE kind = 'group' AND tag IS NULL"
        ).collect()
    }
    assert groups == {None, "grp", "grp/sub"}  # the root, and both parents of the array
    np.testing.assert_array_equal(
        _read(spark_delta, table, "grp/sub/x")[:12, :5, :6], ones
    )


# --- an aligned create: packed where its blocks are made ---------------------------------

ALIGN_SHAPE = (24, 10, 12)
# (chunks, shards, cut): the blocks are cut every ``cut`` cells along each dimension
ALIGNED = {
    "one-chunk-blocks": (
        {"time": 12, "latitude": 5, "longitude": 6},
        False,
        (12, 5, 6),
    ),
    "four-chunk-blocks": (
        {"time": 12, "latitude": 5, "longitude": 6},
        False,
        (12, 10, 12),
    ),
    "edge-chunks": ({"time": 12, "latitude": 4, "longitude": 5}, False, (12, 4, 5)),
    "one-shard-blocks": (
        {"time": 12, "latitude": 5, "longitude": 6},
        {"time": 12, "latitude": 10, "longitude": 12},
        (12, 10, 12),
    ),
}
MISALIGNED = {
    "half-chunks": ({"time": 12, "latitude": 5, "longitude": 6}, False, (6, 5, 6)),
    "chunks-of-a-shard": (
        {"time": 12, "latitude": 5, "longitude": 6},
        {"time": 12, "latitude": 10, "longitude": 12},
        (12, 5, 6),
    ),
}


def _cube_values():
    values = np.arange(np.prod(ALIGN_SHAPE), dtype="float32").reshape(ALIGN_SHAPE)
    values[0:12, 0:5, 0:6] = np.nan  # one chunk all fill
    return values


def _cut_blocks(spark, cut, values):
    """``values`` cut every ``cut`` cells, with latitude and longitude labels."""
    blocks = [
        ("t2m", (t, y, x), values[t : t + cut[0], y : y + cut[1], x : x + cut[2]])
        for t in range(0, ALIGN_SHAPE[0], cut[0])
        for y in range(0, ALIGN_SHAPE[1], cut[1])
        for x in range(0, ALIGN_SHAPE[2], cut[2])
    ]
    blocks += [
        ("latitude", (0,), np.linspace(45.0, 36.0, ALIGN_SHAPE[1])),
        ("longitude", (0,), np.arange(ALIGN_SHAPE[2]) * 1.0),
    ]
    return _blocks(spark, *blocks)


def _refuse(stage):
    def refused(*a, **k):
        raise AssertionError(f"{stage} ran")

    return refused


def _create_cut(spark, drop, tmp_path, label, chunks, shards, cut, values=None):
    table, root = _table(label), tmp_path / f"root_{label}"
    drop(table)
    gbx_zarr_write(
        _cut_blocks(spark, cut, _cube_values() if values is None else values),
        table,
        root=str(root),
        chunks=chunks,
        shards=shards,
        spark=spark,
    )
    return table, root


@pytest.mark.parametrize("case", sorted(ALIGNED))
def test_an_aligned_create_is_packed_where_its_blocks_are_made(
    spark_delta, drop, tmp_path, no_hooks, case
):
    """Blocks of whole units (chunks, or the shards of a sharded array) are encoded and
    packed by the tasks that make them: no explode, no shuffle, no assemble."""
    chunks, shards, cut = ALIGNED[case]
    no_hooks.setattr(_write, "explode_function", _refuse("explode"))
    no_hooks.setattr(_write, "assemble_function", _refuse("assemble"))
    table, _ = _create_cut(spark_delta, drop, tmp_path, "al", chunks, shards, cut)
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), _cube_values())
    if shards:  # a shard's chunks share one pack, as after the shuffle
        packs = spark_delta.sql(
            f"SELECT shard_key, count(DISTINCT file) AS n FROM {table} WHERE kind = "
            "'chunk' AND array = 't2m' AND file IS NOT NULL GROUP BY shard_key"
        ).collect()
        assert packs and all(r["n"] == 1 for r in packs)


@pytest.mark.parametrize("case", sorted(MISALIGNED))
def test_a_misaligned_create_is_assembled(spark_delta, drop, tmp_path, no_hooks, case):
    chunks, shards, cut = MISALIGNED[case]
    no_hooks.setattr(_write, "pack_function", _refuse("pack"))
    table, _ = _create_cut(spark_delta, drop, tmp_path, "mis", chunks, shards, cut)
    np.testing.assert_array_equal(_read(spark_delta, table, "t2m"), _cube_values())


def test_overlapping_whole_chunk_blocks_are_assembled_and_refused(
    spark_delta, drop, tmp_path, no_hooks
):
    """Two blocks of one whole chunk are each aligned, but together they are no aligned
    input: the assembler sees the chunk twice and refuses it (packed where they were
    made, both would land as rows of one key)."""
    table = _table("ov")
    drop(table)
    one = np.ones((12, 5, 6), "float32")
    with pytest.raises(Exception, match="more than once"):  # raised in a task
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), one), ("t2m", (0, 0, 0), one)),
            table,
            root=str(tmp_path / "root_ov"),
            chunks=CHUNKS,
            shards=False,
            spark=spark_delta,
        )


def _manifest_of(spark, table, root):
    """What a store holds, wherever its chunks landed: its documents, and per chunk row
    its key, shard, length, checksum and bytes (``None`` for a row with no file)."""
    docs = sorted(
        (r["kind"], r["array"] or "", r["key"] or "", r["meta"])
        for r in spark.sql(
            f"SELECT kind, array, key, meta FROM {table} "
            "WHERE kind IN ('group', 'array') AND tag IS NULL"
        ).collect()
    )
    chunks = []
    for r in spark.sql(
        f"SELECT array, key, shard_key, file, offset, length, checksum, origin, "
        f"identity FROM {table} WHERE kind = 'chunk' AND tag IS NULL"
    ).collect():
        data = None
        if r["file"] is not None:
            with open(root / r["file"], "rb") as f:
                f.seek(r["offset"])
                data = f.read(r["length"])
        chunks.append(
            (
                r["array"],
                r["key"],
                r["shard_key"],
                r["length"],
                r["checksum"],
                r["origin"],
                r["identity"],
                data,
            )
        )
    return docs, sorted(chunks, key=lambda c: (c[0], c[1]))


@pytest.mark.parametrize("case", sorted(ALIGNED))
def test_an_aligned_create_matches_the_assembled_store_byte_for_byte(
    spark_delta, drop, tmp_path, no_hooks, case
):
    """The packed and the assembled create of the same blocks hold the same documents and
    the same chunk rows, every chunk the same bytes; only the pack a chunk sits in (and
    where in it) may differ."""
    chunks, shards, cut = ALIGNED[case]
    packed, packed_root = _create_cut(
        spark_delta, drop, tmp_path, "pk", chunks, shards, cut
    )
    no_hooks.setattr(_write, "_aligned", lambda plan: False)
    no_hooks.setattr(_write, "pack_function", _refuse("pack"))
    shuffled, shuffled_root = _create_cut(
        spark_delta, drop, tmp_path, "sh", chunks, shards, cut
    )
    got = _manifest_of(spark_delta, packed, packed_root)
    want = _manifest_of(spark_delta, shuffled, shuffled_root)
    assert got[0] == want[0]
    assert len(got[1]) == len(want[1]) and got[1] == want[1]
    np.testing.assert_array_equal(
        _read(spark_delta, packed, "t2m"), _read(spark_delta, shuffled, "t2m")
    )
    np.testing.assert_array_equal(
        _read(spark_delta, packed, "latitude"), _read(spark_delta, shuffled, "latitude")
    )


def test_the_user_a_commit_records_is_asked_once_per_session():
    class Session:
        def __init__(self, user):
            self.user, self.asked = user, 0

        def sql(self, text):
            assert text == "SELECT current_user()"
            self.asked += 1
            return types.SimpleNamespace(first=lambda: [self.user])

    one, two = Session("a@example.com"), Session("b@example.com")
    assert [_write._current_user(one) for _ in range(3)] == ["a@example.com"] * 3
    assert _write._current_user(two) == "b@example.com"
    assert (one.asked, two.asked) == (1, 1)


def test_a_new_store_looks_for_its_write_id_once(spark_delta, drop, tmp_path, no_hooks):
    """The create made its table, so its commit skips the lookup before the MERGE: one
    lookup in all, the one after it."""
    real, looks = _write.M.find_commit_version, []

    def counted(*a, **k):
        looks.append(1)
        return real(*a, **k)

    no_hooks.setattr(_write.M, "find_commit_version", counted)
    table, _, version = _create(spark_delta, drop, tmp_path, label="once")
    assert looks == [1] and version == real(
        spark_delta, table, _write_id_of(spark_delta, table)
    )


def _write_id_of(spark, table):
    return spark.sql(
        f"SELECT key FROM {table} WHERE kind = 'commit' AND tag IS NULL"
    ).first()["key"]
