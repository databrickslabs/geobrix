"""Drift, ``gbx_zarr_sync`` and publish: a registered store other writers change.

The read-time check is Spark-free (a plain store's files as manifest rows). Sync and publish
run on local OSS Delta, where the store is changed the way a vendor would change it: with
``zarr`` itself, behind the table's back.
"""

import json
import os
import shutil
import threading
import time
import uuid

import numpy as np
import pyarrow as pa
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _access  # noqa: E402
from databricks.labs.gbx.zarr import _sync  # noqa: E402
from databricks.labs.gbx.zarr import _manifest as M  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    gbx_zarr_register,
    identity_of,
)
from databricks.labs.gbx.zarr._sync import gbx_zarr_sync, publish  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
    plain_store_rows,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrReadOnly,
    ZarrStoreDrift,
    ZarrUnsupported,
    ZarrWriteConflict,
)

DIMS = H.DIMS
WHOLE = (12, 37, 72)  # one inner chunk of the cube
LAYOUTS = [pytest.param(True, id="sharded"), pytest.param(False, id="unsharded")]


def _identity(path):
    st = os.stat(path)
    return identity_of(st.st_mtime_ns // 1_000_000, st.st_size)


@pytest.fixture
def drop(spark_delta):
    made = []
    yield made.append
    for t in made:
        spark_delta.sql(f"DROP TABLE IF EXISTS {t}")


@pytest.fixture
def registered(spark_delta, drop, tmp_path):
    """``registered(sharded, writable=False, fmt=3, consolidated=False)``: ``(store, table)``
    of a registered cube."""

    def make(sharded, writable=False, fmt=3, consolidated=False):
        store = make_era5_like(
            tmp_path / "base.zarr",
            zarr_format=fmt,
            sharded=sharded if fmt == 3 else None,
        )
        if consolidated:
            zarr.consolidate_metadata(str(store))
        table = f"gbx_zarr_sync_{uuid.uuid4().hex[:8]}"
        drop(table)
        kw = (
            {"writable": True, "overlay_root": str(tmp_path / "ov")} if writable else {}
        )
        gbx_zarr_register(spark_delta, str(store), table, **kw)
        return store, table

    return make


def _touch(path):
    """Move a file's modification time, as another program rewriting it would."""
    stat = os.stat(path)
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 5_000_000_000))


def _overlay_write(spark, table, value=3.0):
    """Write one whole inner chunk of ``t2m`` (chunk 1, 0, 1) into the overlay."""
    block = np.full(WHOLE, value, dtype="float32")
    return gbx_zarr_write(
        H.blocks_frame(spark, ("t2m", (12, 0, 72), block)), table, mode="update"
    )


def _live(spark, table, **where):
    cond = " AND ".join(f"{k} = '{v}'" for k, v in where.items())
    return spark.sql(
        f"SELECT * FROM {table} WHERE tag IS NULL" + (f" AND {cond}" if cond else "")
    ).collect()


def _changes(frame):
    return {(r["change"], r["array"], r["key"]) for r in frame.collect()}


# --- reading ----------------------------------------------------------------------


def test_touched_base_object_raises_drift(tmp_path, monkeypatch):
    """A registered file that changed outside GeoBrix is refused at read, with the way out."""
    store = make_era5_like(tmp_path / "base.zarr", zarr_format=3, sharded=True)
    _, rows, _ = plain_store_rows(store, origin="external", identity=True)
    src = H.registered_source(monkeypatch, store, overlay=tmp_path / "ov", rows=rows)
    assert H.read_blocks(src, variables=["t2m"])  # the recorded identities hold
    _touch(store / "t2m" / "c" / "0" / "0" / "0")
    with pytest.raises(ZarrStoreDrift, match="changed outside GeoBrix") as e:
        H.read_blocks(src, variables=["t2m"])
    assert "t2m/c/0/0/0" in str(e.value) and "gbx_zarr_sync" in e.value.fix


@pytest.mark.parametrize("resolution", [1, 1000])
def test_the_mtime_resolution_governs_every_identity_compare(monkeypatch, resolution):
    """One constant decides whether two modification times of a file are the same: the read
    check, and the sync diff (which publish's drift refusal uses too). The same file seen
    400 ms apart, as two file systems might report it, is a change at 1 ms and none at 1 s.
    """
    monkeypatch.setattr(_access, "MTIME_RESOLUTION_MS", resolution)
    listed, seen = "1792741018000:10", "1792741018400:10"
    moved = resolution == 1

    def row(identity, write_id):
        return dict(
            kind="chunk",
            array="t2m",
            key="c/0/0/0",
            origin="external",
            identity=identity,
            write_id=write_id,
        )

    live = pa.Table.from_pylist([row(listed, "a" * 32)], schema=M._COMMIT_SCHEMA)
    new = pa.Table.from_pylist([row(seen, "b" * 32)], schema=M._COMMIT_SCHEMA)
    diff = _sync._Diff("c" * 32)
    _sync._diff_data(diff, live, new)
    assert diff.report == ([("changed", "t2m", "c/0/0/0")] if moved else [])
    if moved:
        with pytest.raises(ZarrStoreDrift):
            _access.check_identity({"file": "f", "identity": listed}, seen)
    else:
        _access.check_identity({"file": "f", "identity": listed}, seen)


def test_a_document_merge_keeps_the_stores_text_when_its_extent_wins():
    """Where GeoBrix grew an array's extent, a sync lays the larger extent over the store's
    document. When the store's extent is the larger the store's own text stays as it is,
    so the next sync has nothing to report; where ours is larger it is rewritten."""
    from databricks.labs.gbx.zarr._meta import parse_v3

    def doc(n, **attrs):
        return {
            "zarr_format": 3,
            "node_type": "array",
            "shape": [n],
            "data_type": "float32",
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [4]}},
            "chunk_key_encoding": {
                "name": "default",
                "configuration": {"separator": "/"},
            },
            "fill_value": "NaN",
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            "dimension_names": ["x"],
            "attributes": attrs,
        }

    def run(ours, theirs):
        text = json.dumps(theirs, indent=2)  # as a vendor's tool writes it
        base = dict(kind="array", array="x", key="zarr.json")
        live = [dict(base, origin="gbx", write_id="a" * 32, meta=json.dumps(ours))]
        new = [dict(base, origin="external", write_id="b" * 32, meta=text)]
        diff = _sync._Diff("c" * 32)
        _sync._diff_documents(diff, live, new, {"x": parse_v3("x", theirs)})
        return text, diff

    text, diff = run(doc(48, units="K"), doc(60, units="K", long_name="x"))
    assert diff.report == [("both", "x", "zarr.json")]
    assert (diff.rows[0]["meta"], diff.rows[0]["origin"]) == (text, "external")

    text, diff = run(doc(48, units="K"), doc(24, units="K", long_name="x"))
    assert diff.report == [("both", "x", "zarr.json")]
    row = diff.rows[0]
    assert json.loads(row["meta"]) == doc(48, units="K", long_name="x")
    assert row["origin"] == "gbx"

    _, diff = run(
        doc(48, units="K"), doc(24, units="K")
    )  # only the extent: ours is kept
    assert diff.report == []


def test_a_write_that_reads_a_changed_base_object_raises_drift(spark_delta, registered):
    """Read-modify-write checks the identity of what it reads. Only the modification time
    moves here, so nothing but the identity can tell."""
    store, table = registered(sharded=True, writable=True)
    _touch(store / "t2m" / "c" / "0" / "0" / "0")
    part = np.full((6, 10, 10), 9.0, dtype="float32")  # part of chunk 0, 0, 0
    with pytest.raises(Exception, match="manifest recorded"):
        gbx_zarr_write(
            H.blocks_frame(spark_delta, ("t2m", (0, 0, 0), part)),
            table,
            mode="update",
        )


# --- sync -------------------------------------------------------------------------


@pytest.mark.parametrize("sharded", LAYOUTS)
def test_sync_records_changes_and_keeps_overlay(
    spark_delta, registered, tmp_path, sharded
):
    store, table = registered(sharded, writable=True)
    _overlay_write(spark_delta, table)
    (overlaid_row,) = _live(spark_delta, table, kind="chunk", array="t2m", origin="gbx")
    overlaid = [overlaid_row]
    # an unsharded overlay chunk remembers the base object it replaced; inside a shard the
    # shard's own row is that record
    assert (overlaid[0]["identity"] is not None) == (not sharded)

    t2m = zarr.open_array(str(store / "t2m"), mode="r+")
    u10 = zarr.open_array(str(store / "u10"), mode="r+")
    t2m[12:24, 0:37, 72:144] = 7.0  # the vendor writes the chunk we overlaid
    u10[0:12, 0:37, 0:72] = 2.5  # and another
    u10.attrs["long_name"] = "wind"  # an attribute only: the raw document differs
    os.remove(store / "u10" / "c" / "1" / "0" / "0")  # an object is gone
    extra = zarr.open_group(str(store), mode="r+").create_array(
        "extra", shape=(48, 73, 144), chunks=WHOLE, dtype="f4", dimension_names=DIMS
    )
    extra[0:12, 0:37, 0:72] = 1.0  # a new array

    with pytest.raises(ZarrStoreDrift, match="gbx_zarr_sync"):
        H.read_variable(spark_delta, table, "u10")
    shard_or_chunk = "c/0/0/0" if sharded else "c/1/0/1"
    assert _changes(gbx_zarr_sync(spark_delta, table)) == {
        ("both", "t2m", shard_or_chunk),
        ("changed", "u10", "c/0/0/0"),
        ("changed", "u10", "zarr.json"),
        ("deleted", "u10", "c/1/0/0"),
        ("new", "extra", "zarr.json"),
        ("new", "extra", "c/0/0/0"),
    }

    # the table now describes what is there, and the overlay chunk is still ours
    (kept,) = _live(spark_delta, table, kind="chunk", array="t2m", origin="gbx")
    assert (kept["file"], kept["offset"]) == (
        overlaid[0]["file"],
        overlaid[0]["offset"],
    )
    if sharded:
        assert kept["write_id"] == overlaid[0]["write_id"]  # untouched
    else:  # it now remembers the vendor's object
        assert kept["identity"] == _identity(store / "t2m" / "c" / "1" / "0" / "1")
    (row,) = _live(spark_delta, table, array="u10", key="c/0/0/0")
    assert row["identity"] == _identity(store / "u10" / "c" / "0" / "0" / "0")
    assert _live(spark_delta, table, array="u10", key="c/1/0/0") == []
    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()
    np.testing.assert_array_equal(
        H.read_variable(spark_delta, table, "u10")[0:12, 0:37, 0:72], 2.5
    )
    got = H.read_variable(spark_delta, table, "t2m")
    assert (
        got[12:24, 0:37, 72:144] == 3.0
    ).all()  # the overlay wins over the vendor's 7


def test_sync_extends_extent_when_vendor_appends(spark_delta, registered, tmp_path):
    store, table = registered(sharded=False)
    assert H.read_variable(spark_delta, table, "t2m").shape == (48, 73, 144)
    xr = pytest.importorskip("xarray")
    from databricks.labs.gbx.zarr._testing import era5_like_dataset

    later = era5_like_dataset().isel(time=slice(0, 12))
    later = later.assign_coords(time=later.time + np.timedelta64(48, "h"))
    later.to_zarr(str(store), mode="a", append_dim="time", consolidated=False)
    assert xr.open_zarr(str(store), consolidated=False).sizes["time"] == 60

    changes = _changes(gbx_zarr_sync(spark_delta, table))
    assert {("changed", "t2m", "zarr.json"), ("new", "t2m", "c/4/0/0")} <= changes
    assert H.read_variable(spark_delta, table, "t2m").shape == (60, 73, 144)
    np.testing.assert_array_equal(
        H.read_variable(spark_delta, table, "t2m")[48:],
        later.t2m.values,
    )


def test_sync_rejects_incompatible_change(spark_delta, registered):
    store, table = registered(sharded=False)
    before = M.snapshot(spark_delta, table).version
    shutil.rmtree(store / "u10")  # replaced by an array of another dtype
    zarr.open_group(str(store), mode="r+").create_array(
        "u10", shape=(48, 73, 144), chunks=WHOLE, dtype="f8", dimension_names=DIMS
    )
    with pytest.raises(ZarrUnsupported, match="dtype, chunk grid or codecs") as e:
        gbx_zarr_sync(spark_delta, table)
    assert "re-register the store" in e.value.fix
    assert M.snapshot(spark_delta, table).version == before


def test_sync_ignores_check_rows(spark_delta, registered):
    """A check row rewrites a row unchanged (a new row version, the same write_id): it is
    no change to the store."""
    import pyarrow as pa

    _, table = registered(sharded=True)
    (shard,) = _live(spark_delta, table, kind="shard", array="t2m", key="c/0/0/0")
    w = uuid.uuid4().hex
    version = M.commit(
        spark_delta,
        table,
        pa.Table.from_pylist(
            [
                dict(
                    kind="shard",
                    array="t2m",
                    key="c/0/0/0",
                    write_id=w,
                    op="check",
                    expected_write_id=shard["write_id"],
                )
            ]
        ),
        write_id=w,
        meta={},
    )
    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()
    assert M.snapshot(spark_delta, table).version == version  # nothing was committed


@pytest.mark.parametrize("how", ["all-fill write", "overwrite"])
@pytest.mark.parametrize("sharded", LAYOUTS)
def test_an_all_fill_write_stays_deleted_through_sync_and_publish(
    spark_delta, registered, sharded, how
):
    """What GeoBrix deletes from a registered store is a tombstone, not a missing row: the
    base object it hid is not brought back by the next sync, and a publish removes it.
    That holds for fill written over a chunk, and for an overwrite of the whole variable
    (unsharded chunks, and shards, are hidden)."""
    store, table = registered(sharded, writable=True)
    t2m = zarr.open_array(str(store / "t2m"), mode="r")
    fill = t2m.fill_value
    if how == "overwrite":
        block = np.full(WHOLE, 5.0, dtype="float32")
        want = np.full(t2m.shape, fill, dtype="float32")
        want[0:12, 0:37, 0:72] = 5.0
        gbx_zarr_write(
            H.blocks_frame(spark_delta, ("t2m", (0, 0, 0), block)),
            table,
            mode="overwrite",
            variables=["t2m"],
        )
        tombstones, objects_left = (2 if sharded else 15), 1
    else:
        want = t2m[...]
        want[12:24, 0:37, 72:144] = fill
        block = np.full(WHOLE, fill, dtype="float32")
        gbx_zarr_write(
            H.blocks_frame(spark_delta, ("t2m", (12, 0, 72), block)),
            table,
            mode="update",
        )
        tombstones, objects_left = 1, (2 if sharded else 15)

    hidden = [
        r
        for r in _live(spark_delta, table, array="t2m", origin="gbx")
        if r["file"] is None
    ]
    assert len(hidden) == tombstones
    assert all((r["offset"], r["length"]) == (None, 0) for r in hidden)
    # a tombstone remembers the base object it hides, unless a shard row already does
    assert all(
        (r["identity"] is not None) == (how == "overwrite" or not sharded)
        for r in hidden
    )
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), want)

    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()  # not resurrected
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), want)

    publish(spark_delta, table)
    np.testing.assert_array_equal(zarr.open_array(str(store / "t2m"))[...], want)
    left = [
        p for p in (store / "t2m").rglob("*") if p.is_file() and p.name != "zarr.json"
    ]
    assert len(left) == objects_left
    assert _live(spark_delta, table, origin="gbx") == []
    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), want)


def _plant_lock(spark, table, age):
    """A publish lock taken ``age`` seconds ago by a publish that never finished."""
    held = _live(spark, table, kind="lock")
    w = uuid.uuid4().hex
    row = dict(
        kind="lock",
        key="publish",
        write_id=w,
        op="upsert",
        expected_write_id=held[0]["write_id"] if held else None,
        meta=json.dumps({"started": time.time() - age}),
    )
    M.commit(spark, table, pa.Table.from_pylist([row]), write_id=w, meta={})


def test_a_dead_publish_lock_is_cleared_only_on_request(spark_delta, registered):
    """A publish killed hard leaves its lock. Sync and publish both refuse it, and say how to
    clear it. Only a lock at least an hour old can be broken, and only when asked."""
    _, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table)
    _plant_lock(spark_delta, table, age=2 * 3600)
    for call in (gbx_zarr_sync, publish):
        with pytest.raises(ZarrWriteConflict, match="lock") as e:
            call(spark_delta, table)
        assert "break_lock=True" in e.value.fix and "hour" in e.value.fix
        # the public calls, not the private publish() of the module
        assert "gbx_zarr_export(spark, " in e.value.fix
        assert 'method="publish"' in e.value.fix
        assert "gbx_zarr_sync(spark, " in e.value.fix  # the calls as a user types them
        assert " publish(" not in e.value.fix
    assert len(_live(spark_delta, table, kind="lock")) == 1  # refusing left it alone

    _plant_lock(spark_delta, table, age=600)  # this young, it may be a live publish
    with pytest.raises(ZarrWriteConflict, match="10 minutes"):
        gbx_zarr_sync(spark_delta, table, break_lock=True)
    assert len(_live(spark_delta, table, kind="lock")) == 1

    _plant_lock(spark_delta, table, age=2 * 3600)
    assert _changes(gbx_zarr_sync(spark_delta, table, break_lock=True)) == set()
    assert _live(spark_delta, table, kind="lock") == []

    _plant_lock(spark_delta, table, age=2 * 3600)
    publish(spark_delta, table, break_lock=True)
    assert _live(spark_delta, table, kind="lock") == []
    assert _live(spark_delta, table, origin="gbx") == []


@pytest.mark.parametrize("own", [True, False], ids=["own-data", "read-only"])
def test_publish_needs_a_writable_registration(
    spark_delta, registered, drop, tmp_path, own
):
    if own:  # a store that holds its own data has no base to publish into
        from databricks.labs.gbx.zarr._testing import era5_like_dataset

        table = f"gbx_zarr_sync_own_{uuid.uuid4().hex[:8]}"
        drop(table)
        data = era5_like_dataset().isel(time=slice(0, 12))
        gbx_zarr_write(data, table, root=str(tmp_path / "own"), spark=spark_delta)
    else:
        _, table = registered(sharded=True)
    with pytest.raises(ZarrReadOnly) as e:
        publish(spark_delta, table)
    assert "Fix:" in str(e.value)


# --- publish ----------------------------------------------------------------------


@pytest.mark.parametrize("fmt", [3, 2], ids=["v3-sharded", "v2"])
def test_publish_writes_base_and_repoints(spark_delta, registered, tmp_path, fmt):
    store, table = registered(sharded=True, writable=True, fmt=fmt, consolidated=True)
    first = store / "t2m" / ("c/0/0/0" if fmt == 3 else "0.0.0")
    stale = (store / "zarr.json") if fmt == 3 else (store / ".zmetadata")
    assert "consolidated_metadata" in stale.read_text() or fmt == 2 and stale.exists()
    original = zarr.open_array(str(store / "t2m"), mode="r")[...]
    _overlay_write(spark_delta, table)
    new = np.ones(WHOLE, dtype="float32")
    gbx_zarr_write(  # a variable the store lacks: its metadata goes last
        H.blocks_frame(spark_delta, ("sp", (0, 0, 0), new)),
        table,
        mode="update",
        variables=["sp"],
    )
    # a float variable that carries a NaN _FillValue, as a dataset opened from a file does
    wide = era5_like_dataset().isel(time=slice(0, 12))[["u10"]].rename(u10="sq")
    wide["sq"].encoding["_FillValue"] = np.float32("nan")
    gbx_zarr_write(wide, table, mode="update", variables=["sq"], spark=spark_delta)
    want = original.copy()
    want[12:24, 0:37, 72:144] = 3.0

    version = publish(spark_delta, table)

    # the published store opens in xarray, which refuses a v3 float _FillValue that is not
    # its base64 form for the WHOLE store
    with xr.open_zarr(str(store), consolidated=True, chunks=None) as opened:
        np.testing.assert_array_equal(opened.sq.values[0:12], wide.sq.values)
        assert np.isnan(opened.sq.values[12:]).all()
        np.testing.assert_array_equal(opened.t2m.values, want)
        if fmt == 3:
            assert np.isnan(opened.sq.encoding["_FillValue"])

    np.testing.assert_array_equal(zarr.open_array(str(store / "t2m"))[...], want)
    np.testing.assert_array_equal(
        zarr.open_array(str(store / "sp"))[0:12, 0:37, 0:72], new
    )
    # the consolidated copy of the metadata was rebuilt from the documents now in the store,
    # so a reader that demands it sees the new array
    assert "sp" in set(zarr.open_consolidated(str(store)).array_keys())
    assert "sp" in set(zarr.open_group(str(store), mode="r").array_keys())
    assert version == M.snapshot(spark_delta, table).version
    assert (
        _live(spark_delta, table, origin="gbx") == []
    )  # every overlay row is external now
    assert _live(spark_delta, table, kind="lock") == []
    (row,) = _live(
        spark_delta, table, array="t2m", key=first.relative_to(store / "t2m").as_posix()
    )
    assert row["identity"] == _identity(first)
    shutil.rmtree(tmp_path / "ov" / "data")  # the table no longer needs the overlay
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), want)
    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()


def test_publish_records_the_time_a_volume_settles_on(
    spark_delta, registered, monkeypatch
):
    """A Volume moves an object's time up to the next whole second after the rename has
    returned (measured on DBR 19). The identity a publish records must be that one, or every
    read of what it published raises drift."""
    store, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table)
    real, settled = os.stat, set()

    def late(path):
        st = real(path)
        if (
            path not in settled
        ):  # the first look sees the local time, the file keeps the next
            settled.add(path)
            moved = -(-st.st_mtime_ns // 1_000_000_000) * 1_000_000_000
            os.utime(path, ns=(st.st_atime_ns, moved))
        return st

    monkeypatch.setattr(_sync, "_stat", late)
    publish(spark_delta, table)
    assert settled  # the objects went through the late stat
    got = H.read_variable(spark_delta, table, "t2m")
    np.testing.assert_array_equal(got[12:24, 0:37, 72:144], 3.0)
    assert _changes(gbx_zarr_sync(spark_delta, table)) == set()


@pytest.mark.parametrize(
    "n, stat_ms, early_whole, never",
    [
        (1, 0, False, False),  # the time settles 0.3 s after the rename
        (1, 0, False, True),  # it never does: bounded, and a warning
        (600, 5, False, False),  # a big publish: the first look alone takes 3 s
        (1, 0, True, False),  # a local time that fell on a whole second, then settles
    ],
    ids=["settles", "never-does", "big-batch", "whole-second-before-settling"],
)
def test_a_volume_object_is_looked_at_until_its_time_has_settled(
    monkeypatch, caplog, n, stat_ms, early_whole, never
):
    """On a Volume a time with a fraction is not the final one, nor is a whole second seen
    within a second of the write: those objects are looked at again (only those), for at most
    ``_SETTLE_SECONDS`` after the first look at all of them, then the publish warns."""
    now, looks = [0.0], [0]
    base_s = 1_792_741_018
    paths = [f"/Volumes/c/s/v/store/t2m/c/0/0/{i}" for i in range(n)]
    written = {p: 0.002 * i for i, p in enumerate(paths)}  # renamed 2 ms apart
    delay = 0.8 if n > 1 else 0.3
    settles = {p: None if never else w + delay for p, w in written.items()}

    class Stat:
        def __init__(self, ns):
            self.st_mtime_ns, self.st_size = ns, 21

    def stat(p):
        looks[0] += 1
        now[0] += stat_ms / 1000
        late = settles[p] is not None and now[0] >= settles[p]
        early = base_s * 10**9 + (0 if early_whole else 701_000_000)
        return Stat((base_s + 1) * 10**9 if late else early)

    monkeypatch.setattr(_sync, "_stat", stat)
    monkeypatch.setattr(_sync, "_sleep", lambda s: now.__setitem__(0, now[0] + s))
    monkeypatch.setattr(_sync, "_clock", lambda: now[0])
    monkeypatch.setattr(_sync, "_WRITTEN", dict(written), raising=False)
    with caplog.at_level("WARNING", logger=_sync.__name__):
        got = _sync._settle(paths)
    settled = f"{(base_s + 1) * 1000}:21"
    early = f"{base_s * 1000 + (0 if early_whole else 701)}:21"
    assert set(got.values()) == {early if never else settled}
    assert any("settled" in r.getMessage() for r in caplog.records) == never
    first_pass = n * stat_ms / 1000
    assert now[0] <= 2 * first_pass + _sync._SETTLE_SECONDS + _sync._SETTLE_STEP
    assert (
        looks[0] < 2 * n or n == 1
    )  # objects that had settled are not looked at again


def test_the_settle_bounds_cover_the_slowest_measured_volume(monkeypatch, caplog):
    """A Volume's writer stat settled up to 1.93 s after the rename (p90 about 1.3 s). A whole
    second seen inside that window is not trusted, and the bound outlasts it."""
    assert (_sync._QUIET, _sync._SETTLE_SECONDS) == (2.0, 3.0)
    now, base_s = [0.0], 1_792_741_018
    path = "/Volumes/c/s/v/store/t2m/c/0/0/0"

    class Stat:
        def __init__(self, ns):
            self.st_mtime_ns, self.st_size = ns, 21

    def stat(p):  # a whole second throughout, moved on 1.93 s after the write
        return Stat((base_s + (1 if now[0] >= 1.93 else 0)) * 10**9)

    monkeypatch.setattr(_sync, "_stat", stat)
    monkeypatch.setattr(_sync, "_sleep", lambda s: now.__setitem__(0, now[0] + s))
    monkeypatch.setattr(_sync, "_clock", lambda: now[0])
    monkeypatch.setattr(_sync, "_WRITTEN", {path: 0.0}, raising=False)
    with caplog.at_level("WARNING", logger=_sync.__name__):
        got = _sync._settle([path])
    assert got == {path: f"{(base_s + 1) * 1000}:21"}
    assert not [r for r in caplog.records if "settled" in r.getMessage()]


def test_only_the_objects_a_publish_settles_are_remembered(tmp_path, monkeypatch):
    """The write times kept for the settle are for the objects a publish records an identity
    for: a document (or a reference file) is never kept, an object is forgotten once settled,
    and a long-lived driver evicts the oldest entries instead of growing."""
    now = [0.0]
    monkeypatch.setattr(
        _sync, "_VOLUMES", str(tmp_path) + "/"
    )  # these paths count as a Volume
    monkeypatch.setattr(_sync, "_WRITTEN", {})
    monkeypatch.setattr(_sync, "_WRITTEN_MAX", 4)
    monkeypatch.setattr(_sync, "_clock", lambda: now[0])
    monkeypatch.setattr(_sync, "_sleep", lambda s: now.__setitem__(0, now[0] + s))
    _sync._atomic_write(str(tmp_path / "zarr.json"), [b"{}"], "w1")  # a document
    assert _sync._WRITTEN == {}
    objects = [str(tmp_path / "t2m" / "c" / str(i)) for i in range(6)]
    for o in objects:
        _sync._atomic_write(o, [b"x"], "w1", track=True)
    assert list(_sync._WRITTEN) == objects[2:]  # the two oldest were evicted
    _sync._settle(objects[2:])  # (a local file never shows a whole second: it warns)
    assert _sync._WRITTEN == {}


def test_publish_refuses_after_drift(spark_delta, registered):
    store, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table)
    before = zarr.open_array(str(store / "t2m"), mode="r")[...]
    zarr.open_array(str(store / "u10"), mode="r+")[0:12, 0:37, 0:72] = 2.5
    with pytest.raises(ZarrStoreDrift, match="changed since the last sync") as e:
        publish(spark_delta, table)
    assert "gbx_zarr_sync" in e.value.fix
    np.testing.assert_array_equal(zarr.open_array(str(store / "t2m"))[...], before)
    assert _live(spark_delta, table, kind="lock") == []  # the lock is not left behind
    gbx_zarr_sync(spark_delta, table)  # what the Fix says, then it goes through
    publish(spark_delta, table)
    assert _live(spark_delta, table, origin="gbx") == []


def test_concurrent_publish_conflicts(spark_delta, registered, monkeypatch):
    _, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table)
    holding, release, out = threading.Event(), threading.Event(), {}

    def hold():
        holding.set()
        assert release.wait(120)

    def first():
        try:
            out["version"] = publish(spark_delta, table)
        except BaseException as e:  # noqa: BLE001 - reported by the main thread
            out["error"] = e

    monkeypatch.setattr(_sync, "_AFTER_LOCK", hold)
    thread = threading.Thread(target=first)
    thread.start()
    try:
        assert holding.wait(120)
        monkeypatch.setattr(_sync, "_AFTER_LOCK", None)
        with pytest.raises(ZarrWriteConflict, match="publish") as e:
            publish(spark_delta, table)
        assert "Fix:" in str(e.value)
    finally:
        release.set()
        thread.join(300)
    assert "error" not in out and out["version"]
    assert _live(spark_delta, table, kind="lock") == []


def _publish_whose_commit_meets(spark, table, monkeypatch, landing):
    """Run ``publish`` with ``landing()`` happening after its files are in the store and
    before its commit: the way a write that reads the overlay before the publish does.
    """
    original, fired = _sync._restamp, []

    def land_first(rows, base):
        if not fired:
            fired.append(1)
            landing()
        return original(rows, base)

    monkeypatch.setattr(_sync, "_restamp", land_first)
    return publish(spark, table)


def test_a_write_that_lands_during_a_publish_fails_it_with_a_publish_fix(
    spark_delta, registered, monkeypatch
):
    """The generic fix of a conflicting commit ("re-read ... and write again") is wrong for a
    publish: its files are already in the store, so it says to sync and publish again.
    """
    store, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table, value=3.0)
    with pytest.raises(ZarrWriteConflict) as e:
        _publish_whose_commit_meets(
            spark_delta,
            table,
            monkeypatch,
            lambda: _overlay_write(spark_delta, table, value=5.0),
        )
    err = e.value
    assert "already wrote" in err.fix and "in the store" in err.fix
    assert f"gbx_zarr_sync({table!r})" in err.fix and "publish again" in err.fix
    assert "write again" not in err.fix
    assert err.keys and err.winner  # what the conflicting commit told, kept
    assert _live(spark_delta, table, kind="lock") == []  # the lock is released
    monkeypatch.undo()
    gbx_zarr_sync(spark_delta, table)  # what the Fix says, then it goes through
    publish(spark_delta, table)
    np.testing.assert_array_equal(
        zarr.open_array(str(store / "t2m"))[12:24, 0:37, 72:144], 5.0
    )


def test_a_delta_rejection_of_the_publish_commit_is_a_zarr_error_with_the_publish_fix(
    spark_delta, registered, monkeypatch
):
    """``commit`` raises ``ConcurrentCommit`` (not a ``ZarrError``) when Delta rejects the
    MERGE of a writer that really raced it; a publish says what to do about it."""
    _, table = registered(sharded=True, writable=True)
    _overlay_write(spark_delta, table)
    real = M.commit

    def rejected(spark, name, rows, *, write_id, meta, **kw):
        if meta.get("mode") == "publish":
            raise M.ConcurrentCommit("Delta reported ConcurrentAppendException")
        return real(spark, name, rows, write_id=write_id, meta=meta, **kw)

    monkeypatch.setattr(M, "commit", rejected)
    with pytest.raises(ZarrWriteConflict) as e:
        publish(spark_delta, table)
    assert "already wrote" in e.value.fix and "publish again" in e.value.fix
    assert "ConcurrentAppendException" in str(e.value)
    assert _live(spark_delta, table, kind="lock") == []
