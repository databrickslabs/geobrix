"""Drift, ``gbx_zarr_sync`` and publish: a registered store other writers change.

The read-time check is Spark-free (a plain store's files as manifest rows). Sync and publish
run on local OSS Delta, where the store is changed the way a vendor would change it: with
``zarr`` itself, behind the table's back.
"""

import json
import os
import shutil
import threading
import uuid

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _manifest as M  # noqa: E402
from databricks.labs.gbx.zarr import _sync  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    gbx_zarr_register,
    identity_of,
)
from databricks.labs.gbx.zarr._sync import gbx_zarr_sync, publish  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    make_era5_like,
    plain_store_rows,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
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
    want = original.copy()
    want[12:24, 0:37, 72:144] = 3.0

    version = publish(spark_delta, table)

    np.testing.assert_array_equal(zarr.open_array(str(store / "t2m"))[...], want)
    np.testing.assert_array_equal(
        zarr.open_array(str(store / "sp"))[0:12, 0:37, 0:72], new
    )
    # the consolidated copy of the metadata would be stale: it is gone, and a plain open
    # of the group sees the new array
    if fmt == 3:
        assert "consolidated_metadata" not in json.loads(
            (store / "zarr.json").read_text()
        )
    else:
        assert not (store / ".zmetadata").exists()
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
