"""Maintenance of a Zarr store: optimize, vacuum, tags, history and restore.

These run on local OSS Delta. Time is injected (``_maint._utcnow``) so a vacuum can be
asked what it would do a week from now; the Delta history keeps its real timestamps.
"""

import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")
xr = pytest.importorskip("xarray")

from test.zarr import _helpers as H  # noqa: E402

from databricks.labs.gbx.zarr import _maint  # noqa: E402
from databricks.labs.gbx.zarr import _write  # noqa: E402
from databricks.labs.gbx.zarr import _manifest as M  # noqa: E402
from databricks.labs.gbx.zarr._maint import (  # noqa: E402
    gbx_zarr_history,
    gbx_zarr_optimize,
    gbx_zarr_restore,
    gbx_zarr_tag,
    gbx_zarr_untag,
    gbx_zarr_vacuum,
)
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_MARKER,
    gbx_zarr_register,
)
from databricks.labs.gbx.zarr._sync import gbx_zarr_sync, publish  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    era5_like_dataset,
    make_era5_like,
)
from databricks.labs.gbx.zarr._write import gbx_zarr_write  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrOwnershipError,
    ZarrVersionUnavailable,
)

CHUNKS = {"time": 12, "latitude": 5, "longitude": 6}
SHARDS = {"time": 24, "latitude": 10, "longitude": 12}  # two shards per variable
CHUNK = (12, 5, 6)
WHOLE = (12, 37, 72)  # one inner chunk of the registered cube


def _table(label):
    return f"gbx_zarr_maint_{label}_{uuid.uuid4().hex[:8]}"


def _cube():
    """48 hours on a 10 x 12 corner of the ERA5-like cube."""
    return era5_like_dataset().isel(latitude=slice(0, 10), longitude=slice(0, 12))


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


def _store(spark, drop, tmp_path, ds=None, label="s", **kw):
    """A managed store of the small cube: ``(table, root, version)``."""
    table, root = _table(label), tmp_path / f"root_{label}"
    drop(table)
    kw.setdefault("chunks", CHUNKS)
    kw.setdefault("shards", SHARDS)
    version = gbx_zarr_write(
        _cube() if ds is None else ds, table, root=str(root), spark=spark, **kw
    )
    return table, root, version


def _registered(spark, drop, tmp_path, label="r"):
    store = make_era5_like(tmp_path / f"{label}.zarr", zarr_format=3)
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


def _blocks(spark, *blocks):
    return H.blocks_frame(spark, *blocks)


def _fill_block(spark, start=(0, 0, 0), shape=CHUNK, value=np.nan, name="t2m"):
    return _blocks(spark, (name, start, np.full(shape, value, dtype="float32")))


def _live(spark, table, kind, array="t2m"):
    return spark.sql(
        f"SELECT * FROM {table} WHERE kind = '{kind}' AND array = '{array}' "
        "AND tag IS NULL ORDER BY key"
    ).collect()


def _keys(rows):
    return [r["key"] for r in rows]


def _latest(spark, table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def _commit_meta(spark, table, write_id):
    row = spark.sql(
        f"SELECT meta FROM {table} WHERE kind = 'commit' AND key = '{write_id}'"
    ).first()
    return json.loads(row["meta"])


def _hashes(root):
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _at(monkeypatch, delta):
    """Make the vacuum believe it is ``delta`` from now."""
    when = datetime.now(timezone.utc) + delta
    monkeypatch.setattr(_maint, "_utcnow", lambda: when)


def _reasons(frame):
    return {r["file"]: (r["action"], r["reason"]) for r in frame.collect()}


def _by_array(reasons, array):
    return {f: v for f, v in reasons.items() if f.startswith(f"data/{array}/")}


# --- optimize ----------------------------------------------------------------------------


def test_optimize_folds_chunk_rows_and_reads_same(
    spark_delta, drop, tmp_path, no_hooks
):
    table, root, _ = _store(spark_delta, drop, tmp_path)
    want = {n: H.read_variable(spark_delta, table, n) for n in ("t2m", "u10")}
    assert len(_live(spark_delta, table, "chunk")) == 16
    assert _live(spark_delta, table, "shard") == []

    with pytest.raises(ValueError, match="not arrays of this store"):
        gbx_zarr_optimize(spark_delta, table, variables=["nope"])
    gbx_zarr_optimize(spark_delta, table, variables="u10")  # only the one asked for
    assert len(_live(spark_delta, table, "chunk", "u10")) == 0
    assert len(_live(spark_delta, table, "chunk", "t2m")) == 16
    version = gbx_zarr_optimize(spark_delta, table)

    for name in ("t2m", "u10"):
        assert _live(spark_delta, table, "chunk", name) == []
        shards = _live(spark_delta, table, "shard", name)
        assert _keys(shards) == ["c/0/0/0", "c/1/0/0"]
        for row in shards:
            assert (row["origin"], row["shard_key"]) == ("gbx", row["key"])
            assert row["write_id"] != ""
            assert os.path.getsize(root / row["file"]) >= row["offset"] + row["length"]
            assert len(row["checksum"]) == 8
        np.testing.assert_array_equal(
            H.read_variable(spark_delta, table, name), want[name]
        )
    # coordinates are one unsharded chunk each: nothing to fold
    assert len(_live(spark_delta, table, "chunk", "time")) == 1
    meta = _commit_meta(
        spark_delta, table, _live(spark_delta, table, "shard")[0]["write_id"]
    )
    assert (meta["mode"], meta["variables"], meta["shards"]) == ("optimize", ["t2m"], 2)
    assert meta["folded"] == 16

    again = gbx_zarr_optimize(spark_delta, table)  # nothing left to fold
    assert again == version == _latest(spark_delta, table)


def test_optimize_folds_tombstones_and_overlay_shards(
    spark_delta, drop, tmp_path, no_hooks
):
    """A registered store: the folded shard replaces the base shard in the overlay (it keeps
    the base object's identity), chunk tombstones are folded away, packs respect
    ``target_file_mb``, and sync and publish treat the result like any overlay shard."""
    store, table = _registered(spark_delta, drop, tmp_path)
    base = H.direct(store, "t2m")
    pack_dir = tmp_path / "ov_r"
    for start, value in (((0, 0, 0), 3.0), ((24, 0, 0), 4.0)):
        gbx_zarr_write(
            _blocks(
                spark_delta, ("t2m", start, np.full(WHOLE, value, dtype="float32"))
            ),
            table,
            mode="update",
        )
    gbx_zarr_write(_fill_block(spark_delta, (12, 0, 72), WHOLE), table, mode="update")
    tombs = [r for r in _live(spark_delta, table, "chunk") if r["file"] is None]
    assert len(tombs) == 1  # the fill chunk under a live base shard
    want = H.read_variable(spark_delta, table, "t2m")
    ids = {
        r["key"]: r["identity"]
        for r in _live(spark_delta, table, "shard")
        if r["origin"] == "external"
    }

    gbx_zarr_optimize(spark_delta, table, target_file_mb=1)

    assert _live(spark_delta, table, "chunk") == []
    shards = _live(spark_delta, table, "shard")
    assert [(r["key"], r["origin"], r["identity"]) for r in shards] == [
        (k, "gbx", ids[k]) for k in ("c/0/0/0", "c/1/0/0")
    ]
    assert len({r["file"] for r in shards}) == 2  # each shard is about 1 MB: two packs
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), want)
    assert (want[0:12, 0:37, 0:72] == 3.0).all() and np.isnan(
        want[12:24, 0:37, 72:144]
    ).all()
    np.testing.assert_array_equal(
        want[12:24, :, 0:72], base[12:24, :, 0:72]
    )  # untouched cells come from the base shard

    assert gbx_zarr_sync(spark_delta, table).collect() == []  # base unmoved: no change
    publish(spark_delta, table)
    published = H.direct(store, "t2m")
    np.testing.assert_array_equal(published[0:12, 0:37, 0:72], want[0:12, 0:37, 0:72])
    assert np.isnan(published[12:24, 0:37, 72:144]).all()
    assert (published[24:36, 0:37, 0:72] == 4.0).all()
    assert pack_dir.exists()


def test_write_optimize_true_folds_touched_shards(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _store(spark_delta, drop, tmp_path)
    wid = uuid.uuid4().hex
    block = np.full(CHUNK, 7.0, dtype="float32")
    version = gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), block)),
        table,
        mode="update",
        write_id=wid,
        optimize=True,
    )
    assert version == M.find_commit_version(spark_delta, table, wid)
    assert _keys(_live(spark_delta, table, "shard")) == ["c/0/0/0"]
    rest = _live(spark_delta, table, "chunk")  # t2m's other shard keeps its chunk rows
    assert {r["shard_key"] for r in rest} == {"c/1/0/0"} and len(rest) == 8
    assert len(_live(spark_delta, table, "chunk", "u10")) == 16
    got = H.read_variable(spark_delta, table, "t2m")
    assert (got[0:12, 0:5, 0:6] == 7.0).all()


def test_a_failed_fold_after_a_write_says_the_write_committed(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _store(spark_delta, drop, tmp_path)
    wid = uuid.uuid4().hex

    def broken(*args, **kw):
        raise ZarrError("a pack file could not be written", fix="free some space")

    no_hooks.setattr(_maint, "_optimize", broken)
    block = np.full(CHUNK, 7.0, dtype="float32")
    with pytest.raises(ZarrError, match="the write committed") as e:
        gbx_zarr_write(
            _blocks(spark_delta, ("t2m", (0, 0, 0), block)),
            table,
            mode="update",
            write_id=wid,
            optimize=True,
        )
    assert "free some space" in e.value.fix and "gbx_zarr_optimize" in e.value.fix
    landed = M.find_commit_version(spark_delta, table, wid)
    assert (
        landed is not None
    )  # the write stands; a retry with the same id repeats nothing
    assert (H.read_variable(spark_delta, table, "t2m")[0:12, 0:5, 0:6] == 7.0).all()


def test_optimize_skips_a_shard_that_changed_while_it_ran(
    spark_delta, drop, tmp_path, no_hooks, caplog
):
    """Another write replaces a chunk of one shard between the fold's read and its commit:
    that shard is left as it is (and reported), the other folds."""
    table, _, _ = _store(spark_delta, drop, tmp_path)
    real, state = M.commit, {"done": False}

    def interfere(spark, name, rows, **kw):
        if kw["meta"].get("mode") == "optimize" and not state["done"]:
            state["done"] = True
            gbx_zarr_write(_fill_block(spark, value=8.0), name, mode="update")
        return real(spark, name, rows, **kw)

    no_hooks.setattr(M, "commit", interfere)
    with caplog.at_level("WARNING"):
        gbx_zarr_optimize(spark_delta, table, variables="t2m")
    assert state["done"]
    assert [r["key"] for r in _live(spark_delta, table, "shard")] == ["c/1/0/0"]
    left = _live(spark_delta, table, "chunk")
    assert {r["shard_key"] for r in left} == {"c/0/0/0"} and len(left) == 8
    assert "skipped 1 shard" in caplog.text and "t2m/c/0/0/0" in caplog.text
    got = H.read_variable(spark_delta, table, "t2m")
    assert (got[0:12, 0:5, 0:6] == 8.0).all()  # the concurrent write is not lost
    no_hooks.setattr(M, "commit", real)
    gbx_zarr_optimize(spark_delta, table, variables="t2m")  # now it folds
    assert _live(spark_delta, table, "chunk") == []


@pytest.mark.parametrize("kind", ["managed", "registered"])
def test_optimize_of_a_shard_left_empty(spark_delta, drop, tmp_path, no_hooks, kind):
    """Every chunk of a shard written as fill: nothing is left to fold. A managed store
    drops the shard; a registered one keeps a tombstone that remembers the base shard, so a
    sync does not bring its values back."""
    if kind == "managed":
        table, _, _ = _store(spark_delta, drop, tmp_path)
        shape, name = (24, 10, 12), "t2m"
        gbx_zarr_optimize(spark_delta, table)
    else:
        store, table = _registered(spark_delta, drop, tmp_path)
        shape, name = (24, 73, 144), "t2m"
        ident = _live(spark_delta, table, "shard")[0]["identity"]
    gbx_zarr_write(_fill_block(spark_delta, (0, 0, 0), shape), table, mode="update")
    assert (
        len([r for r in _live(spark_delta, table, "chunk") if r["file"] is None]) == 8
    )

    gbx_zarr_optimize(spark_delta, table)

    assert _live(spark_delta, table, "chunk") == []
    first = [r for r in _live(spark_delta, table, "shard") if r["key"] == "c/0/0/0"]
    if kind == "managed":
        assert first == []
    else:
        (tomb,) = first
        assert (tomb["file"], tomb["length"], tomb["origin"], tomb["identity"]) == (
            None,
            0,
            "gbx",
            ident,
        )
        assert gbx_zarr_sync(spark_delta, table).collect() == []
    got = H.read_variable(spark_delta, table, name)
    assert np.isnan(got[0:24]).all() and not np.isnan(got[24:]).any()


# --- vacuum, tags -----------------------------------------------------------------------


@pytest.fixture
def layered(spark_delta, drop, tmp_path, no_hooks):
    """Three generations of ``t2m``: ``v1`` created, ``v2`` and ``v3`` overwrite it, and a
    tag ``keep`` on ``v2`` made after ``v3``. Returns
    ``SimpleNamespace(table, root, w1, v1, v2, v3, values)``."""
    from types import SimpleNamespace

    w1 = uuid.uuid4().hex
    table, root, v1 = _store(spark_delta, drop, tmp_path, label="lay", write_id=w1)
    values, versions = {}, [v1]
    for generation in (2, 3):
        new = np.full((48, 10, 12), float(generation), dtype="float32")
        versions.append(
            gbx_zarr_write(
                _blocks(spark_delta, ("t2m", (0, 0, 0), new)), table, mode="overwrite"
            )
        )
        values[generation] = new
    gbx_zarr_tag(spark_delta, table, "keep", version_as_of=versions[1])
    return SimpleNamespace(
        table=table,
        root=root,
        w1=w1,
        v1=versions[0],
        v2=versions[1],
        v3=versions[2],
        values=values,
    )


def _orphan(layered):
    """A pack file of a write that never committed (a crash after its files, say)."""
    folder = layered.root / "data" / "t2m" / uuid.uuid4().hex
    folder.mkdir(parents=True)
    (folder / "part-0.zpack").write_bytes(b"orphan")
    return f"data/t2m/{folder.name}/part-0.zpack"


def test_vacuum_keeps_live_tagged_and_retained(spark_delta, layered):
    orphan = _orphan(layered)
    got = _reasons(gbx_zarr_vacuum(spark_delta, layered.table, retain="10 days"))
    assert got.pop(orphan) == ("kept", "recent")  # young: its write may be in flight
    props = M._tblprops(spark_delta, layered.table)  # nothing went: no horizon, but the
    assert M.HORIZON_PROP not in props  # Delta retention follows the retain used
    assert props["delta.deletedFileRetentionDuration"] == "interval 10 days"
    kept = _by_array(got, "t2m")
    by_reason = {}
    for file, (action, reason) in kept.items():
        assert action == "kept"
        by_reason.setdefault(reason, set()).add(file.split("/")[2])  # the write_id
    assert sorted(by_reason) == ["live", "retained", "tagged"]
    assert all(a == "kept" for a, _ in got.values())  # nothing is a week old yet
    assert {len(v) for v in by_reason.values()} == {1}  # one write per generation


def test_vacuum_deletes_superseded_after_retain(spark_delta, layered, monkeypatch):
    root, table = layered.root, layered.table
    first = {f for f in _hashes(root) if f.startswith(f"data/t2m/{layered.w1}/")}
    _at(monkeypatch, timedelta(days=8))

    orphan = _orphan(layered)
    before = {f for f in _hashes(root) if f.startswith("data/")}
    dry = _reasons(gbx_zarr_vacuum(spark_delta, table, dry_run=True))
    assert dry.pop(orphan) == ("would_delete", "uncommitted")
    doomed = {f for f, (a, r) in dry.items() if a == "would_delete"}
    assert doomed == first and {dry[f][1] for f in doomed} == {"superseded"}
    assert {f for f in _hashes(root) if f.startswith("data/")} == before  # dry run
    assert M._tblprops(spark_delta, table).get(M.HORIZON_PROP) is None

    done = _reasons(gbx_zarr_vacuum(spark_delta, table))
    assert {f for f, (a, _) in done.items() if a == "deleted"} == doomed | {orphan}
    assert {f for f in _hashes(root) if f.startswith("data/")} == before - doomed - {
        orphan
    }
    assert not (root / "data" / "t2m" / layered.w1).exists()  # emptied folders go too

    # the first generation is gone for good; the commit rows stay, so a replayed write is
    # still recognised and applies nothing twice
    assert M._tblprops(spark_delta, table)[M.HORIZON_PROP]
    with pytest.raises(ZarrVersionUnavailable):
        M.snapshot(spark_delta, table, version_as_of=layered.v1)
    latest = _latest(spark_delta, table)
    replay = gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), layered.values[2])),
        table,
        mode="overwrite",
        write_id=layered.w1,
    )
    assert replay == layered.v1 and _latest(spark_delta, table) == latest
    assert (H.read_variable(spark_delta, table, "t2m") == 3.0).all()


def _micros(spark, table, version):
    from pyspark.sql import functions as F

    return (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .where(f"version = {version}")
        .select(F.unix_micros("timestamp"))
        .first()[0]
    )


def test_vacuum_keeps_what_the_version_at_the_horizon_still_needs(
    spark_delta, drop, tmp_path, no_hooks, monkeypatch
):
    """``t2m`` is written at v1 and left alone by v2 (which overwrites ``u10``), then
    overwritten at v3. A vacuum whose horizon falls between v2 and v3 keeps v2 readable, so
    the ``t2m`` files only v1 and v2 refer to stay; the ``u10`` files of v1 go."""
    table, root, v1 = _store(spark_delta, drop, tmp_path, label="hz")
    new = np.full((48, 10, 12), 2.0, dtype="float32")
    v2 = gbx_zarr_write(
        _blocks(spark_delta, ("u10", (0, 0, 0), new)), table, mode="overwrite"
    )
    v3 = gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), new)), table, mode="overwrite"
    )
    w1 = _first_write(spark_delta, table, v1)
    t2, t3 = (_micros(spark_delta, table, v) for v in (v2, v3))
    assert t3 > t2
    between = datetime.fromtimestamp((t2 + t3) / 2 / 1e6, timezone.utc)
    monkeypatch.setattr(_maint, "_utcnow", lambda: between + timedelta(days=7))

    got = _reasons(gbx_zarr_vacuum(spark_delta, table, dry_run=True))
    assert {
        a for f, (a, _) in _by_array(got, "t2m").items() if f.split("/")[2] == w1
    } == {"kept"}
    assert {v for f, v in _by_array(got, "t2m").items() if f.split("/")[2] == w1} == {
        ("kept", "retained")
    }
    assert {v for f, v in _by_array(got, "u10").items() if f.split("/")[2] == w1} == {
        ("would_delete", "superseded")
    }
    M.snapshot(spark_delta, table, version_as_of=v2)  # still readable after the vacuum
    gbx_zarr_vacuum(spark_delta, table)
    np.testing.assert_array_equal(
        H.read_variable(spark_delta, table, "t2m", version_as_of=v2), _cube().t2m.values
    )
    with pytest.raises(ZarrVersionUnavailable):
        M.snapshot(spark_delta, table, version_as_of=v1)


def test_tag_survives_vacuum_and_reads_by_tag(spark_delta, layered, monkeypatch):
    table = layered.table
    _at(monkeypatch, timedelta(days=30))
    gbx_zarr_vacuum(spark_delta, table)
    for ref, expect in (("tag:keep", 2.0), (None, 3.0)):
        got = H.read_variable(spark_delta, table, "t2m", version_as_of=ref)
        assert (got == expect).all(), ref

    gbx_zarr_tag(spark_delta, table, "latest")
    with pytest.raises(ValueError, match="already exists"):
        gbx_zarr_tag(spark_delta, table, "latest")
    gbx_zarr_untag(spark_delta, table, "keep")
    with pytest.raises(ZarrVersionUnavailable):
        M.snapshot(spark_delta, table, version_as_of="tag:keep")
    with pytest.raises(ZarrVersionUnavailable):
        gbx_zarr_untag(spark_delta, table, "keep")
    dry = _reasons(gbx_zarr_vacuum(spark_delta, table, dry_run=True))
    assert any(a == "would_delete" for a, _ in dry.values())  # nothing holds them now


@pytest.mark.parametrize("case", ["registered", "foreign-owner", "read-only"])
def test_vacuum_never_touches_base_or_unowned_root(
    spark_delta, drop, tmp_path, no_hooks, monkeypatch, case
):
    _at(monkeypatch, timedelta(days=60))
    if case == "foreign-owner":
        table, root, _ = _store(spark_delta, drop, tmp_path)
        marker = root / OWNER_MARKER
        doc = json.loads(marker.read_text())
        marker.write_text(json.dumps({**doc, "table": "main.other.table"}))
        with pytest.raises(ZarrOwnershipError):
            gbx_zarr_vacuum(spark_delta, table)
        assert any((root / "data").rglob("*.zpack"))
        return
    store = make_era5_like(tmp_path / "base.zarr", zarr_format=3)
    table = _table(case.replace("-", "_"))
    drop(table)
    kw = {"writable": True, "overlay_root": str(tmp_path / "ov")}
    gbx_zarr_register(
        spark_delta, str(store), table, **(kw if case == "registered" else {})
    )
    base = _hashes(store)
    if case == "registered":
        for value in (3.0, 4.0):  # the second write supersedes the first one's chunk
            gbx_zarr_write(
                _blocks(
                    spark_delta, ("t2m", (0, 0, 0), np.full(WHOLE, value, "float32"))
                ),
                table,
                mode="update",
            )
    got = _reasons(gbx_zarr_vacuum(spark_delta, table))
    assert _hashes(store) == base
    if case == "read-only":
        assert got == {}  # a read-only registration owns no files
    else:
        assert {r for _, r in got.values()} >= {"live"}
        assert sum(a == "deleted" for a, _ in got.values()) == 1  # the superseded chunk
        assert _hashes(tmp_path / "ov").keys() >= {OWNER_MARKER}


def test_vacuum_retain_floor_and_malformed_retain(
    spark_delta, drop, tmp_path, no_hooks
):
    table, _, _ = _store(spark_delta, drop, tmp_path)
    with pytest.raises(ValueError, match="1 hour"):
        gbx_zarr_vacuum(spark_delta, table, retain="59 minutes")
    short = gbx_zarr_vacuum(
        spark_delta, table, retain="59 minutes", force=True, dry_run=True
    )
    assert short.columns == ["file", "action", "reason"]
    spark_delta.sql(
        f"ALTER TABLE {table} SET TBLPROPERTIES ('geobrix.zarr.retain' = 'soon')"
    )
    with pytest.raises(ZarrError, match="not a duration"):
        gbx_zarr_vacuum(spark_delta, table)


def test_vacuum_of_a_file_mode_managed_store_runs_delta_vacuum(
    spark_delta, drop, tmp_path
):
    """Ruling Z10: the retention property is set, then a plain VACUUM runs (never a RETAIN
    clause, so retentionDurationCheck is never touched)."""
    table = _table("fm")
    drop(table)
    M.create_manifest(
        spark_delta,
        table,
        M.StoreProps("managed", str(tmp_path / "fm"), None, True, "managed", "7 days"),
    )
    rows = gbx_zarr_vacuum(spark_delta, table, retain="8 days").collect()
    props = M._tblprops(spark_delta, table)
    assert props["delta.deletedFileRetentionDuration"] == "interval 8 days"
    assert props[M.HORIZON_PROP]
    assert {r["reason"] for r in rows} <= {"delta"}


# --- history -----------------------------------------------------------------------------


def test_history_lists_versions_with_modes(spark_delta, drop, tmp_path, no_hooks):
    ds = _cube()
    wid = uuid.uuid4().hex
    table, _, v1 = _store(
        spark_delta, drop, tmp_path, ds.isel(time=slice(0, 24)), write_id=wid
    )
    gbx_zarr_write(ds.isel(time=slice(24, 48)), table, mode="append", spark=spark_delta)
    gbx_zarr_write(
        _fill_block(spark_delta, value=5.0), table, mode="update", spark=spark_delta
    )
    gbx_zarr_optimize(spark_delta, table)
    gbx_zarr_tag(spark_delta, table, "t")
    gbx_zarr_untag(spark_delta, table, "t")
    restored = gbx_zarr_restore(spark_delta, table, version_as_of=v1)

    frame = gbx_zarr_history(spark_delta, table)
    assert frame.columns == [
        "version",
        "timestamp",
        "user",
        "mode",
        "write_id",
        "variables",
        "region",
    ]
    rows = frame.collect()
    assert [r["mode"] for r in rows] == [
        "create",
        "append",
        "update",
        "optimize",
        "tag",
        "untag",
        "restore",
    ]
    versions = [r["version"] for r in rows]
    assert (
        versions == sorted(versions) and versions[0] == v1 and versions[-1] == restored
    )
    assert rows[0]["write_id"] == wid and rows[0]["timestamp"] is not None
    assert {"t2m", "u10"} <= set(rows[0]["variables"])
    assert rows[1]["region"]["time"] == [24, 48]  # the appended steps
    assert rows[2]["variables"] == ["t2m"] and rows[2]["region"]["time"] == [0, 12]


# --- restore -----------------------------------------------------------------------------


def test_restore_round_trip(spark_delta, drop, tmp_path, no_hooks):
    table, _, v1 = _store(spark_delta, drop, tmp_path)
    first = H.read_variable(spark_delta, table, "t2m")
    v2 = gbx_zarr_write(
        _fill_block(spark_delta, value=9.0), table, mode="update", spark=spark_delta
    )
    second = H.read_variable(spark_delta, table, "t2m")
    assert not np.array_equal(first, second)

    with pytest.raises(ValueError, match="version_as_of or timestamp_as_of"):
        gbx_zarr_restore(spark_delta, table)
    back = gbx_zarr_restore(spark_delta, table, version_as_of=v1)
    assert back > v2
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), first)
    row = spark_delta.sql(
        f"SELECT key FROM {table} WHERE kind = 'commit' AND meta LIKE '%\"restore\"%'"
    ).first()
    assert _commit_meta(spark_delta, table, row["key"])["from"] == v1
    assert (
        gbx_zarr_restore(spark_delta, table, version_as_of=v1) == back
    )  # already there

    stamp = (
        spark_delta.sql(f"DESCRIBE HISTORY {table}")
        .where(f"version = {v2}")
        .first()["timestamp"]
    )
    gbx_zarr_restore(spark_delta, table, timestamp_as_of=stamp)  # forward again
    np.testing.assert_array_equal(H.read_variable(spark_delta, table, "t2m"), second)


@pytest.mark.parametrize("to", ["before", "after"])
def test_restore_across_a_tombstone(spark_delta, drop, tmp_path, no_hooks, to):
    """A chunk of fill over a live base shard is a tombstone. A restore to a version from
    before it brings the chunk back; one to a version after keeps it hidden."""
    store, table = _registered(spark_delta, drop, tmp_path)
    base = H.direct(store, "t2m")
    before = M.snapshot(spark_delta, table).version
    after = gbx_zarr_write(
        _fill_block(spark_delta, (0, 0, 0), WHOLE), table, mode="update"
    )
    if to == "before":
        gbx_zarr_restore(spark_delta, table, version_as_of=before)
        got = H.read_variable(spark_delta, table, "t2m")
        np.testing.assert_array_equal(got[0:12, 0:37, 0:72], base[0:12, 0:37, 0:72])
        assert [
            r for r in _live(spark_delta, table, "chunk") if r["file"] is None
        ] == []
    else:
        gbx_zarr_restore(spark_delta, table, version_as_of=before)
        gbx_zarr_restore(spark_delta, table, version_as_of=after)
        got = H.read_variable(spark_delta, table, "t2m")
        assert np.isnan(got[0:12, 0:37, 0:72]).all()
        (tomb,) = [r for r in _live(spark_delta, table, "chunk") if r["file"] is None]
        assert (tomb["length"], tomb["origin"]) == (0, "gbx")


@pytest.mark.parametrize("how", ["vacuumed", "files-gone"])
def test_restore_refuses_vacuumed_version(
    spark_delta, drop, tmp_path, no_hooks, monkeypatch, how
):
    table, root, v1 = _store(spark_delta, drop, tmp_path)
    gbx_zarr_write(
        _blocks(spark_delta, ("t2m", (0, 0, 0), np.full((48, 10, 12), 2.0, "float32"))),
        table,
        mode="overwrite",
    )
    if how == "vacuumed":
        _at(monkeypatch, timedelta(days=30))
        gbx_zarr_vacuum(spark_delta, table)
    else:  # the files vanished behind GeoBrix's back
        for pack in (root / "data" / "t2m").rglob("*.zpack"):
            if pack.parent.name == _first_write(spark_delta, table, v1):
                pack.unlink()
    latest = _latest(spark_delta, table)
    with pytest.raises(ZarrVersionUnavailable) as e:
        gbx_zarr_restore(spark_delta, table, version_as_of=v1)
    assert "later version or" in e.value.fix and "tag" in e.value.fix
    assert _latest(spark_delta, table) == latest  # nothing was committed
    np.testing.assert_array_equal(
        H.read_variable(spark_delta, table, "t2m"),
        np.full((48, 10, 12), 2.0, "float32"),
    )


def _first_write(spark, table, version):
    return spark.sql(
        f"SELECT key FROM {table} WHERE kind = 'commit' AND "
        f"_metadata.row_commit_version = {version}"
    ).first()["key"]
