"""``gbx_zarr_register``: a manifest over a store's own objects, nothing copied."""

import json
import logging
import os
import shutil
import threading
import types
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.core import _retry  # noqa: E402
from databricks.labs.gbx.zarr import _register  # noqa: E402
from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._manifest import (  # noqa: E402
    read_props,
    read_rows,
    snapshot,
)
from databricks.labs.gbx.zarr._meta import parse_v2, parse_v3  # noqa: E402
from databricks.labs.gbx.zarr._register import (  # noqa: E402
    OWNER_LEDGER,
    OWNER_MARKER,
    _claim_marker,
    gbx_zarr_register,
    list_objects,
    read_base_metadata,
    write_owner_marker,
)
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrOwnershipError,
    ZarrUnsupported,
)

ERA5_ARRAYS = {"t2m", "u10", "time", "latitude", "longitude"}


def _table(label):
    return f"gbx_zarr_reg_{label}_{uuid.uuid4().hex[:8]}"


def _exists(spark, table):
    return spark.catalog.tableExists(table)


def _live(spark, table, **kw):
    return read_rows(spark, snapshot(spark, table), **kw)


def _store_key(array, key):
    return "/".join(p for p in (array, key) if p)


def _commit_meta(spark, table):
    (row,) = _live(spark, table, kinds="commit").to_pylist()
    return json.loads(row["meta"])


def _open_store(spark, table, base, scratch):
    """A ``ManifestStore`` made only from what the manifest holds (no look at ``base``).

    Arrays GeoBrix cannot decode are left out of ``arrays`` and named in ``unsupported``,
    as a reader does: the store then refuses their chunk keys instead of reading fill.
    """
    rows = _live(spark, table).to_pylist()
    metas = {
        _store_key(r["array"], r["key"]): r["meta"].encode()
        for r in rows
        if r["kind"] in ("group", "array")
    }
    arrays, unsupported = {}, {}
    for key, raw in metas.items():
        prefix, _, name = key.rpartition("/")
        if not prefix:
            continue
        try:
            if name == "zarr.json" and json.loads(raw).get("node_type") == "array":
                arrays[prefix] = parse_v3(prefix, json.loads(raw))
            elif name == ".zarray":
                zattrs = json.loads(metas.get(f"{prefix}/.zattrs", b"{}"))
                arrays[prefix] = parse_v2(prefix, json.loads(raw), zattrs)
        except ZarrUnsupported as e:
            unsupported[prefix] = e.message
    reader = FuseRangeReader(str(scratch), base_root=str(base))
    store = ManifestStore(metas, rows, reader, arrays, unsupported=unsupported)
    return store, arrays


def _assert_reads_like_the_base(store, arrays, base):
    assert arrays
    for name in arrays:
        got = zarr.open_array(store=store, path=name, mode="r")[...]
        want = zarr.open_array(str(base / name), mode="r")[...]
        np.testing.assert_array_equal(got, want, err_msg=name)


def _assert_rows_match_the_testing_helper(spark, table, root):
    """``plain_store_rows`` is how the other suites fake a registered store."""
    fields = ("kind", "array", "key", "shard_key", "file", "offset", "length")
    got = sorted(
        (
            tuple(r[f] for f in fields)
            for r in _live(spark, table, kinds=["shard", "chunk"]).to_pylist()
        ),
        key=repr,
    )
    _, rows, _ = plain_store_rows(root, origin="external")
    assert got == sorted((tuple(r[f] for f in fields) for r in rows), key=repr)


def _tree(root):
    """Every file and directory under ``root`` with its size and mtime."""
    out = {}
    for dirpath, dirs, files in os.walk(root):
        for n in dirs + files:
            p = os.path.join(dirpath, n)
            st = os.stat(p)
            out[os.path.relpath(p, root)] = (
                st.st_size if os.path.isfile(p) else None,
                st.st_mtime_ns,
            )
    return out


def _listing(root):
    """What Spark's file listing would report for ``root``: visible files only."""
    rels = sorted(
        f.relative_to(root).as_posix()
        for f in root.rglob("*")
        if f.is_file()
        and not any(p.startswith((".", "_")) for p in f.relative_to(root).parts)
    )
    return pa.table({"relpath": rels})


def _no_sleep(monkeypatch):
    """Retries without the waits; only the retry module sees the fake clock."""
    monkeypatch.setattr(_retry, "time", types.SimpleNamespace(sleep=lambda s: None))


def _warnings(caplog):
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == _register.__name__ and r.levelno >= logging.WARNING
    ]


def _miss_once(monkeypatch, *suffixes):
    """The first read of each file ending in ``suffixes`` misses, as a FUSE mount can."""
    real, seen = _register._read_file, []

    def read(path):
        if path.endswith(suffixes) and path not in seen:
            seen.append(path)
            raise FileNotFoundError(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", read)
    return seen


def _write(root, files):
    for rel, doc in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(doc if isinstance(doc, str) else json.dumps(doc))
    return root


def _builder(base):
    """What register builds for ``base`` without Spark: documents, arrays, layouts, rows."""
    metas, arrays, unsupported = read_base_metadata(str(base))
    layouts = {a: _register._layout_of(m) for a, m in arrays.items()}
    layouts.update({a: _register._layout_of_unsupported(a, metas) for a in unsupported})
    files = sorted(f for f in base.rglob("*") if f.is_file())
    objects = pa.table(
        {
            "relpath": [f.relative_to(base).as_posix() for f in files],
            "length": [f.stat().st_size for f in files],
            "mtime_ms": [f.stat().st_mtime_ns // 1_000_000 for f in files],
        }
    )
    rows, counts = _register._chunk_rows(objects, layouts, "a" * 32)
    return metas, arrays, unsupported, layouts, rows, counts


def _store_from_rows(metas, arrays, rows, base, scratch):
    reader = FuseRangeReader(str(scratch), base_root=str(base))
    return ManifestStore(metas, rows.to_pylist(), reader, arrays)


def _make_nested(base):
    """Nested groups, a hidden-named array, and a scalar variable (v3)."""
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    fc = g.create_group("fc")
    x = fc.create_array("x", shape=(4, 6), chunks=(2, 3), dtype="f4", fill_value=0.0)
    x[:] = np.arange(24, dtype="f4").reshape(4, 6)
    aux = fc.create_array("_aux", shape=(5,), chunks=(2,), dtype="i4", fill_value=0)
    aux[:] = np.arange(5, dtype="i4")
    crs = g.create_array("crs", shape=(), chunks=(), dtype="i4", fill_value=0)
    crs[...] = 4326  # a scalar variable, as CF stores carry for the grid mapping
    return base


def _make_with_strings(base, zarr_format):
    """A float array plus a string coordinate, which GeoBrix cannot decode."""
    g = zarr.open_group(str(base), mode="w", zarr_format=zarr_format)
    x = g.create_array("x", shape=(4, 6), chunks=(2, 3), dtype="f4", fill_value=0.0)
    x[:] = np.arange(24, dtype="f4").reshape(4, 6)
    station = g.create_array("station", shape=(3,), chunks=(2,), dtype=str)
    station[:] = np.array(["a", "bb", "ccc"], dtype=object)
    return base


# --- the five contract tests ------------------------------------------------------


def test_register_v3_sharded_rows_match_objects(spark_delta, era5_v3_sharded, tmp_path):
    table = _table("v3s")
    version = gbx_zarr_register(
        spark_delta, str(era5_v3_sharded), table, retain="2 days"
    )
    assert isinstance(version, int)
    assert version == snapshot(spark_delta, table).version

    shards = _live(spark_delta, table, kinds="shard")
    on_disk = [
        f
        for arr in ("t2m", "u10")
        for f in (era5_v3_sharded / arr / "c").rglob("*")
        if f.is_file()
    ]
    assert shards.num_rows == len(on_disk) == 4
    assert set(shards.column("array").to_pylist()) == {"t2m", "u10"}
    assert pc.all(pc.equal(shards["key"], shards["shard_key"])).as_py()

    chunks = _live(spark_delta, table, kinds="chunk")  # the unsharded coordinates
    assert set(chunks.column("array").to_pylist()) == {"time", "latitude", "longitude"}
    assert pc.all(pc.is_null(chunks["shard_key"])).as_py()

    for r in [*shards.to_pylist(), *chunks.to_pylist()]:
        f = era5_v3_sharded / r["file"]
        st = f.stat()
        assert r["file"] == f"{r['array']}/{r['key']}"
        assert (r["offset"], r["length"]) == (0, st.st_size)
        assert r["origin"] == "external"
        assert r["identity"] == f"{st.st_mtime_ns // 1_000_000}:{st.st_size}"

    assert _commit_meta(spark_delta, table) == {
        "mode": "register",
        "objects": shards.num_rows + chunks.num_rows,
    }
    props = read_props(spark_delta, table)
    assert (props.kind, props.root) == ("registered", str(era5_v3_sharded))
    assert (props.writable, props.overlay_root) == (False, None)
    assert (props.file_mode, props.retain) == ("fuse", "2 days")

    store, arrays = _open_store(spark_delta, table, era5_v3_sharded, tmp_path)
    assert set(arrays) == ERA5_ARRAYS
    _assert_reads_like_the_base(store, arrays, era5_v3_sharded)
    _assert_rows_match_the_testing_helper(spark_delta, table, era5_v3_sharded)


def test_register_v2_unsharded(spark_delta, era5_v2, tmp_path):
    table = _table("v2")
    gbx_zarr_register(spark_delta, str(era5_v2), table)

    rows = _live(spark_delta, table)
    assert set(rows.column("kind").to_pylist()) == {"group", "array", "chunk", "commit"}
    chunks = rows.filter(pc.equal(rows["kind"], "chunk"))
    assert pc.all(pc.is_null(chunks["shard_key"])).as_py()
    assert "0.0.0" in chunks.filter(pc.equal(chunks["array"], "t2m"))["key"].to_pylist()

    meta_keys = {
        _store_key(r["array"], r["key"])
        for r in rows.to_pylist()
        if r["kind"] in ("group", "array")
    }
    assert {".zgroup", ".zattrs", "t2m/.zarray", "t2m/.zattrs"} <= meta_keys

    store, arrays = _open_store(spark_delta, table, era5_v2, tmp_path)
    _assert_reads_like_the_base(store, arrays, era5_v2)
    _assert_rows_match_the_testing_helper(spark_delta, table, era5_v2)


def test_register_writable_requires_overlay_root(spark_delta, era5_v3_unsharded):
    table = _table("nooverlay")
    with pytest.raises(ValueError, match="overlay_root"):
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, writable=True)
    with pytest.raises(ValueError, match="overlay_root"):
        gbx_zarr_register(
            spark_delta, str(era5_v3_unsharded), table, overlay_root="/tmp/x"
        )
    assert not _exists(spark_delta, table)


def test_overlay_root_exclusive(spark_delta, era5_v3_unsharded, tmp_path):
    first, second = _table("own1"), _table("own2")
    overlay = tmp_path / "overlay"
    gbx_zarr_register(
        spark_delta,
        str(era5_v3_unsharded),
        first,
        writable=True,
        overlay_root=str(overlay),
        file_mode="fuse",
    )
    marker = json.loads((overlay / OWNER_MARKER).read_text())
    owner = _register.qualified_table_name(spark_delta, first)
    assert marker["table"] == owner and marker["kind"] == "overlay"
    props = read_props(spark_delta, first)
    assert (props.writable, props.overlay_root) == (True, str(overlay))
    assert props.file_mode == "fuse"
    _assert_rows_match_the_testing_helper(spark_delta, first, era5_v3_unsharded)

    with pytest.raises(ZarrOwnershipError) as ei:
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            second,
            writable=True,
            overlay_root=str(overlay),
        )
    assert first in str(ei.value) and "Fix:" in str(ei.value)
    assert not _exists(spark_delta, second)
    assert json.loads((overlay / OWNER_MARKER).read_text())["table"] == owner

    write_owner_marker(spark_delta, str(overlay), owner, "overlay")  # its own: no error


def _ledger_owners(spark, root):
    """The tables the ownership ledger of ``root`` names, first claim first."""
    root = str(root)
    if not _register._ledger_exists(spark, _register._ledger_table(root)):
        return []
    return [c["table"] for c in _register._held(spark, _register._ledger_table(root))]


def _empty_ledger(spark, root):
    """Make the ledger of ``root`` with no claim in it, so a claim goes through the ledger's
    compare-and-swap rather than founding it (what a root whose claim was given back has).
    """
    root = str(root)
    os.makedirs(root, exist_ok=True)
    _register._create_ledger(spark, _register._ledger_table(root), root)


@pytest.mark.parametrize("founded", [False, True], ids=["new-root", "empty-ledger"])
def test_concurrent_claims_of_one_root_have_exactly_one_winner(
    spark_delta, tmp_path, monkeypatch, founded
):
    """Eight tables claim one root at the same moment, on a file system whose exclusive
    create is not exclusive: what a Unity Catalog Volume does across nodes (two claimers
    both created the marker, and the recorded owner was often not the winner). The
    ownership ledger, a Delta table in the root, lets exactly one in; the rest are refused
    with a fix, and the marker names the winner. On a new root they race to found the
    ledger (one first version), on an empty one through its compare-and-swap."""
    from databricks.labs.gbx.zarr import _write

    monkeypatch.setattr(_write, "_pause", lambda seconds: None)
    real_open = os.open

    def not_exclusive(path, flags, *a, **k):
        if str(path).endswith(OWNER_MARKER):
            flags &= ~os.O_EXCL
        return real_open(path, flags, *a, **k)

    monkeypatch.setattr(os, "open", not_exclusive)
    root = tmp_path / "ov"
    if founded:  # every claimer goes through the MERGE, none founds the ledger
        _empty_ledger(spark_delta, root)
    names = [f"c.s.t{i}" for i in range(8)]
    barrier, outcome = threading.Barrier(len(names)), {}

    def claim(name):
        barrier.wait()
        try:
            write_owner_marker(spark_delta, str(root), name, "overlay")
            outcome[name] = None
        except BaseException as e:  # noqa: BLE001 - reported by the main thread
            outcome[name] = e

    threads = [threading.Thread(target=claim, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(600)
    winners = [n for n, e in outcome.items() if e is None]
    assert len(winners) == 1, outcome
    for e in (e for e in outcome.values() if e is not None):
        assert isinstance(e, ZarrOwnershipError) and "Fix:" in str(e)
        assert winners[0] in str(e)
    assert json.loads((root / OWNER_MARKER).read_text())["table"] == winners[0]
    assert _ledger_owners(spark_delta, root) == winners


def test_a_claim_that_lands_behind_another_is_taken_back(
    spark_delta, tmp_path, monkeypatch
):
    """Were Delta ever to admit two concurrent claims, the order they committed in decides
    for every reader: the later claimer takes its row out and is refused."""
    root = str(tmp_path / "ov")
    write_owner_marker(spark_delta, root, "c.s.first", "overlay")
    real_held, looks = _register._held, []

    def blind_held(spark, ledger):  # the claimer's first look misses the winner
        looks.append(1)
        return [] if len(looks) == 1 else real_held(spark, ledger)

    def blind_commit(spark, ledger, rows, *, write_id, meta, files):
        row = rows.to_pylist()[0]
        spark.sql(  # a second lock row, as if the MERGE had not seen the first
            f"INSERT INTO {_register._q(ledger)} (kind, key, write_id, meta) "
            f"VALUES ('lock', 'owner', '{write_id}', '{row['meta']}')"
        )
        return 0

    monkeypatch.setattr(_register, "_held", blind_held)
    monkeypatch.setattr(_register._manifest, "commit", blind_commit)
    with pytest.raises(ZarrOwnershipError, match="c.s.first"):
        write_owner_marker(spark_delta, root, "c.s.second", "overlay")
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == ["c.s.first"]


def test_a_claim_whose_commit_landed_but_whose_lookup_failed_still_wins(
    spark_delta, tmp_path, monkeypatch
):
    """The lookup after the MERGE fails every time: the row is in the ledger, which says so."""
    real, calls = _register._manifest.find_commit_version, []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) >= 2:
            raise RuntimeError("transient lookup failure")
        return real(*a, **k)

    root = str(tmp_path / "ov")
    _empty_ledger(spark_delta, root)
    monkeypatch.setattr(_register._manifest, "find_commit_version", flaky)
    assert _register.claim_root(spark_delta, root, "c.s.t", "overlay") is True
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == ["c.s.t"]


def test_a_failed_commit_is_the_error_raised_when_the_lookup_after_it_fails_too(
    spark_delta, tmp_path, monkeypatch
):
    """The claim's commit fails with an error that is not a conflict, and the look that asks
    whether it landed fails as well: the commit error is the one to read, not the lookup's.
    """
    real_held, looks = _register._held, []

    def held(spark, ledger):
        looks.append(1)
        if len(looks) == 1:
            return real_held(spark, ledger)
        raise RuntimeError("lookup failed")

    def commit(*a, **k):
        raise RuntimeError("commit failed")

    root = str(tmp_path / "ov")
    _empty_ledger(spark_delta, root)
    monkeypatch.setattr(_register, "_held", held)
    monkeypatch.setattr(_register._manifest, "commit", commit)
    with pytest.raises(RuntimeError, match="commit failed"):
        _register.claim_root(spark_delta, root, "c.s.t", "overlay")
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == []


def test_a_claim_whose_read_back_fails_leaves_no_row_behind(
    spark_delta, tmp_path, monkeypatch
):
    """The claim lands, then the read that confirms it fails: the claimer takes its own row out
    before it raises, so the ledger does not name a table whose create failed (every other table
    would be refused the root until someone cleaned it up)."""
    real_held, looks = _register._held, []

    def held(spark, ledger):
        looks.append(1)
        if (
            len(looks) == 2
        ):  # the first look is before the claim, the second confirms it
            raise RuntimeError("read-back failed")
        return real_held(spark, ledger)

    root = str(tmp_path / "ov")
    _empty_ledger(spark_delta, root)
    monkeypatch.setattr(_register, "_held", held)
    with pytest.raises(RuntimeError, match="read-back failed"):
        _register.claim_root(spark_delta, root, "c.s.t", "overlay")
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == []
    assert _register.claim_root(spark_delta, root, "c.s.other", "overlay") is True


def test_a_claim_that_cannot_be_taken_back_still_raises_the_read_back_error(
    spark_delta, tmp_path, monkeypatch, caplog
):
    """The read that confirms the claim fails and so does deleting the claim's row: the read-back
    error is the one raised (the unlock failure is only logged), and the row is left in the ledger.
    """
    real_held, looks = _register._held, []

    def held(spark, ledger):
        looks.append(1)
        if len(looks) == 2:
            raise RuntimeError("read-back failed")
        return real_held(spark, ledger)

    def unlock(*a, **k):
        raise RuntimeError("unlock failed")

    root = str(tmp_path / "ov")
    _empty_ledger(spark_delta, root)
    monkeypatch.setattr(_register, "_held", held)
    monkeypatch.setattr(_register, "_unlock", unlock)
    with caplog.at_level(logging.WARNING, logger=_register._LOG.name):
        with pytest.raises(RuntimeError, match="read-back failed"):
            _register.claim_root(spark_delta, root, "c.s.t", "overlay")
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("could not take claim" in m and "unlock failed" in m for m in warned)
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == ["c.s.t"]


def _refused(what):
    def refused(*a, **k):
        raise AssertionError(f"{what} ran")

    return refused


def test_a_new_root_is_claimed_by_founding_its_ledger_in_one_commit(
    spark_delta, tmp_path, monkeypatch
):
    """A root with no ledger: one statement creates the ledger with the claim in it. No
    CREATE IF NOT EXISTS, no read, no MERGE, no read back; the ledger has one version.
    """
    root = str(tmp_path / "ov")
    monkeypatch.setattr(_register, "_create_ledger", _refused("create"))
    monkeypatch.setattr(_register, "_held", _refused("read"))
    monkeypatch.setattr(_register._manifest, "commit", _refused("merge"))
    assert _register.claim_root(spark_delta, root, "c.s.t", "overlay") is True
    monkeypatch.undo()
    ledger = _register._ledger_table(root)
    assert spark_delta.sql(f"DESCRIBE HISTORY {ledger}").count() == 1
    held = _register._held(spark_delta, ledger)
    assert [c["table"] for c in held] == ["c.s.t"] and held[0]["kind"] == "overlay"
    kinds = sorted(
        r["kind"]
        for r in spark_delta.sql(
            f"SELECT kind FROM {ledger} WHERE tag IS NULL"
        ).collect()
    )
    assert kinds == ["commit", "lock"]  # the rows a claim through the MERGE leaves
    assert json.loads((tmp_path / "ov" / OWNER_MARKER).read_text())["table"] == "c.s.t"
    # the claim is the first: another table is refused, the owner claims again for free
    with pytest.raises(ZarrOwnershipError, match="c.s.t"):
        _register.claim_root(spark_delta, root, "c.s.other", "overlay")
    assert _register.claim_root(spark_delta, root, "c.s.t", "overlay") is False
    _register.release_root(spark_delta, root, "c.s.t", "overlay")
    assert _ledger_owners(spark_delta, root) == []
    assert _register.claim_root(spark_delta, root, "c.s.other", "overlay") is True


def test_a_founding_statement_that_landed_but_raised_is_still_this_claim(
    spark_delta, tmp_path, monkeypatch
):
    """The founding statement lands and then reports a failure (a lost reply): the claim
    goes on through the ledger, finds its own row first, and has won."""
    real = _register._found_ledger

    def landed_but_raised(*a, **k):
        assert real(*a, **k)
        return False

    root = str(tmp_path / "ov")
    monkeypatch.setattr(_register, "_found_ledger", landed_but_raised)
    monkeypatch.setattr(_register._manifest, "commit", _refused("merge"))
    assert _register._claim_ledger(spark_delta, root, "c.s.t", "overlay") is True
    monkeypatch.undo()
    assert _ledger_owners(spark_delta, root) == ["c.s.t"]


def _stray_founding(root):
    """A ledger directory as a founder's CREATE TABLE AS SELECT leaves it before (or without)
    its commit: a data file, no Delta log."""
    ledger = root / OWNER_LEDGER
    ledger.mkdir(parents=True)
    (ledger / "part-00000-founder.snappy.parquet").write_bytes(b"PAR1 not committed")
    return ledger


def test_a_root_whose_founder_never_commits_is_refused_with_a_fix(
    spark_delta, tmp_path, monkeypatch
):
    """Files in _gbx_owner and no Delta log: a founder is in flight. The claimer waits on the
    claim's jittered budget, not three short tries, and a root still stuck raises an ownership
    error whose Fix names the directory and when deleting it is safe. Once it is deleted
    (the founder died), the next claim wins."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    ledger = _stray_founding(root)
    pauses = []

    def founder_writing(seconds):  # a live founder: it keeps writing while we wait
        pauses.append(seconds)
        (ledger / f"part-{len(pauses):05d}.snappy.parquet").write_bytes(b"PAR1")

    monkeypatch.setattr(_write, "_pause", founder_writing)
    with pytest.raises(ZarrOwnershipError) as ei:
        _register.claim_root(spark_delta, str(root), "c.s.t", "overlay")
    assert len(pauses) == _write._ATTEMPTS
    assert OWNER_LEDGER in str(ei.value) and str(ledger) in ei.value.fix
    assert "delete" in ei.value.fix and "Delta holds no table" in ei.value.fix
    assert not (root / OWNER_MARKER).exists()
    shutil.rmtree(ledger)
    assert _register.claim_root(spark_delta, str(root), "c.s.t", "overlay") is True
    assert _ledger_owners(spark_delta, root) == ["c.s.t"]


def _age(path, seconds):
    """Date every file under ``path`` ``seconds`` back."""
    when = __import__("time").time() - seconds
    for f in path.rglob("*"):
        os.utime(f, (when, when))


def test_a_dead_founders_leftovers_are_cleared_after_the_wait_and_the_claim_wins(
    spark_delta, tmp_path, monkeypatch, caplog
):
    """A founding statement that wrote its data files and then failed (its own call, or one
    that died) leaves _gbx_owner without a table. After the claim's whole wait, files that are
    all older than the start of that wait are a dead founder's: they alone are removed (logged
    at WARNING with their ages) and the founding is tried once more, and wins."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    ledger = _stray_founding(root)
    (ledger / "sub").mkdir()
    (ledger / "sub" / "part-1.snappy.parquet").write_bytes(b"PAR1")
    _age(ledger, 600)
    pauses = []
    monkeypatch.setattr(_write, "_pause", pauses.append)
    with caplog.at_level(logging.WARNING, logger=_register._LOG.name):
        assert _register.claim_root(spark_delta, str(root), "c.s.t", "overlay") is True
    assert len(pauses) == _write._ATTEMPTS  # it waited the whole budget first
    assert _ledger_owners(spark_delta, root) == ["c.s.t"]
    warned = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("2 file(s)" in m and "s old" in m for m in warned), warned
    assert not (ledger / "part-00000-founder.snappy.parquet").exists()


def test_a_founding_that_fails_after_writing_its_files_does_not_wedge_its_own_root(
    spark_delta, tmp_path, monkeypatch
):
    """The call's own founding statement writes a data file into _gbx_owner and then fails
    before its commit: the same call waits its budget, finds only its own (older) file,
    clears it and founds the ledger on the second try."""
    from databricks.labs.gbx.zarr import _write

    real, tries = _register._found_ledger, []

    def failing_once(spark, root, claim_id, body, table):
        tries.append(1)
        if len(tries) == 1:
            ledger = os.path.join(root, OWNER_LEDGER)
            os.makedirs(ledger)
            with open(
                os.path.join(ledger, "part-00000-mine.snappy.parquet"), "wb"
            ) as f:
                f.write(b"PAR1 written, never committed")
            return False  # the statement raised after writing it
        return real(spark, root, claim_id, body, table)

    monkeypatch.setattr(_register, "_found_ledger", failing_once)
    monkeypatch.setattr(_write, "_pause", lambda seconds: None)
    root = tmp_path / "ov"
    assert _register.claim_root(spark_delta, str(root), "c.s.t", "overlay") is True
    assert len(tries) == 2 and _ledger_owners(spark_delta, root) == ["c.s.t"]


def test_leftovers_with_one_fresh_file_are_kept_and_the_root_is_stuck(
    spark_delta, tmp_path, monkeypatch
):
    """One file written after the wait began: a founder may be alive, so nothing is
    removed and the stuck error is raised as before."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    ledger = _stray_founding(root)
    _age(ledger, 600)
    pauses = []

    def one_fresh(seconds):
        pauses.append(seconds)
        if len(pauses) == 3:
            (ledger / "part-fresh.snappy.parquet").write_bytes(b"PAR1")

    monkeypatch.setattr(_write, "_pause", one_fresh)
    with pytest.raises(ZarrOwnershipError, match="Delta does not take for a table"):
        _register.claim_root(spark_delta, str(root), "c.s.t", "overlay")
    assert sorted(p.name for p in ledger.iterdir()) == [
        "part-00000-founder.snappy.parquet",
        "part-fresh.snappy.parquet",
    ]


def test_a_real_ledger_is_never_cleared_however_old(spark_delta, tmp_path, monkeypatch):
    """A ledger Delta takes for a table is never cleaned, even with every file dated long
    before the wait: the claimer reads it and is refused by the owner's name."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    assert _register.claim_root(spark_delta, str(root), "c.s.first", "overlay") is True
    ledger = root / OWNER_LEDGER
    _age(ledger, 10 * 86400)
    before = sorted(str(p.relative_to(ledger)) for p in ledger.rglob("*"))
    monkeypatch.setattr(_write, "_pause", lambda seconds: None)
    assert _register._clear_dead_founder(spark_delta, str(root), since=1e18) is True
    with pytest.raises(ZarrOwnershipError, match="c.s.first"):
        _register.claim_root(spark_delta, str(root), "c.s.second", "overlay")
    assert sorted(str(p.relative_to(ledger)) for p in ledger.rglob("*")) == before


def test_a_claimer_waits_for_a_founder_in_flight_and_finds_the_winner(
    spark_delta, tmp_path, monkeypatch
):
    """A claimer that meets a founder's files waits; when the founder commits, it reads the
    ledger and is refused with the winner's name, not a write error."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    ledger = _stray_founding(root)
    pauses = []

    def founder_lands(seconds):
        pauses.append(seconds)
        if len(pauses) == 2:
            shutil.rmtree(ledger)  # the founder's statement commits its first version
            body = {"table": "c.s.first", "kind": "overlay", "format": "1"}
            claim = uuid.uuid4().hex
            assert _register._found_ledger(
                spark_delta, str(root), claim, body, "c.s.first"
            )

    monkeypatch.setattr(_write, "_pause", founder_lands)
    with pytest.raises(ZarrOwnershipError, match="c.s.first"):
        _register.claim_root(spark_delta, str(root), "c.s.second", "overlay")
    assert len(pauses) == 2
    assert _ledger_owners(spark_delta, root) == ["c.s.first"]


def test_a_founding_statement_that_fails_is_logged_as_a_warning(tmp_path, caplog):
    class Session:
        def sql(self, text):
            raise RuntimeError("the founding statement failed")

    body = {"table": "c.s.t", "kind": "overlay", "format": "1"}
    with caplog.at_level(logging.WARNING, logger=_register._LOG.name):
        assert not _register._found_ledger(
            Session(), str(tmp_path / "ov"), "a" * 32, body, "c.s.t"
        )
    warned = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warned and "the founding statement failed" in warned[0].getMessage()


def test_a_founded_ledger_has_the_schema_of_a_created_one(spark_delta, tmp_path):
    """The two ways a ledger comes to be (founded with a claim; created empty, then claimed)
    give one schema, nullability included."""
    founded, created = tmp_path / "a", tmp_path / "b"
    assert _register.claim_root(spark_delta, str(founded), "c.s.t", "overlay") is True
    _empty_ledger(spark_delta, created)

    def schema(root):
        return [
            (f.name, f.dataType.simpleString(), f.nullable)
            for f in spark_delta.read.format("delta")
            .load(str(root / OWNER_LEDGER))
            .schema.fields
        ]

    assert schema(founded) == schema(created)


def test_a_ledger_whose_first_log_version_was_retired_is_still_a_ledger(
    spark_delta, tmp_path, monkeypatch
):
    """Delta's log cleanup removes old version files once a checkpoint covers them. A ledger
    in that state (made here by Delta itself: checkpoints every 2 commits, a one-day log
    retention, the early versions dated 40 days back) is asked of Delta, not of its files:
    it is a ledger, its owner claims it again at once and another table is refused by name,
    with no wait."""
    from databricks.labs.gbx.zarr import _write

    root = tmp_path / "ov"
    ledger = _register._ledger_table(str(root))
    body = json.dumps(
        {"table": "c.s.t", "kind": "overlay", "format": "1"}, sort_keys=True
    )
    spark_delta.sql(  # a ledger with c.s.t's claim, made to checkpoint and clean early
        f"CREATE TABLE {ledger} USING DELTA TBLPROPERTIES ("
        "'delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'true', "
        "'delta.checkpointInterval' = '2', 'delta.logRetentionDuration' = 'interval 1 days') "
        "AS SELECT CAST('lock' AS STRING) AS kind, CAST(NULL AS STRING) AS array, "
        "'owner' AS key, CAST(NULL AS STRING) AS shard_key, CAST(NULL AS STRING) AS file, "
        "CAST(NULL AS BIGINT) AS offset, CAST(NULL AS BIGINT) AS length, "
        "CAST(NULL AS STRING) AS checksum, CAST(NULL AS STRING) AS origin, "
        "CAST(NULL AS STRING) AS identity, 'a' AS write_id, CAST(NULL AS STRING) AS tag, "
        f"'{body}' AS meta"
    )
    _register._claim_marker(str(root), "c.s.t", "overlay")
    insert = f"INSERT INTO {ledger} (kind, key, write_id) VALUES ('commit', 'k{{}}', 'w{{}}')"
    for i in range(3):
        spark_delta.sql(insert.format(i, i))
    log_dir = root / OWNER_LEDGER / "_delta_log"
    past = __import__("time").time() - 40 * 86400
    for f in log_dir.iterdir():
        os.utime(f, (past, past))
    for i in range(3, 6):
        spark_delta.sql(insert.format(i, i))  # the next checkpoint cleans the old log
    assert not (log_dir / ("0" * 20 + ".json")).exists(), sorted(  # version 0 is gone
        p.name for p in log_dir.iterdir()
    )
    assert _register._ledger_state(spark_delta, str(root)) == "delta"
    pauses = []
    monkeypatch.setattr(_write, "_pause", pauses.append)
    assert _register.claim_root(spark_delta, str(root), "c.s.t", "overlay") is False
    with pytest.raises(ZarrOwnershipError, match="c.s.t"):
        _register.claim_root(spark_delta, str(root), "c.s.other", "overlay")
    assert pauses == []


def test_a_founders_directory_with_data_files_and_no_log_is_founding(
    spark_delta, tmp_path
):
    assert _register._ledger_state(spark_delta, str(tmp_path)) == "absent"
    (tmp_path / OWNER_LEDGER).mkdir()
    assert _register._ledger_state(spark_delta, str(tmp_path)) == "absent"  # empty
    _stray_founding(tmp_path / "f")
    assert _register._ledger_state(spark_delta, str(tmp_path / "f")) == "founding"


def test_the_stuck_root_fix_says_when_deleting_is_safe_and_how_long_it_waited(tmp_path):
    ledger = _stray_founding(tmp_path / "a")
    err = _register._stuck(str(tmp_path / "a"), 20)
    assert "safe to delete" in err.fix and str(ledger) in err.fix
    assert "no create, register, write or maintenance call" in err.fix
    assert "3 minutes" in str(err) and "4.5" in str(err)


def test_an_unreadable_ledger_directory_is_a_zarr_error_with_a_fix(
    spark_delta, tmp_path, monkeypatch
):
    """A refused listing is not retried as a transient miss and does not escape raw."""
    from databricks.labs.gbx.zarr.errors import ZarrError

    real = os.listdir

    def refused(path):
        if str(path).endswith(OWNER_LEDGER):
            raise PermissionError(13, "Permission denied", str(path))
        return real(path)

    monkeypatch.setattr(os, "listdir", refused)
    with pytest.raises(ZarrError) as ei:
        _register._ledger_state(spark_delta, str(tmp_path))
    assert "Fix:" in str(ei.value) and OWNER_LEDGER in str(ei.value)


def test_a_delta_error_that_is_not_a_missing_table_is_a_zarr_error(tmp_path):
    """Only Delta's "not a Delta table" means a founder is in flight; any other error from
    the question is raised with a Fix, not guessed at."""
    from databricks.labs.gbx.zarr.errors import ZarrError

    class Session:
        def sql(self, text):
            raise RuntimeError("the warehouse is unreachable")

    _stray_founding(tmp_path)
    with pytest.raises(ZarrError, match="unreachable") as ei:
        _register._ledger_state(Session(), str(tmp_path))
    assert "Fix:" in str(ei.value)


@pytest.mark.parametrize("condition", [None, "DELTA_SOME_OTHER_CLASS"])
def test_delta_saying_not_a_delta_table_in_words_is_founding(tmp_path, condition):
    """An error with no class, or one this code does not know, that says the path "is not a
    Delta table" is that answer: a founder in flight, not a hard error."""

    class NotATable(Exception):
        def getCondition(self):
            return condition

    class Session:
        def sql(self, text):
            raise NotATable("[X] `/x/_gbx_owner` is not a Delta table.")

    _stray_founding(tmp_path)
    assert _register._ledger_state(Session(), str(tmp_path)) == "founding"


def test_the_zarr_package_reads_no_delta_log_files():
    """Ledgers and manifests are asked of Delta, never read from its log's files: no zarr
    module names the log directory or a log version file."""
    import pathlib

    pkg = pathlib.Path(_register.__file__).parent
    hits = [
        f"{f.name}: {needle}"
        for f in sorted(pkg.glob("*.py"))
        for needle in ("_delta_log", "0" * 20)
        if needle in f.read_text()
    ]
    assert hits == []


def test_register_never_writes_into_base(spark_delta, era5_v3_sharded, tmp_path):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_sharded, base)
    before = _tree(base)

    gbx_zarr_register(spark_delta, str(base), _table("ro"))
    gbx_zarr_register(
        spark_delta,
        str(base),
        _table("rw"),
        writable=True,
        overlay_root=str(tmp_path / "overlay"),
    )

    assert _tree(base) == before
    assert not (base / OWNER_MARKER).exists()


# --- file modes -------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["external", "managed"])
def test_file_modes_that_need_file_columns_are_not_available_yet(
    spark_delta, era5_v3_unsharded, mode
):
    table = _table(mode)
    with pytest.raises(ZarrUnsupported) as ei:
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table, file_mode=mode)
    assert "Fix:" in str(ei.value) and (
        "fuse" in ei.value.fix or "default" in ei.value.fix
    )
    assert not _exists(spark_delta, table)


def test_an_unknown_file_mode_is_a_value_error_with_a_fix(
    spark_delta, era5_v3_unsharded
):
    with pytest.raises(ValueError, match=r"file_mode.*Fix:"):
        gbx_zarr_register(
            spark_delta, str(era5_v3_unsharded), _table("bad"), file_mode="s3"
        )


# --- owners -----------------------------------------------------------------------


def test_table_names_are_qualified_and_lower_cased(spark_delta):
    q = _register.qualified_table_name
    assert q(spark_delta, "T") == "spark_catalog.default.t"
    assert q(spark_delta, "Default.T") == "spark_catalog.default.t"
    assert q(spark_delta, "spark_catalog.DEFAULT.`T`") == "spark_catalog.default.t"


def test_the_same_name_in_two_schemas_is_two_owners(spark_delta, tmp_path):
    overlay = tmp_path / "overlay"
    first = _register.qualified_table_name(spark_delta, "shared_name")
    write_owner_marker(spark_delta, str(overlay), first, "overlay")
    spark_delta.sql("CREATE SCHEMA IF NOT EXISTS gbx_zarr_other")
    try:
        spark_delta.sql("USE gbx_zarr_other")
        other = _register.qualified_table_name(spark_delta, "shared_name")
        assert other == "spark_catalog.gbx_zarr_other.shared_name" != first
        with pytest.raises(ZarrOwnershipError, match="already owned"):
            write_owner_marker(spark_delta, str(overlay), other, "overlay")
    finally:
        spark_delta.sql("USE default")
        spark_delta.sql("DROP SCHEMA IF EXISTS gbx_zarr_other CASCADE")
    # the same table spelled another way is still the owner
    again = _register.qualified_table_name(spark_delta, "DEFAULT.Shared_Name")
    write_owner_marker(spark_delta, str(overlay), again, "overlay")


def test_an_unreadable_owner_marker_is_refused(tmp_path):
    (tmp_path / OWNER_MARKER).write_text("{not json")
    with pytest.raises(ZarrOwnershipError, match="not a readable") as ei:
        _claim_marker(str(tmp_path), "c.s.t", "overlay")
    assert "Fix:" in str(ei.value)


def test_a_marker_of_another_kind_is_a_conflict(tmp_path):
    _claim_marker(str(tmp_path), "c.s.t", "overlay")
    with pytest.raises(ZarrOwnershipError, match="already owned"):
        _claim_marker(str(tmp_path), "c.s.t", "managed")
    with pytest.raises(ValueError, match="kind"):  # checked before any claim is tried
        write_owner_marker(None, str(tmp_path), "c.s.t", "scratch")


def test_a_marker_that_cannot_be_written_is_a_zarr_error(tmp_path, monkeypatch):
    def refuse(*a, **k):
        raise PermissionError("read-only file system")

    monkeypatch.setattr(os, "open", refuse)
    with pytest.raises(ZarrError) as ei:
        _claim_marker(str(tmp_path / "ov"), "c.s.t", "overlay")
    assert "Fix:" in str(ei.value) and "ov" in str(ei.value)
    monkeypatch.undo()
    assert not (tmp_path / "ov" / OWNER_MARKER).exists()


def test_a_marker_taken_over_before_the_read_back_is_not_removed(tmp_path, monkeypatch):
    overlay = tmp_path / "ov"
    theirs = json.dumps({"table": "other.s.t", "kind": "overlay", "format": "1"})
    real = _register._check_owner

    def taken_over(marker, table, kind):
        # between this call's write and its read-back another table took the root
        (overlay / OWNER_MARKER).write_text(theirs)
        return real(marker, table, kind)

    monkeypatch.setattr(_register, "_check_owner", taken_over)
    with pytest.raises(ZarrOwnershipError, match="other.s.t"):
        _claim_marker(str(overlay), "c.s.t", "overlay")
    assert (overlay / OWNER_MARKER).read_text() == theirs


def test_a_marker_whose_write_fails_is_removed(tmp_path, monkeypatch):
    """The write stops part way: what landed is exactly this call's bytes, so it goes."""
    real_write, calls = os.write, []

    def half_then_full(fd, data):
        calls.append(len(data))
        if len(calls) == 1:
            return real_write(fd, bytes(data[:10]))  # a short write: the loop goes on
        raise OSError("disk full")

    monkeypatch.setattr(os, "write", half_then_full)
    with pytest.raises(ZarrError, match="disk full") as ei:
        _claim_marker(str(tmp_path / "ov"), "c.s.t", "overlay")
    assert "Fix:" in str(ei.value) and len(calls) == 2
    assert not (tmp_path / "ov" / OWNER_MARKER).exists()


def test_a_foreign_overlay_is_refused_before_the_store_is_read(
    spark_delta, era5_v3_unsharded, tmp_path, monkeypatch
):
    overlay = tmp_path / "overlay"
    _claim_marker(str(overlay), "someone.else.table", "overlay")

    def not_reached(*a, **k):
        raise AssertionError("the store was read before the overlay was checked")

    monkeypatch.setattr(_register, "_scan", not_reached)
    monkeypatch.setattr(_register, "read_base_metadata", not_reached)
    with pytest.raises(ZarrOwnershipError, match="someone.else.table"):
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            _table("foreign"),
            writable=True,
            overlay_root=str(overlay),
        )


def test_the_marker_is_removed_when_the_manifest_cannot_be_created(
    spark_delta, era5_v3_unsharded, tmp_path, monkeypatch
):
    def boom(*a, **k):
        raise RuntimeError("create failed")

    monkeypatch.setattr(_register, "create_manifest", boom)
    table = _table("nomanifest")
    fresh, kept = tmp_path / "fresh", tmp_path / "kept"
    write_owner_marker(  # claimed by an earlier attempt of the same table
        spark_delta,
        str(kept),
        _register.qualified_table_name(spark_delta, table),
        "overlay",
    )
    for overlay in (fresh, kept):
        with pytest.raises(RuntimeError, match="create failed"):
            gbx_zarr_register(
                spark_delta,
                str(era5_v3_unsharded),
                table,
                writable=True,
                overlay_root=str(overlay),
            )
    owner = _register.qualified_table_name(spark_delta, table)
    assert not (fresh / OWNER_MARKER).exists()  # this call made it, so it removed it
    assert _ledger_owners(spark_delta, fresh) == []  # and its claim in the ledger
    assert (kept / OWNER_MARKER).exists()  # this call did not
    assert _ledger_owners(spark_delta, kept) == [owner]
    assert not _exists(spark_delta, table)


# --- refusals leave nothing behind ------------------------------------------------


def test_an_existing_table_is_refused_before_any_marker_is_written(
    spark_delta, era5_v3_unsharded, tmp_path
):
    table = _table("again")
    gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table)
    overlay = tmp_path / "ov"
    with pytest.raises(ZarrOwnershipError):
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            table,
            writable=True,
            overlay_root=str(overlay),
        )
    assert not (overlay / OWNER_MARKER).exists()


@pytest.mark.parametrize("where", ["inside", "around", "same"])
def test_an_overlay_that_overlaps_the_base_is_refused(
    spark_delta, era5_v3_unsharded, tmp_path, where
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_unsharded, base)
    overlay = {
        "inside": base / "overlay",
        "around": tmp_path,
        "same": base,
    }[where]
    before = _tree(base)
    table = _table("overlap")
    with pytest.raises(ZarrOwnershipError) as ei:
        gbx_zarr_register(
            spark_delta, str(base), table, writable=True, overlay_root=str(overlay)
        )
    assert "Fix:" in str(ei.value)
    assert not _exists(spark_delta, table)
    assert _tree(base) == before


def test_a_directory_that_is_not_a_store_is_refused(spark_delta, tmp_path):
    (tmp_path / "plain").mkdir()
    (tmp_path / "plain" / "notes.txt").write_text("hello")
    for target in (tmp_path / "plain", tmp_path / "missing"):
        table = _table("nostore")
        with pytest.raises(ZarrError) as ei:
            gbx_zarr_register(spark_delta, str(target), table)
        assert "Fix:" in str(ei.value)
        assert not _exists(spark_delta, table)


@pytest.mark.parametrize(
    "path, text",
    [
        ("s3://bucket/cube.zarr", "Volume"),
        ("/dbfs/mnt/cube.zarr", "DBFS"),
        ("dbfs:/mnt/cube.zarr", "DBFS"),
    ],
)
def test_a_path_that_is_not_on_a_volume_is_refused(spark_delta, path, text):
    with pytest.raises(ZarrUnsupported, match=text) as ei:
        gbx_zarr_register(spark_delta, path, _table("elsewhere"))
    assert "Fix:" in str(ei.value) and "symbolic" not in str(ei.value)


def test_a_volume_uri_is_a_bare_path():
    assert (
        _register._local_root("dbfs:/Volumes/c/s/v/x.zarr") == "/Volumes/c/s/v/x.zarr"
    )


@pytest.mark.parametrize(
    "spelling",
    [
        "/Volumes/c/s/v/x.zarr",
        "dbfs:/Volumes/c/s/v/x.zarr",
        "dbfs:///Volumes/c/s/v/x.zarr",
        "/dbfs/Volumes/c/s/v/x.zarr",
        " /dbfs/Volumes/c/s/v/x.zarr/ ",
        "file:/Volumes/c/s/v/x.zarr",
    ],
)
def test_every_spelling_of_a_volume_path_is_the_same_bare_path(spelling):
    assert _register._local_root(spelling) == "/Volumes/c/s/v/x.zarr"


@pytest.mark.parametrize(
    "path",
    [
        "/dbfs",
        "/dbfs/mnt/x.zarr",
        "dbfs:/mnt/x.zarr",
        "dbfs:///mnt/x.zarr",
        "dbfs:mnt/x.zarr",
        "dbfs:///x.zarr",
    ],
)
def test_the_other_dbfs_paths_are_still_refused(path):
    with pytest.raises(ZarrUnsupported, match="DBFS") as ei:
        _register._local_root(path)
    assert "Fix:" in str(ei.value) and "/Volumes/" in ei.value.fix


V3_GROUP = {"zarr_format": 3, "node_type": "group", "attributes": {}}
REFUSALS = {
    "v3 root array": (
        {"zarr.json": {"zarr_format": 3, "node_type": "array"}},
        "single",
    ),
    "v3 other format": (
        {"zarr.json": {"zarr_format": 2, "node_type": "group"}},
        "zarr_format",
    ),
    "unknown node type": (
        {
            "zarr.json": V3_GROUP,
            "weird/zarr.json": {"zarr_format": 3, "node_type": "banana"},
        },
        "node_type",
    ),
    "v2 root array": (
        {".zarray": {"shape": [1], "chunks": [1], "dtype": "<f4"}},
        "single",
    ),
    "not json": ({"zarr.json": "{not json"}, "not valid JSON"),
    "malformed structure": (  # a regular grid with no configuration: a KeyError, not a refusal
        {
            "zarr.json": V3_GROUP,
            "a/zarr.json": {
                "zarr_format": 3,
                "node_type": "array",
                "shape": [2],
                "data_type": "float32",
                "chunk_grid": {"name": "regular"},
                "codecs": [],
            },
        },
        "malformed",
    ),
}


@pytest.mark.parametrize("case", list(REFUSALS))
def test_stores_that_cannot_be_registered_are_refused(tmp_path, case):
    files, text = REFUSALS[case]
    with pytest.raises(ZarrUnsupported, match=text) as ei:
        read_base_metadata(str(_write(tmp_path / "s.zarr", files)))
    assert "Fix:" in str(ei.value)


@pytest.mark.parametrize("case", ["v3 root array", "unknown node type"])
def test_a_refused_store_leaves_no_table_and_no_marker(spark_delta, tmp_path, case):
    base = _write(tmp_path / "s.zarr", REFUSALS[case][0])
    table = _table("refused")
    overlay = tmp_path / "overlay"
    with pytest.raises(ZarrUnsupported):
        gbx_zarr_register(
            spark_delta, str(base), table, writable=True, overlay_root=str(overlay)
        )
    assert not _exists(spark_delta, table)
    assert not (overlay / OWNER_MARKER).exists()


def test_a_failed_commit_leaves_no_empty_table(
    spark_delta, era5_v3_unsharded, tmp_path, monkeypatch
):
    def boom(*a, **k):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(_register, "commit", boom)
    table = _table("boom")
    overlay = tmp_path / "overlay"
    with pytest.raises(RuntimeError, match="commit failed"):
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            table,
            writable=True,
            overlay_root=str(overlay),
        )
    assert not _exists(spark_delta, table)
    assert not (overlay / OWNER_MARKER).exists()


def test_a_table_that_was_not_made_by_this_call_is_never_dropped(
    spark_delta, era5_v3_unsharded, monkeypatch
):
    table = _table("precious")
    gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table)
    before = _live(spark_delta, table).num_rows
    # get past the early check, so the failure comes from creating the table
    monkeypatch.setattr(_register, "_table_exists", lambda spark, name: False)
    with pytest.raises(ZarrOwnershipError, match="already exists"):
        gbx_zarr_register(spark_delta, str(era5_v3_unsharded), table)
    assert _live(spark_delta, table).num_rows == before


# --- stores that are not tidy -----------------------------------------------------


def test_zero_byte_and_stray_objects_do_not_break_the_store(
    spark_delta, era5_v3_unsharded, tmp_path, caplog, monkeypatch
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_unsharded, base)
    hole = base / "t2m" / "c" / "1" / "0" / "0"
    hole.write_bytes(b"")  # a chunk that was truncated to nothing
    (base / "README.txt").write_text("vendor notes")
    (base / "t2m" / "c" / "00" / "0" / "0").parent.mkdir(parents=True)
    (base / "t2m" / "c" / "00" / "0" / "0").write_bytes(b"not a canonical key")
    (base / "t2m" / "thumbs.db").write_bytes(b"x")

    # Spark 4.0 leaves zero-byte files out of its listing; other runtimes may not.
    # Report the second one as empty too, as such a runtime would.
    real_scan = _register._scan

    def scan(*a, **k):
        t = real_scan(*a, **k)
        length = pc.if_else(
            pc.equal(t["relpath"], "t2m/c/2/0/0"), pa.scalar(0, pa.int64()), t["length"]
        )
        return t.set_column(1, "length", length)

    monkeypatch.setattr(_register, "_scan", scan)

    table = _table("messy")
    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        gbx_zarr_register(spark_delta, str(base), table)

    warned = _warnings(caplog)
    assert any("zero-byte" in w and "t2m/c/2/0/0" in w for w in warned)
    assert any("not chunks" in w and "README.txt" in w for w in warned)
    skipped = _commit_meta(spark_delta, table)["skipped"]
    assert skipped["stray"] == 3 and skipped["empty"] >= 1

    rows = _live(spark_delta, table, kinds="chunk")
    assert pc.all(pc.greater(rows["length"], 0)).as_py()
    t2m_keys = set(rows.filter(pc.equal(rows["array"], "t2m"))["key"].to_pylist())
    assert not {"c/1/0/0", "c/2/0/0"} & t2m_keys and "c/0/0/0" in t2m_keys
    assert not any(
        "README" in f or "00" in f.split("/") for f in rows["file"].to_pylist()
    )

    store, _ = _open_store(spark_delta, table, base, tmp_path)  # no drift at build
    got = zarr.open_array(store=store, path="t2m", mode="r")
    want = zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")[:]
    np.testing.assert_array_equal(got[:12], want[:12])  # chunks that are there
    hole = got[12:24, :37, :72]  # the truncated chunk reads as the fill value
    np.testing.assert_array_equal(hole, np.full(hole.shape, got.fill_value, hole.dtype))


def test_rows_for_empty_and_stray_objects(era5_v3_sharded):
    """The row builder, given what a listing might return, whatever Spark drops."""
    _, _, arrays = plain_store_rows(era5_v3_sharded)
    layouts = {n: _register._layout_of(m) for n, m in arrays.items()}
    objects = pa.table(
        {
            "relpath": [
                "t2m/c/0/0/0",  # a shard
                "t2m/c/1/0/0",  # a shard, but empty
                "t2m/c/00/0/0",  # not a canonical key
                "t2m/c/0/0",  # wrong number of dimensions
                "t2m/zarr.json",  # metadata
                "zarr.json",  # metadata
                "elsewhere/data.bin",  # no array
                "time/c/0",  # an unsharded chunk
            ],
            "length": [10, 0, 5, 5, 7, 7, 5, 8],
            "mtime_ms": [1, 2, 3, 4, 5, 6, 7, 8],
        }
    )
    rows, counts = _register._chunk_rows(objects, layouts, "a" * 32)
    got = {(r["kind"], r["array"], r["key"]): r for r in rows.to_pylist()}
    assert set(got) == {("shard", "t2m", "c/0/0/0"), ("chunk", "time", "c/0")}
    shard = got[("shard", "t2m", "c/0/0/0")]
    assert (shard["shard_key"], shard["identity"], shard["length"]) == (
        "c/0/0/0",
        "1:10",
        10,
    )
    assert got[("chunk", "time", "c/0")]["shard_key"] is None
    assert (counts["empty"], counts["stray"]) == (1, 3)


def test_nested_groups_and_hidden_names_in_an_unusual_path(
    spark_delta, tmp_path, monkeypatch
):
    base = _make_nested(tmp_path / "_my data [v1]" / "cube.zarr")
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "fc/x/zarr.json", "fc/_aux/zarr.json")

    table = _table("nested")
    gbx_zarr_register(spark_delta, str(base), table)
    assert len(missed) == 2  # both were found on the retry

    chunks = _live(spark_delta, table, kinds="chunk")
    by_array = {
        a: sorted(chunks.filter(pc.equal(chunks["array"], a))["key"].to_pylist())
        for a in ("fc/x", "fc/_aux")
    }
    assert len(by_array["fc/x"]) == 4  # 2 x 2 chunks
    assert by_array["fc/_aux"] == ["c/0", "c/1", "c/2"]  # Spark skips "_" names

    groups = {
        _store_key(r["array"], r["key"])
        for r in _live(spark_delta, table, kinds="group").to_pylist()
    }
    assert groups == {"zarr.json", "fc/zarr.json"}

    store, arrays = _open_store(spark_delta, table, base, tmp_path / "scratch")
    assert set(arrays) == {"fc/x", "fc/_aux", "crs"}
    _assert_reads_like_the_base(store, arrays, base)

    # the default listing finds the hidden array too, so a sync would not delete it
    listed = set(list_objects(spark_delta, str(base))["relpath"].to_pylist())
    registered = set(chunks["file"].to_pylist())
    assert registered <= listed and "fc/_aux/c/0" in listed


def test_every_v3_chunk_key_encoding_registers(tmp_path):
    base = tmp_path / "v3.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    keys = {
        "dflt": {"name": "default", "separator": "/"},
        "dflt_dot": {"name": "default", "separator": "."},
        "v2dot": {"name": "v2", "separator": "."},
        "v2slash": {"name": "v2", "separator": "/"},
    }
    for name, enc in keys.items():
        x = g.create_array(
            name,
            shape=(4, 6),
            chunks=(2, 3),
            dtype="f4",
            fill_value=0.0,
            chunk_key_encoding=enc,
        )
        x[:] = np.arange(24, dtype="f4").reshape(4, 6)
    expected = {"dflt": "c/1/1", "dflt_dot": "c.1.1", "v2dot": "1.1", "v2slash": "1/1"}

    metas, arrays, _, _, rows, _ = _builder(base)  # what register builds, without Spark
    for array, last in expected.items():
        got = rows.filter(pc.equal(rows["array"], array))["key"].to_pylist()
        assert len(got) == 4 and last in got, (array, got)
    store = _store_from_rows(metas, arrays, rows, base, tmp_path / "scratch")
    _assert_reads_like_the_base(store, arrays, base)


def test_a_v2_store_with_slash_separated_keys_gets_its_chunk_rows(tmp_path):
    base = tmp_path / "v2.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=2)
    y = g.create_array(
        "y",
        shape=(4, 6),
        chunks=(2, 3),
        dtype="f4",
        fill_value=0.0,
        chunk_key_encoding={"name": "v2", "separator": "/"},
    )
    y[:] = np.arange(24, dtype="f4").reshape(4, 6)

    metas, arrays, _, _, rows, counts = _builder(base)
    assert sorted(rows["key"].to_pylist()) == ["0/0", "0/1", "1/0", "1/1"]
    assert set(rows["kind"].to_pylist()) == {"chunk"}
    assert pc.all(pc.is_null(rows["shard_key"])).as_py()
    assert (counts["stray"], counts["empty"]) == (0, 0)
    store = _store_from_rows(metas, arrays, rows, base, tmp_path / "scratch")
    _assert_reads_like_the_base(store, arrays, base)


# --- arrays GeoBrix cannot decode -------------------------------------------------


def test_an_array_that_cannot_be_decoded_is_still_registered(
    spark_delta, tmp_path, caplog
):
    base = _make_with_strings(tmp_path / "s.zarr", 3)
    metas, arrays, unsupported = read_base_metadata(str(base))
    assert set(arrays) == {"x"} and set(unsupported) == {"station"}
    assert "not supported" in unsupported["station"]

    table = _table("strings3")
    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        gbx_zarr_register(spark_delta, str(base), table)
    assert any("station" in w for w in _warnings(caplog))
    assert _commit_meta(spark_delta, table)["unsupported"] == unsupported

    rows = _live(spark_delta, table)
    meta_rows = [
        r
        for r in rows.to_pylist()
        if r["array"] == "station" and r["key"] == "zarr.json"
    ]
    assert [r["kind"] for r in meta_rows] == ["array"]  # not "group"
    station = rows.filter(
        pc.and_(pc.equal(rows["array"], "station"), pc.equal(rows["kind"], "chunk"))
    )
    assert sorted(station["file"].to_pylist()) == [
        "station/c/0",
        "station/c/1",
    ]
    assert _commit_meta(spark_delta, table).get("skipped") is None  # not strays

    store, arrays = _open_store(spark_delta, table, base, tmp_path / "scratch")
    assert set(arrays) == {"x"}
    _assert_reads_like_the_base(store, arrays, base)
    # reading the array GeoBrix cannot decode raises; it never reads as fill ('')
    with pytest.raises(ZarrUnsupported) as ei:
        zarr.open_array(store=store, path="station", mode="r")[:]
    assert "'station'" in str(ei.value) and "Fix:" in str(ei.value)


def test_a_v2_object_coordinate_keeps_its_documents_and_chunk_rows(tmp_path):
    base = _make_with_strings(tmp_path / "s.zarr", 2)
    metas, arrays, unsupported, layouts, rows, counts = _builder(base)
    assert set(arrays) == {"x"} and set(unsupported) == {"station"}
    assert "|O" in unsupported["station"] and layouts["station"] is not None
    assert "station/.zarray" in metas
    station = rows.filter(pc.equal(rows["array"], "station"))
    assert sorted(station["key"].to_pylist()) == ["0", "1"]
    assert set(station["kind"].to_pylist()) == {"chunk"}
    assert (counts["stray"], counts["empty"]) == (0, 0)
    store = _store_from_rows(metas, arrays, rows, base, tmp_path / "scratch")
    _assert_reads_like_the_base(store, arrays, base)


def test_an_array_without_a_readable_layout_is_labelled_not_counted_as_stray(
    tmp_path, monkeypatch
):
    base = _make_with_strings(tmp_path / "s.zarr", 3)
    monkeypatch.setattr(_register, "_layout_of_unsupported", lambda path, metas: None)
    _, _, unsupported, layouts, rows, counts = _builder(base)
    assert layouts["station"] is None
    assert "station" not in set(rows["array"].to_pylist())  # no rows for it
    assert counts["stray"] == 0 and counts["no_layout"] == 2  # not strays
    labelled = _register._label_unlaid(unsupported, layouts)
    assert "no layout" in labelled["station"]
    assert labelled["station"].startswith(unsupported["station"])
    # an array whose layout parsed keeps its reason as it was
    same = _register._label_unlaid({"a": "why"}, {"a": layouts["x"]})
    assert same == {"a": "why"}


# --- metadata: transient misses ---------------------------------------------------


@pytest.mark.parametrize("hint", ["listing", "walk only"])
def test_a_v3_document_that_misses_once_is_not_dropped(
    era5_v3_sharded, monkeypatch, hint
):
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "t2m/zarr.json", "u10/zarr.json")
    listing = _listing(era5_v3_sharded) if hint == "listing" else None
    _, arrays, _ = read_base_metadata(str(era5_v3_sharded), listing=listing)
    assert len(missed) == 2 and set(arrays) == ERA5_ARRAYS


def test_a_v2_document_that_misses_once_is_not_dropped(era5_v2, monkeypatch):
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "t2m/.zarray", "u10/.zarray")
    _, arrays, _ = read_base_metadata(str(era5_v2))
    assert len(missed) == 2 and set(arrays) == ERA5_ARRAYS


def test_a_zattrs_that_misses_once_is_not_lost(era5_v2, monkeypatch):
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "t2m/.zattrs", "/.zattrs")
    metas, arrays, _ = read_base_metadata(str(era5_v2))
    assert any(m.endswith("t2m/.zattrs") for m in missed) and len(missed) >= 2
    assert ".zattrs" in metas and "t2m/.zattrs" in metas  # root and array
    assert arrays["t2m"].dims == ("time", "latitude", "longitude")
    assert arrays["t2m"].attrs.get("units") == "K"


def test_a_root_zgroup_that_misses_once_with_a_zmetadata_is_retried(
    era5_v2, tmp_path, monkeypatch
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "base.zarr/.zgroup")
    metas, arrays, _ = read_base_metadata(str(base))
    assert len(missed) == 1 and ".zgroup" in metas and set(arrays) == ERA5_ARRAYS


def test_a_root_with_a_zmetadata_and_no_zgroup_is_still_refused(
    era5_v2, tmp_path, monkeypatch
):
    _no_sleep(monkeypatch)
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    (base / ".zgroup").unlink()
    with pytest.raises(ZarrError, match=r"\.zmetadata but no \.zgroup") as ei:
        read_base_metadata(str(base))
    assert "Fix:" in str(ei.value)


def test_a_stale_consolidated_entry_is_tried_once_over(era5_v2, tmp_path, monkeypatch):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    shutil.rmtree(base / "u10")  # .zmetadata still names it
    _no_sleep(monkeypatch)
    real, reads = _register._read_file, []

    def read(path):
        if path.endswith("u10/.zarray"):
            reads.append(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", read)
    _, arrays, _ = read_base_metadata(str(base))
    assert set(arrays) == ERA5_ARRAYS - {"u10"}
    assert len(reads) == _register._HINTED_ATTEMPTS  # not once more as a plain probe


def test_a_directory_the_listing_omits_is_found_through_the_scan(
    era5_v3_sharded, monkeypatch
):
    real = _register._list_dirs
    omitted = []

    def list_dirs(path):
        names = real(path)
        if path == str(era5_v3_sharded) and not omitted:
            omitted.append("t2m")
            return [n for n in names if n != "t2m"]
        return names

    monkeypatch.setattr(_register, "_list_dirs", list_dirs)
    _, arrays, _ = read_base_metadata(
        str(era5_v3_sharded), listing=_listing(era5_v3_sharded)
    )
    assert omitted and set(arrays) == ERA5_ARRAYS


def test_a_document_the_listing_names_but_cannot_be_read_is_an_error(
    era5_v3_sharded, monkeypatch
):
    _no_sleep(monkeypatch)
    real = _register._read_file

    def read(path):
        if path.endswith("t2m/zarr.json"):
            raise FileNotFoundError(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", read)
    with pytest.raises(ZarrError, match="t2m/zarr.json") as ei:
        read_base_metadata(str(era5_v3_sharded), listing=_listing(era5_v3_sharded))
    assert "Fix:" in str(ei.value)


# --- metadata: consolidated documents ---------------------------------------------


@pytest.mark.filterwarnings("ignore:Consolidated metadata")
def test_consolidated_v3_metadata_opens_like_the_base(spark_delta, tmp_path, caplog):
    base = tmp_path / "base.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    y = g.create_group("fc").create_array(
        "y", shape=(4,), chunks=(2,), dtype="f4", fill_value=0.0
    )
    y[:] = 1
    z = g.create_array("z", shape=(4,), chunks=(2,), dtype="f4", fill_value=0.0)
    z[:] = 2
    zarr.consolidate_metadata(str(base))
    assert "consolidated_metadata" in json.loads((base / "zarr.json").read_text())

    table = _table("consolidated3")
    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        gbx_zarr_register(spark_delta, str(base), table)
    assert not _warnings(caplog)

    store, arrays = _open_store(spark_delta, table, base, tmp_path / "scratch")
    assert set(arrays) == {"z", "fc/y"}
    want = zarr.open_group(str(base), mode="r", use_consolidated=True)
    got = zarr.open_group(store=store, mode="r", use_consolidated=True)
    for name in ("z", "fc/y"):
        assert got[name].shape == want[name].shape
    _assert_reads_like_the_base(store, arrays, base)


@pytest.mark.filterwarnings("ignore:Consolidated metadata")
def test_stale_v3_consolidated_metadata_is_reported_and_not_trusted(tmp_path, caplog):
    base = tmp_path / "base.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    z = g.create_array("z", shape=(4,), chunks=(2,), dtype="f4", fill_value=0.0)
    zarr.consolidate_metadata(str(base))
    z.resize((6,))  # the array's own document moves on; the consolidated one does not

    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        metas, arrays, _ = read_base_metadata(str(base))
    assert arrays["z"].shape == (6,)  # the per-array document is the truth
    assert json.loads(metas["z/zarr.json"])["shape"] == [6]
    assert any("consolidated" in w and "z/zarr.json" in w for w in _warnings(caplog))


def test_v2_consolidated_metadata_is_kept_and_checked(
    spark_delta, era5_v2, tmp_path, caplog
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    assert (base / ".zmetadata").exists()

    table = _table("consolidated2")
    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        gbx_zarr_register(spark_delta, str(base), table)
    assert not _warnings(caplog)

    rows = _live(spark_delta, table, kinds=["group", "array"]).to_pylist()
    docs = {_store_key(r["array"], r["key"]): r["meta"] for r in rows}
    assert ".zmetadata" in docs
    for key, text in docs.items():  # every document is the file, byte for byte
        assert text.encode() == (base / key).read_bytes(), key
    store, arrays = _open_store(spark_delta, table, base, tmp_path)
    _assert_reads_like_the_base(store, arrays, base)


def test_stale_v2_consolidated_metadata_is_reported_and_not_trusted(
    era5_v2, tmp_path, caplog
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    zarr.consolidate_metadata(str(base))
    zarray = json.loads((base / "t2m" / ".zarray").read_text())
    zarray["shape"][0] += 24
    (base / "t2m" / ".zarray").write_text(json.dumps(zarray))

    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        _, arrays, _ = read_base_metadata(str(base))
    assert arrays["t2m"].shape[0] == zarray["shape"][0]
    assert any("t2m/.zarray" in w for w in _warnings(caplog))


def test_unreadable_consolidated_metadata_is_reported_not_fatal(
    era5_v2, tmp_path, caplog
):
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v2, base)
    (base / ".zmetadata").write_text("{not json")
    with caplog.at_level(logging.WARNING, logger=_register.__name__):
        metas, arrays, _ = read_base_metadata(str(base))
    assert set(arrays) == ERA5_ARRAYS and ".zmetadata" in metas
    assert any(".zmetadata" in w for w in _warnings(caplog))


def test_metadata_documents_are_kept_byte_for_byte(era5_v3_sharded):
    metas, arrays, unsupported = read_base_metadata(str(era5_v3_sharded))
    assert set(metas) == {"zarr.json", *(f"{a}/zarr.json" for a in ERA5_ARRAYS)}
    for key, raw in metas.items():
        assert raw == (era5_v3_sharded / key).read_bytes()
    assert arrays["t2m"].shard is not None and arrays["time"].shard is None
    assert unsupported == {}


# --- the listing ------------------------------------------------------------------


def test_list_objects_reports_each_object_once_with_its_identity(
    spark_delta, era5_v3_sharded
):
    objects = list_objects(spark_delta, str(era5_v3_sharded))
    assert objects.column_names == ["relpath", "length", "mtime_ms"]
    assert objects["relpath"].to_pylist() == sorted(objects["relpath"].to_pylist())

    plain = {
        f.relative_to(era5_v3_sharded).as_posix(): f.stat()
        for f in era5_v3_sharded.rglob("*")
        if f.is_file()
    }
    got = {r["relpath"]: r for r in objects.to_pylist()}
    assert set(got) == set(plain)
    for rel, st in plain.items():
        assert got[rel]["length"] == st.st_size
        assert got[rel]["mtime_ms"] == st.st_mtime_ns // 1_000_000


def test_a_store_with_no_chunks_registers_its_metadata_only(spark_delta, tmp_path):
    base = tmp_path / "empty.zarr"
    g = zarr.open_group(str(base), mode="w", zarr_format=3)
    g.create_array("x", shape=(4,), chunks=(2,), dtype="f4", fill_value=0.0)
    table = _table("nochunks")
    gbx_zarr_register(spark_delta, str(base), table)
    rows = _live(spark_delta, table)
    assert set(rows.column("kind").to_pylist()) == {"group", "array", "commit"}


# --- identity ---------------------------------------------------------------------


def test_identity_is_floor_epoch_milliseconds_and_size():
    assert _register.identity_of(1_792_741_018_982, 3) == "1792741018982:3"


@pytest.mark.parametrize(
    "a, b, resolution, same",
    [
        ("1000:7", "1000:7", 1, True),
        ("1000:7", "1001:7", 1, False),  # exact at the default resolution
        ("1000:7", "1000:8", 1, False),  # size is always exact
        ("1000:7", "1999:7", 1000, True),  # the same second
        ("1000:7", "2000:7", 1000, False),
        ("1999:7", "2000:7", 1000, False),  # buckets, not tolerance
        ("1000:7", "1999:8", 1000, False),  # size is exact even when mtime is coarse
    ],
)
def test_same_identity_compares_size_exactly_and_mtime_at_a_resolution(
    a, b, resolution, same
):
    assert _register.same_identity(a, b, mtime_resolution_ms=resolution) is same


def test_same_identity_rejects_bad_input():
    with pytest.raises(ValueError, match="identity"):
        _register.same_identity("1000", "1000:7")
    with pytest.raises(ValueError, match="resolution"):
        _register.same_identity("1:2", "1:2", mtime_resolution_ms=0)


# --- markers, readers, probes and unreadable documents ----------------


def _failing_reads(monkeypatch, failures: int):
    """Make the first ``failures`` binary reads of an owner marker raise EIO."""
    real_open, failed = open, []

    def flaky(path, mode="r", *a, **k):
        if str(path).endswith(OWNER_MARKER) and mode == "rb" and len(failed) < failures:
            failed.append(path)
            raise OSError(5, "Input/output error")
        return real_open(path, mode, *a, **k)

    monkeypatch.setattr(_register, "open", flaky, raising=False)
    return failed


def test_a_marker_read_back_that_misses_once_is_retried_and_kept(tmp_path, monkeypatch):
    """Carry R1: a FUSE miss right after the write is retried, not reported. The marker is
    this call's, valid, and stays."""
    _no_sleep(monkeypatch)
    failed = _failing_reads(monkeypatch, 1)
    assert _register._claim_marker(str(tmp_path / "ov"), "c.s.t", "overlay") is True
    assert failed  # the first read-back did miss
    marker = json.loads((tmp_path / "ov" / OWNER_MARKER).read_text())
    assert marker["table"] == "c.s.t" and marker["kind"] == "overlay"


def test_a_marker_that_cannot_be_read_back_is_removed_not_left_behind(
    tmp_path, monkeypatch
):
    """Every read-back attempt fails: the marker this call made names this table and
    nobody can tell, so it goes (it holds exactly the bytes written), and the caller gets
    a typed error with a fix, not a claim of foreign ownership."""
    _no_sleep(monkeypatch)
    failed = _failing_reads(monkeypatch, 10)  # the read-back's ten attempts
    with pytest.raises(ZarrError, match="Input/output error") as ei:
        _claim_marker(str(tmp_path / "ov"), "c.s.t", "overlay")
    assert len(failed) == 10 and "Fix:" in str(ei.value)
    assert not isinstance(ei.value, ZarrOwnershipError)
    assert isinstance(ei.value.__cause__, OSError)
    assert not (tmp_path / "ov" / OWNER_MARKER).exists()
    monkeypatch.undo()
    _claim_marker(str(tmp_path / "ov"), "c.s.t", "overlay")  # nothing is in the way


def test_only_a_marker_holding_exactly_the_written_bytes_is_removed(tmp_path):
    """No prefix rule: a marker that merely starts with what this call wrote (or that this
    call's bytes merely start with) is someone else's."""
    marker = tmp_path / OWNER_MARKER
    marker.write_bytes(b'{"kind"')
    assert not _register._remove_marker_if_ours(str(marker), b'{"kind": "overlay"}')
    assert marker.exists()
    marker.write_bytes(b'{"kind": "overlay", "table": "x"}')
    assert not _register._remove_marker_if_ours(str(marker), b'{"kind"')
    assert marker.exists()
    assert _register._remove_marker_if_ours(str(marker), marker.read_bytes())
    assert not marker.exists()


def test_a_writer_checks_the_owner_marker_before_writing(tmp_path, monkeypatch):
    from databricks.labs.gbx.zarr._register import check_owner_marker

    _no_sleep(monkeypatch)
    with pytest.raises(ZarrOwnershipError, match="no owner marker") as ei:
        check_owner_marker(str(tmp_path), "c.s.t", "overlay")
    assert "Fix:" in str(ei.value)
    _claim_marker(str(tmp_path), "c.s.t", "overlay")
    check_owner_marker(str(tmp_path), "c.s.t", "overlay")  # it is ours
    with pytest.raises(ZarrOwnershipError, match="already owned"):
        check_owner_marker(str(tmp_path), "other.s.t", "overlay")
    _failing_reads(monkeypatch, 1)
    check_owner_marker(str(tmp_path), "c.s.t", "overlay")  # a one-off miss is retried


def test_a_zmetadata_that_misses_once_is_read_again(tmp_path, monkeypatch):
    """Carry R2: the attempt that finds .zgroup may miss a healthy .zmetadata once; the
    directory shows it, so it is read again and its consolidated row is not lost."""
    _no_sleep(monkeypatch)
    g = zarr.open_group(str(tmp_path / "s.zarr"), mode="w", zarr_format=2)
    g.create_array("a", shape=(2,), chunks=(2,), dtype="i4")
    zarr.consolidate_metadata(str(tmp_path / "s.zarr"))
    real, missed = _register._read_file, []

    def once(path):
        if path.endswith(".zmetadata") and not missed:
            missed.append(path)
            raise FileNotFoundError(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", once)
    fmt, found = _register._root_info(str(tmp_path / "s.zarr"))
    assert fmt == 2 and missed and found[".zmetadata"] is not None


def test_a_stale_hint_reads_the_other_node_documents_once(tmp_path, monkeypatch):
    """Optional carry: once a hinted document is missing, the hint is stale and the other
    names are read once, not retried with backoff."""
    _no_sleep(monkeypatch)
    reader = _register._Reader(str(tmp_path), hinted={"x/.zarray"})
    reads = []
    real = _register._read_file

    def count(path):
        reads.append(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", count)
    assert reader.probe("x", (".zarray", ".zgroup")) is None
    zgroup = [r for r in reads if r.endswith(".zgroup")]
    assert len(zgroup) == 1


def test_a_document_that_cannot_be_read_for_lack_of_permission_is_not_retried(
    tmp_path, monkeypatch
):
    calls = []

    def deny(path):
        calls.append(path)
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(_register, "_read_file", deny)
    with pytest.raises(ZarrError, match="permission denied") as ei:
        _register._Reader(str(tmp_path)).doc("a/zarr.json")
    assert "a/zarr.json" in str(ei.value) and "Fix:" in str(ei.value)
    assert isinstance(ei.value.__cause__, PermissionError)
    assert len(calls) == 1  # waiting would not help


def test_a_directory_that_cannot_be_listed_is_an_error_after_the_retries(
    tmp_path, monkeypatch
):
    _no_sleep(monkeypatch)
    calls = []

    def broken(path):
        calls.append(path)
        raise OSError(5, "Input/output error")

    monkeypatch.setattr(_register, "_list_dirs", broken)
    with pytest.raises(ZarrError, match="cannot list") as ei:
        _register._Reader(str(tmp_path)).dirs("grp")
    assert str(tmp_path / "grp") in str(ei.value) and "Fix:" in str(ei.value)
    assert isinstance(ei.value.__cause__, OSError)
    assert len(calls) == _register._PROBE_ATTEMPTS


def _count_reads(monkeypatch):
    """Every call of the metadata file read, by path relative to nothing: the full path."""
    real, reads = _register._read_file, []

    def read(path):
        reads.append(path)
        return real(path)

    monkeypatch.setattr(_register, "_read_file", read)
    return reads


def test_a_document_the_listing_proves_absent_is_probed_once(
    era5_v3_sharded, tmp_path, monkeypatch
):
    """Spark's listing shows every visible file of a Zarr v3 store, so a directory it holds
    no zarr.json for is not worth retrying. A hidden name never shows, and with no listing
    nothing is proven: both keep the retries."""
    base = tmp_path / "base.zarr"
    shutil.copytree(era5_v3_sharded, base)
    for stray in ("junk", "_hidden"):
        (base / stray).mkdir()
        (base / stray / "notes.txt").write_text("x")
    _no_sleep(monkeypatch)
    reads = _count_reads(monkeypatch)

    def probes(rel):
        return sum(p.endswith(f"/{rel}/zarr.json") for p in reads)

    _, arrays, _ = read_base_metadata(str(base), listing=_listing(base))
    assert set(arrays) == ERA5_ARRAYS
    assert probes("junk") == 1
    assert probes("_hidden") == _register._PROBE_ATTEMPTS
    reads.clear()
    read_base_metadata(str(base))  # no listing: nothing is proven absent
    assert probes("junk") == _register._PROBE_ATTEMPTS


def test_a_hidden_array_the_listing_cannot_show_still_survives_a_miss(
    tmp_path, monkeypatch
):
    base = _make_nested(tmp_path / "base.zarr")  # fc/_aux has a hidden name
    _no_sleep(monkeypatch)
    missed = _miss_once(monkeypatch, "fc/_aux/zarr.json")
    _, arrays, _ = read_base_metadata(str(base), listing=_listing(base))
    assert len(missed) == 1 and "fc/_aux" in arrays


def _node_type_store(base):
    ok = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [2],
        "data_type": "float32",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [2]}},
        "chunk_key_encoding": {"name": "default"},
        "fill_value": 0,
        "codecs": [],
    }
    return _write(
        base,
        {
            "zarr.json": V3_GROUP,
            "ok/zarr.json": ok,
            "weird/zarr.json": {"zarr_format": 3, "node_type": "banana"},
        },
    )


def test_a_bad_node_type_is_an_unsupported_array_to_a_reader_and_a_refusal_to_register(
    tmp_path,
):
    base = _node_type_store(tmp_path / "s.zarr")
    metas, arrays, unsupported = read_base_metadata(str(base), tolerant=True)
    assert set(arrays) == {"ok"} and set(unsupported) == {"weird"}
    assert "node_type" in unsupported["weird"] and "banana" in unsupported["weird"]
    assert "weird/zarr.json" in metas  # its document is kept
    with pytest.raises(ZarrUnsupported, match="node_type"):
        read_base_metadata(str(base))


def _two(base, fmt):
    g = zarr.open_group(str(base), mode="w", zarr_format=fmt)
    for name in ("a", "b"):
        kw = {"dimension_names": ("k",)} if fmt == 3 else {}
        arr = g.create_array(name, shape=(4,), chunks=(2,), dtype="f4", **kw)
        arr[:] = 1
    return base


@pytest.mark.parametrize("fmt", [2, 3])
def test_a_document_that_is_not_json_is_an_unsupported_array_in_either_format(
    tmp_path, fmt
):
    """One rule for both formats: the array is registered as one nobody can decode, and
    its neighbours are not lost."""
    base = _two(tmp_path / "s.zarr", fmt)
    (base / "b" / ("zarr.json" if fmt == 3 else ".zarray")).write_text("{not json")
    _, arrays, unsupported = read_base_metadata(str(base))
    assert set(arrays) == {"a"} and set(unsupported) == {"b"}
    assert "not valid JSON" in unsupported["b"]


def _corrupt_group(base):
    _make_nested(base)
    (base / "fc" / "zarr.json").write_text("{oops")
    return base


@pytest.mark.parametrize("tolerant", [False, True])
def test_a_corrupt_group_is_not_an_array_and_the_arrays_under_it_say_why(
    tmp_path, tolerant
):
    base = _corrupt_group(tmp_path / "s.zarr")
    metas, arrays, unsupported = read_base_metadata(str(base), tolerant=tolerant)
    assert set(arrays) == {"crs"}
    assert set(unsupported) == {"fc/x", "fc/_aux"}  # the group itself is not among them
    for reason in unsupported.values():
        assert "group document unreadable" in reason and "'fc'" in reason
        assert "not valid JSON" in reason
    assert "fc/zarr.json" in metas  # its document is kept


def test_asking_for_an_array_under_a_corrupt_group_is_a_typed_refusal(tmp_path):
    from databricks.labs.gbx.zarr import _read

    base = _corrupt_group(tmp_path / "s.zarr")
    src = _read._open_source(None, str(base), "path", None, None)
    assert "fc" not in src.unsupported and set(src.arrays) == {"crs"}
    with pytest.raises(ZarrUnsupported, match="group document unreadable") as ei:
        _read._choose_variables(["fc/x"], src)
    assert "'fc/x'" in str(ei.value) and "Fix:" in str(ei.value)
    with pytest.raises(ValueError, match="unknown variable"):
        _read._choose_variables(["fc"], src)  # the group is not a variable at all


def test_a_corrupt_group_registers_as_a_group_and_its_arrays_are_refused(
    spark_delta, tmp_path
):
    from databricks.labs.gbx.zarr._read import gbx_zarr_read

    base = _corrupt_group(tmp_path / "s.zarr")
    table = _table("badgroup")
    gbx_zarr_register(spark_delta, str(base), table)
    docs = {
        (r["array"], r["kind"])
        for r in _live(spark_delta, table, kinds=["group", "array"]).to_pylist()
    }
    assert ("fc", "group") in docs and ("fc/x", "array") in docs
    assert ("crs", "array") in docs and ("fc", "array") not in docs
    assert sorted(_commit_meta(spark_delta, table)["unsupported"]) == [
        "fc/_aux",
        "fc/x",
    ]
    assert gbx_zarr_read(spark_delta, table, variables=["crs"]).count() == 1
    with pytest.raises(ZarrUnsupported, match="group document unreadable"):
        gbx_zarr_read(spark_delta, table, variables=["fc/x"])
    spark_delta.sql(f"DROP TABLE IF EXISTS {table}")


# --- a marker stays with a table that may hold a commit ------------------


def test_discard_says_whether_it_dropped_the_table(monkeypatch):
    sql = []

    class Session:
        def sql(self, text):
            sql.append(text)

    wid = "a" * 32
    monkeypatch.setattr(_register, "find_commit_version", lambda s, t, w: None)
    assert _register._discard(Session(), "c.s.t", wid) is True and sql
    monkeypatch.setattr(_register, "find_commit_version", lambda s, t, w: 3)
    assert _register._discard(Session(), "c.s.t", wid) is False  # it committed

    def unknown(*a):
        raise RuntimeError("lookup failed")

    monkeypatch.setattr(_register, "find_commit_version", unknown)
    assert _register._discard(Session(), "c.s.t", wid) is False and len(sql) == 1


def test_a_registration_whose_commit_may_have_landed_keeps_its_marker(
    spark_delta, era5_v3_unsharded, tmp_path, monkeypatch
):
    real = _register.commit

    def landed_then_lost(*a, **k):
        real(*a, **k)
        raise RuntimeError("the connection dropped after the MERGE")

    def unknown(*a, **k):
        raise RuntimeError("lookup failed")

    monkeypatch.setattr(_register, "commit", landed_then_lost)
    monkeypatch.setattr(_register, "find_commit_version", unknown)
    table, overlay = _table("kept"), tmp_path / "overlay"
    with pytest.raises(RuntimeError, match="dropped after the MERGE"):
        gbx_zarr_register(
            spark_delta,
            str(era5_v3_unsharded),
            table,
            writable=True,
            overlay_root=str(overlay),
        )
    assert _exists(spark_delta, table) and (overlay / OWNER_MARKER).exists()
    spark_delta.sql(f"DROP TABLE IF EXISTS {table}")
