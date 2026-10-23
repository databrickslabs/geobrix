"""The read-only store over manifest rows: virtual shards, chunk overrides, range reads."""

import asyncio
import gc
import os
import pickle
import threading

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from zarr.abc.store import (  # noqa: E402
    OffsetByteRequest,
    RangeByteRequest,
    SuffixByteRequest,
)
from zarr.core.buffer import default_buffer_prototype  # noqa: E402

from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrReadOnly,
    ZarrStoreDrift,
)

PROTO = default_buffer_prototype()


def _run(coro):
    return asyncio.run(coro)


def _alist(agen):
    async def collect():
        return [k async for k in agen]

    return _run(collect())


def _get(store, key, byte_range=None):
    buf = _run(store.get(key, PROTO, byte_range))
    return None if buf is None else buf.to_bytes()


class Spy:
    """A reader that records every call and the thread it ran on."""

    def __init__(self, inner):
        self.inner, self.calls, self.threads = inner, [], set()

    def read(self, regions):
        self.calls.append(list(regions))
        self.threads.add(threading.get_ident())
        return self.inner.read(regions)

    def fetched(self):
        return sum(r.length for call in self.calls for r in call)


def test_manifest_store_reads_like_the_plain_store(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v3_sharded)), arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    want = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[:]
    np.testing.assert_array_equal(got, want)


def _registered(store, tmp_path):
    """A registered store: its own files are external, under ``store``; what GeoBrix
    adds (an override chunk) is under ``tmp_path``."""
    metas, rows, arrays = plain_store_rows(store, origin="external")
    return metas, rows, arrays, FuseRangeReader(str(tmp_path), base_root=str(store))


def test_chunk_row_overrides_one_inner_chunk(era5_v3_sharded, tmp_path):
    metas, rows, arrays, reader = _registered(era5_v3_sharded, tmp_path)
    rows2, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=7.0
    )
    st = ManifestStore(metas, rows2, reader, arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    np.testing.assert_array_equal(got, expected)
    assert (got[12:24, :37, 72:] == 7.0).all() and not (got == 7.0).all()


def test_an_override_equal_to_the_fill_value_is_still_written(
    era5_v3_sharded, tmp_path
):
    metas, rows, arrays, reader = _registered(era5_v3_sharded, tmp_path)
    fill = float(arrays["t2m"].fill_value)
    rows2, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=fill
    )
    assert (tmp_path / rows2[-1]["file"]).stat().st_size == rows2[-1]["length"] > 0
    st = ManifestStore(metas, rows2, reader, arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    np.testing.assert_array_equal(got, expected)
    np.testing.assert_array_equal(
        got[12:24, :37, 72:], np.full((12, 37, 72), fill, dtype="float32")
    )


def test_v2_unsharded_store(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="u10", mode="r")[:],
        zarr.open_array(str(era5_v2 / "u10"), mode="r")[:],
    )


def test_v3_unsharded_store(era5_v3_unsharded):
    metas, rows, arrays = plain_store_rows(era5_v3_unsharded)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v3_unsharded)), arrays)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:],
        zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")[:],
    )
    group = zarr.open_group(store=st, mode="r")  # coordinates too, and the group itself
    assert sorted(group.array_keys()) == ["latitude", "longitude", "t2m", "time", "u10"]


def test_a_shard_made_only_of_chunk_rows_reads_the_rest_as_fill(
    era5_v3_sharded, tmp_path
):
    metas, rows, arrays, reader = _registered(era5_v3_sharded, tmp_path)
    meta = arrays["t2m"]
    rows2, expected = rewrite_one_inner_chunk(
        rows, meta, tmp_path, chunk=(1, 0, 1), value=7.0
    )
    rows3 = [
        r
        for r in rows2
        if not (r["kind"] == "shard" and r["key"] == "c/0/0/0" and r["array"] == "t2m")
    ]
    assert len(rows3) == len(rows2) - 1
    st = ManifestStore(metas, rows3, reader, arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    # shard (0, 0, 0) lost its shard row: only the override survives in it
    lost = expected.copy()
    lost[:24, :74, :144] = np.float32(meta.fill_value)
    lost[12:24, :37, 72:] = 7.0
    np.testing.assert_array_equal(got, lost)
    assert _get(st, "t2m/c/0/0/0") is not None  # the key exists through the override


def test_external_rows_are_read_from_the_base_root(era5_v3_sharded, tmp_path):
    """A registered store keeps its own files under the base root; the origin of each
    row reaches the reader through the shard index read and every chunk region."""
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    external = [{**r, "origin": "external"} for r in rows]
    reader = FuseRangeReader(str(tmp_path / "overlay"), base_root=str(era5_v3_sharded))
    st = ManifestStore(metas, external, reader, arrays)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:],
        zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[:],
    )


def test_a_shard_whose_index_is_at_the_start(tmp_path):
    from zarr.codecs import ShardingCodec

    root = tmp_path / "start.zarr"
    data = np.arange(8 * 8, dtype="int32").reshape(8, 8)
    zarr.create_array(
        str(root),
        name="a",
        shape=data.shape,
        chunks=(4, 4),
        dtype="int32",
        serializer=ShardingCodec(chunk_shape=(2, 2), index_location="start"),
        compressors=None,
    )[:] = data
    metas, rows, arrays = plain_store_rows(root)
    assert arrays["a"].shard.index_location == "start"
    spy = Spy(FuseRangeReader(str(root)))
    st = ManifestStore(metas, rows, spy, arrays)
    arr = zarr.open_array(store=st, path="a", mode="r")
    np.testing.assert_array_equal(arr[:], data)
    np.testing.assert_array_equal(arr[1:3, 5:7], data[1:3, 5:7])


def test_a_partial_read_fetches_only_the_chunks_it_needs(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    spy = Spy(FuseRangeReader(str(era5_v3_sharded)))
    st = ManifestStore(metas, rows, spy, arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[0:5, 0:10, 0:10]
    want = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[0:5, 0:10, 0:10]
    np.testing.assert_array_equal(got, want)
    total = sum(r["length"] for r in rows if r["array"] == "t2m")
    assert 0 < spy.fetched() < 0.2 * total  # one chunk of sixteen, plus a small index


def test_blocking_reads_run_off_the_event_loop_thread(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    spy = Spy(FuseRangeReader(str(era5_v2)))
    st = ManifestStore(metas, rows, spy, arrays)
    assert _get(st, "u10/0.0.0") is not None
    assert spy.threads and threading.get_ident() not in spy.threads


@pytest.mark.parametrize(
    "fixture,chunk_key",
    [("era5_v3_sharded", "t2m/c/0/0/0"), ("era5_v2", "t2m/0.0.0")],
)
def test_listing_is_exactly_the_keys_of_the_plain_store(fixture, chunk_key, request):
    root = request.getfixturevalue(fixture)
    metas, rows, arrays = plain_store_rows(root)
    st = ManifestStore(metas, rows, FuseRangeReader(str(root)), arrays)
    on_disk = {
        p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()
    } - {".zmetadata"}
    assert set(_alist(st.list())) == on_disk
    assert set(_alist(st.list_prefix("t2m/"))) == {
        k for k in on_disk if k.startswith("t2m/")
    }
    assert set(_alist(st.list_dir(""))) == {k.split("/")[0] for k in on_disk}
    assert set(_alist(st.list_dir("t2m"))) == {
        k.split("/")[1] for k in on_disk if k.startswith("t2m/")
    }
    assert _run(st.exists(chunk_key)) and _run(
        st.exists("t2m/zarr.json" if "sharded" in fixture else "t2m/.zarray")
    )
    assert not _run(st.exists("t2m")) and not _run(st.exists("t2m/c/9/9/9"))


def test_get_honours_every_kind_of_byte_range(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v3_sharded)), arrays)
    for key in ("t2m/c/0/0/0", "t2m/zarr.json", "time/c/0"):
        whole = _get(st, key)
        n = len(whole)
        assert n > 40
        assert _get(st, key, RangeByteRequest(3, 30)) == whole[3:30]
        assert _get(st, key, RangeByteRequest(n - 5, n + 99)) == whole[n - 5 :]
        assert _get(st, key, OffsetByteRequest(11)) == whole[11:]
        assert _get(st, key, SuffixByteRequest(17)) == whole[-17:]
        assert _get(st, key, SuffixByteRequest(n + 50)) == whole
    assert _get(st, "t2m/c/9/9/9") is None
    assert _get(st, "nope/zarr.json") is None
    got = _run(st.get_partial_values(PROTO, [("t2m/zarr.json", None), ("nope", None)]))
    assert got[0].to_bytes() == metas["t2m/zarr.json"] and got[1] is None


def test_getsize_does_not_read_chunk_data(era5_v3_sharded, era5_v2):
    for root, chunk_key, meta_key in (
        (era5_v3_sharded, "t2m/c/0/0/0", "t2m/zarr.json"),
        (era5_v2, "u10/0.0.0", "u10/.zarray"),
    ):
        metas, rows, arrays = plain_store_rows(root)
        spy = Spy(FuseRangeReader(str(root)))
        st = ManifestStore(metas, rows, spy, arrays)
        size = _run(st.getsize(chunk_key))
        assert spy.fetched() < 0.1 * size  # at most a shard index
        assert size == len(_get(st, chunk_key))
        assert _run(st.getsize(meta_key)) == len(metas[meta_key])
    with pytest.raises(FileNotFoundError):
        _run(st.getsize("nope"))


def test_read_only(era5_v2):
    """zarr refuses to open a read-only store with a writing mode, and a write through
    an array opened with mode 'r' reaches the store, which refuses it."""
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    with pytest.raises(ValueError, match="read-only"):
        zarr.open_array(store=st, path="u10", mode="r+")
    with pytest.raises(ZarrReadOnly):
        zarr.open_array(store=st, path="u10", mode="r")[0, 0, 0] = 1.0


def test_the_store_itself_refuses_writes(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    assert st.read_only and st.with_read_only(True) is st
    assert not (st.supports_writes or st.supports_deletes or st.supports_partial_writes)
    assert st.supports_listing
    assert st == st and st != ManifestStore(metas, rows, FuseRangeReader("/x"), arrays)
    for call in (
        lambda: _run(st.set("u10/0.0.0", PROTO.buffer.from_bytes(b"x"))),
        lambda: _run(st.delete("u10/0.0.0")),
        lambda: st.with_read_only(False),
    ):
        with pytest.raises(ZarrReadOnly) as ei:
            call()
        assert "Fix:" in str(ei.value)


# --- tombstones -----------------------------------------------------------------


def _row(array, key, shard_key=None, **fields):
    base = dict(
        kind="chunk",
        array=array,
        key=key,
        shard_key=shard_key if shard_key is not None else key,
        file="pack.bin",
        offset=0,
        length=4,
        checksum=None,
        origin="gbx",
        identity=None,
        write_id="2" * 32,
        tag=None,
        meta=None,
    )
    return {**base, **fields}


def _tombstone(array, key, shard_key=None, **fields):
    """A deleted chunk: no file, nothing to read."""
    fields = {"file": None, "offset": None, "length": 0, **fields}
    return _row(array, key, shard_key, **fields)


@pytest.mark.parametrize("length", [0, None])
def test_a_tombstone_hides_one_inner_chunk_of_a_shard(era5_v3_sharded, length):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    meta = arrays["t2m"]
    gone = _tombstone("t2m", "c/1/0/1", "c/0/0/0", length=length)
    st = ManifestStore(
        metas, [*rows, gone], FuseRangeReader(str(era5_v3_sharded)), arrays
    )
    want = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[:]
    want[12:24, :37, 72:] = np.float32(meta.fill_value)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:], want
    )


def test_a_tombstone_makes_an_unsharded_chunk_key_absent(era5_v3_unsharded):
    metas, rows, arrays = plain_store_rows(era5_v3_unsharded)
    kept = [r for r in rows if not (r["array"] == "t2m" and r["key"] == "c/0/0/0")]
    assert len(kept) == len(rows) - 1
    st = ManifestStore(
        metas,
        [*kept, _tombstone("t2m", "c/0/0/0")],
        FuseRangeReader(str(era5_v3_unsharded)),
        arrays,
    )
    assert not _run(st.exists("t2m/c/0/0/0")) and _get(st, "t2m/c/0/0/0") is None
    assert "t2m/c/0/0/0" not in _alist(st.list())
    want = zarr.open_array(str(era5_v3_unsharded / "t2m"), mode="r")[:]
    want[:12, :37, :72] = np.float32(arrays["t2m"].fill_value)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:], want
    )


@pytest.mark.parametrize("shard_row", ["absent", "tombstone"])
def test_a_shard_with_only_tombstones_is_absent(era5_v3_sharded, shard_row):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    meta = arrays["t2m"]
    kept = [r for r in rows if not (r["array"] == "t2m" and r["key"] == "c/0/0/0")]
    extra = [_tombstone("t2m", "c/0/0/0", "c/0/0/0")]
    if shard_row == "tombstone":
        extra.append(_tombstone("t2m", "c/0/0/0", kind="shard"))
    st = ManifestStore(
        metas, [*kept, *extra], FuseRangeReader(str(era5_v3_sharded)), arrays
    )
    assert not _run(st.exists("t2m/c/0/0/0")) and _get(st, "t2m/c/0/0/0") is None
    assert "t2m/c/0/0/0" not in _alist(st.list())
    assert "t2m/c/1/0/0" in _alist(st.list())  # the other shard is untouched
    want = zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[:]
    want[:24] = np.float32(meta.fill_value)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:], want
    )


@pytest.mark.parametrize(
    "fields",
    [
        dict(length=None),  # a file with no length
        dict(offset=None),  # a file with no offset
        dict(length=0),  # a file and nothing to read from it
        dict(file=None, length=5),  # bytes to read and no file
        dict(offset=-1),
        dict(length=-4),
        dict(file=""),  # an empty name is not a path (and not a tombstone)
        dict(file="", length=0),
        dict(file="", length=None),
        dict(file="", offset=None),
        dict(file=7),
        dict(file="a\0b"),
    ],
)
@pytest.mark.parametrize("kind", ["chunk", "shard"])
def test_a_row_that_is_neither_a_tombstone_nor_a_region_is_drift(
    era5_v3_sharded, fields, kind
):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    # chunk (1, 0, 0) lies in shard (0, 0, 0); the shard row is its own shard's key
    shard_key = "c/0/0/0" if kind == "chunk" else "c/1/0/0"
    bad = _row("t2m", "c/1/0/0", shard_key, kind=kind, **fields)
    with pytest.raises(ZarrStoreDrift) as ei:
        ManifestStore(
            metas, [*rows, bad], FuseRangeReader(str(era5_v3_sharded)), arrays
        )
    assert "t2m/c/1/0/0" in str(ei.value) and "Fix:" in str(ei.value)


def test_an_unsharded_row_without_a_region_is_drift(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    bad = _row("u10", "0.0.0", length=None)
    with pytest.raises(ZarrStoreDrift):
        ManifestStore(metas, [bad], FuseRangeReader(str(era5_v2)), arrays)


# --- shard_key ------------------------------------------------------------------


def test_a_chunk_row_in_the_wrong_shard_is_drift(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    wrong = _row(
        "t2m", "c/1/0/1", "c/1/0/0"
    )  # chunk (1, 0, 1) belongs to shard (0, 0, 0)
    with pytest.raises(ZarrStoreDrift) as ei:
        ManifestStore(
            metas, [*rows, wrong], FuseRangeReader(str(era5_v3_sharded)), arrays
        )
    assert "c/1/0/1" in str(ei.value) and "gbx_zarr_sync" in ei.value.fix


def test_a_shard_row_whose_shard_key_is_not_its_key_is_drift(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    wrong = [
        (
            {**r, "shard_key": "c/1/0/0"}
            if r["array"] == "t2m" and r["key"] == "c/0/0/0"
            else r
        )
        for r in rows
    ]
    with pytest.raises(ZarrStoreDrift) as ei:
        ManifestStore(metas, wrong, FuseRangeReader(str(era5_v3_sharded)), arrays)
    assert "gbx_zarr_sync" in ei.value.fix


def test_a_null_shard_key_is_taken_from_the_key(era5_v3_sharded, tmp_path):
    metas, rows, arrays, reader = _registered(era5_v3_sharded, tmp_path)
    rows2, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=7.0
    )
    rows2[-1]["shard_key"] = None
    st = ManifestStore(metas, rows2, reader, arrays)
    np.testing.assert_array_equal(
        zarr.open_array(store=st, path="t2m", mode="r")[:], expected
    )


# --- containment, end to end ----------------------------------------------------


@pytest.mark.parametrize("how", ["absolute", "dotdot"])
def test_a_row_cannot_point_outside_the_store(era5_v3_unsharded, tmp_path, how):
    outside = tmp_path / "outside.bin"
    outside.write_bytes(bytes(range(100)))
    metas, rows, arrays = plain_store_rows(era5_v3_unsharded)
    target = (
        str(outside)
        if how == "absolute"
        else os.path.relpath(outside, era5_v3_unsharded)
    )
    rows = [
        (
            {**r, "file": target, "length": 100}
            if r["array"] == "t2m" and r["key"] == "c/0/0/0"
            else r
        )
        for r in rows
    ]
    reader = FuseRangeReader(str(era5_v3_unsharded), attempts=1)
    st = ManifestStore(metas, rows, reader, arrays)
    with pytest.raises(ZarrStoreDrift) as ei:
        _get(st, "t2m/c/0/0/0")
    assert "resolves outside the store root" in str(ei.value)
    assert _get(st, "t2m/c/0/0/1") is not None  # the other chunks still read


# --- one pack file --------------------------------------------------------------


def test_shard_and_chunk_rows_share_one_pack_file_at_non_zero_offsets(
    era5_v3_sharded, tmp_path
):
    """The writer's layout: many objects back to back in one file, none at offset 0."""
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    meta = arrays["t2m"]
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    rows2, expected = rewrite_one_inner_chunk(
        rows, meta, scratch, chunk=(1, 0, 1), value=7.0
    )
    override = rows2[-1]
    parts = {
        "c/0/0/0": (era5_v3_sharded / "t2m/c/0/0/0").read_bytes(),
        "c/1/0/0": (era5_v3_sharded / "t2m/c/1/0/0").read_bytes(),
        "override": (scratch / override["file"]).read_bytes(),
    }
    pack, at = bytearray(b"\xaa" * 37), {}
    for name, data in parts.items():
        at[name] = (len(pack), len(data))
        pack += data + b"\x55" * 11  # a gap after each object
    (tmp_path / "pack.bin").write_bytes(bytes(pack))
    assert all(offset > 0 for offset, _ in at.values())

    packed = [
        _row(
            "t2m",
            "c/0/0/0",
            kind="shard",
            offset=at["c/0/0/0"][0],
            length=at["c/0/0/0"][1],
        ),
        _row(
            "t2m",
            "c/1/0/0",
            kind="shard",
            offset=at["c/1/0/0"][0],
            length=at["c/1/0/0"][1],
        ),
        _row(
            "t2m",
            override["key"],
            override["shard_key"],
            offset=at["override"][0],
            length=at["override"][1],
        ),
    ]
    only_t2m = {
        k: v for k, v in metas.items() if k == "zarr.json" or k.startswith("t2m/")
    }
    st = ManifestStore(only_t2m, packed, FuseRangeReader(str(tmp_path)), {"t2m": meta})
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    np.testing.assert_array_equal(got, expected)


# --- lifecycle ------------------------------------------------------------------


def test_the_store_pickles_after_reads(era5_v3_sharded):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v3_sharded)), arrays)
    first = _get(st, "t2m/c/0/0/0")  # starts the pool and caches the shard
    clone = pickle.loads(pickle.dumps(st))
    assert clone is not st and clone != st and clone.read_only
    assert _get(clone, "t2m/c/0/0/0") == first
    np.testing.assert_array_equal(
        zarr.open_array(store=clone, path="t2m", mode="r")[:],
        zarr.open_array(str(era5_v3_sharded / "t2m"), mode="r")[:],
    )


def test_a_pickled_closed_store_is_still_closed(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    assert _get(st, "u10/0.0.0")
    st.close()
    clone = pickle.loads(pickle.dumps(st))
    assert clone._closed
    with pytest.raises(ZarrError) as ei:
        _get(clone, "u10/0.0.0")
    assert "closed" in str(ei.value) and "Fix:" in str(ei.value)
    # an open store stays open through a round trip
    live = pickle.loads(
        pickle.dumps(ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays))
    )
    assert not live._closed and _get(live, "u10/0.0.0")


def test_a_closed_store_refuses_reads_instead_of_restarting_its_pool(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    assert _get(st, "u10/0.0.0")
    st.close()
    with pytest.raises(ZarrError) as ei:
        _get(st, "u10/0.0.0")
    assert "closed" in str(ei.value) and "Fix:" in str(ei.value)


def test_the_pool_is_shut_down_when_the_store_is_collected(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    assert _get(st, "u10/0.0.0")
    pool = st._pool
    del st
    gc.collect()
    with pytest.raises(RuntimeError):
        pool.submit(int)


def test_the_shard_cache_is_bounded(era5_v3_sharded, monkeypatch):
    monkeypatch.setattr(ManifestStore, "_SHARD_CACHE_SIZE", 1)
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    spy = Spy(FuseRangeReader(str(era5_v3_sharded)))
    st = ManifestStore(metas, rows, spy, arrays)
    a, b = _get(st, "t2m/c/0/0/0"), _get(st, "t2m/c/1/0/0")
    assert len(st._virtual) == 1
    assert _get(st, "t2m/c/0/0/0") == a and _get(st, "t2m/c/1/0/0") == b
    assert len(st._virtual) == 1
    # every read of an evicted shard reads its index again: index + chunk bytes, 4 times
    assert len(spy.calls) == 8


def test_directory_deletes_are_read_only_errors_too(era5_v2):
    metas, rows, arrays = plain_store_rows(era5_v2)
    st = ManifestStore(metas, rows, FuseRangeReader(str(era5_v2)), arrays)
    for call in (lambda: st.delete_dir("u10"), st.clear):
        with pytest.raises(ZarrReadOnly) as ei:
            _run(call())
        assert "Fix:" in str(ei.value)
