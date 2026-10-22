"""The read-only store over manifest rows: virtual shards, chunk overrides, range reads."""

import asyncio
import os
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
from databricks.labs.gbx.zarr._shard import Region  # noqa: E402
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import ZarrReadOnly, ZarrStoreDrift  # noqa: E402

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


def test_chunk_row_overrides_one_inner_chunk(era5_v3_sharded, tmp_path):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
    rows2, expected = rewrite_one_inner_chunk(
        rows, arrays["t2m"], tmp_path, chunk=(1, 0, 1), value=7.0
    )
    st = ManifestStore(metas, rows2, FuseRangeReader(str(era5_v3_sharded)), arrays)
    got = zarr.open_array(store=st, path="t2m", mode="r")[:]
    np.testing.assert_array_equal(got, expected)
    assert (got[12:24, :37, 72:] == 7.0).all() and not (got == 7.0).all()


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
    metas, rows, arrays = plain_store_rows(era5_v3_sharded)
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
    st = ManifestStore(metas, rows3, FuseRangeReader(str(era5_v3_sharded)), arrays)
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


# --- the FUSE range reader ------------------------------------------------------


@pytest.fixture
def packs(tmp_path):
    (tmp_path / "base").mkdir()
    (tmp_path / "over").mkdir()
    (tmp_path / "base" / "a.bin").write_bytes(bytes(range(100)))
    (tmp_path / "over" / "b.bin").write_bytes(bytes(range(100, 200)))
    return tmp_path


def test_reader_resolves_relative_files_and_external_origin(packs):
    reader = FuseRangeReader(str(packs / "over"), base_root=str(packs / "base"))
    got = reader.read(
        [
            Region("b.bin", 2, 3),
            Region("a.bin", 10, 4, "external"),
            Region(str(packs / "base" / "a.bin"), 0, 2),  # absolute: used as given
        ]
    )
    assert got == [bytes([102, 103, 104]), bytes([10, 11, 12, 13]), bytes([0, 1])]
    # without a base_root an external file resolves under the root
    assert FuseRangeReader(str(packs / "base")).read(
        [Region("a.bin", 5, 2, "external")]
    ) == [bytes([5, 6])]


def test_reader_coalesces_adjacent_ranges_and_keeps_the_order(packs, monkeypatch):
    preads = []
    real = os.pread

    def counting(fd, n, offset):
        preads.append((offset, n))
        return real(fd, n, offset)

    monkeypatch.setattr(os, "pread", counting)
    reader = FuseRangeReader(str(packs / "base"))
    regions = [
        Region("a.bin", 30, 10),  # adjacent to the next one in file order
        Region("a.bin", 10, 20),
        Region("a.bin", 50, 5),  # a gap: its own read
        Region("a.bin", 35, 10),  # overlaps the first
        Region("a.bin", 7, 0),  # empty
    ]
    got = reader.read(regions)
    assert got == [
        bytes(range(30, 40)),
        bytes(range(10, 30)),
        bytes(range(50, 55)),
        bytes(range(35, 45)),
        b"",
    ]
    assert preads == [(10, 35), (50, 5)]


def test_reader_reports_a_missing_or_short_file_as_drift(packs):
    reader = FuseRangeReader(str(packs / "base"), attempts=1)
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("gone.bin", 0, 4)])
    assert "gone.bin" in str(ei.value) and "gbx_zarr_sync" in ei.value.fix
    with pytest.raises(ZarrStoreDrift) as ei:
        reader.read([Region("a.bin", 90, 20)])  # the file has 100 bytes
    assert "a.bin" in str(ei.value) and "Fix:" in str(ei.value)
