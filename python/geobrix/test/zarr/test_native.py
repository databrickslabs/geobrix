"""The native engine: a box of a manifest array read as batched byte ranges and decoded in a pool."""

import copy
import json
import os
import threading
import tracemalloc

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.zarr import _native  # noqa: E402
from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box  # noqa: E402
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import (  # noqa: E402
    plain_store_rows,
    rewrite_one_inner_chunk,
)
from databricks.labs.gbx.zarr.errors import (  # noqa: E402
    ZarrError,
    ZarrStoreDrift,
    ZarrUnsupported,
)

FIXTURES = ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"]
BOXES = [
    (
        slice(0, 5),
        slice(60, 73),
        slice(140, 144),
    ),  # crosses the edge chunk of every axis
    (slice(11, 13), slice(36, 38), slice(71, 73)),  # straddles a chunk corner
    (slice(0, 48), slice(0, 73), slice(0, 144)),  # everything
    (slice(23, 25), slice(None), slice(None)),  # one slab across a shard boundary
    (slice(7, 8), slice(5, 6), slice(9, 10)),  # one value
]


class Spy:
    """A reader that records every call."""

    def __init__(self, inner):
        self.inner, self.calls = inner, []

    def read(self, regions):
        self.calls.append(list(regions))
        return self.inner.read(regions)


def _direct(path, name="t2m"):
    return zarr.open_array(str(path / name), mode="r")[:]


def _store(path, reader=None):
    metas, rows, arrays = plain_store_rows(path)
    return ManifestStore(metas, rows, reader or FuseRangeReader(str(path)), arrays)


@pytest.mark.parametrize("fixture", FIXTURES)
def test_native_reads_like_zarr_direct(fixture, request):
    path = request.getfixturevalue(fixture)
    store = _store(path)
    for name in ("t2m", "u10"):
        want = _direct(path, name)
        arr = open_manifest_array(store, name, pipeline="native")
        assert arr.shape == want.shape and np.dtype(arr.dtype) == want.dtype
        for box in BOXES:
            got = read_box(arr, box)
            np.testing.assert_array_equal(got, want[box])
            assert got.dtype == want.dtype


@pytest.mark.parametrize("fixture", FIXTURES)
def test_native_reads_the_coordinates_too(fixture, request):
    path = request.getfixturevalue(fixture)
    store = _store(path)
    for name in ("time", "latitude", "longitude"):
        arr = open_manifest_array(store, name, pipeline="native")
        np.testing.assert_array_equal(
            read_box(arr, (slice(None),)), _direct(path, name)
        )


def test_native_reads_overrides_tombstones_and_gaps_like_the_default_pipeline(
    era5_v3_sharded, tmp_path
):
    metas, rows, arrays = plain_store_rows(era5_v3_sharded, origin="external")
    meta = arrays["t2m"]
    rows2, _ = rewrite_one_inner_chunk(rows, meta, tmp_path, chunk=(1, 0, 1), value=7.0)
    tomb = dict(rows2[-1], file=None, offset=None, length=0, write_id="3" * 32)
    tomb["key"] = meta.key_encoding.chunk_key((0, 1, 0))
    tomb["shard_key"] = meta.key_encoding.chunk_key((0, 0, 0))
    gone = [r for r in rows2 if not (r["array"] == "t2m" and r["key"] == "c/1/0/0")]
    reader = FuseRangeReader(str(tmp_path), base_root=str(era5_v3_sharded))
    st = ManifestStore(metas, [*gone, tomb], reader, arrays)
    want = zarr.open_array(store=st, path="t2m", mode="r")[:]
    fill = np.float32(meta.fill_value)  # may be NaN: compare with assert_array_equal
    assert (want[12:24, :37, 72:] == 7.0).all()  # the override
    np.testing.assert_array_equal(want[:12, 37:74, :72], fill)  # the tombstone
    np.testing.assert_array_equal(want[24:], fill)  # the absent shard
    arr = open_manifest_array(st, "t2m", pipeline="native")
    np.testing.assert_array_equal(read_box(arr, (slice(None),) * 3), want)
    for box in BOXES:
        np.testing.assert_array_equal(read_box(arr, box), want[box])


def test_native_reads_the_files_of_a_box_once_each(era5_v3_sharded):
    """A box inside one shard asks the file for its index and then for all its chunks in
    one call: one open per file and per call, not one per chunk."""
    spy = Spy(FuseRangeReader(str(era5_v3_sharded)))
    st = _store(era5_v3_sharded, spy)
    arr = open_manifest_array(st, "t2m", pipeline="native")
    got = read_box(arr, (slice(0, 1), slice(None), slice(None)))  # 4 chunks, 1 shard
    np.testing.assert_array_equal(got, _direct(era5_v3_sharded)[0:1])
    assert len(spy.calls) == 2  # the shard's index, then the four chunks together
    assert sorted(len(c) for c in spy.calls) == [1, 4]


def test_native_open_count_follows_files_not_chunks(era5_v3_sharded, monkeypatch):
    opens = []
    real = os.open

    def counting(path, *a, **k):
        opens.append(path)
        return real(path, *a, **k)

    monkeypatch.setattr(os, "open", counting)
    st = _store(era5_v3_sharded)
    arr = open_manifest_array(st, "t2m", pipeline="native")
    read_box(arr, (slice(None),) * 3)  # 8 chunks in 2 shards
    shards = {p for p in opens}
    assert len(shards) == 2 and len(opens) <= 4  # index + data per shard at most


def test_native_is_the_same_from_many_threads(era5_v3_sharded):
    st = _store(era5_v3_sharded)
    arr = open_manifest_array(st, "t2m", pipeline="native")
    want = _direct(era5_v3_sharded)
    errors, results = [], {}

    def run(i):
        try:
            box = BOXES[i % len(BOXES)]
            results[i] = np.array_equal(read_box(arr, box), want[box])
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and all(results.values()) and len(results) == 20


def test_native_surfaces_drift_from_the_reader(era5_v3_unsharded):
    metas, rows, arrays = plain_store_rows(era5_v3_unsharded)
    broken = [
        (
            {**r, "file": "no/such/file"}
            if r["array"] == "t2m" and r["key"] == "c/0/0/0"
            else r
        )
        for r in rows
    ]
    st = ManifestStore(
        metas, broken, FuseRangeReader(str(era5_v3_unsharded), attempts=1), arrays
    )
    arr = open_manifest_array(st, "t2m", pipeline="native")
    with pytest.raises(ZarrError) as ei:
        read_box(arr, (slice(0, 5), slice(0, 5), slice(0, 5)))
    assert "no/such/file" in str(ei.value) and "Fix:" in str(ei.value)
    # a box that does not touch the missing file still reads
    read_box(arr, (slice(12, 20), slice(0, 5), slice(0, 5)))


def test_native_reports_a_chunk_that_does_not_decode_as_drift(
    era5_v3_unsharded, tmp_path
):
    junk = tmp_path / "junk.bin"
    junk.write_bytes(bytes(range(200)))
    metas, rows, arrays = plain_store_rows(era5_v3_unsharded)
    broken = [
        (
            {**r, "file": str(junk), "length": 200}
            if r["array"] == "t2m" and r["key"] == "c/0/0/0"
            else r
        )
        for r in rows
    ]
    reader = FuseRangeReader(str(era5_v3_unsharded), base_root=str(tmp_path))
    arr = open_manifest_array(
        ManifestStore(metas, broken, reader, arrays), "t2m", pipeline="native"
    )
    with pytest.raises(ZarrStoreDrift) as ei:
        read_box(arr, (slice(0, 5), slice(0, 5), slice(0, 5)))
    assert "(0, 0, 0)" in str(ei.value) and "could not be decoded" in str(ei.value)
    assert "Fix:" in str(ei.value)


@pytest.mark.parametrize("sharded", [False, True], ids=["one file per chunk", "shards"])
def test_native_holds_only_a_bounded_window_of_compressed_bytes(
    tmp_path, monkeypatch, sharded
):
    """A box of many chunks is read through a window, not all at once: the memory above
    the result stays far below the stored size (incompressible data: stored ~ decoded).
    """
    shape, chunks = (64, 256, 256), (4, 32, 32)
    full = np.random.default_rng(5).random(shape, dtype=np.float32)
    group = zarr.open_group(str(tmp_path / "s.zarr"), mode="w", zarr_format=3)
    arr = group.create_array(
        name="a",
        shape=shape,
        chunks=chunks,
        dtype="f4",
        shards=(16, 128, 128) if sharded else None,
        compressors=[zarr.codecs.ZstdCodec(level=1)],
    )
    arr[:] = full
    stored = sum(
        f.stat().st_size
        for f in (tmp_path / "s.zarr" / "a").rglob("*")
        if f.is_file() and f.name != "zarr.json"
    )
    assert stored > 14 << 20
    monkeypatch.setattr(_native, "_INFLIGHT_BYTES", 512 << 10)
    st = _store(tmp_path / "s.zarr")
    native = open_manifest_array(st, "a", pipeline="native")
    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        got = read_box(native, (slice(None),) * 3)
        current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    np.testing.assert_array_equal(got, full)
    extra = peak - current  # what was held beyond the result
    assert extra < stored // 4, f"{extra} bytes above the result, stored {stored}"


def test_native_refuses_a_call_from_the_stores_own_threads(era5_v3_sharded):
    """The store's pool runs the reads; a read that waits on that pool from inside it
    would deadlock once the pool is busy, so it is refused."""
    st = _store(era5_v3_sharded)
    arr = open_manifest_array(st, "t2m", pipeline="native")
    box = (slice(None),) * 3
    inside_read = st._executor().submit(read_box, arr, box)
    with pytest.raises(ZarrError) as ei:
        inside_read.result(timeout=30)
    assert "Fix:" in str(ei.value) and "own threads" in str(ei.value)
    coords = [(0, 0, 0), (2, 0, 0)]  # two shards
    inside_regions = st._executor().submit(st.chunk_regions, "t2m", coords)
    with pytest.raises(ZarrError):
        inside_regions.result(timeout=30)
    # from the caller's own thread both work
    assert read_box(arr, box).shape == (48, 73, 144)
    assert len(st.chunk_regions("t2m", coords)) == 2


def test_a_memory_error_in_a_decode_is_not_reported_as_a_corrupt_file(era5_v2):
    arr = open_manifest_array(_store(era5_v2), "t2m", pipeline="native")

    def out_of_memory(data):
        raise MemoryError("no room")

    arr._decode = out_of_memory
    with pytest.raises(MemoryError):
        read_box(arr, (slice(0, 2),))


def test_a_closed_store_refuses_native_reads(era5_v2):
    st = _store(era5_v2)
    arr = open_manifest_array(st, "t2m", pipeline="native")
    st.close()
    with pytest.raises(ZarrError) as ei:
        read_box(arr, (slice(0, 2),))
    assert "closed" in str(ei.value) and "Fix:" in str(ei.value)


def _with_codecs(path, array, codecs):
    """The fixture's store with ``array``'s codec chain replaced (a read is not attempted)."""
    metas, rows, arrays = plain_store_rows(path)
    from databricks.labs.gbx.zarr._meta import parse_v3

    doc = copy.deepcopy(arrays[array].doc)
    doc["codecs"] = codecs
    metas = dict(metas, **{f"{array}/zarr.json": json.dumps(doc).encode()})
    arrays = dict(arrays, **{array: parse_v3(array, doc)})
    return ManifestStore(metas, rows, FuseRangeReader(str(path)), arrays)


@pytest.mark.parametrize(
    "codecs",
    [
        [
            {"name": "transpose", "configuration": {"order": [2, 1, 0]}},
            {"name": "bytes", "configuration": {"endian": "little"}},
        ],
        [
            {"name": "bytes", "configuration": {"endian": "little"}},
            {"name": "bitround", "configuration": {"keepbits": 8}},
        ],
    ],
)
def test_native_refuses_a_codec_it_does_not_decode(era5_v3_unsharded, codecs):
    st = _with_codecs(era5_v3_unsharded, "t2m", codecs)
    with pytest.raises(ZarrUnsupported) as ei:
        open_manifest_array(st, "t2m", pipeline="native")
    assert "Fix:" in str(ei.value) and "pipeline='default'" in ei.value.fix


def test_auto_always_returns_the_same_kind_of_array(era5_v3_unsharded):
    """``auto`` is native or, for a codec native does not decode, zarr's pipeline; either
    way the caller gets an object with ``shape``, ``dtype``, ``engine`` and ``pipeline``.
    """
    codecs = [
        {"name": "transpose", "configuration": {"order": [2, 1, 0]}},
        {"name": "bytes", "configuration": {"endian": "little"}},
    ]
    st = _with_codecs(era5_v3_unsharded, "t2m", codecs)
    fell_back = open_manifest_array(st, "t2m")
    usual = open_manifest_array(st, "u10")
    assert not isinstance(fell_back, zarr.Array) and not isinstance(usual, zarr.Array)
    assert (fell_back.engine, fell_back.pipeline) == ("manifest", "default")
    assert (usual.engine, usual.pipeline) == ("manifest", "native")
    assert fell_back.shape == usual.shape == (48, 73, 144)
