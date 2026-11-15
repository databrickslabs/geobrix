"""Pack files and inner-chunk encoding: what a write puts in the store."""

import numpy as np
import pytest

zarr = pytest.importorskip("zarr")

from databricks.labs.gbx.zarr._access import FuseRangeReader  # noqa: E402
from databricks.labs.gbx.zarr._engine import open_manifest_array, read_box  # noqa: E402
from databricks.labs.gbx.zarr._keys import shard_of  # noqa: E402
from databricks.labs.gbx.zarr._pack import (  # noqa: E402
    PackWriter,
    crc32c_hex,
    encode_inner_chunk,
    is_fill,
)
from databricks.labs.gbx.zarr._store import ManifestStore  # noqa: E402
from databricks.labs.gbx.zarr._testing import plain_store_rows  # noqa: E402


def _override(rows, meta, chunk, file, offset, length):
    """``rows`` plus one gbx chunk row for ``chunk`` at ``file[offset:offset+length]``."""
    key = meta.key_encoding.chunk_key(chunk)
    shard_key = (
        meta.key_encoding.chunk_key(shard_of(chunk, meta.shard.cps))
        if meta.shard is not None
        else None
    )
    kept = [r for r in rows if not (r["array"] == meta.name and r["key"] == key)]
    return kept + [
        dict(
            kind="chunk",
            array=meta.name,
            key=key,
            shard_key=shard_key,
            file=file,
            offset=offset,
            length=length,
            origin="gbx",
        )
    ]


@pytest.mark.parametrize("layout", ["era5_v3_sharded", "era5_v3_unsharded", "era5_v2"])
def test_encode_inner_chunk_round_trip(request, tmp_path, layout):
    """Chunks encoded alone, packed in one file and laid over the store as chunk rows,
    read back through ``ManifestStore`` as the blocks written; an edge chunk (36 of 37
    latitudes) is padded with the fill value like zarr pads it."""
    store = request.getfixturevalue(layout)
    metas, rows, arrays = plain_store_rows(store, origin="external")
    meta = arrays["t2m"]
    rng = np.random.default_rng(3)
    chunks = [(1, 0, 1), (2, 1, 0)]  # (2, 1, 0) is an edge chunk along latitude
    blocks = {}
    writer = PackWriter(
        str(tmp_path / "data" / "t2m" / "w1"), reader_root=str(tmp_path)
    )
    placed = []
    for chunk in chunks:
        shape = tuple(
            min(c, n - k * c) for k, c, n in zip(chunk, meta.chunks, meta.shape)
        )
        block = rng.normal(size=shape).astype(meta.dtype)
        blocks[chunk] = block
        data = encode_inner_chunk(meta, block)
        placed.append((chunk, *writer.add(data)))
    path = writer.close()
    assert path.startswith("data/t2m/w1/part-") and path.endswith(".zpack")
    for chunk, offset, length in placed:
        rows = _override(rows, meta, chunk, path, offset, length)
    ms = ManifestStore(
        metas, rows, FuseRangeReader(str(tmp_path), base_root=str(store)), arrays
    )
    try:
        arr = open_manifest_array(ms, "t2m")
        for chunk, block in blocks.items():
            box = tuple(
                slice(k * c, k * c + n)
                for k, c, n in zip(chunk, meta.chunks, block.shape)
            )
            np.testing.assert_array_equal(read_box(arr, box), block)
        untouched = (slice(0, 12), slice(0, 37), slice(0, 72))
        np.testing.assert_array_equal(
            read_box(arr, untouched),
            zarr.open_array(str(store / "t2m"), mode="r")[untouched],
        )
    finally:
        ms.close()


def test_a_chunk_is_encoded_as_zarr_stores_it(era5_v3_unsharded):
    """The bytes are a standard chunk: zarr decodes them with the array's own codecs."""
    from zarr.core.buffer import default_buffer_prototype
    from zarr.storage import MemoryStore

    from databricks.labs.gbx.zarr._meta import inner_meta_doc

    _, _, arrays = plain_store_rows(era5_v3_unsharded)
    meta = arrays["t2m"]
    block = np.arange(np.prod(meta.chunks), dtype="float32").reshape(meta.chunks)
    data = encode_inner_chunk(meta, block)
    import json

    proto = default_buffer_prototype()
    held = {
        "zarr.json": proto.buffer.from_bytes(json.dumps(inner_meta_doc(meta)).encode()),
        meta.key_encoding.chunk_key((0, 0, 0)): proto.buffer.from_bytes(data),
    }
    got = zarr.open_array(store=MemoryStore(store_dict=held), mode="r")[...]
    np.testing.assert_array_equal(got, block)


def test_a_pack_is_one_file_of_the_added_bytes(tmp_path):
    dest = tmp_path / "root" / "data" / "a" / "w"
    writer = PackWriter(str(dest), reader_root=str(tmp_path / "root"))
    assert writer.add(b"abc") == (0, 3)
    assert writer.add(memoryview(b"defgh")) == (3, 5)
    assert not dest.exists()  # staged locally until it is closed
    path = writer.close()
    assert writer.close() == path  # closing again changes nothing
    files = list(dest.iterdir())
    assert [f.name for f in files] == [path.rpartition("/")[2]]
    assert files[0].read_bytes() == b"abcdefgh"
    with pytest.raises(ValueError):
        writer.add(b"x")


def test_an_empty_or_aborted_pack_makes_no_file(tmp_path):
    empty = PackWriter(str(tmp_path / "d"), reader_root=str(tmp_path))
    assert empty.close() is None and not (tmp_path / "d").exists()
    aborted = PackWriter(str(tmp_path / "e"), reader_root=str(tmp_path))
    aborted.add(b"bytes")
    aborted.abort()
    assert not (tmp_path / "e").exists()


def test_crc32c_and_fill_checks():
    assert crc32c_hex(b"123456789") == "e3069283"  # the CRC-32C check value
    assert is_fill(np.full((2, 2), np.nan, "f4"), float("nan"))
    assert not is_fill(np.array([np.nan, 1.0]), float("nan"))
    assert is_fill(np.zeros(3, "i2"), 0) and not is_fill(np.array([0, 1]), 0)
    assert not is_fill(np.zeros(3), None)  # no fill value: nothing is fill


def test_a_commit_looks_its_packs_up_side_by_side(monkeypatch):
    """``require_packs`` asks for every pack at once (a mount stat or an object head is a
    round trip each), and still names the ones that are gone."""
    import threading
    import time

    from databricks.labs.gbx.zarr import _pack
    from databricks.labs.gbx.zarr.errors import ZarrError

    threads, lock = set(), threading.Lock()

    def stat(path):
        with lock:
            threads.add(threading.get_ident())
        time.sleep(0.05)
        if path.endswith("gone.zpack"):
            raise FileNotFoundError(path)

    monkeypatch.setattr(_pack, "stat_for", lambda access: stat)
    files = [f"data/t/{i}.zpack" for i in range(8)]
    _pack.require_packs("/r", files, what="write")
    assert len(threads) > 1
    with pytest.raises(ZarrError, match=r"1 pack file\(s\).*data/t/gone.zpack"):
        _pack.require_packs("/r", [*files, "data/t/gone.zpack"], what="write")


def test_a_pack_lookup_error_that_is_not_a_missing_file_fails_the_check(monkeypatch):
    """Looked up side by side, a pack whose lookup fails for another reason (a refused
    request) raises that error: it is not reported as gone, nor swallowed."""
    from databricks.labs.gbx.zarr import _pack

    def stat(path):
        if path.endswith("3.zpack"):
            raise PermissionError(13, "refused", path)

    monkeypatch.setattr(_pack, "stat_for", lambda access: stat)
    with pytest.raises(PermissionError, match="refused"):
        _pack.require_packs("/r", [f"data/t/{i}.zpack" for i in range(8)], what="write")
