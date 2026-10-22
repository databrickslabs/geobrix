"""The virtual shard: a shard index synthesized over chunk regions in other files."""

import numpy as np
import pytest

from databricks.labs.gbx.zarr._shard import (
    MISSING,
    Region,
    VirtualShard,
    decode_index,
    encode_index,
    index_nbytes,
)
from databricks.labs.gbx.zarr.errors import ZarrStoreDrift, ZarrUnsupported

CODECS = ({"name": "bytes", "configuration": {"endian": "little"}}, {"name": "crc32c"})


def test_index_round_trip_and_checksum():
    e = np.array([[0, 10], [MISSING, MISSING], [10, 5], [15, 7]], dtype="<u8")
    buf = encode_index(e, CODECS)
    assert len(buf) == index_nbytes(4, CODECS) == 68
    assert (decode_index(buf, 4, CODECS) == e).all()
    bad = bytearray(buf)
    bad[0] ^= 1
    with pytest.raises(ZarrStoreDrift):
        decode_index(bytes(bad), 4, CODECS)


def test_virtual_shard_layout_and_pieces():
    vs = VirtualShard(4, {0: Region("a", 100, 10), 3: Region("b", 0, 7)}, CODECS, "end")
    assert vs.size == 10 + 7 + 68
    idx = decode_index(b"".join(p for p in vs.pieces(vs.size - 68, vs.size)), 4, CODECS)
    assert idx[0].tolist() == [0, 10] and idx[3].tolist() == [10, 7]
    assert idx[1].tolist() == [MISSING, MISSING]
    ps = vs.pieces(5, 12)
    assert ps == [Region("a", 105, 5), Region("b", 0, 2)]


def test_a_shard_with_its_index_at_the_start_offsets_chunks_past_the_index():
    vs = VirtualShard(2, {0: Region("a", 0, 6), 1: Region("b", 9, 4)}, CODECS, "start")
    ib = index_nbytes(2, CODECS)
    assert vs.size == ib + 10
    index = b"".join(vs.pieces(0, ib))
    assert decode_index(index, 2, CODECS).tolist() == [[ib, 6], [ib + 6, 4]]
    # a range that straddles the index, the first chunk and the second one
    assert vs.pieces(ib - 2, ib + 8) == [
        index[-2:],
        Region("a", 0, 6),
        Region("b", 9, 2),
    ]
    assert vs.pieces(vs.size, vs.size + 5) == [] and vs.pieces(3, 3) == []


def test_pieces_clamp_to_the_shard_and_the_whole_shard_covers_every_byte():
    regions = {1: Region("a", 0, 3), 2: Region("b", 5, 2), 4: Region("a", 3, 4)}
    vs = VirtualShard(5, regions, CODECS, "end")
    whole = vs.pieces(0, vs.size + 100)
    assert sum(len(p) if isinstance(p, bytes) else p.length for p in whole) == vs.size
    assert [p for p in whole if isinstance(p, Region)] == [
        regions[1],
        regions[2],
        regions[4],
    ]


def test_an_index_of_the_wrong_size_is_drift_naming_the_file():
    e = np.zeros((3, 2), dtype="<u8")
    with pytest.raises(ZarrStoreDrift) as ei:
        decode_index(encode_index(e, CODECS)[:-1], 3, CODECS, source="s/c/0.shard")
    assert "s/c/0.shard" in str(ei.value) and "Fix:" in str(ei.value)


def test_big_endian_and_checksum_free_indexes_round_trip():
    plain = ({"name": "bytes", "configuration": {"endian": "big"}},)
    e = np.array([[7, 9], [MISSING, MISSING]], dtype="<u8")
    buf = encode_index(e, plain)
    assert len(buf) == index_nbytes(2, plain) == 32
    assert buf[:8] == (7).to_bytes(8, "big")
    assert (decode_index(buf, 2, plain) == e).all()


def test_an_index_codec_other_than_bytes_and_crc32c_is_unsupported():
    with pytest.raises(ZarrUnsupported):
        index_nbytes(2, ({"name": "bytes"}, {"name": "zstd"}))
