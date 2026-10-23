"""The virtual shard: a Zarr v3 shard whose chunks live in many files.

A sharded array stores one object per shard: the compressed inner chunks back to back,
plus an index of ``(offset, nbytes)`` per chunk (``sharding_indexed``). GeoBrix keeps
the chunks as byte ranges of pack files and lets a chunk row replace one inner chunk
without rewriting the shard. ``VirtualShard`` presents those ranges to zarr as one
shard object: the chunk bytes laid out in inner-index order, and an index synthesized
over that layout, at the start or the end as the array's codecs say.

The functions here read and write the shard index itself: ``index_nbytes``,
``encode_index`` and ``decode_index``.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass

import numpy as np

from databricks.labs.gbx.zarr.errors import ZarrStoreDrift, ZarrUnsupported

__all__ = [
    "MISSING",
    "Region",
    "index_nbytes",
    "encode_index",
    "decode_index",
    "VirtualShard",
]

MISSING = 2**64 - 1  # (offset, nbytes) of a chunk the shard does not hold
_CHECKSUM_BYTES = 4
_ENTRY_BYTES = 16  # two uint64


@dataclass(frozen=True)
class Region:
    """``length`` bytes at ``offset`` of ``file``.

    ``file`` is a manifest ``file`` value (relative to the store, or absolute);
    ``origin`` is the manifest ``origin`` of the row it came from, which says which
    root a relative ``file`` is under.
    """

    file: str
    offset: int
    length: int
    origin: str = "gbx"


def _index_layout(index_codecs) -> tuple[str, bool]:
    """``(numpy byte-order character, has_checksum)`` of the index codecs.

    The index is a fixed-size array of uint64 pairs, so ``bytes`` is the one codec that
    shapes it and ``crc32c`` the one that may follow it.
    """
    names = [c.get("name") for c in index_codecs]
    if names not in (["bytes"], ["bytes", "crc32c"]):
        raise ZarrUnsupported(
            f"shard index codecs {names} are not supported",
            fix="rewrite the array with index_codecs bytes and crc32c",
        )
    endian = (index_codecs[0].get("configuration") or {}).get("endian", "little")
    return ("<" if endian == "little" else ">"), len(names) == 2


def index_nbytes(n_chunks: int, index_codecs) -> int:
    """Size of the encoded index of a shard of ``n_chunks`` chunks."""
    _, checksum = _index_layout(index_codecs)
    return _ENTRY_BYTES * n_chunks + (_CHECKSUM_BYTES if checksum else 0)


def encode_index(entries: np.ndarray, index_codecs) -> bytes:
    """``entries`` (``(n, 2)`` offset and nbytes per chunk) as the shard's index bytes."""
    order, checksum = _index_layout(index_codecs)
    body = np.ascontiguousarray(entries, dtype=f"{order}u8").tobytes()
    if not checksum:
        return body
    from numcodecs import CRC32C

    return CRC32C().encode(body).tobytes()


def decode_index(buf: bytes, n_chunks: int, index_codecs, *, source=None) -> np.ndarray:
    """The ``(n_chunks, 2)`` uint64 entries in ``buf``, its checksum verified.

    ``source`` names the file the bytes came from, for the error message.
    """
    order, checksum = _index_layout(index_codecs)
    where = f" in {source}" if source else ""
    fix = (
        "run gbx_zarr_sync if the store changed outside GeoBrix; "
        "otherwise the file is corrupt"
    )
    expected = index_nbytes(n_chunks, index_codecs)
    if len(buf) != expected:
        raise ZarrStoreDrift(
            f"shard index{where} has {len(buf)} bytes, expected {expected}", fix=fix
        )
    body = bytes(buf)
    if checksum:
        from numcodecs import CRC32C

        try:
            body = CRC32C().decode(body).tobytes()
        except RuntimeError:  # numcodecs: stored and computed checksums differ
            raise ZarrStoreDrift(
                f"shard index checksum mismatch{where}", fix=fix
            ) from None
    return (
        np.frombuffer(body, dtype=f"{order}u8").reshape(n_chunks, 2).astype(np.uint64)
    )


class VirtualShard:
    """A shard object assembled from chunk regions, with a synthesized index.

    ``chunks`` maps an inner index (C-order position in the shard) to the region
    holding that chunk's bytes, or to ``None`` for a chunk the shard does not hold. The
    chunks are laid out in inner-index order; the index sits after them
    (``index_location="end"``) or before them (``"start"``), and every chunk the shard
    lacks is ``(MISSING, MISSING)`` in it.
    """

    def __init__(
        self,
        n_chunks: int,
        chunks: dict[int, Region | None],
        index_codecs,
        index_location: str,
    ):
        if index_location not in ("start", "end"):
            raise ValueError(
                f"index_location must be 'start' or 'end', got {index_location!r}"
            )
        bad = [i for i in chunks if not 0 <= i < n_chunks]
        if bad:
            raise ValueError(
                f"chunk indexes {bad} are outside a shard of {n_chunks} chunks"
            )
        index_size = index_nbytes(n_chunks, index_codecs)
        data_start = index_size if index_location == "start" else 0
        entries = np.full((n_chunks, 2), MISSING, dtype=np.uint64)
        starts: list[int] = []  # virtual offset of each non-empty region, ascending
        regions: list[Region] = []
        pos = data_start
        for i in sorted(chunks):
            region = chunks[i]
            if region is None:  # stays (MISSING, MISSING)
                continue
            entries[i] = (pos, region.length)
            if region.length:
                starts.append(pos)
                regions.append(region)
            pos += region.length
        self._starts, self._regions = starts, regions
        self._data = (data_start, pos)  # virtual [start, stop) of the chunk bytes
        index_start = 0 if index_location == "start" else pos
        self._index_at = (index_start, index_start + index_size)
        self._index = encode_index(entries, index_codecs)
        self.size = pos + (index_size if index_location == "end" else 0)

    def pieces(self, start: int, stop: int) -> list[bytes | Region]:
        """The virtual bytes ``[start, stop)`` as ordered parts.

        A ``bytes`` part is a slice of the synthesized index; a ``Region`` is a slice of a
        chunk's bytes in its file. ``stop`` is clamped to ``size``.
        """
        stop = min(stop, self.size)
        pos = max(start, 0)
        out: list[bytes | Region] = []
        index_lo, index_hi = self._index_at
        while pos < stop:
            if index_lo <= pos < index_hi:
                end = min(stop, index_hi)
                out.append(self._index[pos - index_lo : end - index_lo])
                pos = end
                continue
            j = bisect.bisect_right(self._starts, pos) - 1
            region = self._regions[j]
            skip = pos - self._starts[j]
            take = min(region.length - skip, stop - pos)
            out.append(Region(region.file, region.offset + skip, take, region.origin))
            pos += take
        return out
