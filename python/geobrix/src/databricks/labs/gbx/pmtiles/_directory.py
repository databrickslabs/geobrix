"""PMTiles v3 header and directory decoding, shared by every reader of an archive.

Import-light on purpose (the standard library; the ``pmtiles`` package is imported
inside the functions): the driver-side inspector and the vizx viewers use it without
pulling in PySpark or rasterio, and the ``pmtiles_gbx`` reader builds on it.
"""

from __future__ import annotations

import gzip
from typing import Callable, Iterator, Tuple

_HEADER_LEN = 127  # PMTiles v3 fixed header


def _read_header(get_bytes) -> dict:
    from pmtiles.tile import deserialize_header

    return deserialize_header(get_bytes(0, _HEADER_LEN))


def _read_directory(get_bytes, header: dict, offset: int, length: int) -> list:
    """The ``pmtiles.tile.Entry`` list of the directory at ``offset``/``length``,
    decoded by the archive's declared ``internal_compression``. The library's
    ``deserialize_directory`` always gunzips, but the spec allows uncompressed
    directories (the heavyweight pmtiles writer stores them so). Only none and
    gzip are readable; any other compression is refused rather than guessed."""
    from pmtiles.tile import Compression, deserialize_directory

    raw = get_bytes(offset, length)
    comp = header["internal_compression"]
    if comp is Compression.GZIP:
        return deserialize_directory(raw)
    if comp is Compression.NONE:
        # The library has no raw parser; a directory is a few KB, so re-gzipping
        # (cheapest level) it for the library's own parser costs nothing.
        return deserialize_directory(gzip.compress(raw, compresslevel=1))
    raise ValueError(
        f"pmtiles_gbx: internal compression {comp.value} ({comp.name}) is not supported"
    )


def _iter_entries(get_bytes, header: dict, offset: int, length: int):
    """``((z, x, y), entry)`` for every addressed tile under one directory, in
    tile-id order (a run-length entry yields one pair per tile it addresses).
    Reads directories only -- never tile data. Same traversal (and order) as
    ``pmtiles.reader.all_tiles``, with directories decoded by the header."""
    from pmtiles.tile import tileid_to_zxy

    for entry in _read_directory(get_bytes, header, offset, length):
        if entry.run_length > 0:
            for i in range(entry.run_length):
                yield tileid_to_zxy(entry.tile_id + i), entry
        else:
            yield from _iter_entries(
                get_bytes,
                header,
                header["leaf_directory_offset"] + entry.offset,
                entry.length,
            )


def _memory_source(raw) -> Callable[[int, int], bytes]:
    """``get_bytes(offset, length)`` over archive bytes held in memory."""

    def get_bytes(offset: int, length: int) -> bytes:
        return raw[offset : offset + length]

    return get_bytes


def iter_tiles(raw) -> Iterator[Tuple[Tuple[int, int, int], bytes]]:
    """``((z, x, y), payload)`` for every tile of an in-memory archive, in
    tile-id order -- the shape and order of ``pmtiles.reader.all_tiles``. Unlike
    it, directories are decoded by the header's ``internal_compression``, so the
    uncompressed-directory archives the heavyweight writer produces read too.
    The one driver-side tile iterator (``pmtiles_info`` and the vizx viewers)."""
    get_bytes = _memory_source(raw)
    header = _read_header(get_bytes)
    base = header["tile_data_offset"]
    for zxy, entry in _iter_entries(
        get_bytes, header, header["root_offset"], header["root_length"]
    ):
        yield zxy, get_bytes(base + entry.offset, entry.length)
