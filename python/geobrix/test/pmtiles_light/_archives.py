"""Shared PMTiles archive fixtures for the pmtiles / vizx / ds test dirs."""

import gzip


def uncompressed_internals(raw: bytes) -> bytes:
    """Rewrite a library-written PMTiles archive (gzip root, leaves, metadata)
    so its root directory, leaf directories and metadata are stored
    uncompressed and the header declares ``internal_compression = NONE`` -- the
    layout the heavyweight writer produces. The tile data is copied unchanged;
    offsets and lengths are recomputed in the writer's section order (header,
    root, metadata, leaves, tile data)."""
    from pmtiles.tile import (
        Compression,
        Entry,
        deserialize_directory,
        deserialize_header,
        serialize_directory,
        serialize_header,
    )

    h = deserialize_header(raw[:127])
    assert h["internal_compression"] == Compression.GZIP

    def section(offset, length):
        return raw[offset : offset + length]

    def plain(entries):
        # serialize_directory gzips; the archive wants the bare varint stream
        return gzip.decompress(serialize_directory(entries))

    leaves = b""
    root_entries = []
    for e in deserialize_directory(section(h["root_offset"], h["root_length"])):
        if e.run_length > 0:  # tile entry in the root: kept as is
            root_entries.append(e)
            continue
        leaf = plain(
            deserialize_directory(
                section(h["leaf_directory_offset"] + e.offset, e.length)
            )
        )
        root_entries.append(Entry(e.tile_id, len(leaves), len(leaf), 0))
        leaves += leaf
    root = plain(root_entries)
    metadata = gzip.decompress(section(h["metadata_offset"], h["metadata_length"]))
    tiles = section(h["tile_data_offset"], h["tile_data_length"])

    h = dict(h, internal_compression=Compression.NONE)
    h["root_offset"] = 127
    h["root_length"] = len(root)
    h["metadata_offset"] = h["root_offset"] + len(root)
    h["metadata_length"] = len(metadata)
    h["leaf_directory_offset"] = h["metadata_offset"] + len(metadata)
    h["leaf_directory_length"] = len(leaves)
    h["tile_data_offset"] = h["leaf_directory_offset"] + len(leaves)
    return serialize_header(h) + root + metadata + leaves + tiles
