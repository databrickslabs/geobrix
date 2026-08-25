"""Tile-archive backends: turn one shard's sorted (tileid, bytes) stream into a
container file. ``PMTilesBackend`` is the first; MBTiles/MVT-dir slot in later."""

from __future__ import annotations

from typing import Iterator, Protocol, Tuple

from databricks.labs.gbx.ds.tiles._header import HeaderInfo

SortedTiles = Iterator[Tuple[int, bytes]]  # ascending tileid


class TileArchiveBackend(Protocol):
    def assemble(
        self, sorted_tiles: SortedTiles, header_info: HeaderInfo, out_path: str
    ) -> None: ...


class PMTilesBackend:
    """Assemble a single ``.pmtiles`` archive from ascending-tileid tiles."""

    def assemble(
        self, sorted_tiles: SortedTiles, header_info: HeaderInfo, out_path: str
    ) -> None:
        from pmtiles.writer import Writer

        with open(out_path, "wb") as f:
            writer = Writer(f)
            for tileid, data in sorted_tiles:
                writer.write_tile(tileid, data)
            writer.finalize(header_info.header_dict(), header_info.metadata)
