"""DataSourceReaders for pmtiles_gbx: raster (per-tile mosaic pyramid) + archive.

Both readers need random access to their inputs (rio-tiler window reads of the
source rasters; per-tile range reads of the archive), which UC Volume FUSE
serves poorly. Each input is therefore copied to worker-local disk once per
worker process, sequentially, through a bounded :class:`StageCache` and shared
by every partition that worker reads. Archive planning reads only the header
and directories, straight from the source.
"""

from __future__ import annotations

import contextlib
import gzip
import logging
import os
from functools import lru_cache
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition

from databricks.labs.gbx.ds import _listing, _quarantine, _xyz_mosaic
from databricks.labs.gbx.pyrx.core.local_temp import (
    StageCache,
    copy_source_sequential,
    retried_source_signature,
)

_LOG = logging.getLogger(__name__)

_HEADER_LEN = 127  # PMTiles v3 fixed header
_MAX_DEPTH = 4  # directory nesting the PMTiles v3 spec allows (root + leaves)
_DIR_CACHE = 8  # deserialized directories kept per partition read


# ---------------------------------------------------------------------------
# Worker-local staging (process-global; see local_temp.StageCache)
# ---------------------------------------------------------------------------


def _stage_render_source(source_path: str, local_path: str) -> None:
    """Stage a source raster render-ready: copy it, then (RGBA imagery) replace
    the copy with its RGB form so tile masks come from the footprint, not an
    alpha band that reads ~0 after decimation (see ``to_render_rgb``)."""
    copy_source_sequential(source_path, local_path)
    try:
        rgb = _xyz_mosaic.to_render_rgb(local_path)
        if rgb != local_path:
            os.replace(rgb, local_path)
    except BaseException:
        for leftover in (local_path, local_path + ".rgb.tif"):
            with contextlib.suppress(OSError):
                os.remove(leftover)
        raise


# Each archive / source raster is copied to local disk once per worker process
# and shared by every partition read there, within a bounded cache.
_ARCHIVE_STAGE = StageCache(
    "gbx_pmtiles_arc_", copy_source_sequential, retried_source_signature
)
_RENDER_STAGE = StageCache(
    "gbx_pmtiles_rst_", _stage_render_source, retried_source_signature
)


# ---------------------------------------------------------------------------
# PMTiles range reads
# ---------------------------------------------------------------------------


def _range_source(fh) -> Callable[[int, int], bytes]:
    """``get_bytes(offset, length)`` over an open binary file (seek + read). A
    range past the end of the file is cut at it (fewer bytes, or none): a
    garbled header or directory entry can hold any offset and length, and a read
    of that length would try to allocate it."""
    size = os.fstat(fh.fileno()).st_size

    def get_bytes(offset: int, length: int) -> bytes:
        if offset < 0 or length <= 0 or offset >= size:
            return b""
        fh.seek(offset)
        return fh.read(min(length, size - offset))

    return get_bytes


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


def _list_tiles(path: str) -> List[Tuple[int, int, int]]:
    """Every addressed tile of an archive, from its header and directories
    (a few sequential section reads, no tile data)."""
    with _listing._retry_transient(lambda: open(path, "rb")) as fh:
        get_bytes = _range_source(fh)
        header = _read_header(get_bytes)
        return [
            zxy
            for zxy, _entry in _iter_entries(
                get_bytes, header, header["root_offset"], header["root_length"]
            )
        ]


class _TileLookup:
    """Tile bytes by (z, x, y) over ``get_bytes``, with the header read once and
    the last few directories kept deserialized. ``pmtiles.reader.Reader.get``
    re-reads and re-parses the header, root and leaf directory for every tile;
    this resolves tiles by the same search (``find_tile``, leaf descent up to
    the spec's depth) but reads each directory once while a partition's
    consecutive tiles fall in it."""

    def __init__(self, get_bytes):
        self._get_bytes = get_bytes
        self._header = _read_header(get_bytes)
        self._directory = lru_cache(maxsize=_DIR_CACHE)(
            lambda offset, length: _read_directory(
                get_bytes, self._header, offset, length
            )
        )

    def get(self, z: int, x: int, y: int) -> Optional[bytes]:
        found = self.lookup(z, x, y)
        return None if found is None else found[0]

    def lookup(self, z: int, x: int, y: int) -> Optional[Tuple[bytes, int]]:
        """``(tile bytes, length its directory entry declares)``, or None."""
        from pmtiles.tile import find_tile, zxy_to_tileid

        h = self._header
        tile_id = zxy_to_tileid(z, x, y)
        offset, length = h["root_offset"], h["root_length"]
        for _ in range(_MAX_DEPTH):
            entry = find_tile(self._directory(offset, length), tile_id)
            if entry is None:
                return None
            if entry.run_length == 0:  # leaf directory pointer
                offset = h["leaf_directory_offset"] + entry.offset
                length = entry.length
                continue
            data = self._get_bytes(h["tile_data_offset"] + entry.offset, entry.length)
            return data, entry.length
        return None


class _TilesPartition(InputPartition):
    def __init__(self, tiles: List[Tuple[int, int, int]], sources: List[str]):
        self.tiles = tiles
        self.sources = sources


def _chunk(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


class PMtilesRasterReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError(
                "pmtiles_gbx raster reader requires a 'path' (dir of COGs)."
            )
        self.filter_regex = options.get("filterRegex", r".*\.tif$")
        self.min_z = int(options.get("minZoom", "0"))
        self.max_z = int(options.get("maxZoom", "0"))
        self.tiles_per_partition = int(options.get("tilesPerPartition", "64"))
        self.tile_format = options.get("tileFormat", "png").upper()
        ps = options.get("pixelSelection", "first").lower()
        if ps != "first":
            raise ValueError(
                f"pmtiles_gbx v1 supports pixelSelection='first' only; got {ps!r}"
            )
        bbox_opt = options.get("bbox")
        self.bbox = tuple(float(v) for v in bbox_opt.split(",")) if bbox_opt else None
        if self.bbox is not None and len(self.bbox) != 4:
            raise ValueError("pmtiles_gbx bbox must be 'minx,miny,maxx,maxy'")

    def partitions(self) -> Sequence[InputPartition]:
        import morecantile
        import rasterio
        from rasterio.warp import transform_bounds

        sources = _listing.list_files(self.path, self.filter_regex)
        if not sources:
            raise ValueError(f"pmtiles_gbx raster reader: no rasters under {self.path}")
        bbox = self.bbox or _xyz_mosaic.source_bounds_union(sources)
        # per-source WGS84 bounds, to attach only intersecting sources to each chunk
        src_bounds = []
        for p in sources:
            with rasterio.open(p) as ds:
                src_bounds.append(
                    (p, transform_bounds(ds.crs, "EPSG:4326", *ds.bounds))
                )
        tiles = _xyz_mosaic.enumerate_tiles(bbox, self.min_z, self.max_z)
        if not tiles:
            _LOG.warning(
                "pmtiles_gbx raster reader: AOI/bbox intersects zero tiles for zoom %s..%s under %s"
                " — emitting empty result",
                self.min_z,
                self.max_z,
                self.path,
            )
        # sort by (z, x, y) for spatial contiguity within each zoom
        tiles.sort()
        parts: List[InputPartition] = []
        tms = _xyz_mosaic._tms()
        for chunk in _chunk(tiles, self.tiles_per_partition):
            # combined WGS84 bbox of the chunk's tiles
            cb = [tms.bounds(morecantile.Tile(x, y, z)) for z, x, y in chunk]
            cw = min(b.left for b in cb)
            cs = min(b.bottom for b in cb)
            ce = max(b.right for b in cb)
            cn = max(b.top for b in cb)
            needed = [
                p
                for p, (w, s, e, n) in src_bounds
                if not (e < cw or w > ce or n < cs or s > cn)
            ]
            parts.append(_TilesPartition(chunk, needed or sources))
        return parts

    def read(self, partition: "_TilesPartition") -> Iterator[Tuple]:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # FUSE-safe + vsimem-bug-safe: each source is staged (sequential copy, made
        # render-ready RGB) to NODE-LOCAL disk once per worker process -- local disk
        # supports the random window seeks rio-tiler needs; UC-Volume FUSE does not
        # -- and render_tile gets the local PATHS. render_tile takes paths (NOT open
        # datasets): on rasterio 1.5.0 / rio-tiler 9.0.6, passing open in-memory
        # MemoryFile datasets to mosaic_reader corrupts a sibling dataset's vsimem
        # bytes (TIFFReadDirectory failure).
        with contextlib.ExitStack() as staged:
            local_paths = [
                staged.enter_context(_RENDER_STAGE.use(p)) for p in partition.sources
            ]
            for z, x, y in partition.tiles:
                png = _xyz_mosaic.render_tile(
                    z, x, y, local_paths, tile_format=self.tile_format
                )
                if png is not None:
                    yield (z, x, y, png)


class PMtilesArchiveReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError(
                "pmtiles_gbx archive reader requires a 'path' (.pmtiles file)."
            )
        self.path = _listing.to_local_path(self.path)
        self.tiles_per_partition = int(options.get("tilesPerPartition", "2048"))

    def partitions(self) -> Sequence[InputPartition]:
        # Header + directories only: planning never reads tile data.
        entries = _list_tiles(self.path)
        return [
            _TilesPartition(list(c), [self.path])
            for c in _chunk(entries, self.tiles_per_partition)
        ]

    def read(self, partition: "_TilesPartition") -> Iterator[Tuple]:
        # Range reads on a worker-local copy (staged once per worker process): a
        # partition reads the header, the directories its tiles fall in and its
        # own tiles -- not the whole archive.
        # With read_paths' on_error="skip" (a hook on the reader) a tile whose
        # bytes are shorter than its directory entry (a truncated archive) is a
        # bad record; without one it is returned as read.
        hook = _quarantine.hook_of(self)
        with (
            _ARCHIVE_STAGE.use(partition.sources[0]) as local,
            open(local, "rb") as fh,
        ):
            tiles = _TileLookup(_range_source(fh))
            for z, x, y in partition.tiles:
                found = tiles.lookup(z, x, y)
                if found is None:
                    continue
                data, declared = found
                if hook is not None and len(data) != declared:
                    hook(
                        f"tile={z}/{x}/{y}",
                        _quarantine.SHORT_TILE,
                        f"tile has {len(data)} of the {declared} bytes its "
                        "directory entry declares: the archive is truncated",
                    )
                    continue
                yield (z, x, y, bytes(data))
