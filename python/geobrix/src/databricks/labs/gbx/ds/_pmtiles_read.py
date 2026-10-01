"""DataSourceReaders for pmtiles_gbx: raster (per-tile mosaic pyramid) + archive."""

from __future__ import annotations

import logging
from typing import Dict, Iterator, List, Sequence, Tuple

from pyspark.sql.datasource import DataSourceReader, InputPartition

from databricks.labs.gbx.ds import _listing, _xyz_mosaic

_LOG = logging.getLogger(__name__)


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
        import os
        import shutil

        from databricks.labs.gbx.pyrx import _env
        from databricks.labs.gbx.pyrx.core.local_temp import new_local_temp_dir

        _env.configure_gdal_env()
        # FUSE-safe + vsimem-bug-safe: sequentially copy each source's bytes to a
        # NODE-LOCAL temp file (local disk supports the random window seeks rio-tiler
        # needs; UC-Volume FUSE does not), then hand render_tile the local PATHS.
        # render_tile takes paths (NOT open datasets): on rasterio 1.5.0 / rio-tiler
        # 9.0.6, passing open in-memory MemoryFile datasets to mosaic_reader corrupts a
        # sibling dataset's vsimem bytes (TIFFReadDirectory failure) — Task 1 confirmed.
        tmpdir = new_local_temp_dir("gbx_pmtiles_src_")
        local_paths: List[str] = []
        try:
            for i, p in enumerate(partition.sources):
                lp = os.path.join(tmpdir, f"src_{i}.tif")
                with open(p, "rb") as src_fh, open(lp, "wb") as dst_fh:
                    shutil.copyfileobj(src_fh, dst_fh)  # sequential, FUSE-safe
                # RGBA imagery (e.g. NAIP) -> render from RGB so the tile mask comes from
                # the footprint, not an alpha band that reads ~0 post-decimation (which
                # would mask the valid imagery to transparent). Strip on the LOCAL copy.
                local_paths.append(_xyz_mosaic.to_render_rgb(lp))
            for z, x, y in partition.tiles:
                png = _xyz_mosaic.render_tile(
                    z, x, y, local_paths, tile_format=self.tile_format
                )
                if png is not None:
                    yield (z, x, y, png)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)


class PMtilesArchiveReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.path = options.get("path")
        if not self.path:
            raise ValueError(
                "pmtiles_gbx archive reader requires a 'path' (.pmtiles file)."
            )
        self.path = _listing.to_local_path(self.path)
        self.tiles_per_partition = int(options.get("tilesPerPartition", "2048"))

    def _entries(self) -> List[Tuple[int, int, int]]:
        from pmtiles.reader import MemorySource, all_tiles

        with open(self.path, "rb") as fh:
            raw = fh.read()
        return [(z, x, y) for (z, x, y), _ in all_tiles(MemorySource(raw))]

    def partitions(self) -> Sequence[InputPartition]:
        entries = self._entries()
        return [
            _TilesPartition(list(c), [self.path])
            for c in _chunk(entries, self.tiles_per_partition)
        ]

    def read(self, partition: "_TilesPartition") -> Iterator[Tuple]:
        from pmtiles.reader import MemorySource, Reader

        # FUSE-safe: read archive bytes sequentially, then serve tiles in memory
        with open(partition.sources[0], "rb") as fh:
            raw = fh.read()
        reader = Reader(MemorySource(raw))
        for z, x, y in partition.tiles:
            data = reader.get(z, x, y)
            if data is not None:
                yield (z, x, y, bytes(data))
