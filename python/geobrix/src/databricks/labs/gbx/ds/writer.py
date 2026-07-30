"""gtiff_gbx / raster_gbx writer (DataSource V2 write path).

Enforces the exact (source, tile) schema like the heavy GDAL writer. Writer
options are path/nameCol/ext only; the on-disk encoding comes from tile.metadata
(see _write.tile_to_bytes). Pure Python (Serverless).
"""

from __future__ import annotations

import glob
import hashlib
import os
import uuid
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _write
from databricks.labs.gbx.ds.raster import reader_schema


@dataclass
class RasterCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_write_schema(schema: StructType) -> None:
    """Exact (source, tile) — extras OR missing both fail (matches GDAL writer)."""
    expected = reader_schema()
    if [f.name for f in schema.fields] != [f.name for f in expected.fields]:
        raise ValueError(
            f"raster writer requires exactly columns "
            f"{[f.name for f in expected.fields]}, got {[f.name for f in schema.fields]}"
        )


def _safe_name(raster_bytes: bytes, cellid: int) -> str:
    """Opaque, collision-free fallback name when no nameCol: content hash + uuid.

    PySpark's DataSourceWriter does not expose partition/task ids (Scala uses
    pid_tid), so the uuid suffix keeps names unique across partitions. NOT
    byte-identical to heavy's MurmurHash3_pid_tid -- use nameCol for control.
    """
    h = hashlib.sha1(raster_bytes + str(cellid).encode()).hexdigest()[:12]
    return f"{h}_{uuid.uuid4().hex[:8]}"


class RasterGbxWriter(DataSourceWriter):
    def __init__(
        self,
        path: str,
        schema: StructType,
        overwrite: bool,
        name_col: Optional[str] = None,
        ext: str = "tif",
        force_driver: Optional[str] = None,
        cog: bool = False,
        cog_blocksize: int = 512,
        cog_overview_resampling: str = "AVERAGE",
        cog_compression: str = "DEFLATE",
    ):
        assert_write_schema(schema)
        if name_col and name_col not in [f.name for f in schema.fields]:
            raise ValueError(
                f"nameCol {name_col!r} is not a column; available: "
                f"{[f.name for f in schema.fields]} (overwrite 'source')."
            )
        from databricks.labs.gbx.ds._listing import to_local_path

        # The output path may arrive dbfs:-qualified; strip the scheme once so all
        # os.* writes operate on the bare FUSE path.
        self.path = to_local_path(path)
        self.overwrite = overwrite
        self.name_col = name_col
        self.ext = ext
        self.force_driver = force_driver
        self.cog = cog
        self.cog_blocksize = cog_blocksize
        self.cog_overview_resampling = cog_overview_resampling
        self.cog_compression = cog_compression
        # Use self.path (scheme stripped), NOT the raw path: a dbfs:/file:-qualified
        # path makes os.path.isdir(path) False, which would silently skip the
        # overwrite cleanup and leave stale tiles from a prior write mixed in.
        if overwrite and os.path.isdir(self.path):
            for stale in glob.glob(os.path.join(self.path, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        os.makedirs(self.path, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            tile = row["tile"]
            cellid = tile["cellid"]
            raster_bytes = bytes(tile["raster"])
            metadata = dict(tile["metadata"] or {})
            if self.cog:
                from rasterio.io import MemoryFile

                from databricks.labs.gbx.pyrx.core import analysis as _analysis
                from databricks.labs.gbx.pyrx.core import cog as _cog

                info = _cog.detect_cog(metadata, raster_bytes)
                if not info.is_cog:
                    with MemoryFile(raster_bytes) as mf:
                        with mf.open() as ds:
                            raster_bytes = _analysis.cog_convert(
                                ds,
                                self.cog_compression,
                                self.cog_blocksize,
                                self.cog_overview_resampling,
                            )
                    metadata = _cog.stamp_format_metadata(raster_bytes, metadata)
            if self.name_col:
                raw_name = row[self.name_col]
                name = os.path.basename(str(raw_name)) if raw_name is not None else ""
                if not name:
                    name = _safe_name(raster_bytes, cellid)
            else:
                name = _safe_name(raster_bytes, cellid)
            out_bytes = _write.tile_to_bytes(
                cellid, raster_bytes, metadata, self.force_driver
            )
            out = os.path.join(self.path, f"{name}.{self.ext}")
            with open(out, "wb") as fh:
                fh.write(out_bytes)
            written.append(out)
        return RasterCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, RasterCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
