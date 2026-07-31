"""cog_gbx writer — master-COG file preparation.

Accepts PATH-bearing rows (from file_gbx), converts each source to ONE master
COG (internally tiled + overviews, no split) via cog_convert_file
(rasterio.shutil.copy driver="COG"), and writes it FUSE-safe to the output
Volume. GDAL reads the source natively (cloud-native sequential read over FUSE)
— the source never passes through the Python heap. The COG output is written to
a worker-local temp file, then copied bytes-only to the output Volume. Pixels
never ride in a Spark column — accumulation-proof for the per-partition path.

MEMORY CEILING (large single files on Serverless):
  A single very large source (~1+ GiB striped GeoTIFF) cannot be COG-converted
  INSIDE a Databricks Serverless DataSource-V2 write task: the serverless
  worker's tight memory envelope (~312 MiB baseline) is exhausted by GDAL's
  overview-build transient on such a source, and EVERY in-worker read/copy
  mechanism was exhausted proving this (Python copyfileobj → OOM; GDAL-direct
  read over FUSE → OOM; WorkspaceClient/dbutils cannot even be constructed in a
  serverless worker). It is NOT a patchable code layer — it is inherent to the
  DS-V2 write sandbox. This writer is validated for moderate files. For very
  large single COGs, prepare them on a classic cluster (roomier per-task
  memory) or via a driver-orchestrated path. Multi-GiB-on-Serverless is a
  tracked follow-on (candidate: an rst_* file-preparation function callable
  outside the DS-V2 write sandbox).
"""

from __future__ import annotations

import glob
import os
import shutil
import tempfile
from dataclasses import dataclass
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from databricks.labs.gbx.ds import _listing


@dataclass
class CogCommitMessage(WriterCommitMessage):
    paths: List[str]


def assert_path_schema(schema: StructType) -> None:
    """cog_gbx writer requires a 'path' column (the file_gbx output)."""
    names = [f.name for f in schema.fields]
    if "path" not in names:
        raise ValueError(
            f"cog_gbx writer requires a 'path' column (file_gbx output); got {names}"
        )


class CogGbxWriter(DataSourceWriter):
    def __init__(
        self,
        path,
        schema,
        overwrite,
        cog_blocksize=512,
        cog_overview_resampling="AVERAGE",
        cog_compression="DEFLATE",
        name_col=None,
        ext="tif",
        cog_subdataset=None,
        cog_skip_if_exists=True,
    ):
        assert_path_schema(schema)
        self.out_dir = _listing.to_local_path(path)
        self.overwrite = overwrite
        self.cog_blocksize = int(cog_blocksize)
        self.cog_overview_resampling = cog_overview_resampling
        self.cog_compression = cog_compression
        self.name_col = name_col
        self.ext = ext
        self.cog_subdataset = cog_subdataset
        self.cog_skip_if_exists = cog_skip_if_exists
        if overwrite and os.path.isdir(self.out_dir):
            for stale in glob.glob(os.path.join(self.out_dir, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            src_volume = _listing.to_local_path(str(row["path"]))
            # output name: derive from source basename (or name_col if given)
            if self.name_col and row[self.name_col] is not None:
                base = os.path.basename(str(row[self.name_col]))
            else:
                base = os.path.basename(src_volume)
            stem = os.path.splitext(base)[0]
            out_path = os.path.join(self.out_dir, f"{stem}.{self.ext}")

            # Skip when the output already exists (idempotent resume).
            if self.cog_skip_if_exists and os.path.exists(out_path):
                written.append(out_path)
                continue

            # Build a NetCDF subdataset URI when requested.
            conv_src = src_volume
            if self.cog_subdataset:
                conv_src = f'NETCDF:"{src_volume}":{self.cog_subdataset}'

            # Pass the source path directly to cog_convert_file. GDAL (via
            # rasterio.shutil.copy driver="COG") reads the source natively
            # block-by-block — no Python-heap copy of the whole file. Only the
            # COG output (local temp → copyfile) touches the Python heap.
            fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd)
            try:
                cog_convert_file(
                    conv_src,
                    tmp,
                    compression=self.cog_compression,
                    blocksize=self.cog_blocksize,
                    overview_resampling=self.cog_overview_resampling,
                )
                shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe on /Volumes
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written.append(out_path)
        return CogCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        for msg in messages:
            if isinstance(msg, CogCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
