"""cog_gbx writer — master-COG file preparation.

Accepts PATH-bearing rows (from file_gbx), opens each source file on the
Volume, converts it to ONE master COG (internally tiled + overviews, no split)
via the shared analysis.cog_convert (driver="COG"), and writes it FUSE-safe to
the output Volume. Pixels never ride in a Spark column — accumulation-proof.
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
    def __init__(self, path, schema, overwrite, cog_blocksize=512,
                 cog_overview_resampling="AVERAGE", cog_compression="DEFLATE",
                 name_col=None, ext="tif"):
        assert_path_schema(schema)
        self.out_dir = _listing.to_local_path(path)
        self.overwrite = overwrite
        self.cog_blocksize = int(cog_blocksize)
        self.cog_overview_resampling = cog_overview_resampling
        self.cog_compression = cog_compression
        self.name_col = name_col
        self.ext = ext
        if overwrite and os.path.isdir(self.out_dir):
            for stale in glob.glob(os.path.join(self.out_dir, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert
        import rasterio

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            src = _listing.to_local_path(str(row["path"]))
            # output name: derive from source basename (or nameCol if given)
            if self.name_col and row[self.name_col] is not None:
                base = os.path.basename(str(row[self.name_col]))
            else:
                base = os.path.basename(src)
            stem = os.path.splitext(base)[0]
            out_path = os.path.join(self.out_dir, f"{stem}.{self.ext}")

            # Convert to a master COG. Build to local temp then sequential-copy to
            # the Volume (FUSE-safe). cog_convert handles the driver="COG" encode.
            with rasterio.open(src) as ds:
                cog_bytes = cog_convert(
                    ds, self.cog_compression, self.cog_blocksize,
                    self.cog_overview_resampling,
                )
            fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd)
            try:
                with open(tmp, "wb") as fh:
                    fh.write(cog_bytes)
                shutil.copy(tmp, out_path)  # sequential → FUSE-safe on /Volumes
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
