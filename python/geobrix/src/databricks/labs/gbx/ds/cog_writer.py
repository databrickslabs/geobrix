"""cog_gbx writer — master-COG file preparation.

Accepts PATH-bearing rows (from file_gbx), converts each source to ONE master
COG via cog_convert_file (rasterio.shutil.copy driver="COG"), and writes it
FUSE-safe to the output Volume.

Source staging strategy:
  1. Databricks Serverless (dbutils available): stage via dbutils.fs.cp, which
     routes the bytes through the JVM — NOT the Python worker heap. This is
     the only path that avoids OOM for large (1+ GiB) /Volumes sources.
  2. Fallback (local dev/test, no dbutils): plain shutil.copyfile. Local sources
     are normal filesystem files, so no FUSE/heap issue.

Pixels never ride in a Spark column — accumulation-proof and OOM-safe for large files.
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


def _get_dbutils_fs():
    """Return a dbutils.fs handle (Databricks Serverless) or None (local dev/test).

    dbutils.fs.cp routes Volume→local byte copies through the JVM, bypassing
    the Python worker heap. On Serverless this is the only path that avoids OOM
    for large /Volumes sources. Outside Databricks (local tests, Docker) this
    returns None and callers fall back to plain shutil.copyfile.

    Constructed lazily at write()-time (not import-time) so the ImportError /
    init failure path only fires if actually executed on a Databricks worker.
    """
    try:
        from databricks.sdk import WorkspaceClient  # type: ignore[import]

        w = WorkspaceClient()
        return w.dbutils.fs
    except Exception:
        # ImportError (no SDK), credential error, or any init failure →
        # fall back to plain local copy (suitable for local dev / test).
        return None


def _stage_source(dbutils_fs, src_uri: str, local_dst: str) -> None:
    """Copy the source file to *local_dst* for GDAL to read from local disk.

    Two paths:
      - dbutils_fs available (Databricks Serverless): uses dbutils.fs.cp with
        URI schemes ``dbfs:/Volumes/...`` → ``file:/local/...``.  Bytes route
        through JVM; Python worker heap is untouched.
      - dbutils_fs is None (local dev / test / Docker): plain shutil.copyfile.
        Sources are already on a real filesystem — no FUSE/heap concern.
    """
    if dbutils_fs is not None:
        # src_uri is already the dbfs:-qualified form (file_gbx path column).
        # Ensure it is scheme-qualified for dbutils.fs.cp.
        cp_src = src_uri if src_uri.startswith("dbfs:") else "dbfs:" + src_uri
        cp_dst = "file:" + local_dst
        dbutils_fs.cp(cp_src, cp_dst)
    else:
        # Local fallback: source is a plain filesystem path.
        src_local = _listing.to_local_path(src_uri)
        shutil.copyfile(src_local, local_dst)


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
    ):
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
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

        os.makedirs(self.out_dir, exist_ok=True)
        # Try to get a dbutils handle once per write() call (cached locally).
        # dbutils.fs.cp routes bytes through the JVM, bypassing the Python-worker
        # heap — the only path proven safe for large /Volumes sources on Serverless.
        dbutils_fs = _get_dbutils_fs()

        written: List[str] = []
        for row in iterator:
            # The file_gbx path column stores the dbfs:-qualified URI; to_local_path
            # strips the scheme for basename extraction / local paths.
            src_uri = str(row["path"])           # dbfs:/Volumes/... (cp source)
            src_local_bare = _listing.to_local_path(src_uri)  # /Volumes/... (naming)

            # output name: derive from source basename (or name_col if given)
            if self.name_col and row[self.name_col] is not None:
                base = os.path.basename(str(row[self.name_col]))
            else:
                base = os.path.basename(src_local_bare)
            stem = os.path.splitext(base)[0]
            out_path = os.path.join(self.out_dir, f"{stem}.{self.ext}")

            fd_src, local_src = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd_src)
            fd_dst, tmp_dst = tempfile.mkstemp(suffix=f".{self.ext}")
            os.close(fd_dst)
            try:
                # Stage source to local disk (avoids Python-heap FUSE read).
                _stage_source(dbutils_fs, src_uri, local_src)
                # Stream-convert local src → local temp COG (block-by-block).
                cog_convert_file(
                    local_src, tmp_dst,
                    compression=self.cog_compression,
                    blocksize=self.cog_blocksize,
                    overview_resampling=self.cog_overview_resampling,
                )
                shutil.copyfile(tmp_dst, out_path)  # bytes-only → FUSE-safe on /Volumes
            finally:
                for p in (local_src, tmp_dst):
                    if os.path.exists(p):
                        os.remove(p)
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
