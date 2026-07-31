"""cog_gbx writer — master-COG file preparation. Two modes:

DEFAULT (per-partition, ``driverMode=false``):
  Accepts PATH-bearing rows (from file_gbx) and converts each source to ONE
  master COG via cog_convert_file (rasterio.shutil.copy driver="COG") on the
  EXECUTOR. GDAL reads the source natively; the COG is written to a worker-local
  temp then copied bytes-only to the output Volume. Pixels never ride a Spark
  column (accumulation-proof).
  CEILING: a single very large source (~1+ GiB) cannot be converted inside the
  Serverless DS-V2 write task — the worker's tight per-task memory envelope
  (~1 GB per-PySpark-UDF cap) is exhausted by GDAL's overview-build transient
  (confirmed: Python copyfileobj / GDAL-direct read both OOM; WorkspaceClient/
  dbutils cannot even be constructed in a worker). Inherent to the DS-V2 write
  sandbox. So the default mode is for MODERATE files.

driverMode (``driverMode=true``):
  write() on executors gathers only the source path strings (cap-safe, no GDAL,
  no pixels); commit() on the DRIVER runs prepare_cogs over the full list. The
  driver is NOT under the ~1 GB per-UDF cap (validated: 10 GiB striped source →
  valid COG at ~2 GiB driver RSS), so this handles large single files and large
  batches one-at-a-time.

  ⚠ COMMIT TIMEOUT — Spark Connect channel cancellation:
    commit() runs the conversion INSIDE the ``.save()`` gRPC call. On Databricks
    Serverless (Spark Connect), a commit() that blocks for many minutes can have
    its channel cancelled — surfacing as
    ``UnknownException: (java.nio.channels.CancelledKeyException)`` and a FAILED
    run — even though nothing is wrong with the conversion itself. Risk grows
    with corpus size / per-file convert time (rough throughput ~1 GB/min).
    HOW TO AVOID IT: skip the writer and call the lower-level driver function
    directly from your notebook — it is plain Python on the driver with NO Spark
    RPC, so there is no channel to cancel:

        from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs
        # `sources` = a dir, a file, or a list mixing both (file_gbx not required)
        summary = prepare_cogs(sources, out_dir, blocksize=512, verbose=True)

    prepare_cogs is the robust path for large / long-running preparation; the
    driverMode writer is a convenience wrapper best used for smaller/faster
    batches where the .save() call completes well within the channel timeout.
    prepare_cogs is idempotent (skip_if_exists), so re-running after a timeout
    only fills the gaps.
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
        driver_mode=False,
        driver_mode_verbose=True,
        cog_bigtiff="YES",
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
        self.driver_mode = driver_mode
        self.driver_mode_verbose = driver_mode_verbose
        self.cog_bigtiff = cog_bigtiff
        if overwrite and os.path.isdir(self.out_dir):
            for stale in glob.glob(os.path.join(self.out_dir, f"*.{ext}")):
                try:
                    os.remove(stale)
                except OSError:
                    pass

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        if self.driver_mode:
            # Gather source path strings only — NO conversion on the executor
            # (cap-safe: no GDAL, no pixels). Conversion happens on driver.
            paths = [str(row["path"]) for row in iterator]
            return CogCommitMessage(paths=paths)

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
                    bigtiff=self.cog_bigtiff,
                )
                shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe on /Volumes
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written.append(out_path)
        return CogCommitMessage(paths=written)

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        if self.driver_mode:
            # NOTE: this runs INSIDE the .save() Spark Connect RPC. A long-blocking
            # commit (large corpus / big files, ~1 GB/min) can have its channel
            # cancelled → java.nio.channels.CancelledKeyException + a FAILED run.
            # If you hit that, bypass the writer and call prepare_cogs directly on
            # the driver (plain Python, no Spark RPC) — see this module's docstring.
            from databricks.labs.gbx.ds._listing import to_local_path
            from databricks.labs.gbx.pyrx.core.preparer import prepare_cogs

            all_paths = []
            for m in messages:
                if isinstance(m, CogCommitMessage):
                    all_paths.extend(to_local_path(p) for p in m.paths)
            prepare_cogs(
                all_paths,
                self.out_dir,
                blocksize=self.cog_blocksize,
                resampling=self.cog_overview_resampling,
                compression=self.cog_compression,
                subdataset=self.cog_subdataset,
                skip_if_exists=self.cog_skip_if_exists,
                verbose=self.driver_mode_verbose,
                bigtiff=self.cog_bigtiff,
            )
        return None

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        if self.driver_mode:
            # In driverMode, CogCommitMessage.paths holds SOURCE paths (write()
            # gathered references; commit() does the conversion via prepare_cogs).
            # Removing them would delete user input.  Do nothing on abort —
            # prepare_cogs is idempotent (skip_if_exists) so a re-run is cheap.
            return None
        for msg in messages:
            if isinstance(msg, CogCommitMessage):
                for p in msg.paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass
