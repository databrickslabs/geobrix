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
  driver is NOT under the ~1 GB per-UDF cap, so this handles large single files
  and large batches one-at-a-time.

  MEMORY FOOTPRINT (why standard Serverless is enough):
    COG generation streams block-by-block (bounded GDAL cache) and processes one
    file at a time, so peak memory is dominated by GDAL's overview-build transient
    and is essentially FLAT regardless of source size OR batch count — measured
    ~2.0-2.1 GiB RSS for 1.5 GiB, 10x1.5 GiB, and a single 10 GiB source alike.
    That fits comfortably under a STANDARD Serverless driver (~16 GiB), so no
    special compute is needed. A HIGH-MEMORY Serverless driver (~32 GiB) only buys
    extra headroom for much larger single files / more margin — useful, not
    required. Crucially there is NO memory tier that helps the SPARK (worker)
    profile: worker tasks are capped at ~1 GB per PySpark UDF regardless of
    instance size — which is exactly why the distributed paths (this writer's
    default per-partition mode, and a scalar-UDF approach) OOM on large files
    while DRIVER-orchestrated preparation does not.

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


def _is_v2_envelope(schema: StructType) -> bool:
    """True when *schema* is a (source, tile) envelope whose ``tile`` is a struct.

    Accepts either the v1 tile struct (``cellid, raster, metadata``) or the v2
    virtual struct (8 fields). ``_to_virtual_tile`` normalizes both, so the
    writer only needs to know it is receiving a tile envelope (not a top-level
    ``path`` column).
    """
    names = [f.name for f in schema.fields]
    if names != ["source", "tile"]:
        return False
    return isinstance(schema["tile"].dataType, StructType)


def assert_path_schema(schema: StructType) -> None:
    """cog_gbx writer accepts EITHER a top-level 'path' column (file_gbx output)
    OR a (source, tile) v1/v2 envelope (a virtual-tile DataFrame)."""
    names = [f.name for f in schema.fields]
    if "path" in names:
        return
    if _is_v2_envelope(schema):
        return
    raise ValueError(
        "cog_gbx writer requires a top-level 'path' column (file_gbx output) "
        f"or a (source, tile) tile envelope; got {names}"
    )


class CogGbxWriter(DataSourceWriter):
    def __init__(
        self,
        path,
        schema,
        overwrite,
        cog_blocksize=512,
        cog_overview_resampling="AVERAGE",
        # Unified compression surface (Task 5).
        # ``compress`` = "auto" | "zstd" | "deflate" | "lzw" | "none".
        # Deprecated: ``cog_compression`` is the old option; maps to ``compress``.
        # When compress == "auto", resolves to "ZSTD" (the spec ZSTD baseline).
        # If BOTH are given, compress wins.
        compress="auto",
        compress_level=None,
        predictor=None,
        cog_compression=None,
        name_col=None,
        ext="tif",
        cog_subdataset=None,
        cog_skip_if_exists=True,
        driver_mode=False,
        driver_mode_verbose=True,
        cog_bigtiff="YES",
    ):
        assert_path_schema(schema)
        self.tile_envelope = _is_v2_envelope(schema)
        self.out_dir = _listing.to_local_path(path)
        self.overwrite = overwrite
        self.cog_blocksize = int(cog_blocksize)
        self.cog_overview_resampling = cog_overview_resampling
        # Resolve compress: explicit compress wins over deprecated cog_compression.
        if compress == "auto" and cog_compression is not None:
            self.compress = cog_compression.lower()
        else:
            self.compress = compress
        self.compress_level = compress_level
        self.predictor = predictor
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

    def _resolved_cog_compression(self) -> str:
        """Return the compression string for cog_convert_file / prepare_cogs.

        ``cog_convert_file`` expects a codec name that rio-cogeo recognises (e.g.
        ``"DEFLATE"``, ``"LZW"``, ``"ZSTD"``). When the writer's compress is
        ``"auto"``, resolve to ``"ZSTD"`` — the spec ZSTD baseline applies to
        COG outputs as well as GTiff outputs.  (``cog_convert_file`` routes
        "ZSTD" through the compression authority, which uses the balanced default
        level since decoded_bytes is unavailable in the streaming path.)
        """
        c = str(self.compress).lower()
        if c == "auto":
            return "ZSTD"
        if c == "none":
            return "RAW"
        return c.upper()

    def write(self, iterator: Iterator) -> WriterCommitMessage:
        if self.tile_envelope:
            # v2 (source, tile) envelope. driverMode over v2 tiles is a follow-on
            # (see module docstring / task-5): the path-gather-then-driver-convert
            # path only makes sense for whole-file virtual tiles, and windowed/
            # clipped/materialized tiles would need bytes on the driver. For now
            # v2-tile input runs in DEFAULT (per-partition) mode only.
            if self.driver_mode:
                raise ValueError(
                    "cog_gbx driverMode does not yet support (source, tile) tile "
                    "input; use a top-level 'path' column (file_gbx) with "
                    "driverMode, or DEFAULT mode with a tile DataFrame."
                )
            return self._write_tiles(iterator)

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
                    compression=self._resolved_cog_compression(),
                    blocksize=self.cog_blocksize,
                    overview_resampling=self.cog_overview_resampling,
                    bigtiff=self.cog_bigtiff,
                    compress_level=self.compress_level,
                    predictor=self.predictor,
                )
                shutil.copyfile(tmp, out_path)  # bytes-only → FUSE-safe on /Volumes
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written.append(out_path)
        return CogCommitMessage(paths=written)

    def _out_path_for(self, vt, row) -> str:
        """Output COG path for a v2 tile row (name_col > tile.path > source > cellid)."""
        base = None
        if self.name_col and row[self.name_col] is not None:
            base = os.path.basename(str(row[self.name_col]))
        elif vt.path:
            base = os.path.basename(str(vt.path))
        elif "source" in row and row["source"]:
            base = os.path.basename(str(row["source"]))
        if not base:
            base = str(vt.cellid)
        stem = os.path.splitext(base)[0]
        return os.path.join(self.out_dir, f"{stem}.{self.ext}")

    def _is_whole_file_virtual(self, vt) -> bool:
        """A virtual tile that covers the FULL source extent with no pending
        clip or reprojection.

        Whole-file ⟺ raster is None AND clip_polygon is None AND crs is None
        (a pending warp would be silently dropped by a path-direct convert)
        AND the window is None (implicit whole-file) OR equals (0, 0, srcW, srcH).
        Such a tile can be path-direct converted (no pixels through the Python
        heap). A sub-window, a clip, or a pending warp must be materialized first
        so the output honors it.

        Source dims come from the reader-stamped ``metadata["width"]/["height"]``
        (strings) — NO staging/opening of the source just to read dims. If those
        are absent (a non-reader-produced virtual tile), we conservatively return
        False (materialize) so a windowed tile is never wrongly path-converted.
        """
        if not vt.is_virtual() or vt.clip_polygon is not None or vt.crs is not None:
            return False
        if vt.window is None:
            return True
        col_off, row_off, width, height = vt.window
        if col_off != 0 or row_off != 0:
            return False
        meta = vt.metadata or {}
        try:
            src_w = int(meta["width"])
            src_h = int(meta["height"])
        except (KeyError, TypeError, ValueError):
            # Dims unknown → do NOT path-direct; materialize is always safe.
            return False
        return width == src_w and height == src_h

    def _bytes_to_cog(self, raster_bytes: bytes, out_path: str) -> None:
        """Convert in-memory raster bytes to a COG at *out_path* (FUSE-safe)."""
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file

        fd, tmp_src = tempfile.mkstemp(suffix=f".{self.ext}")
        os.close(fd)
        fd2, tmp_out = tempfile.mkstemp(suffix=f".{self.ext}")
        os.close(fd2)
        try:
            with open(tmp_src, "wb") as fh:
                fh.write(raster_bytes)
            cog_convert_file(
                tmp_src,
                tmp_out,
                compression=self._resolved_cog_compression(),
                blocksize=self.cog_blocksize,
                overview_resampling=self.cog_overview_resampling,
                bigtiff=self.cog_bigtiff,
                compress_level=self.compress_level,
                predictor=self.predictor,
            )
            shutil.copyfile(tmp_out, out_path)  # bytes-only → FUSE-safe on /Volumes
        finally:
            for t in (tmp_src, tmp_out):
                if os.path.exists(t):
                    os.remove(t)

    def _write_tiles(self, iterator: Iterator) -> WriterCommitMessage:
        """DEFAULT-mode conversion for a v2 (source, tile) envelope.

        Whole-file virtual tile → PATH-DIRECT convert (cog_convert_file on the
        source path, no bytes round-trip). Windowed/clipped virtual tile →
        materialize the window/clip to bytes, then convert those bytes.
        Materialized tile (raster set) → materialize_to_bytes is a no-op; convert
        its bytes.
        """
        from databricks.labs.gbx.pyrx.core.analysis import cog_convert_file
        from databricks.labs.gbx.pyrx.core.open_tile import (
            _to_virtual_tile,
            materialize_to_bytes,
        )

        os.makedirs(self.out_dir, exist_ok=True)
        written: List[str] = []
        for row in iterator:
            vt = _to_virtual_tile(row["tile"])
            out_path = self._out_path_for(vt, row)

            if self.cog_skip_if_exists and os.path.exists(out_path):
                written.append(out_path)
                continue

            if self._is_whole_file_virtual(vt):
                # PATH-DIRECT: GDAL reads the source natively block-by-block; no
                # pixels touch the Python heap (same as the file_gbx path).
                src_local = _listing.to_local_path(str(vt.path))
                fd, tmp = tempfile.mkstemp(suffix=f".{self.ext}")
                os.close(fd)
                try:
                    cog_convert_file(
                        src_local,
                        tmp,
                        compression=self._resolved_cog_compression(),
                        blocksize=self.cog_blocksize,
                        overview_resampling=self.cog_overview_resampling,
                        bigtiff=self.cog_bigtiff,
                        compress_level=self.compress_level,
                        predictor=self.predictor,
                    )
                    shutil.copyfile(tmp, out_path)
                finally:
                    if os.path.exists(tmp):
                        os.remove(tmp)
            else:
                # Windowed/clipped virtual → materialize the window/clip; a
                # materialized tile → materialize_to_bytes is a no-op. Convert
                # the resulting bytes to a COG.
                raster_bytes = materialize_to_bytes(vt).raster
                self._bytes_to_cog(raster_bytes, out_path)

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
                compression=self._resolved_cog_compression(),
                compress_level=self.compress_level,
                predictor=self.predictor,
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
