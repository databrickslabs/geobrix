"""Reader benchmark mode: time the light raster reader (raster_gbx) per-file.

Pure-local path: open each file with rasterio, split into tiles via pyrx
core tiling, re-encode each tile — measures the end-to-end reader cost on
the local filesystem without Spark overhead.

Spark-path: register the raster_gbx data source and time
spark.read.format("raster_gbx").load(path).count() over a corpus directory.

Cluster format-read: generic spark.read.format(...).load(path).count() wrapper
for comparing light (raster_gbx) vs heavy (gdal) readers on the same cluster.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.runner import capture_env, peak_rss_mb, time_iters


def measure_parallelism(spark, df):
    """(input_partitions, slots_available) for a timed df, Connect-safe.

    input_partitions: number of partitions that carry >=1 row, via
    spark_partition_id() (no .rdd -> Connect-safe). slots_available: cluster task
    slots via bench.runner._bench_parallelism (classic:
    sparkContext.defaultParallelism; Connect: spark.sql.shuffle.partitions). The
    QA report derives slots-used = min(input_partitions, slots_available) and
    idle = slots_available - slots-used from these two numbers."""
    from pyspark.sql import functions as F

    from databricks.labs.gbx.bench.runner import _bench_parallelism

    parts = int(df.select(F.spark_partition_id().alias("_pid")).distinct().count())
    return parts, int(_bench_parallelism(spark))


def _read_one_file_light(file_path: str, size_mib: int) -> int:
    """Open a raster file, compute tiles, return tile count."""
    import rasterio

    from databricks.labs.gbx.pyrx.core import tiling as core_tiling

    size_bytes = os.path.getsize(file_path)
    with rasterio.open(file_path) as ds:
        tiles = core_tiling.make_tiles(ds, size_in_mb=size_mib, size_bytes=size_bytes)
        return len(tiles)


def run_pure_local_reader(
    files: List[str],
    run_id: str,
    warmup: int,
    measured: int,
    size_mib: int = 16,
    where: str = "venv",
) -> List[ResultRow]:
    """Time the light raster reader on a list of local file paths.

    One ResultRow is emitted per file. ``iter_median_s`` is the median
    wall-clock over ``measured`` iterations for that single file.
    """
    env = capture_env(where)
    out: List[ResultRow] = []
    for file_path in files:
        try:
            stats = time_iters(
                lambda f=file_path: _read_one_file_light(f, size_mib),
                warmup,
                measured,
            )
            ms = stats["iter_median_ms"]
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn="raster_read",
                    category="reader",
                    mode="pure-core",
                    tile_px=0,
                    bands=0,
                    dtype="",
                    srid=0,
                    rows=1,
                    nodata_frac=0.0,
                    warmup_iters=stats["warmup_iters"],
                    measured_iters=stats["measured_iters"],
                    iter_median_s=ms / 1000.0,
                    iter_min_s=stats["iter_min_ms"] / 1000.0,
                    iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                    iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                    avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                    throughput_mpix_s=0.0,
                    throughput_rows_s=(1.0 / (ms / 1000.0)) if ms else 0.0,
                    peak_rss_mb=peak_rss_mb(),
                    status="ok",
                    note=os.path.basename(file_path),
                    output_fingerprint="",
                    **env,
                )
            )
        except Exception as e:  # noqa: BLE001
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn="raster_read",
                    category="reader",
                    mode="pure-core",
                    tile_px=0,
                    bands=0,
                    dtype="",
                    srid=0,
                    rows=1,
                    nodata_frac=0.0,
                    warmup_iters=warmup,
                    measured_iters=0,
                    iter_median_s=0.0,
                    iter_min_s=0.0,
                    iter_p90_s=0.0,
                    throughput_mpix_s=0.0,
                    throughput_rows_s=0.0,
                    peak_rss_mb=0.0,
                    status="error",
                    note=str(e)[:300],
                    output_fingerprint="",
                    **env,
                )
            )
    return out


def run_spark_path_reader(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    size_mib: int = 16,
    where: str = "venv",
    split_plan_read: bool = False,
) -> List[ResultRow]:
    """Time the raster_gbx Spark data source over a corpus directory.

    Registers the light DS, then times
    ``spark.read.format("raster_gbx").option("sizeInMB", ...).load(path).count()``.
    One ResultRow is emitted covering the whole directory.

    When ``split_plan_read=True``, times ``RasterGbxReader.partitions()``
    separately (driver-side planning) and records the result as ``plan_s`` in the
    emitted ``ResultRow``. The total ``.count()`` iteration time is unchanged.
    This isolates the listing/header-open planning cost from the executor read cost.
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    def _job():
        return (
            spark.read.format("raster_gbx")
            .option("sizeInMB", str(size_mib))
            .load(path)
            .count()
        )

    # Optional: time planning separately to isolate the listing/header-open cost.
    _plan_s = 0.0
    if split_plan_read:
        import time as _time

        from databricks.labs.gbx.ds.raster import RasterGbxReader

        _plan_start = _time.monotonic()
        try:
            RasterGbxReader({"path": path, "sizeInMB": str(size_mib)}).partitions()
        except Exception:  # noqa: BLE001
            pass  # planning failure handled by the timed _job below
        _plan_s = _time.monotonic() - _plan_start

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        # Count the actual row count from one call so we can record it.
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        out = [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=int(actual_rows),
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(
                    (ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0
                ),
                per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(
                    (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
                ),
                peak_rss_mb=peak_rss_mb(),
                status="ok",
                note=os.path.basename(path.rstrip("/\\")),
                output_fingerprint="",
                plan_s=_plan_s,
                **env,
            )
        ]
    except Exception as e:  # noqa: BLE001
        out = [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="error",
                note=str(e)[:300],
                output_fingerprint="",
                plan_s=0.0,
                **env,
            )
        ]
    return out


def run_virtual_tile_pixel_read(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    where: str = "cluster",
    disable_file: bool = False,
    manifest: Optional[str] = None,
) -> List[ResultRow]:
    """Time a pixel-reading operation over virtual tiles from the light raster reader.

    Loads a directory as virtual tiles (``virtualTiles=true``), then applies
    ``rst_avg`` (the geobrix pyrx pixel accessor) on each tile to force actual pixel
    reads via the shipping FILE path.  ``rst_avg`` calls ``file_ref_arg(tile_col)``
    internally, which mints a FILE byte-range reference when ``file_supported()``
    is True; otherwise falls back to the plain FUSE-path read.  This is the real
    geobrix code path — ``file_ref_arg`` / ``open_windowed_via_fileref`` are both on
    the critical path, so the FILE-on vs FILE-off comparison is valid.

    ``disable_file=True`` sets ``GBX_DISABLE_FILE=1`` before the run; this causes
    ``file_supported()`` to return False → ``file_ref_arg`` returns ``lit(None)`` →
    the UDF falls back to the FUSE read (no FILE byte-range). Compare FILE-on vs
    FILE-off to measure the FILE byte-range read win in the virtual-tile reader path.

    **Cluster-only:** intended for manual at-scale runs on a dedicated cluster (not CI).
    Run the listing comparative first (``run_spark_path_reader`` with ``split_plan_read``),
    then this leg to measure executor-side read cost separately.

    Runbook:
    1. Confirm ``bench-corpus-reader-10k`` exists at
       ``/Volumes/geospatial_docs/geobrix/sample-data/bench-corpus-reader-10k``
       (10,000 tiny 256px / 1-band / float32 tiles; generated separately).
    2. Stage the wheel: ``gbx:data:push-wheel``.
    3. Run FILE-on (no env override).  Pass ``SPARK_WARMUP, SPARK_MEASURED`` (0/1)
       from the notebook globals — this is a spark-path leg, not a pure-core microbench::

         run_virtual_tile_pixel_read(spark, corpus_dir, run_id, SPARK_WARMUP, SPARK_MEASURED)

    4. Run FILE-off::

         run_virtual_tile_pixel_read(spark, corpus_dir, run_id, SPARK_WARMUP, SPARK_MEASURED,
                                     disable_file=True)

    5. Compare ``iter_median_s`` and ``throughput_rows_s`` between the two result rows.
    """
    import os

    if disable_file:
        os.environ["GBX_DISABLE_FILE"] = "1"
    else:
        os.environ.pop("GBX_DISABLE_FILE", None)

    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    import pyspark.sql.functions as _F

    from databricks.labs.gbx.pyrx import functions as _pyrx

    def _job():
        reader = spark.read.format("raster_gbx").option("virtualTiles", "true")
        if manifest is not None:
            reader = reader.option("manifest", manifest)
            load_path = path or "/"
        else:
            load_path = path
        return (
            reader.load(load_path)
            .select(_pyrx.rst_avg(_F.col("tile")).alias("avg"))
            .count()
        )

    mode_label = "virtual-pixel-read-no-file" if disable_file else "virtual-pixel-read"
    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        return [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read_pixels",
                category="reader",
                mode=mode_label,
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=int(actual_rows),
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(
                    (ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0
                ),
                per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(
                    (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
                ),
                peak_rss_mb=peak_rss_mb(),
                status="ok",
                note=f"virtual-tile pixel read ({mode_label})",
                output_fingerprint="",
                **env,
            )
        ]
    except Exception as e:  # noqa: BLE001
        return [
            ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="raster_read_pixels",
                category="reader",
                mode=mode_label,
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                iter_total_wall_clock_s=0.0,
                avg_wall_clock_s=0.0,
                per_tile_avg_s=0.0,
                per_tile_avg_ms=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=peak_rss_mb(),
                status="error",
                note=str(e)[:500],
                output_fingerprint="",
                **env,
            )
        ]


def run_format_read(
    spark,
    path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    fmt: str,
    options: Optional[Dict[str, str]] = None,
    where: str = "venv",
    size_mib: int = 16,
    ingest_table: Optional[str] = None,
) -> "ResultRow":
    """Time spark.read.format(fmt).load(path).count() on-cluster.

    For fmt=="raster_gbx": registers the light data source first.
    For fmt=="gdal": ensures the heavyweight GDAL driver is initialised.
    When ``ingest_table`` is set, the timed job writes the read DataFrame to
    that Delta table (mode="overwrite") and returns its row count -- a real
    ingest that forces materialization.  When None the behavior is unchanged
    (plain .count()).
    Returns a single ResultRow (mode="spark-path", category="reader").
    """
    env = capture_env(where)

    if fmt.endswith("_gbx"):
        # register() installs ALL light DataSources (raster_gbx, gtiff_gbx, pmtiles_gbx,
        # vector_gbx + the vector *_gbx). Registering only for fmt=="raster_gbx" left vector
        # formats (geojson_gbx, shapefile_gbx, ...) unregistered -> DATA_SOURCE_NOT_FOUND.
        from databricks.labs.gbx.ds.register import register

        register(spark)
    elif fmt in ("gdal", "gtiff_gdal", "netcdf_gdal"):
        # Heavy raster readers need the GDAL drivers initialised (registered via the
        # synchronized GDALManager guard inside rasterx.register). netcdf_gdal is the
        # heavy leg of the NetCDF raster bench; gtiff_gdal/gdal are the GeoTIFF readers.
        try:
            from databricks.labs.gbx.rasterx import functions as _rx

            _rx.register(spark)
        except Exception:  # noqa: BLE001
            pass  # best-effort GDAL init; failure surfaces in the timed call

    def _job():
        reader = spark.read.format(fmt)
        if options:
            for k, v in options.items():
                reader = reader.option(k, str(v))
        # sizeInMB is honored by the light raster reader (raster_gbx) AND the heavy
        # raster readers (netcdf_gdal / gdal / gtiff_gdal). Broadened beyond raster_gbx
        # so the netcdf raster leg can pass size_mib=-1 to BOTH tiers -- one tile per
        # grid variable -- giving the heavy reader the same one-tile-per-var granularity
        # as light (a fair heavy-vs-light comparison).
        # NOTE (re-baseline): the existing GeoTIFF reader leg calls fmt="gdal" without an
        # explicit size_mib, so it now inherits the default (16) and tiles at ~16 MB where
        # it previously read whole-image. This aligns it with the light raster_gbx leg
        # (already 16) -- fairer -- but GeoTIFF heavy reader-bench numbers are NOT
        # comparable across this change.
        if fmt in ("raster_gbx", "netcdf_gdal", "gdal", "gtiff_gdal"):
            reader = reader.option("sizeInMB", str(size_mib))
        df = reader.load(path)
        if ingest_table:
            # Write to a managed table.  On Databricks the default format is Delta;
            # on local Spark it defaults to Parquet.  Either satisfies the row-count
            # assertion -- we avoid hardcoding format("delta") so local tests work
            # without the Delta connector.
            df.write.mode("overwrite").saveAsTable(ingest_table)
            return spark.table(ingest_table).count()
        return df.count()

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        try:
            actual_rows = _job()
        except Exception:  # noqa: BLE001
            actual_rows = 0
        actual_rows = int(actual_rows)
        # basename computed outside the f-string: a backslash in an f-string expression
        # is a SyntaxError before Python 3.12, which trips flake8/linters on older hosts.
        _src_name = os.path.basename(path.rstrip("/\\"))
        _note = (
            f"{fmt} -> {ingest_table}" if ingest_table else f"{fmt} over {_src_name}"
        )
        # A 0-row read is not a valid throughput measurement (wrong corpus/options: e.g.
        # swaths in a raster dir, or a missing group/variables option). Mark it so it is
        # visible in the results table instead of masquerading as a clean "ok".
        _status = "ok" if actual_rows > 0 else "empty"
        if actual_rows == 0:
            _note = f"{_note} -- READ 0 ROWS (check corpus/options)"
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=f"read_{fmt}",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=actual_rows,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0,
            per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(
                (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
            ),
            peak_rss_mb=peak_rss_mb(),
            status=_status,
            note=_note,
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=f"read_{fmt}",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_format_write(
    spark,
    input_path: str,
    out_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    write_api: str,
    read_fmt: str = "raster_gbx",
    write_fmt: str = "gtiff_gbx",
    mode: str = "overwrite",
    options: Optional[Dict[str, str]] = None,
    label: str = "",
    where: str = "venv",
) -> "ResultRow":
    """Time spark.write.format(write_fmt).save(out_path) on a pre-read input DataFrame.

    ``mode`` is the Spark write mode: the light gtiff_gbx writer supports
    "overwrite"; the heavy gtiff_gdal writer is append-only ("overwrite" raises
    UNSUPPORTED_FEATURE truncate), so pass mode="append" for it.

    ``label`` (default "") distinguishes otherwise-identical legs in the store: when
    non-empty it is appended to the success ResultRow's ``note`` as " [<label>]" so a
    parts leg and a singleFile leg (same fn+fmt) don't collide. Empty label leaves the
    note byte-for-byte unchanged.

    Reads the input directory once via ``read_fmt`` (same reader for both tiers so
    write cost is isolated), caches it, then times repeated ``write.format(write_fmt)``
    calls. Returns a single ResultRow (mode="spark-path", category="writer").

    **Merge shape (post-hoc directory merge).** When ``options["merge"] == "true"``,
    the timed write is a *post-hoc* merge: it folds the ``.nc`` files ALREADY on disk
    in ``out_path`` into one, WITHOUT re-running the DataFrame. That needs the output
    dir pre-populated with parts, so this function first does a ONE-SHOT UNTIMED setup
    write of the read ``df`` to ``out_path`` in plain PARTS mode (a copy of ``options``
    with ``merge``/``singleFile``/``keepParts`` stripped). The timed ``_job`` then runs
    the caller's ``options`` verbatim (with ``merge=true``); the caller passes
    ``keepParts=true`` so the parts survive across warmup+measured iterations (a merge
    that deleted parts on iteration 1 would find an empty dir on iteration 2 and error).
    """
    env = capture_env(where)
    _note_suffix = f" [{label}]" if label else ""
    # Merge shape is inferred from the options dict (no new positional param, so all
    # existing callers are unchanged): merge=true means the timed write folds on-disk
    # parts and must be seeded with a one-shot untimed parts write before timing.
    _merge_mode = bool(options) and str(options.get("merge", "")).lower() == "true"

    # Register light DS (always needed for raster_gbx reader/writer).
    from databricks.labs.gbx.ds.register import register

    register(spark)

    # Best-effort heavy init when either format is a heavyweight GDAL format.
    _heavy_fmts = {"gdal", "gtiff_gdal"}
    if read_fmt in _heavy_fmts or write_fmt in _heavy_fmts:
        try:
            from databricks.labs.gbx.rasterx import functions as _rx

            _rx.register(spark)
        except Exception:  # noqa: BLE001
            pass

    # Read the input once and cache — isolates write cost from read cost.
    try:
        reader = spark.read.format(read_fmt)
        if options:
            for k, v in options.items():
                reader = reader.option(k, str(v))
        df = reader.load(input_path)
        df = df.cache()
        n = int(df.count())
        if n == 0:
            # A 0-row read is never a valid write measurement -- fail loud rather than
            # timing an empty write and reporting a meaningless "ok" (the input corpus
            # or read options are wrong: e.g. swaths in a raster dir, or a missing
            # group/variables option). Raising routes to the error ResultRow below.
            raise ValueError(
                f"{read_fmt} read of {input_path} returned 0 rows "
                f"(options={options}); nothing to write -- check the corpus/options."
            )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )

    # Merge shape: seed the output dir with PARTS once (untimed) so the timed merge
    # has files to fold. Strip the merge/singleFile/keepParts flags so this setup
    # write is a plain parts write; overwrite so a stale dir can't taint the merge.
    if _merge_mode:
        try:
            _setup_opts = {
                k: v
                for k, v in options.items()
                if k not in ("merge", "singleFile", "keepParts")
            }
            sw = df.write.format(write_fmt).mode("overwrite")
            for k, v in _setup_opts.items():
                sw = sw.option(k, str(v))
            sw.save(out_path)
            # Verify the seed actually landed parts: the timed merge folds the .nc
            # files already on disk, so a seed that wrote nothing would surface as a
            # cryptic "no .nc files to merge" from the timed job. Fail loud here with
            # the setup context instead (a stale wheel without this seed path, or a
            # writer that dropped no files, is the usual cause).
            import glob as _glob

            _seeded = _glob.glob(os.path.join(out_path, "*.nc"))
            if not _seeded:
                raise ValueError(
                    f"merge setup-parts write produced no .nc files under {out_path} "
                    f"(write_fmt={write_fmt}, setup_opts={_setup_opts}); the timed merge "
                    f"would have nothing to fold. Check the writer staged parts (and that "
                    f"the deployed wheel includes the merge-setup path)."
                )
        except Exception as e:  # noqa: BLE001
            return ResultRow(
                run_id=run_id,
                api=write_api,
                fn="raster_write",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                iter_total_wall_clock_s=0.0,
                avg_wall_clock_s=0.0,
                per_tile_avg_s=0.0,
                per_tile_avg_ms=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="error",
                note=f"merge setup-parts write failed: {str(e)[-400:]}",
                output_fingerprint="",
                **env,
            )

    def _job():
        w = df.write.format(write_fmt).mode(mode)
        if options:
            for k, v in options.items():
                w = w.option(k, str(v))
        w.save(out_path)

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{write_fmt} write of {n} tiles{_note_suffix}",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=write_api,
            fn="raster_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_pmtiles_write(
    spark,
    out_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    n_tiles: int = 1000,
    shard_zoom: int = 0,
    write_fmt: str = "pmtiles_gbx",
    where: str = "venv",
) -> "ResultRow":
    """Time a PMTiles write of ``n_tiles`` synthetic PNG tiles.

    ``write_fmt`` is ``'pmtiles_gbx'`` (light) or ``'pmtiles'`` (heavy).
    Generates distinct (z, x, y) tiles, caches the DataFrame, then times
    repeated ``write.format(write_fmt)`` calls. Returns a single ResultRow
    (mode="spark-path", category="writer").
    """
    env = capture_env(where)

    if write_fmt == "pmtiles_gbx":
        from databricks.labs.gbx.ds.register import register

        register(spark)

    # Build n_tiles distinct (z, x, y) synthetic PNG tiles.
    # z is chosen so that side*side >= n_tiles (no duplicate addresses).
    png_header = b"\x89PNG\r\n\x1a\n"
    z = max(1, (max(1, n_tiles) - 1).bit_length() // 2 + 1)
    # Ensure side^2 covers n_tiles.
    while (2**z) ** 2 < n_tiles:
        z += 1
    side = 2**z
    rows_data = []
    for i in range(n_tiles):
        x = i % side
        y = (i // side) % side
        rows_data.append((z, x, y, bytearray(png_header + i.to_bytes(4, "big"))))
    df = spark.createDataFrame(
        rows_data, schema="z int, x int, y int, bytes binary"
    ).cache()
    n = int(df.count())

    def _write():
        writer = df.write.format(write_fmt).mode("overwrite")
        if write_fmt == "pmtiles_gbx":
            writer = writer.option("shardZoom", str(shard_zoom))
        writer.save(out_path)

    try:
        stats = time_iters(_write, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api="lightweight" if write_fmt == "pmtiles_gbx" else "heavyweight",
            fn=write_fmt,
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{write_fmt} write of {n} tiles",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight" if write_fmt == "pmtiles_gbx" else "heavyweight",
            fn=write_fmt,
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_vector_write(
    spark,
    src_path: str,
    out_dir: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    fmt: str,
    where: str = "venv",
    src_is_table: bool = False,
) -> "ResultRow":
    """Time write.format(fmt) for a light vector writer, with read-back parity.

    Light-only: there is no heavy vector writer tier.  When ``src_is_table``
    is True, reads the source from a pre-existing Spark table (``src_path`` is
    the table name); otherwise reads ``src_path`` via the ``fmt`` light reader.
    Caches the source DataFrame, then times repeated
    ``write.format(fmt).mode("overwrite").save(target)`` calls (no coalesce --
    the two-phase writer merges fragments on commit), writing to a distinct
    ``out_dir/iter.m<i>`` per iteration to avoid append/overwrite contention.
    After timing, reads back the last written target and asserts that the
    non-null geometry count equals the source count.

    Returns a single ResultRow with category="writer", mode="spark-path",
    fn="write_<fmt>", api="lightweight".  On any error returns status="error"
    with the exception in ``note`` (does not raise).
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    # Read source once and count to establish the feature count for parity.
    try:
        if src_is_table:
            df = spark.table(src_path)
        else:
            df = spark.read.format(fmt).load(src_path)
        df = df.cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )

    # Build per-iteration target paths so repeated writes go to fresh directories.
    # Use a format-appropriate extension -- OpenFileGDB (file_gdb_gbx) requires a
    # `.gdb` path or GDAL's CreateDataSource returns None; the others are lenient but
    # a natural extension keeps the round-trip realistic.
    _ext = {
        "geojson_gbx": ".geojson",
        "shapefile_gbx": ".shp",
        "gpkg_gbx": ".gpkg",
        "file_gdb_gbx": ".gdb",
        "vector_gbx": ".geojson",
        "ogr_gbx": ".geojson",
    }.get(fmt, "")
    _targets = [f"{out_dir}/iter.m{i}{_ext}" for i in range(max(1, measured))]
    _iter_idx = [0]

    def _job():
        target = _targets[_iter_idx[0] % len(_targets)]
        _iter_idx[0] += 1
        df.write.format(fmt).mode("overwrite").save(target)

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]

        # Read-back parity: last written target (index measured-1, clamped to len).
        _last = _targets[(max(1, measured) - 1) % len(_targets)]
        try:
            back = spark.read.format(fmt).load(_last)
            # Derive geometry column name from the schema: the geom col has a sibling
            # "<col>_srid" field.  Use the first such pair found.
            _srid_fields = [
                f.name for f in back.schema.fields if f.name.endswith("_srid")
            ]
            if _srid_fields:
                _gcol = _srid_fields[0][: -len("_srid")]
                import pyspark.sql.functions as _F

                _back_n = int(back.filter(_F.col(_gcol).isNotNull()).count())
            else:
                _back_n = int(back.count())
            if _back_n != n:
                return ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn=f"write_{fmt}",
                    category="writer",
                    mode="spark-path",
                    tile_px=0,
                    bands=0,
                    dtype="",
                    srid=0,
                    rows=n,
                    nodata_frac=0.0,
                    warmup_iters=stats["warmup_iters"],
                    measured_iters=stats["measured_iters"],
                    iter_median_s=ms / 1000.0,
                    iter_min_s=stats["iter_min_ms"] / 1000.0,
                    iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                    iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                    avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                    per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
                    per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
                    throughput_mpix_s=0.0,
                    throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                    peak_rss_mb=peak_rss_mb(),
                    status="error",
                    note=f"parity FAIL: wrote {n}, read back {_back_n} ({fmt})",
                    output_fingerprint="",
                    **env,
                )
        except Exception as _pe:  # noqa: BLE001
            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn=f"write_{fmt}",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=n,
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
                per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                peak_rss_mb=peak_rss_mb(),
                status="error",
                note=f"readback error: {str(_pe)[-450:]}",
                output_fingerprint="",
                **env,
            )

        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{fmt} write+readback of {n} features",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn=f"write_{fmt}",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_mvt_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_features: int = 500,
    n_tiles: int = 10,
    where: str = "cluster",
) -> "ResultRow":
    """Time a grouped st_asmvt aggregation over synthetic in-memory features.

    Builds ``n_features`` features distributed across ``n_tiles`` (z,x,y) keys,
    each with a WKB polygon (tile-local coordinates) plus a mixed-type attrs struct
    (int id, double score, str label). Registers the chosen tier, caches the
    DataFrame, then times a ``groupBy("z","x","y").agg(st_asmvt(...))`` job to
    completion. Returns a single ResultRow (mode="spark-path", category="mvt").

    ``api`` controls which tier is registered and timed:
        "lightweight"  → ``databricks.labs.gbx.pyvx.functions``
        "heavyweight"  → ``databricks.labs.gbx.vectorx.functions``
    """
    env = capture_env(where)

    # Register the tier.
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
            asmvt_fn = vx.st_asmvt
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
            asmvt_fn = hx.st_asmvt
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"register error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build a synthetic features DataFrame with n_features rows across n_tiles keys.
    # Each tile gets roughly equal features.  Geometries are small squares in
    # tile-local [0, 4096] coordinates (WKB); attrs is a struct with native types.
    try:
        import pyspark.sql.functions as _F
        from pyspark.sql.types import (
            BinaryType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
        from shapely import to_wkb as _to_wkb
        from shapely.geometry import box as _box

        # Build tile addresses: a small z=3 grid so (z,x,y) is always valid.
        z = 3
        tile_addresses = [(z, i % 8, (i // 8) % 8) for i in range(n_tiles)]

        rows_data = []
        for i in range(n_features):
            tz, tx, ty = tile_addresses[i % n_tiles]
            # Spread each tile's squares across the FULL [0, 4096] tile extent on a
            # coarse grid. The heavy MVT driver (OGR, EPSG:3857, single 0/0/0 tile)
            # quantizes the whole layer to EXTENT=4096, so squares packed into a tiny
            # coordinate band collapse to sub-pixel and the driver drops them (empty
            # tile). Light keeps them (it treats the coords as already tile-local), so
            # a packed band silently breaks light-vs-heavy parity. A 16x16 grid over
            # the extent (step 256) keeps every square distinct + above the
            # quantization floor in both tiers.
            slot = (i // n_tiles) % 256
            cx = 128 + (slot % 16) * 256
            cy = 128 + (slot // 16) * 256
            geom = _box(cx - 32, cy - 32, cx + 32, cy + 32)
            wkb = bytes(_to_wkb(geom))
            rows_data.append((tz, tx, ty, wkb, i, float(i) * 0.1, f"feat_{i}"))

        schema = StructType(
            [
                StructField("z", IntegerType(), False),
                StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False),
                StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True),
                StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ]
        )
        raw_df = spark.createDataFrame(rows_data, schema=schema)
        # Pack id/score/label into a attrs struct so the aggregator gets a struct column.
        df = raw_df.select(
            "z",
            "x",
            "y",
            "geom",
            _F.struct(
                _F.col("id"),
                _F.col("score"),
                _F.col("label"),
            ).alias("attrs"),
        ).cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"dataframe build error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    def _job():
        import pyspark.sql.functions as _F2

        return (
            df.groupBy("z", "x", "y")
            .agg(
                asmvt_fn(_F2.col("geom"), _F2.col("attrs"), _F2.lit("layer")).alias(
                    "mvt"
                )
            )
            .count()
        )

    try:
        # Guard against a tier that "succeeds" (10 groups counted) but emits all-NULL or
        # all-empty MVT blobs -- e.g. the heavy OGR MVT driver dropping every feature to a
        # sub-pixel collapse. Counting groups alone masks that, so validate (once, untimed)
        # that at least one group produced a non-empty blob; a collapse becomes a status=
        # "error" row (via the except below), not a misleading "ok".
        import pyspark.sql.functions as _F3

        _validation = (
            df.groupBy("z", "x", "y")
            .agg(
                asmvt_fn(_F3.col("geom"), _F3.col("attrs"), _F3.lit("layer")).alias(
                    "mvt"
                )
            )
            .collect()
        )
        _nonempty = sum(
            1 for _r in _validation if _r["mvt"] and len(bytes(_r["mvt"])) > 0
        )
        if _nonempty == 0:
            raise RuntimeError(
                f"st_asmvt {api} produced {len(_validation)} group(s) but every MVT blob "
                "is NULL/empty -- features collapsed (check coordinate extent vs the "
                "encoder's quantization)."
            )
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        n_tile_groups = min(n, n_tiles)
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(
                (ms / n_tile_groups / 1000.0) if (ms and n_tile_groups) else 0.0
            ),
            per_tile_avg_ms=(ms / n_tile_groups) if (ms and n_tile_groups) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(
                (n_tile_groups / (ms / 1000.0)) if (ms and n_tile_groups) else 0.0
            ),
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"st_asmvt {api} {n} features -> {n_tile_groups} tiles",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="st_asmvt",
            category="mvt",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def run_pmtiles_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_tiles: int = 1000,
    n_groups: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time a grouped pmtiles_agg over synthetic in-memory PNG tiles.

    Folds ``n_tiles`` synthetic PNG tiles distributed across ``n_groups``
    (z,x,y) addresses into PMTiles archive(s) via a
    ``groupBy(g).agg(pmtiles_agg(tile, z, x, y))`` job. Registers the chosen
    tier, caches the DataFrame, then times the aggregation to completion.
    Returns a single ResultRow (mode="spark-path", category="pmtiles_agg").

    Both tiers resolve the same canonical ``pmtiles_agg`` wrapper
    (``databricks.labs.gbx.pmtiles.functions.pmtiles_agg`` ->
    ``gbx_pmtiles_agg``); ``api`` only controls which register path installs
    the SQL function:
        "lightweight"  -> ``register_pmtiles_agg`` (pure-Python grouped agg)
        "heavyweight"  -> ``functions.register`` (Scala UDAF via the JAR)
    """
    env = capture_env(where)

    # Register the tier.
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pmtiles import register_pmtiles_agg

            register_pmtiles_agg(spark)
        else:
            from databricks.labs.gbx.pmtiles import functions as _pt_reg

            _pt_reg.register(spark)
        from databricks.labs.gbx.pmtiles import functions as pt

        agg_fn = pt.pmtiles_agg
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="pmtiles_agg",
            category="pmtiles_agg",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"register error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build n_tiles distinct (z, x, y) synthetic PNG tiles spread across n_groups
    # group keys. z is chosen so side*side >= n_tiles (no duplicate addresses).
    try:
        from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

        png_header = b"\x89PNG\r\n\x1a\n"
        z = max(1, (max(1, n_tiles) - 1).bit_length() // 2 + 1)
        while (2**z) ** 2 < n_tiles:
            z += 1
        side = 2**z
        n_groups = max(1, int(n_groups))

        rows_data = []
        for i in range(n_tiles):
            x = i % side
            y = (i // side) % side
            rows_data.append(
                (
                    i % n_groups,
                    bytearray(png_header + i.to_bytes(4, "big")),
                    z,
                    x,
                    y,
                )
            )
        schema = StructType(
            [
                StructField("g", IntegerType(), False),
                StructField("tile", BinaryType(), True),
                StructField("z", IntegerType(), False),
                StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False),
            ]
        )
        df = spark.createDataFrame(rows_data, schema=schema).cache()
        n = int(df.count())
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="pmtiles_agg",
            category="pmtiles_agg",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"dataframe build error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    def _job():
        import pyspark.sql.functions as _F2

        return (
            df.groupBy("g")
            .agg(
                agg_fn(_F2.col("tile"), _F2.col("z"), _F2.col("x"), _F2.col("y")).alias(
                    "arc"
                )
            )
            .count()
        )

    try:
        # Guard against a tier that "succeeds" (groups counted) but emits all-NULL or
        # all-empty archives. Counting groups alone masks that, so validate (once,
        # untimed) that every group produced a non-empty PMTiles blob; an empty result
        # becomes a status="error" row (via the except below), not a misleading "ok".
        import pyspark.sql.functions as _F3

        _validation = (
            df.groupBy("g")
            .agg(
                agg_fn(_F3.col("tile"), _F3.col("z"), _F3.col("x"), _F3.col("y")).alias(
                    "arc"
                )
            )
            .collect()
        )
        _nonempty = sum(
            1 for _r in _validation if _r["arc"] and len(bytes(_r["arc"])) > 0
        )
        if _nonempty == 0:
            raise RuntimeError(
                f"pmtiles_agg {api} produced {len(_validation)} group(s) but every "
                "archive blob is NULL/empty."
            )
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="pmtiles_agg",
            category="pmtiles_agg",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"pmtiles_agg {api} {n} tiles -> {len(_validation)} archive(s)",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="pmtiles_agg",
            category="pmtiles_agg",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def _error_row(fn, category, run_id, api, warmup, env, e, **extra):
    """Module-private helper: build a status='error' ResultRow with zeroed timing.

    Factors the repeated 20-field error-row pattern across the FILE bench legs.
    ``**extra`` forwards keyword arguments specific to each leg (e.g.
    ``file_mode``, ``layout``) so callers stay readable.
    """
    return ResultRow(
        run_id=run_id,
        api=api,
        fn=fn,
        category=category,
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        rows=0,
        nodata_frac=0.0,
        warmup_iters=warmup,
        measured_iters=0,
        iter_median_s=0.0,
        iter_min_s=0.0,
        iter_p90_s=0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=0.0,
        peak_rss_mb=0.0,
        status="error",
        note=str(e)[-500:],
        output_fingerprint="",
        **extra,
        **env,
    )


def run_gtiff_file_read(
    spark,
    source: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    file_mode: str,
    api: str = "lightweight",
    where: str = "cluster",
) -> "ResultRow":
    """Time a FILE-mode GeoTIFF read via gbx_file_read + count.

    ``file_mode`` ∈ {"fuse", "external", "managed"}.  ``source`` is a Volume dir
    for fuse/external, or a FILE-table name for managed.

    On a FUSE-only tier (local[2], no FILE), external/managed raise ValueError from
    ``gbx_file_read`` → clean ``status="na_by_design"`` row (FILE unavailable note).

    Returns a single ResultRow (mode="spark-path", category="reader").
    """
    from databricks.labs.gbx.ds.file_gbx import gbx_file_read
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)
    access = {"fuse": "auto", "external": "external", "managed": "managed"}[file_mode]

    def _job():
        refs = gbx_file_read(spark, source, access=access)
        return int(refs.count())

    try:
        # Probe once: on a FUSE-only tier, external/managed raise ValueError →
        # clean na_by_design skip (local has no FILE tier; this is the on-cluster gate).
        try:
            n0 = _job()
        except ValueError as ve:
            return ResultRow(
                run_id=run_id,
                api=api,
                fn="gtiff_file_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="na_by_design",
                note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
                output_fingerprint="",
                file_mode=file_mode,
                **env,
            )
        refs = gbx_file_read(spark, source, access=access)
        parts, slots = measure_parallelism(spark, refs)
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        _src_name = os.path.basename(str(source).rstrip("/\\"))
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="gtiff_file_read",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n0,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n0 / 1000.0) if (ms and n0) else 0.0,
            per_tile_avg_ms=(ms / n0) if (ms and n0) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n0 / (ms / 1000.0)) if (ms and n0) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok" if n0 > 0 else "empty",
            note=f"gtiff FILE read [{file_mode}] over {_src_name}",
            output_fingerprint="",
            file_mode=file_mode,
            input_partitions=parts,
            launched_tasks=parts,
            slots_available=slots,
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return _error_row(
            "gtiff_file_read",
            "reader",
            run_id,
            api,
            warmup,
            env,
            e,
            file_mode=file_mode,
        )


def run_gtiff_file_write(
    spark,
    tile_df,
    target: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    file_mode: str,
    filespace: Optional[str] = None,
    layout: str = "order",
    where: str = "cluster",
) -> "ResultRow":
    """Time a FILE-mode GeoTIFF write: FILE table (managed/external) or FUSE gtiff_gbx.

    ``file_mode`` ∈ {"fuse", "external", "managed"}.  For FUSE, uses
    ``tile_df.write.format("gtiff_gbx").mode("overwrite").save(target)``.  For FILE
    modes, delegates to ``write_file_table(..., file_mode=file_mode, ...)``.

    ``tile_df`` may carry virtual tiles (``tile.path`` set, raster None) — this leg
    thereby covers virtual-tile → Volume/FILE-table write.

    Records ``file_mode``, ``layout``, and ``measure_parallelism(spark, tile_df)``.
    Performs a read-back correctness check after timing.

    On a FUSE-only tier (local[2]), FILE modes raise ValueError →
    ``status="na_by_design"`` (FILE unavailable).

    Returns a single ResultRow (mode="spark-path", category="writer").
    """
    from databricks.labs.gbx.ds.register import register
    from databricks.labs.gbx.pyrx.file_table import write_file_table

    register(spark)
    env = capture_env(where)
    parts, slots = measure_parallelism(spark, tile_df)
    n = int(tile_df.count())

    if file_mode == "fuse":

        def _job():
            tile_df.write.format("gtiff_gbx").mode("overwrite").save(target)

    else:  # managed or external

        def _job():
            write_file_table(
                spark,
                tile_df,
                target,
                file_mode=file_mode,
                filespace=filespace,
                layout=layout,
                overwrite=True,
            )

    # Probe once: on FUSE-only tier, external/managed raise ValueError → na_by_design.
    try:
        _job()
    except ValueError as ve:
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn="gtiff_file_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="na_by_design",
            note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
            output_fingerprint="",
            file_mode=file_mode,
            layout=layout,
            input_partitions=parts,
            launched_tasks=parts,
            slots_available=slots,
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return _error_row(
            "gtiff_file_write",
            "writer",
            run_id,
            "lightweight",
            warmup,
            env,
            e,
            file_mode=file_mode,
            layout=layout,
        )

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]

        # Correctness check: read back and compare count.
        if file_mode == "fuse":
            back_n = int(spark.read.format("gtiff_gbx").load(target).count())
        else:
            from databricks.labs.gbx.pyrx.file_table import read_file_table

            back_n = int(read_file_table(spark, target).count())

        _ok = back_n == n
        _status = "ok" if _ok else "error"
        _note = f"gtiff FILE write [{file_mode}] {n} tiles" + (
            f" -- readback {back_n} != {n}" if not _ok else ""
        )
        return ResultRow(
            run_id=run_id,
            api="lightweight",
            fn="gtiff_file_write",
            category="writer",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
            per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status=_status,
            note=_note,
            output_fingerprint="",
            file_mode=file_mode,
            layout=layout,
            input_partitions=parts,
            launched_tasks=parts,
            slots_available=slots,
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return _error_row(
            "gtiff_file_write",
            "writer",
            run_id,
            "lightweight",
            warmup,
            env,
            e,
            file_mode=file_mode,
            layout=layout,
        )


def run_gpkg_file_read(
    spark,
    source: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    file_mode: str,
    chunk_size: int = 10000,
    api: str = "lightweight",
    where: str = "cluster",
) -> "ResultRow":
    """Time a FILE-mode GeoPackage read via vector_file_read / gpkg_gbx + count.

    ``file_mode`` in {"fuse", "external", "managed"}.  ``source`` is a Volume
    dir for fuse/external (a directory of .gpkg files), or a FILE-table name
    for managed.

    - fuse: reads the dir via ``spark.read.format("gpkg_gbx").option("chunkSize",
      chunk_size).load(source)``; records ``chunk_size`` in the result.
    - external: ``vector_file_read(spark, source, driver="GPKG", access="external")``
      raises ValueError on a FUSE-only tier → ``status="na_by_design"``.
    - managed: ``read_file_table(spark, source)`` → filter non-null path + count.

    Timed ``_job`` = ``df.filter("geometry IS NOT NULL").count()`` (fuse/external)
    or ``df.filter(F.col("tile.path").isNotNull()).count()`` (managed).

    Records ``chunk_size``, ``file_mode``, and ``measure_parallelism`` on the
    input df.

    Returns a single ResultRow (mode="spark-path", category="reader").
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    # --- build the input df and _job based on file_mode -------------------- #
    if file_mode == "fuse":
        df = (
            spark.read.format("gpkg_gbx")
            .option("chunkSize", str(chunk_size))
            .load(source)
        )
        # Detect geometry column via *_srid sibling (robust to any GPKG layer name).
        _srid_cols = [f.name for f in df.schema.fields if f.name.endswith("_srid")]
        _gcol = _srid_cols[0][: -len("_srid")] if _srid_cols else "geometry"

        def _job():
            import pyspark.sql.functions as _F

            return int(df.filter(_F.col(_gcol).isNotNull()).count())

    elif file_mode == "external":
        # Probe once: on a FUSE-only tier, vector_file_read raises ValueError →
        # clean na_by_design skip (no FILE tier available locally).
        try:
            from databricks.labs.gbx.pyvx.file_read import vector_file_read

            df = vector_file_read(spark, source, driver="GPKG", access="external")
        except ValueError as ve:
            return ResultRow(
                run_id=run_id,
                api=api,
                fn="gpkg_file_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="na_by_design",
                note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
                output_fingerprint="",
                file_mode=file_mode,
                chunk_size=chunk_size,
                **env,
            )

        def _job():
            import pyspark.sql.functions as _F

            return int(df.filter(_F.col("geometry").isNotNull()).count())

    else:  # managed
        from databricks.labs.gbx.pyrx.file_table import read_file_table

        df = read_file_table(spark, source)

        def _job():
            import pyspark.sql.functions as _F

            return int(df.filter(_F.col("tile.path").isNotNull()).count())

    # --- probe once for count + parallelism --------------------------------- #
    try:
        try:
            n0 = _job()
        except ValueError as ve:
            return ResultRow(
                run_id=run_id,
                api=api,
                fn="gpkg_file_read",
                category="reader",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="na_by_design",
                note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
                output_fingerprint="",
                file_mode=file_mode,
                chunk_size=chunk_size,
                **env,
            )
        parts, slots = measure_parallelism(spark, df)
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        _src_name = os.path.basename(str(source).rstrip("/\\"))
        return ResultRow(
            run_id=run_id,
            api=api,
            fn="gpkg_file_read",
            category="reader",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=n0,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / n0 / 1000.0) if (ms and n0) else 0.0,
            per_tile_avg_ms=(ms / n0) if (ms and n0) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(n0 / (ms / 1000.0)) if (ms and n0) else 0.0,
            peak_rss_mb=peak_rss_mb(),
            status="ok" if n0 > 0 else "empty",
            note=f"gpkg FILE read [{file_mode}] over {_src_name}",
            output_fingerprint="",
            file_mode=file_mode,
            chunk_size=chunk_size,
            input_partitions=parts,
            launched_tasks=parts,
            slots_available=slots,
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return _error_row(
            "gpkg_file_read",
            "reader",
            run_id,
            api,
            warmup,
            env,
            e,
            file_mode=file_mode,
            chunk_size=chunk_size,
        )


def _gpkg_fuse_readback(spark, path: str, n: int) -> Tuple[str, str]:
    """Read back a fuse-written GPKG and return (status, note) for the ResultRow.

    Finds the geometry column via the *_srid sibling, counts non-null geometries,
    and returns ("ok", ...) if the count matches *n*, ("error", ...) otherwise.
    On any exception returns ("error", truncated exception message).
    """
    try:
        import pyspark.sql.functions as _F

        back = spark.read.format("gpkg_gbx").load(path)
        _srid_fields = [f.name for f in back.schema.fields if f.name.endswith("_srid")]
        if _srid_fields:
            _gcol = _srid_fields[0][: -len("_srid")]
            back_n = int(back.filter(_F.col(_gcol).isNotNull()).count())
        else:
            back_n = int(back.count())
        ok = back_n == n
        note = f"gpkg FILE write [fuse] {n} features" + (
            f" -- readback {back_n} != {n}" if not ok else ""
        )
        return "ok" if ok else "error", note
    except Exception as e:  # noqa: BLE001
        return "error", f"readback error: {str(e)[-450:]}"


def run_gpkg_file_write(
    spark,
    local_out: str,
    target: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    file_mode: str,
    filespace: Optional[str] = None,
    layout: str = "order",
    where: str = "cluster",
) -> "ResultRow":
    """Time a FILE-mode GeoPackage write: FILE table (managed/external) or FUSE gpkg_gbx.

    ``file_mode`` in {"fuse", "external", "managed"}.  ``local_out`` is a path
    to a local/FUSE-accessible .gpkg file (the assembled source to write from).

    - fuse: reads ``local_out`` via ``spark.read.format("gpkg_gbx")``, caches the
      DataFrame, then times ``df.write.format("gpkg_gbx").mode("overwrite").save(target_iter)``
      per iteration into fresh sub-directories to avoid append/overwrite contention.
      Performs a read-back correctness check on the last written target.
    - managed/external: delegates to ``vector_file_write(spark, local_out, target,
      driver="GPKG", file_mode=file_mode, ...)``.  On a FUSE-only tier both modes
      raise ValueError → ``status="na_by_design"``.

    Records ``file_mode``, ``layout``, and ``measure_parallelism`` on the source df.

    Returns a single ResultRow (mode="spark-path", category="writer").
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)
    env = capture_env(where)

    if file_mode == "fuse":
        # Read the assembled .gpkg file into a cached DataFrame.
        try:
            df = spark.read.format("gpkg_gbx").load(local_out)
            df = df.cache()
            n = int(df.count())
        except Exception as e:  # noqa: BLE001
            return _error_row(
                "gpkg_file_write",
                "writer",
                run_id,
                "lightweight",
                warmup,
                env,
                e,
                file_mode=file_mode,
                layout=layout,
            )

        parts, slots = measure_parallelism(spark, df)

        # Per-iteration target paths to avoid append/overwrite contention.
        # Include .gpkg extension so the writer places the file at the exact
        # target path (without extension, GDAL appends .gpkg, producing a
        # path different from what the readback expects).
        _targets = [f"{target}/iter.m{i}.gpkg" for i in range(max(1, measured))]
        _iter_idx = [0]

        def _job():
            t = _targets[_iter_idx[0] % len(_targets)]
            _iter_idx[0] += 1
            df.write.format("gpkg_gbx").mode("overwrite").save(t)

        # Probe once: fuse write should not raise ValueError, but guard anyway.
        try:
            _job()
        except ValueError as ve:
            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="gpkg_file_write",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="na_by_design",
                note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
                output_fingerprint="",
                file_mode=file_mode,
                layout=layout,
                input_partitions=parts,
                launched_tasks=parts,
                slots_available=slots,
                **env,
            )
        except Exception as e:  # noqa: BLE001
            return _error_row(
                "gpkg_file_write",
                "writer",
                run_id,
                "lightweight",
                warmup,
                env,
                e,
                file_mode=file_mode,
                layout=layout,
            )

        try:
            stats = time_iters(_job, warmup, measured)
            ms = stats["iter_median_ms"]

            # Read-back correctness check on the last written target.
            _last = _targets[(max(1, measured) - 1) % len(_targets)]
            _status, _note = _gpkg_fuse_readback(spark, _last, n)

            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="gpkg_file_write",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=n,
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
                per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                peak_rss_mb=peak_rss_mb(),
                status=_status,
                note=_note,
                output_fingerprint="",
                file_mode=file_mode,
                layout=layout,
                input_partitions=parts,
                launched_tasks=parts,
                slots_available=slots,
                **env,
            )
        except Exception as e:  # noqa: BLE001
            return _error_row(
                "gpkg_file_write",
                "writer",
                run_id,
                "lightweight",
                warmup,
                env,
                e,
                file_mode=file_mode,
                layout=layout,
            )

    else:  # managed or external
        from databricks.labs.gbx.pyvx.file_write import vector_file_write

        # Read the source gpkg to get parallelism info.
        try:
            df_src = spark.read.format("gpkg_gbx").load(local_out)
            parts, slots = measure_parallelism(spark, df_src)
            n = int(df_src.count())
        except Exception:  # noqa: BLE001
            parts, slots, n = 0, 0, 0

        def _job():
            vector_file_write(
                spark,
                local_out,
                target,
                driver="GPKG",
                file_mode=file_mode,
                filespace=filespace,
                layout=layout,
                overwrite=True,
            )

        # Probe once: on FUSE-only tier, managed/external raise ValueError → na_by_design.
        try:
            _job()
        except ValueError as ve:
            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="gpkg_file_write",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=0,
                nodata_frac=0.0,
                warmup_iters=warmup,
                measured_iters=0,
                iter_median_s=0.0,
                iter_min_s=0.0,
                iter_p90_s=0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=0.0,
                peak_rss_mb=0.0,
                status="na_by_design",
                note=f"FILE {file_mode} unavailable on this tier: {str(ve)[:160]}",
                output_fingerprint="",
                file_mode=file_mode,
                layout=layout,
                input_partitions=parts,
                launched_tasks=parts,
                slots_available=slots,
                **env,
            )
        except Exception as e:  # noqa: BLE001
            return _error_row(
                "gpkg_file_write",
                "writer",
                run_id,
                "lightweight",
                warmup,
                env,
                e,
                file_mode=file_mode,
                layout=layout,
            )

        try:
            stats = time_iters(_job, warmup, measured)
            ms = stats["iter_median_ms"]

            # Correctness check: read back from the FILE table.
            import pyspark.sql.functions as _F

            from databricks.labs.gbx.pyrx.file_table import read_file_table

            back_n = int(
                read_file_table(spark, target)
                .filter(_F.col("tile.path").isNotNull())
                .count()
            )
            _ok = back_n == n
            _status = "ok" if _ok else "error"
            _note = f"gpkg FILE write [{file_mode}] {n} features" + (
                f" -- readback {back_n} != {n}" if not _ok else ""
            )
            return ResultRow(
                run_id=run_id,
                api="lightweight",
                fn="gpkg_file_write",
                category="writer",
                mode="spark-path",
                tile_px=0,
                bands=0,
                dtype="",
                srid=0,
                rows=n,
                nodata_frac=0.0,
                warmup_iters=stats["warmup_iters"],
                measured_iters=stats["measured_iters"],
                iter_median_s=ms / 1000.0,
                iter_min_s=stats["iter_min_ms"] / 1000.0,
                iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
                avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
                per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
                throughput_mpix_s=0.0,
                throughput_rows_s=(n / (ms / 1000.0)) if (ms and n) else 0.0,
                peak_rss_mb=peak_rss_mb(),
                status=_status,
                note=_note,
                output_fingerprint="",
                file_mode=file_mode,
                layout=layout,
                input_partitions=parts,
                launched_tasks=parts,
                slots_available=slots,
                **env,
            )
        except Exception as e:  # noqa: BLE001
            return _error_row(
                "gpkg_file_write",
                "writer",
                run_id,
                "lightweight",
                warmup,
                env,
                e,
                file_mode=file_mode,
                layout=layout,
            )


def _tin_result_row(
    *,
    run_id: str,
    api: str,
    fn: str,
    category: str,
    env: dict,
    rows: int,
    status: str,
    note: str,
    stats: Optional[dict] = None,
    warmup: int = 0,
) -> "ResultRow":
    """Compact ResultRow builder for the TIN/legacy spark-path legs.

    When ``stats`` is None (error path) the timing fields are zeroed and
    ``measured_iters`` is 0; otherwise they are filled from ``time_iters``
    output and per-row metrics are amortized over ``rows`` (output rows --
    triangles / interpolated points / decoded geometries)."""
    if stats is None:
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category=category,
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=rows,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status=status,
            note=note[-500:],
            output_fingerprint="",
            **env,
        )
    ms = stats["iter_median_ms"]
    return ResultRow(
        run_id=run_id,
        api=api,
        fn=fn,
        category=category,
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        rows=rows,
        nodata_frac=0.0,
        warmup_iters=stats["warmup_iters"],
        measured_iters=stats["measured_iters"],
        iter_median_s=ms / 1000.0,
        iter_min_s=stats["iter_min_ms"] / 1000.0,
        iter_p90_s=stats["iter_p90_ms"] / 1000.0,
        iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
        avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
        per_tile_avg_s=(ms / rows / 1000.0) if (ms and rows) else 0.0,
        per_tile_avg_ms=(ms / rows) if (ms and rows) else 0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=(rows / (ms / 1000.0)) if (ms and rows) else 0.0,
        peak_rss_mb=peak_rss_mb(),
        status=status,
        note=note[-500:],
        output_fingerprint="",
        **env,
    )


def run_legacy_aswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_legacyaswkb decode over ``n_rows`` synthetic legacy structs.

    Light (``api="lightweight"``) registers ``databricks.labs.gbx.pyvx`` and
    times ``SELECT gbx_st_legacyaswkb(g) FROM v``. Heavy (``api="heavyweight"``)
    registers ``databricks.labs.gbx.vectorx.jts.legacy`` -- the SAME SQL name --
    and times the same query. (The shared name means a light+heavy parity cell
    must collect light BEFORE registering heavy; that ordering lives in the
    cluster cell, not here -- each call registers exactly one tier.)
    Returns a single ResultRow (mode="spark-path", category="legacy").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_legacy_structs

    env = capture_env(where)
    fn = "st_legacyaswkb"
    cat = "legacy"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx.jts.legacy import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_legacy_structs(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_legacy_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _legacy_bench_v"
        ).count()

    try:
        # Untimed validation: confirm the decode produces non-null WKB.
        _val = spark.sql("SELECT gbx_st_legacyaswkb(g) AS w FROM _legacy_bench_v").head(
            1
        )
        if not _val or _val[0]["w"] is None or len(bytes(_val[0]["w"])) == 0:
            raise RuntimeError("st_legacyaswkb produced null/empty WKB")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"st_legacyaswkb {api} decoded {n} geometries",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_triangulate(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_triangulate over ``n_rows`` synthetic point arrays.

    Light registers pyvx UDTFs and times the SQL ``LATERAL`` TVF; heavy
    registers vectorx and times the JVM generator-Column form (the surfaces
    occupy different catalog paths and coexist). Records ``rows`` = number of
    output triangles. Returns one ResultRow (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_triangulate"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.triangle FROM _tin_bench_v, LATERAL "
                "gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_triangulate",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.lit("constrained"),
                ).alias("triangle")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_triangulate produced no triangles")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_triangulate {api} -> {n_out} triangles",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_interp_bbox(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_interpolateelevationbbox over ``n_rows`` synthetic point arrays.

    Light = SQL ``LATERAL`` UDTF; heavy = JVM generator-Column. Records
    ``rows`` = number of interpolated grid points. Returns one ResultRow
    (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_interpolateelevationbbox"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.elevation_point AS p FROM _tin_bench_v, LATERAL "
                "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, "
                "xmin, ymin, xmax, ymax, w, h, srid, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_interpolateelevationbbox",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.col("xmin"),
                    _f.col("ymin"),
                    _f.col("xmax"),
                    _f.col("ymax"),
                    _f.col("w"),
                    _f.col("h"),
                    _f.col("srid"),
                    _f.lit("constrained"),
                ).alias("p")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_interpolateelevationbbox produced no points")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_interpolateelevationbbox {api} -> {n_out} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_interp_geom(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 5,
    n_points: int = 25,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_st_interpolateelevationgeom over ``n_rows`` synthetic point arrays.

    Light = SQL ``LATERAL`` UDTF (arg order: pts, bl, mt, st, spf, origin, cols,
    rows, cell_x, cell_y, mode); heavy = JVM generator-Column (same arg order).
    Records ``rows`` = number of interpolated grid points. Returns one ResultRow
    (mode="spark-path", category="tin").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_tin_points

    env = capture_env(where)
    fn = "st_interpolateelevationgeom"
    cat = "tin"
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyvx import functions as vx

            vx.register(spark)
        else:
            from databricks.labs.gbx.vectorx import functions as hx

            hx.register(spark)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_tin_points(n_rows, n_points=n_points)
        df = spark.createDataFrame(data, schema=schema).cache()
        df.count()
        df.createOrReplaceTempView("_tin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    if api == "lightweight":

        def _job():
            return spark.sql(
                "SELECT t.elevation_point AS p FROM _tin_bench_v, LATERAL "
                "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, "
                "origin, cols, rows_n, cell_x, cell_y, 'constrained') t"
            ).count()

    else:
        import pyspark.sql.functions as _f

        def _job():
            return df.select(
                _f.call_function(
                    "gbx_st_interpolateelevationgeom",
                    _f.col("pts"),
                    _f.col("bl"),
                    _f.col("mt"),
                    _f.col("st"),
                    _f.col("spf"),
                    _f.col("origin"),
                    _f.col("cols"),
                    _f.col("rows_n"),
                    _f.col("cell_x"),
                    _f.col("cell_y"),
                    _f.lit("constrained"),
                ).alias("p")
            ).count()

    try:
        n_out = int(_job())
        if n_out <= 0:
            raise RuntimeError("st_interpolateelevationgeom produced no points")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n_out,
            status="ok",
            note=f"st_interpolateelevationgeom {api} -> {n_out} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def _register_quadbin(spark, api: str) -> None:
    """Register exactly one quadbin tier.

    light  -> ``databricks.labs.gbx.pygx`` (gx.register: spark.udf only).
    heavy  -> ``databricks.labs.gbx.gridx.quadbin`` (JVM Scala UDFs).

    Both tiers expose the SAME ``gbx_quadbin_*`` SQL names, so registering one
    overwrites the other in the session catalog.  Each ``run_quadbin_*`` call
    therefore registers a single tier; the light-vs-heavy parity gate (which
    must collect light BEFORE re-registering heavy) lives in the cluster cell,
    not here.
    """
    if api == "lightweight":
        from databricks.labs.gbx.pygx import functions as gx

        gx.register(spark)
    else:
        from databricks.labs.gbx.gridx.quadbin import functions as hx

        hx.register(spark)


def run_quadbin_pointascell(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_pointascell (scalar lon/lat -> cell) over ``n_rows`` points.

    Light registers pygx (``gbx_quadbin_pointascell`` via spark.udf); heavy
    registers gridx.quadbin (the SAME SQL name, JVM). Records ``rows`` = number
    of cells produced. Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_points

    env = capture_env(where)
    fn = "quadbin_pointascell"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_points(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_pointascell(lon, lat, {res}) AS cell "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm non-null cell ids are produced.
        _val = spark.sql(
            f"SELECT gbx_quadbin_pointascell(lon, lat, {res}) AS cell "
            "FROM _quadbin_bench_v WHERE lon IS NOT NULL LIMIT 1"
        ).head(1)
        if not _val or _val[0]["cell"] is None:
            raise RuntimeError("quadbin_pointascell produced null cell")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_pointascell {api} encoded {n} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_polyfill(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_polyfill (geom -> ARRAY<cell>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.quadbin (same SQL name). Records
    ``rows`` = number of input polygons (each producing a cell array). Returns one
    ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_polygons

    env = capture_env(where)
    fn = "quadbin_polyfill"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_polyfill(geom, {res}) AS cells "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty cell array.
        _val = spark.sql(
            f"SELECT gbx_quadbin_polyfill(geom, {res}) AS cells "
            "FROM _quadbin_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["cells"]:
            raise RuntimeError("quadbin_polyfill produced empty cell array")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_polyfill {api} filled {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_tessellate(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_tessellate (geom -> ARRAY<STRUCT<cell,geom>>) over polygons.

    Light registers pygx; heavy registers gridx.quadbin (same SQL name). Records
    ``rows`` = number of input polygons. Returns one ResultRow (mode="spark-path",
    category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_polygons

    env = capture_env(where)
    fn = "quadbin_tessellate"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_tessellate(geom, {res}) AS chips "
            "FROM _quadbin_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty chip array with bytes.
        _val = spark.sql(
            f"SELECT gbx_quadbin_tessellate(geom, {res}) AS chips "
            "FROM _quadbin_bench_v LIMIT 1"
        ).head(1)
        _chips = _val[0]["chips"] if _val else None
        if (
            not _chips
            or _chips[0]["geom"] is None
            or len(bytes(_chips[0]["geom"])) == 0
        ):
            raise RuntimeError("quadbin_tessellate produced empty/null chips")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_tessellate {api} tessellated {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_cellunion_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_cellunion_agg (grouped aggregate) over ``n_rows`` cell ids.

    Streams one cell id per row, grouped by a small key set, unioning each
    group's cell boundaries into one EWKB MultiPolygon.  Light registers pygx
    (a GROUPED_AGG pandas UDF); heavy registers gridx.quadbin (the SAME SQL
    name, a JVM TypedImperativeAggregate). Records ``rows`` = number of input
    cells. Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cellid_arrays

    env = capture_env(where)
    fn = "quadbin_cellunion_agg"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cellid_arrays(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_agg_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _quadbin_agg_v GROUP BY group"
        ).count()

    try:
        # Untimed validation: confirm each group produced a non-empty union blob.
        _val = spark.sql(
            "SELECT gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _quadbin_agg_v GROUP BY group"
        ).collect()
        _nonempty = sum(1 for _r in _val if _r["u"] and len(bytes(_r["u"])) > 0)
        if _nonempty == 0:
            raise RuntimeError("quadbin_cellunion_agg produced 0 non-empty union blobs")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_cellunion_agg {api} unioned {n} cells -> {_nonempty} groups",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_resolution(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_resolution (scalar cell -> INT) over ``n_rows`` cell ids.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact INT equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_resolution"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm the resolution comes back as the input res.
        _val = spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["r"] != res:
            raise RuntimeError("quadbin_resolution produced unexpected resolution")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_resolution {api} resolved {n} cells",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_kring(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_kring (scalar cell -> ARRAY<LONG>) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact sorted
    cell-set per row at a fixed ``k``.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_kring"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_quadbin_kring(cell, {k}) AS ring FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring (k=1 -> up to 9 cells).
        _val = spark.sql(
            f"SELECT gbx_quadbin_kring(cell, {k}) AS ring FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("quadbin_kring produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_kring {api} ringed {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_distance(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_distance (scalar (cell_a, cell_b) -> INT) over ``n_rows`` pairs.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input pairs. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): exact INT equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cell_pairs

    env = capture_env(where)
    fn = "quadbin_distance"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cell_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_pair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _quadbin_pair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null integer distance comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _quadbin_pair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["d"] is None:
            raise RuntimeError("quadbin_distance produced null distance")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_distance {api} measured {n} pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_aswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_aswkb (scalar cell -> EWKB polygon) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): decoded geometry
    within 1e-6 + SRID 4326.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_aswkb"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty EWKB polygon comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("quadbin_aswkb produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_aswkb {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_centroid(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 12,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_centroid (scalar cell -> EWKB point) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.quadbin (the SAME SQL name).
    Records ``rows`` = number of input cells. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell): decoded point
    within 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cells

    env = capture_env(where)
    fn = "quadbin_centroid"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_quadbin_cell_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _quadbin_cell_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty EWKB point comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _quadbin_cell_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("quadbin_centroid produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_centroid {api} encoded {n} cell centroids",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_quadbin_cellunion(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 8,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_quadbin_cellunion (scalar ARRAY<cell> -> EWKB) over grouped cell arrays.

    Reuses the cellunion_agg corpus (``generate_quadbin_cellid_arrays``):
    collect each group's cells into an ARRAY<LONG>, then call the scalar
    ``gbx_quadbin_cellunion`` on the array. Light registers pygx; heavy registers
    gridx.quadbin (the SAME SQL name). Records ``rows`` = number of unioned arrays
    (one per group). Returns one ResultRow (mode="spark-path", category="grid").
    Parity (cluster cell): decoded union geometry via symmetric-difference-area
    < 1e-6 (member-ordering-robust, like the cellunion_agg leg).
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_quadbin_cellid_arrays

    env = capture_env(where)
    fn = "quadbin_cellunion"
    cat = "grid"
    try:
        _register_quadbin(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_quadbin_cellid_arrays(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema)
        df.createOrReplaceTempView("_quadbin_cellsrc_v")
        # Collapse the streamed (group, cell) rows into one ARRAY<cell> per group
        # so the scalar cellunion gets the same cell sets the agg unions.
        arr_df = spark.sql(
            "SELECT group, collect_list(cell) AS cells "
            "FROM _quadbin_cellsrc_v GROUP BY group"
        ).cache()
        n = int(arr_df.count())
        arr_df.createOrReplaceTempView("_quadbin_cellarr_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_quadbin_cellunion(cells) AS u FROM _quadbin_cellarr_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty union blob comes back.
        _val = spark.sql(
            "SELECT gbx_quadbin_cellunion(cells) AS u FROM _quadbin_cellarr_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["u"] is None or len(bytes(_val[0]["u"])) == 0:
            raise RuntimeError("quadbin_cellunion produced empty/null union")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"quadbin_cellunion {api} unioned {n} cell arrays",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def _register_bng(spark, api: str) -> None:
    """Register exactly one BNG tier.

    light  -> ``databricks.labs.gbx.pygx`` (gx.register: spark.udf/udtf only).
    heavy  -> ``databricks.labs.gbx.gridx.bng`` (JVM Scala UDFs).

    Both tiers expose the SAME ``gbx_bng_*`` SQL names, so registering one
    overwrites the other in the session catalog.  Each ``run_bng_*`` call
    therefore registers a single tier; the light-vs-heavy parity gate (which
    must collect light BEFORE re-registering heavy) lives in the cluster cell,
    not here.
    """
    if api == "lightweight":
        from databricks.labs.gbx.pygx import functions as gx

        gx.register(spark)
    else:
        from databricks.labs.gbx.gridx.bng import functions as hx

        hx.register(spark)


def run_bng_pointascell(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_pointascell (geom centroid -> STRING cell) over ``n_rows`` points.

    Light registers pygx (``gbx_bng_pointascell`` via spark.udf); heavy registers
    gridx.bng (the SAME SQL name, JVM). Inputs are EPSG:27700 WKT points (BNG
    eastings/northings, not WGS84). Records ``rows`` = number of cells produced.
    Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_points

    env = capture_env(where)
    fn = "bng_pointascell"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_points(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_pointascell(geom, '{res}') AS cell FROM _bng_bench_v"
        ).count()

    try:
        # Untimed validation: confirm non-null cell ids are produced.
        _val = spark.sql(
            f"SELECT gbx_bng_pointascell(geom, '{res}') AS cell "
            "FROM _bng_bench_v WHERE geom IS NOT NULL LIMIT 1"
        ).head(1)
        if not _val or _val[0]["cell"] is None:
            raise RuntimeError("bng_pointascell produced null cell")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_pointascell {api} encoded {n} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_polyfill(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_polyfill (geom -> ARRAY<STRING cell>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.bng (same SQL name). Inputs are
    EPSG:27700 WKT polygons. Records ``rows`` = number of input polygons (each
    producing a cell array). Returns one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_polyfill"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_polyfill(geom, '{res}') AS cells FROM _bng_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty cell array.
        _val = spark.sql(
            f"SELECT gbx_bng_polyfill(geom, '{res}') AS cells FROM _bng_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["cells"]:
            raise RuntimeError("bng_polyfill produced empty cell array")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_polyfill {api} filled {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_tessellate(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_tessellate (geom -> ARRAY<STRUCT<cellid,core,chip>>) over polygons.

    Light registers pygx; heavy registers gridx.bng (same SQL name). Inputs are
    EPSG:27700 WKT polygons. Records ``rows`` = number of input polygons. Returns
    one ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_tessellate"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_tessellate(geom, '{res}') AS chips FROM _bng_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty chip array with cellids.
        _val = spark.sql(
            f"SELECT gbx_bng_tessellate(geom, '{res}') AS chips FROM _bng_bench_v LIMIT 1"
        ).head(1)
        _chips = _val[0]["chips"] if _val else None
        if not _chips or _chips[0]["cellid"] is None:
            raise RuntimeError("bng_tessellate produced empty/null chips")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_tessellate {api} tessellated {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_kring(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_kring (scalar STRING cell -> ARRAY<STRING>) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact sorted cell-set equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_kring"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_kring(cell, {k}) AS ring FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring comes back.
        _val = spark.sql(
            f"SELECT gbx_bng_kring(cell, {k}) AS ring FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("bng_kring produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_kring {api} ringed {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_cellunion_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_cellunion_agg (grouped aggregate) over ``n_rows`` chip structs.

    Streams one chip ``STRUCT<cellid, core, chip>`` per row, grouped by a small
    key set, dissolving each group's chip geometries into one blob.  Light
    registers pygx (a GROUPED_AGG pandas UDF returning BINARY); heavy registers
    gridx.bng (the SAME SQL name, a JVM TypedImperativeAggregate returning a
    STRUCT). Records ``rows`` = number of input chips. Returns one ResultRow
    (mode="spark-path", category="grid"). Parity (cluster cell) compares the
    decoded chip GEOMETRY (light BINARY vs heavy struct's chip field).
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_chip_groups

    env = capture_env(where)
    fn = "bng_cellunion_agg"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_chip_groups(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_agg_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT group, gbx_bng_cellunion_agg(chip) AS u "
            "FROM _bng_agg_v GROUP BY group"
        ).count()

    try:
        # Untimed validation: confirm each group produced a non-null union.
        _val = spark.sql(
            "SELECT gbx_bng_cellunion_agg(chip) AS u " "FROM _bng_agg_v GROUP BY group"
        ).collect()
        _nonempty = sum(1 for _r in _val if _r["u"] is not None)
        if _nonempty == 0:
            raise RuntimeError("bng_cellunion_agg produced 0 non-null unions")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_cellunion_agg {api} dissolved {n} chips -> {_nonempty} groups",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_aswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_aswkb (scalar STRING cell -> WKB polygon) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): decoded geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_aswkb"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_aswkb(cell) AS g FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKB polygon comes back.
        _val = spark.sql(
            "SELECT gbx_bng_aswkb(cell) AS g FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("bng_aswkb produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_aswkb {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_aswkt(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_aswkt (scalar STRING cell -> WKT polygon) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): decoded geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_aswkt"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_aswkt(cell) AS g FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKT string comes back.
        _val = spark.sql(
            "SELECT gbx_bng_aswkt(cell) AS g FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["g"]:
            raise RuntimeError("bng_aswkt produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_aswkt {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_centroid(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_centroid (scalar STRING cell -> WKB point) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): decoded point distance < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_centroid"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_centroid(cell) AS g FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKB point comes back.
        _val = spark.sql(
            "SELECT gbx_bng_centroid(cell) AS g FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("bng_centroid produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_centroid {api} encoded {n} cell centroids",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_cellarea(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_cellarea (scalar STRING cell -> DOUBLE sq km) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact scalar equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_cellarea"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_cellarea(cell) AS a FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null area comes back.
        _val = spark.sql(
            "SELECT gbx_bng_cellarea(cell) AS a FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["a"] is None:
            raise RuntimeError("bng_cellarea produced null area")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_cellarea {api} measured {n} cells",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_distance(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_distance (scalar (cell_a, cell_b) -> LONG) over ``n_rows`` pairs.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input pairs. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact scalar equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cell_pairs

    env = capture_env(where)
    fn = "bng_distance"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cell_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_pair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_distance(cell_a, cell_b) AS d FROM _bng_pair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null distance comes back.
        _val = spark.sql(
            "SELECT gbx_bng_distance(cell_a, cell_b) AS d FROM _bng_pair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["d"] is None:
            raise RuntimeError("bng_distance produced null distance")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_distance {api} measured {n} pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_euclideandistance(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_euclideandistance (scalar (cell_a, cell_b) -> LONG) over pairs.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input pairs. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact scalar equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cell_pairs

    env = capture_env(where)
    fn = "bng_euclideandistance"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cell_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_pair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_euclideandistance(cell_a, cell_b) AS d FROM _bng_pair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null distance comes back.
        _val = spark.sql(
            "SELECT gbx_bng_euclideandistance(cell_a, cell_b) AS d "
            "FROM _bng_pair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["d"] is None:
            raise RuntimeError("bng_euclideandistance produced null distance")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_euclideandistance {api} measured {n} pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_eastnorthasbng(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_eastnorthasbng ((e, n, res) -> STRING cell) over ``n_rows`` points.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Inputs
    are EPSG:27700 eastings/northings (DOUBLE, BNG, not WGS84). Records ``rows`` =
    number of input points. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact STRING cell-id equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_eastnorth

    env = capture_env(where)
    fn = "bng_eastnorthasbng"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_eastnorth(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_eastnorth_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_eastnorthasbng(e, n, '{res}') AS cell "
            "FROM _bng_eastnorth_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null cell id comes back.
        _val = spark.sql(
            f"SELECT gbx_bng_eastnorthasbng(e, n, '{res}') AS cell "
            "FROM _bng_eastnorth_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["cell"] is None:
            raise RuntimeError("bng_eastnorthasbng produced null cell")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_eastnorthasbng {api} encoded {n} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_kloop(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_kloop (scalar STRING cell -> ARRAY<STRING>) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exact sorted cell-set equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_kloop"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_kloop(cell, {k}) AS ring FROM _bng_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring comes back.
        _val = spark.sql(
            f"SELECT gbx_bng_kloop(cell, {k}) AS ring FROM _bng_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("bng_kloop produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_kloop {api} looped {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_geomkring(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_geomkring (geom -> ARRAY<STRING>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Inputs
    are EPSG:27700 WKT polygons. Records ``rows`` = number of input polygons.
    Returns one ResultRow (mode="spark-path", category="grid"). Parity (cluster
    cell): exact sorted cell-set equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_geomkring"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    # Heavy BNG_GeometryKRing.eval only accepts Int resolution (no String
    # overload); function-info canonical is int too. Pass the int index.
    from databricks.labs.gbx.pygx import _bng

    res_i = _bng.get_resolution(res)

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_geomkring(geom, {res_i}, {k}) AS ring FROM _bng_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring comes back.
        _val = spark.sql(
            f"SELECT gbx_bng_geomkring(geom, {res_i}, {k}) AS ring "
            "FROM _bng_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("bng_geomkring produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_geomkring {api} ringed {n} polygons (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_geomkloop(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_geomkloop (geom -> ARRAY<STRING>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.bng (the SAME SQL name). Inputs
    are EPSG:27700 WKT polygons. Records ``rows`` = number of input polygons.
    Returns one ResultRow (mode="spark-path", category="grid"). Parity (cluster
    cell): exact sorted cell-set equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_geomkloop"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    # Heavy BNG_GeometryKLoop.eval only accepts Int resolution (no String
    # overload); function-info canonical is int too. Pass the int index.
    from databricks.labs.gbx.pygx import _bng

    res_i = _bng.get_resolution(res)

    def _job():
        return spark.sql(
            f"SELECT gbx_bng_geomkloop(geom, {res_i}, {k}) AS ring FROM _bng_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring comes back.
        _val = spark.sql(
            f"SELECT gbx_bng_geomkloop(geom, {res_i}, {k}) AS ring "
            "FROM _bng_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("bng_geomkloop produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_geomkloop {api} looped {n} polygons (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_cellintersection(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_cellintersection (two chip STRUCTs -> chip STRUCT) over ``n_rows``.

    Both chip structs cover the SAME cell (a core chip + a full-cell chip), so the
    intersection produces the whole cell. Light registers pygx; heavy registers
    gridx.bng (the SAME SQL name). Records ``rows`` = number of chip pairs. Returns
    one ResultRow (mode="spark-path", category="grid"). Parity (cluster cell):
    decoded .chip geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_chip_pairs

    env = capture_env(where)
    fn = "bng_cellintersection"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_chip_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_chippair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_cellintersection("
            "struct(lid, lcore, lchip), struct(rid, rcore, rchip)) AS c "
            "FROM _bng_chippair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null chip struct comes back.
        _val = spark.sql(
            "SELECT gbx_bng_cellintersection("
            "struct(lid, lcore, lchip), struct(rid, rcore, rchip)) AS c "
            "FROM _bng_chippair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["c"] is None or _val[0]["c"]["chip"] is None:
            raise RuntimeError("bng_cellintersection produced null chip")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_cellintersection {api} intersected {n} chip pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_cellunion(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_cellunion (two chip STRUCTs -> chip STRUCT) over ``n_rows`` pairs.

    Both chip structs cover the SAME cell (a core chip + a full-cell chip), so the
    union produces the whole cell. Light registers pygx; heavy registers gridx.bng
    (the SAME SQL name). Records ``rows`` = number of chip pairs. Returns one
    ResultRow (mode="spark-path", category="grid"). Parity (cluster cell): decoded
    .chip geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_chip_pairs

    env = capture_env(where)
    fn = "bng_cellunion"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_chip_pairs(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_chippair_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT gbx_bng_cellunion("
            "struct(lid, lcore, lchip), struct(rid, rcore, rchip)) AS c "
            "FROM _bng_chippair_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null chip struct comes back.
        _val = spark.sql(
            "SELECT gbx_bng_cellunion("
            "struct(lid, lcore, lchip), struct(rid, rcore, rchip)) AS c "
            "FROM _bng_chippair_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["c"] is None or _val[0]["c"]["chip"] is None:
            raise RuntimeError("bng_cellunion produced null chip")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_cellunion {api} unioned {n} chip pairs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_cellintersection_agg(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_cellintersection_agg (grouped aggregate) over ``n_rows`` chips.

    Mirror of cellunion_agg over the SAME-CELL chip groups (``generate_bng_chip_groups``;
    heavy enforces one cell id per aggregate). Light registers pygx (a GROUPED_AGG
    pandas UDF returning BINARY); heavy registers gridx.bng (the SAME SQL name, a
    JVM TypedImperativeAggregate returning a STRUCT). Records ``rows`` = number of
    input chips. Returns one ResultRow (mode="spark-path", category="grid"). Parity
    (cluster cell) compares the decoded chip GEOMETRY (light BINARY vs heavy
    struct's chip field).
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_chip_groups

    env = capture_env(where)
    fn = "bng_cellintersection_agg"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_chip_groups(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_agg_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            "SELECT group, gbx_bng_cellintersection_agg(chip) AS u "
            "FROM _bng_agg_v GROUP BY group"
        ).count()

    try:
        # Untimed validation: confirm each group produced a non-null intersection.
        _val = spark.sql(
            "SELECT gbx_bng_cellintersection_agg(chip) AS u "
            "FROM _bng_agg_v GROUP BY group"
        ).collect()
        _nonempty = sum(1 for _r in _val if _r["u"] is not None)
        if _nonempty == 0:
            raise RuntimeError("bng_cellintersection_agg produced 0 non-null results")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=(
                f"bng_cellintersection_agg {api} intersected {n} chips "
                f"-> {_nonempty} groups"
            ),
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_kringexplode(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_kringexplode (UDTF: STRING cell -> rows of cellid) over cells.

    A table-valued generator: timed via a SQL LATERAL query (NOT the Column API;
    the Python wrapper raises NotImplementedError). Both tiers register the SAME
    ``gbx_bng_kringexplode`` SQL name (light a pygx UDTF, heavy a JVM
    CollectionGenerator), so the LATERAL ``_job`` is tier-agnostic. Records
    ``rows`` = number of input cells. Returns one ResultRow (mode="spark-path",
    category="grid"). Parity (cluster cell): exploded cellid set == kring's set.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_kringexplode"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT t.cellid FROM _bng_cell_bench_v, "
            f"LATERAL gbx_bng_kringexplode(cell, {k}) t"
        ).count()

    try:
        # Untimed validation: confirm the LATERAL generator yields a row.
        _val = spark.sql(
            f"SELECT t.cellid FROM _bng_cell_bench_v, "
            f"LATERAL gbx_bng_kringexplode(cell, {k}) t LIMIT 1"
        ).head(1)
        if not _val:
            raise RuntimeError("bng_kringexplode produced no rows")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_kringexplode {api} exploded {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_kloopexplode(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_kloopexplode (UDTF: STRING cell -> rows of cellid) over cells.

    Mirror of kringexplode (k-loop / hollow ring). Timed via SQL LATERAL; both
    tiers register the SAME ``gbx_bng_kloopexplode`` SQL name (tier-agnostic job).
    Records ``rows`` = number of input cells. Parity (cluster cell): exploded
    cellid set == kloop's set.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_cells

    env = capture_env(where)
    fn = "bng_kloopexplode"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT t.cellid FROM _bng_cell_bench_v, "
            f"LATERAL gbx_bng_kloopexplode(cell, {k}) t"
        ).count()

    try:
        # Untimed validation: confirm the LATERAL generator yields a row.
        _val = spark.sql(
            f"SELECT t.cellid FROM _bng_cell_bench_v, "
            f"LATERAL gbx_bng_kloopexplode(cell, {k}) t LIMIT 1"
        ).head(1)
        if not _val:
            raise RuntimeError("bng_kloopexplode produced no rows")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_kloopexplode {api} exploded {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_geomkringexplode(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_geomkringexplode (UDTF: geom -> rows of cellid) over polygons.

    A table-valued generator: timed via SQL LATERAL over WKT polygons. Both tiers
    register the SAME ``gbx_bng_geomkringexplode`` SQL name (tier-agnostic job).
    Records ``rows`` = number of input polygons. Parity (cluster cell): exploded
    cellid set == geomkring's set.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_geomkringexplode"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    # Align resolution to the int index for parity with the canonical
    # function-info form (heavy accepts Int or String here).
    from databricks.labs.gbx.pygx import _bng

    res_i = _bng.get_resolution(res)

    def _job():
        return spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_geomkringexplode(geom, {res_i}, {k}) t"
        ).count()

    try:
        # Untimed validation: confirm the LATERAL generator yields a row.
        _val = spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_geomkringexplode(geom, {res_i}, {k}) t LIMIT 1"
        ).head(1)
        if not _val:
            raise RuntimeError("bng_geomkringexplode produced no rows")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_geomkringexplode {api} exploded {n} polygons (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_geomkloopexplode(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_geomkloopexplode (UDTF: geom -> rows of cellid) over polygons.

    Mirror of geomkringexplode (k-loop). Timed via SQL LATERAL; both tiers
    register the SAME ``gbx_bng_geomkloopexplode`` SQL name (tier-agnostic job).
    Records ``rows`` = number of input polygons. Parity (cluster cell): exploded
    cellid set == geomkloop's set.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_geomkloopexplode"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    # Align resolution to the int index for parity with the canonical
    # function-info form (heavy accepts Int or String here).
    from databricks.labs.gbx.pygx import _bng

    res_i = _bng.get_resolution(res)

    def _job():
        return spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_geomkloopexplode(geom, {res_i}, {k}) t"
        ).count()

    try:
        # Untimed validation: confirm the LATERAL generator yields a row.
        _val = spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_geomkloopexplode(geom, {res_i}, {k}) t LIMIT 1"
        ).head(1)
        if not _val:
            raise RuntimeError("bng_geomkloopexplode produced no rows")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_geomkloopexplode {api} exploded {n} polygons (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_bng_tessellateexplode(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res="1km",
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_bng_tessellateexplode (UDTF: geom -> rows of (cellid, core, chip)).

    A table-valued generator: timed via SQL LATERAL over WKT polygons. Both tiers
    register the SAME ``gbx_bng_tessellateexplode`` SQL name (tier-agnostic job).
    Records ``rows`` = number of input polygons. Parity (cluster cell): exploded
    cellid set == tessellate's chip cellid set.
    """
    from databricks.labs.gbx.bench.corpus_vector import generate_bng_polygons

    env = capture_env(where)
    fn = "bng_tessellateexplode"
    cat = "grid"
    try:
        _register_bng(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_bng_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_bng_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_tessellateexplode(geom, '{res}') t"
        ).count()

    try:
        # Untimed validation: confirm the LATERAL generator yields a row.
        _val = spark.sql(
            f"SELECT t.cellid FROM _bng_bench_v, "
            f"LATERAL gbx_bng_tessellateexplode(geom, '{res}') t LIMIT 1"
        ).head(1)
        if not _val:
            raise RuntimeError("bng_tessellateexplode produced no rows")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"bng_tessellateexplode {api} exploded {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def _make_synthetic_geotiff(
    n_regions: int = 4,
    size: int = 64,
    *,
    bands: int = 1,
    bounds: tuple = (-0.5, 51.3, 0.5, 51.7),
    checkerboard: int = 0,
) -> bytes:
    """Build a minimal in-memory GeoTIFF with distinct value structure.

    The tile is ``size x size`` pixels, ``bands``-band float32, EPSG:4326.

    Value structure (drives the fan-out of each consuming function):
      * ``checkerboard > 0``: paint a ``checkerboard x checkerboard`` grid of
        alternating integer values -- yields MANY connected components for
        rst_polygonize and many distinct cell measures for the grid counters.
      * otherwise: split into ``n_regions`` horizontal bands of distinct value
        (the original behaviour) -- a handful of large components.

    ``bounds`` is the WGS84 (minx, miny, maxx, maxy) extent; a wider extent
    spans more H3 cells / XYZ tiles. ``bands`` > 1 drives rst_separatebands.

    Returns raw GTiff bytes suitable for the ``raster`` field of a tile struct.
    """
    import io as _io

    import numpy as np
    from rasterio.crs import CRS
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    def _one_band(seed: float) -> "np.ndarray":
        arr = np.zeros((size, size), dtype=np.float32)
        if checkerboard and checkerboard > 0:
            # Paint a checkerboard of distinct values: adjacent cells differ so
            # polygonize sees ``checkerboard^2`` separate connected components.
            cell = max(1, size // checkerboard)
            last = checkerboard - 1
            for by in range(checkerboard):
                r0 = by * cell
                r1 = size if by == last else (by + 1) * cell
                for bx in range(checkerboard):
                    c0 = bx * cell
                    c1 = size if bx == last else (bx + 1) * cell
                    # Distinct value per block; +seed so bands differ.
                    val = float((by * checkerboard + bx) % 251 + 1) + seed
                    arr[r0:r1, c0:c1] = val
        else:
            step = max(1, size // n_regions)
            for i in range(n_regions):
                row_start = i * step
                row_end = (i + 1) * step if i < n_regions - 1 else size
                arr[row_start:row_end, :] = float(i + 1) + seed
        return arr

    minx, miny, maxx, maxy = bounds
    transform = from_bounds(minx, miny, maxx, maxy, size, size)
    crs = CRS.from_epsg(4326)

    buf = _io.BytesIO()
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            dtype="float32",
            width=size,
            height=size,
            count=max(1, bands),
            crs=crs,
            transform=transform,
        ) as ds:
            for b in range(max(1, bands)):
                ds.write(_one_band(seed=float(b) * 0.0), b + 1)
        buf.write(mf.read())
    return buf.getvalue()


# Fan-out functions covered by run_fanout_udtf.  Order is stable for parity loops.
FANOUT_FUNCTIONS = [
    "rst_polygonize",
    "rst_h3_rastertogridcount",
    "rst_xyzpyramid",
    "rst_h3_tessellate",
    "rst_retile",
    "rst_tooverlappingtiles",
    "rst_maketiles",
    "rst_separatebands",
]

# Per-function synthetic-input + invocation spec.  ``scale`` (default 1.0) is the
# tunable that dials the fan-out up/down; sizes below are the scale=1.0 defaults
# chosen to be meaningful yet finish in a couple of minutes on ~20 workers.
#
# Each entry returns (tile_kwargs, light_lateral, heavy_lateral) where the LATERAL
# fragments are the part AFTER "LATERAL" in the SQL, and ``heavy_lateral`` already
# flattens the heavy tier to the SAME granularity as the light flat UDTF rows:
#   * polygonize  -> heavy ARRAY<struct>           -> explode  (single)
#   * gridcount   -> heavy ARRAY<ARRAY<struct>>    -> explode∘explode (double)
#   * 5 tilers    -> heavy CollectionGenerator     -> LATERAL VIEW gbx_.. (no explode)
#   * xyzpyramid  -> heavy CollectionGenerator emits flat rows -> LATERAL VIEW (no explode)


def _fanout_spec(fn: str, scale: float):
    """Return (tile_kwargs, light_sql, heavy_sql) for a fan-out function.

    ``light_sql`` / ``heavy_sql`` are full SQL strings over the temp view
    ``_fanout_bench_ras`` (column ``tile``) that each produce FLAT rows so the
    two row counts are directly comparable (the flatten-both parity gate).
    """
    s = max(0.1, float(scale))

    if fn == "rst_polygonize":
        # Many connected components -> large polygon fan-out.
        cb = max(2, int(round(16 * (s**0.5))))
        size = max(64, int(round(256 * (s**0.5))))
        tile_kwargs = dict(size=size, checkerboard=cb)
        light = "SELECT t.* FROM _fanout_bench_ras, LATERAL gbx_rst_polygonize(tile, 1, 4) t"
        # Heavy returns ARRAY<struct> -> single explode.
        heavy = (
            "SELECT p.* FROM _fanout_bench_ras "
            "LATERAL VIEW explode(gbx_rst_polygonize(tile, 1, 4)) e AS p"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_h3_rastertogridcount":
        # Fine H3 resolution + wide extent -> many cells.
        res = 9 if s <= 1.0 else 10
        # Wider extent at higher scale -> more cells.
        span = 0.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_h3_rastertogridcount(tile, {res}) t"
        )
        # Heavy returns ARRAY<ARRAY<struct>> (bands x cells) -> DOUBLE explode.
        heavy = (
            "SELECT c.* FROM _fanout_bench_ras "
            f"LATERAL VIEW explode(gbx_rst_h3_rastertogridcount(tile, {res})) eb AS band_cells "
            "LATERAL VIEW explode(band_cells) ec AS c"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_xyzpyramid":
        # Deep zoom range over a multi-degree extent -> thousands of tiles.
        min_z = 4
        max_z = 9 if s <= 1.0 else 10
        span = 1.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_xyzpyramid(tile, {min_z}, {max_z}, 'PNG', 256, 'bilinear') t"
        )
        # Heavy generator emits flat rows directly -> LATERAL VIEW, NO explode.
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_xyzpyramid(tile, {min_z}, {max_z}, 'PNG', 256, 'bilinear') t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_h3_tessellate":
        res = 8 if s <= 1.0 else 9
        span = 0.5 * (s**0.5)
        bounds = (-span, 51.5 - span, span, 51.5 + span)
        tile_kwargs = dict(size=max(128, int(round(256 * (s**0.5)))), bounds=bounds)
        # Pass explicit mode='covering' (the default) so the bench leg is
        # unambiguous and a future 'centroid' variant can be added by changing
        # this one argument. Heavy uses LATERAL VIEW (CollectionGenerator, flat
        # rows, no explode) -- same pattern as rst_xyzpyramid.
        mode = "covering"
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_h3_tessellate(tile, {res}, '{mode}') t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_h3_tessellate(tile, {res}, '{mode}') t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn in ("rst_retile", "rst_tooverlappingtiles"):
        # Large raster with small tile size -> many tiles.
        size = max(512, int(round(1024 * (s**0.5))))
        tw = th = 64
        tile_kwargs = dict(size=size)
        if fn == "rst_retile":
            light = (
                "SELECT t.* FROM _fanout_bench_ras, "
                f"LATERAL gbx_rst_retile(tile, {tw}, {th}) t"
            )
            heavy = (
                "SELECT t.* FROM _fanout_bench_ras "
                f"LATERAL VIEW gbx_rst_retile(tile, {tw}, {th}) t AS tile"
            )
        else:
            overlap = 8
            light = (
                "SELECT t.* FROM _fanout_bench_ras, "
                f"LATERAL gbx_rst_tooverlappingtiles(tile, {tw}, {th}, {overlap}) t"
            )
            heavy = (
                "SELECT t.* FROM _fanout_bench_ras "
                f"LATERAL VIEW gbx_rst_tooverlappingtiles(tile, {tw}, {th}, {overlap}) t AS tile"
            )
        return tile_kwargs, light, heavy

    if fn == "rst_maketiles":
        # Large raster + small per-tile MB budget -> many power-of-4 sub-tiles.
        size = max(512, int(round(1024 * (s**0.5))))
        size_mb = 1
        tile_kwargs = dict(size=size)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, "
            f"LATERAL gbx_rst_maketiles(tile, {size_mb}) t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            f"LATERAL VIEW gbx_rst_maketiles(tile, {size_mb}) t AS tile"
        )
        return tile_kwargs, light, heavy

    if fn == "rst_separatebands":
        # MANY bands (hyperspectral / large-band case) -> large per-row fan-out.
        nbands = max(8, int(round(64 * s)))
        tile_kwargs = dict(size=64, bands=nbands)
        light = (
            "SELECT t.* FROM _fanout_bench_ras, LATERAL gbx_rst_separatebands(tile) t"
        )
        heavy = (
            "SELECT t.* FROM _fanout_bench_ras "
            "LATERAL VIEW gbx_rst_separatebands(tile) t AS tile"
        )
        return tile_kwargs, light, heavy

    raise ValueError(f"unknown fanout fn: {fn}")


# =============================================================================
# Custom-grid bench legs (gbx_custom_* light pygx vs heavy gridx.custom).
#
# All legs build the SAME fixed grid struct via gbx_custom_grid(...) (see
# corpus_vector.CUSTOM_GRID_SQL) so both tiers consume an identical STRUCT.  Cell
# ids are BIGINT; geometry outputs are plain WKB, no SRID (the grid's srid is
# metadata only).  pointascell uses the geometry's FIRST coordinate (heavy
# geom.getCoordinate), NOT the centroid -- unlike BNG.  Representative spread
# (per the plan): pointascell (geom->BIGINT, scalar encode), polyfill
# (geom->ARRAY<BIGINT>), kring (cell-in->ARRAY<BIGINT>), cellaswkb (cell->WKB,
# the UDF-boundary leg).  Parity (cluster cell): exact cell-id/set; decoded geom.
# =============================================================================


def _register_custom(spark, api: str) -> None:
    """Register exactly one custom-grid tier.

    light  -> ``databricks.labs.gbx.pygx`` (gx.register: spark.udf only).
    heavy  -> ``databricks.labs.gbx.gridx.custom`` (JVM Scala expressions).

    Both tiers expose the SAME ``gbx_custom_*`` SQL names, so registering one
    overwrites the other in the session catalog.  Each ``run_custom_*`` call
    therefore registers a single tier; the light-vs-heavy parity gate (which must
    collect light BEFORE re-registering heavy) lives in the cluster cell.
    """
    if api == "lightweight":
        from databricks.labs.gbx.pygx import functions as gx

        gx.register(spark)
    else:
        from databricks.labs.gbx.gridx.custom import functions as hx

        hx.register(spark)


def run_custom_pointascell(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_pointascell (geom first-coord -> BIGINT cell) over ``n_rows`` points.

    Light registers pygx (``gbx_custom_pointascell`` via spark.udf); heavy registers
    gridx.custom (the SAME SQL name, JVM). The grid is built inline via
    gbx_custom_grid(...).  Records ``rows`` = number of cells produced.  Returns one
    ResultRow (mode="spark-path", category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_points,
    )

    env = capture_env(where)
    fn = "custom_pointascell"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_points(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_pointascell(geom, {CUSTOM_GRID_SQL}, {res}) AS cell "
            "FROM _custom_bench_v"
        ).count()

    try:
        # Untimed validation: confirm non-null cell ids are produced.
        _val = spark.sql(
            f"SELECT gbx_custom_pointascell(geom, {CUSTOM_GRID_SQL}, {res}) AS cell "
            "FROM _custom_bench_v WHERE geom IS NOT NULL LIMIT 1"
        ).head(1)
        if not _val or _val[0]["cell"] is None:
            raise RuntimeError("custom_pointascell produced null cell")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_pointascell {api} encoded {n} points",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_polyfill(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_polyfill (geom -> ARRAY<BIGINT cell>) over ``n_rows`` WKT polygons.

    Light registers pygx; heavy registers gridx.custom (same SQL name). The grid is
    built inline via gbx_custom_grid(...).  Records ``rows`` = number of input polygons
    (each producing a cell array).  Returns one ResultRow (mode="spark-path",
    category="grid").
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_polygons,
    )

    env = capture_env(where)
    fn = "custom_polyfill"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_polygons(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_polyfill(geom, {CUSTOM_GRID_SQL}, {res}) AS cells "
            "FROM _custom_bench_v"
        ).count()

    try:
        # Untimed validation: confirm at least one non-empty cell array.
        _val = spark.sql(
            f"SELECT gbx_custom_polyfill(geom, {CUSTOM_GRID_SQL}, {res}) AS cells "
            "FROM _custom_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["cells"]:
            raise RuntimeError("custom_polyfill produced empty cell array")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_polyfill {api} filled {n} polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_kring(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    k: int = 1,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_kring (scalar BIGINT cell -> ARRAY<BIGINT>) over ``n_rows`` cells.

    Light registers pygx; heavy registers gridx.custom (the SAME SQL name). The grid is
    built inline via gbx_custom_grid(...).  Records ``rows`` = number of input cells.
    Returns one ResultRow (mode="spark-path", category="grid").  Parity (cluster cell):
    exact sorted cell-set equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_cells,
    )

    env = capture_env(where)
    fn = "custom_kring"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_kring(cell, {CUSTOM_GRID_SQL}, {k}) AS ring "
            "FROM _custom_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty ring comes back.
        _val = spark.sql(
            f"SELECT gbx_custom_kring(cell, {CUSTOM_GRID_SQL}, {k}) AS ring "
            "FROM _custom_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["ring"]:
            raise RuntimeError("custom_kring produced empty ring")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_kring {api} ringed {n} cells (k={k})",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_cellaswkb(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_cellaswkb (scalar BIGINT cell -> WKB polygon, no SRID) over cells.

    Light registers pygx; heavy registers gridx.custom (the SAME SQL name). The grid is
    built inline via gbx_custom_grid(...).  This is the UDF-boundary leg (tile/geometry
    bytes cross JVM<->Python).  Records ``rows`` = number of input cells.  Returns one
    ResultRow (mode="spark-path", category="grid").  Parity (cluster cell): decoded
    geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_cells,
    )

    env = capture_env(where)
    fn = "custom_cellaswkb"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_cellaswkb(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKB polygon comes back.
        _val = spark.sql(
            f"SELECT gbx_custom_cellaswkb(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("custom_cellaswkb produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_cellaswkb {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_cellaswkt(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_cellaswkt (scalar BIGINT cell -> WKT polygon string, no SRID) over cells.

    Light registers pygx; heavy registers gridx.custom (the SAME SQL name). The grid is
    built inline via gbx_custom_grid(...).  Mirrors run_custom_cellaswkb but the cell ->
    geometry crosses the JVM<->Python boundary as a WKT STRING rather than WKB bytes.
    Records ``rows`` = number of input cells.  Returns one ResultRow (mode="spark-path",
    category="grid").  Parity (cluster cell): decoded geometry sym-diff area < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_cells,
    )

    env = capture_env(where)
    fn = "custom_cellaswkt"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_cellaswkt(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKT polygon string comes back.
        _val = spark.sql(
            f"SELECT gbx_custom_cellaswkt(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or not _val[0]["g"]:
            raise RuntimeError("custom_cellaswkt produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_cellaswkt {api} encoded {n} cell polygons",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_centroid(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    res: int = 0,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_centroid (scalar BIGINT cell -> WKB point, no SRID) over cells.

    Light registers pygx; heavy registers gridx.custom (the SAME SQL name). The grid is
    built inline via gbx_custom_grid(...).  Mirrors run_custom_cellaswkb but returns the
    cell centroid point (WKB) rather than the cell polygon.  Records ``rows`` = number of
    input cells.  Returns one ResultRow (mode="spark-path", category="grid").  Parity
    (cluster cell): decoded point distance < 1e-6.
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_cells,
    )

    env = capture_env(where)
    fn = "custom_centroid"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    try:
        data, schema = generate_custom_cells(n_rows, res=res)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_cell_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT gbx_custom_centroid(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-empty WKB point comes back.
        _val = spark.sql(
            f"SELECT gbx_custom_centroid(cell, {CUSTOM_GRID_SQL}) AS g "
            "FROM _custom_cell_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["g"] is None or len(bytes(_val[0]["g"])) == 0:
            raise RuntimeError("custom_centroid produced empty/null geometry")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_centroid {api} encoded {n} cell centroids",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_custom_grid(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    n_rows: int = 1000,
    where: str = "cluster",
) -> "ResultRow":
    """Time gbx_custom_grid (8 scalar args -> validated grid STRUCT) over ``n_rows`` rows.

    Light registers pygx (a validating @udf returning CUSTOM_GRID_SCHEMA); heavy registers
    gridx.custom (the SAME SQL name, a JVM expression).  gbx_custom_grid is the validating
    STRUCT constructor consumed by every other gbx_custom_* op -- a per-row scalar
    construction, not a geo op.  This leg times constructing the grid struct from the 8
    CUSTOM_GRID_ARGS literals once per corpus row.  Both tiers build the SAME struct (same
    8 named fields), so it is parity-comparable on the struct field tuple.  Records
    ``rows`` = number of constructions.  Returns one ResultRow (mode="spark-path",
    category="grid").  Parity (cluster cell): exact struct field-tuple equality.
    """
    from databricks.labs.gbx.bench.corpus_vector import (
        CUSTOM_GRID_SQL,
        generate_custom_points,
    )

    env = capture_env(where)
    fn = "custom_grid"
    cat = "grid"
    try:
        _register_custom(spark, api)
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"register error: {e}",
            warmup=warmup,
        )

    # Reuse the points corpus purely to get n_rows rows to construct the grid over
    # (the geom column is unused; gbx_custom_grid takes only literal scalar args).
    try:
        data, schema = generate_custom_points(n_rows)
        df = spark.createDataFrame(data, schema=schema).cache()
        n = int(df.count())
        df.createOrReplaceTempView("_custom_bench_v")
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=0,
            status="error",
            note=f"dataframe build error: {e}",
            warmup=warmup,
        )

    def _job():
        return spark.sql(
            f"SELECT {CUSTOM_GRID_SQL} AS grid FROM _custom_bench_v"
        ).count()

    try:
        # Untimed validation: confirm a non-null grid struct comes back.
        _val = spark.sql(
            f"SELECT {CUSTOM_GRID_SQL} AS grid FROM _custom_bench_v LIMIT 1"
        ).head(1)
        if not _val or _val[0]["grid"] is None:
            raise RuntimeError("custom_grid produced null struct")
        stats = time_iters(_job, warmup, measured)
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="ok",
            note=f"custom_grid {api} built {n} grid structs",
            stats=stats,
        )
    except Exception as e:  # noqa: BLE001
        return _tin_result_row(
            run_id=run_id,
            api=api,
            fn=fn,
            category=cat,
            env=env,
            rows=n,
            status="error",
            note=str(e),
            warmup=warmup,
        )


def run_fanout_udtf(
    spark,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    api: str,
    fn: str,
    scale: float = 1.0,
    where: str = "cluster",
) -> "ResultRow":
    """Time one of the 8 fan-out functions, light (UDTF) vs heavy (generator/array).

    Builds a per-function synthetic GeoTIFF tile sized to drive ``fn`` into a
    LARGE fan-out (the regime where streaming UDTFs help -- see ``_fanout_spec``),
    wraps it in a one-row DataFrame matching the tile struct schema, then times
    the flattened invocation to completion. Returns a single ResultRow
    (mode="spark-path", category="fanout").

    Both tiers are invoked via SQL and flattened to the SAME granularity so the
    output row counts are directly comparable (flatten-both parity):
        * light = streaming UDTF via ``LATERAL gbx_<fn>(...)`` -> already flat.
        * heavy = its Scala counterpart, flattened to match:
            - ARRAY<struct>        (polygonize)  -> single ``explode``
            - ARRAY<ARRAY<struct>> (gridcount)   -> double ``explode``
            - CollectionGenerator  (5 tilers)    -> ``LATERAL VIEW gbx_.. (no explode)``
            - CollectionGenerator emitting flat rows (xyzpyramid) -> ``LATERAL VIEW``

    ``api`` controls which tier is timed:
        "lightweight"  -> registers pyrx UDTFs
        "heavyweight"  -> registers rasterx (needs the JAR -> cluster-only)
    ``fn`` is one of ``FANOUT_FUNCTIONS``.
    ``scale`` dials the synthetic fan-out up/down (default 1.0).
    """
    env = capture_env(where)

    # Resolve the per-function synthetic-input + invocation spec up front so a bad
    # fn name fails loudly rather than silently benching nothing.
    try:
        tile_kwargs, light_sql, heavy_sql = _fanout_spec(fn, scale)
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"spec error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Register the tier.
    try:
        if api == "lightweight":
            from databricks.labs.gbx.pyrx import functions as prx

            prx.register(spark)
        else:
            from databricks.labs.gbx.rasterx import functions as rx

            rx.register(spark)
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=peak_rss_mb(),
            status="error",
            note=f"register error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build a synthetic tile DataFrame (one row).
    try:
        from pyspark.sql.types import (
            BinaryType,
            LongType,
            MapType,
            StringType,
            StructField,
            StructType,
        )

        tile_bytes = _make_synthetic_geotiff(**tile_kwargs)
        _w = int(tile_kwargs.get("size", 64))
        _bands = int(tile_kwargs.get("bands", 1))
        tile_schema = StructType(
            [
                StructField("cellid", LongType(), nullable=False),
                StructField("raster", BinaryType(), nullable=False),
                StructField(
                    "metadata",
                    MapType(StringType(), StringType()),
                    nullable=True,
                ),
            ]
        )
        tile_row = (
            0,
            bytearray(tile_bytes),
            {
                "driver": "GTiff",
                "width": str(_w),
                "height": str(_w),
                "count": str(_bands),
            },
        )
        df = spark.createDataFrame([tile_row], schema=tile_schema)
        import pyspark.sql.functions as _F

        # Wrap as a struct column named "tile" matching the gbx_rst_* UDTF expectation.
        df = df.select(_F.struct("cellid", "raster", "metadata").alias("tile")).cache()
        df.count()  # materialise

        # Register as a temp view for SQL path.
        df.createOrReplaceTempView("_fanout_bench_ras")
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=f"dataframe build error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    # Build the job closure: both tiers go through SQL and are flattened to the
    # SAME granularity (flatten-both parity).  ``light_sql`` is the streaming UDTF
    # LATERAL form; ``heavy_sql`` flattens the Scala counterpart per _fanout_spec.
    try:
        sql = light_sql if api == "lightweight" else heavy_sql

        def _job():
            return spark.sql(sql).count()

        # Validate once (untimed): guard against 0-row / all-empty output.
        actual_rows = int(_job())
        if actual_rows == 0:
            raise RuntimeError(
                f"{fn} ({api}) produced 0 output rows -- check tile content or "
                "registration (all-empty/null guard)."
            )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=0,
            bands=0,
            dtype="",
            srid=0,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=f"job build/validate error: {str(e)[-400:]}",
            output_fingerprint="",
            **env,
        )

    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=_w,
            bands=_bands,
            dtype="float32",
            srid=4326,
            rows=actual_rows,
            nodata_frac=0.0,
            warmup_iters=stats["warmup_iters"],
            measured_iters=stats["measured_iters"],
            iter_median_s=ms / 1000.0,
            iter_min_s=stats["iter_min_ms"] / 1000.0,
            iter_p90_s=stats["iter_p90_ms"] / 1000.0,
            iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
            avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
            per_tile_avg_s=(ms / actual_rows / 1000.0) if (ms and actual_rows) else 0.0,
            per_tile_avg_ms=(ms / actual_rows) if (ms and actual_rows) else 0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=(
                (actual_rows / (ms / 1000.0)) if (ms and actual_rows) else 0.0
            ),
            peak_rss_mb=peak_rss_mb(),
            status="ok",
            note=f"{fn} {api} -> {actual_rows} output rows",
            output_fingerprint="",
            **env,
        )
    except Exception as e:  # noqa: BLE001
        return ResultRow(
            run_id=run_id,
            api=api,
            fn=fn,
            category="fanout",
            mode="spark-path",
            tile_px=_w,
            bands=_bands,
            dtype="float32",
            srid=4326,
            rows=0,
            nodata_frac=0.0,
            warmup_iters=warmup,
            measured_iters=0,
            iter_median_s=0.0,
            iter_min_s=0.0,
            iter_p90_s=0.0,
            iter_total_wall_clock_s=0.0,
            avg_wall_clock_s=0.0,
            per_tile_avg_s=0.0,
            per_tile_avg_ms=0.0,
            throughput_mpix_s=0.0,
            throughput_rows_s=0.0,
            peak_rss_mb=0.0,
            status="error",
            note=str(e)[-500:],
            output_fingerprint="",
            **env,
        )


def list_corpus_files(corpus_dir: str, filter_regex: str = r".*\.tif$") -> List[str]:
    """Return all files under corpus_dir whose basename matches ``filter_regex``.

    The reader bench is format-parameterized: GeoTIFF pools filter on the default
    ``.*\\.tif$`` while a NetCDF pool passes ``.*\\.nc$``. Mirrors the
    ``filterRegex`` option honored by both the light (netcdf_gbx) and heavy
    (netcdf_gdal / gdal) directory readers so the host-side listing and the
    on-cluster reader see the SAME set of files. Recurses into subdirectories.
    """
    import glob
    import re

    pat = re.compile(filter_regex)
    all_files = sorted(glob.glob(os.path.join(corpus_dir, "**", "*"), recursive=True))
    return [
        f for f in all_files if os.path.isfile(f) and pat.match(os.path.basename(f))
    ]


def _list_tifs(corpus_dir: str) -> List[str]:
    """Return all *.tif / *.tiff paths under corpus_dir (GeoTIFF pure-local leg).

    Preserves the original tif-then-tiff ordering exactly so the GeoTIFF bench is
    byte-for-byte unchanged; NetCDF and other formats use ``list_corpus_files``.
    """
    import glob

    tifs = sorted(glob.glob(os.path.join(corpus_dir, "**", "*.tif"), recursive=True))
    tifs += sorted(glob.glob(os.path.join(corpus_dir, "**", "*.tiff"), recursive=True))
    return tifs


def stage_netcdf_corpus(
    spark,
    corpus_dir: str,
    bbox: Optional[List[float]] = None,
    temporal: Optional[str] = None,
    partitions: Optional[int] = None,
):
    """Stage real Sentinel-5P L2 CH4 NetCDF granules into ``corpus_dir`` for the
    NetCDF VECTOR (swath) reader bench (download-and-stop mode).

    Bench-only helper -- NOT product code and NOT run at import time. The human
    invokes it once (interactively / from a staging notebook) to populate the
    ``{CORPUS}/netcdf-swath`` pool that the NetCDF reader-bench VECTOR leg reads.
    S5P L2 granules are SWATHS (irregular per-pixel lat/lon), so they are read in
    the light ``netcdf_gbx`` VECTOR mode -- there is no heavy swath path, so this
    leg is a light-only throughput measurement, not a heavy-vs-light comparison.
    (The heavy-vs-light RASTER leg uses regular-grid NASA-NEX granules staged by
    ``stage_nasanex_corpus`` into ``{CORPUS}/netcdf``.)

    Staging is DECOUPLED from ``read()``: the bench cell only globs the pool, so
    the download happens at stage time only, while the bench itself does not.

    Granules land as ``{item_id}.nc`` (matching the ``.*\\.nc$`` filter the bench
    passes). Returns the download-manifest DataFrame. Prints the resulting file
    count so a partial/empty stage is never silent.
    """
    from databricks.labs.gbx.sample.tropomi import TropomiDownloader

    # Default AOI: a modest box over a CH4 hotspot region (Permian Basin) with a
    # short window -- enough real swath granules for a throughput bench without
    # pulling the whole archive. Override via bbox/temporal for other AOIs.
    if bbox is None:
        bbox = [-104.5, 31.0, -101.5, 33.0]
    if temporal is None:
        temporal = "2021-01-01/2021-01-08"
    print(
        f"stage_netcdf_corpus: downloading S5P L2 CH4 swath granules to {corpus_dir} "
        f"(bbox={bbox}, temporal={temporal})",
        flush=True,
    )
    manifest = TropomiDownloader().download(
        bbox, corpus_dir, temporal=temporal, partitions=partitions, spark=spark
    )
    # StacClient.download returns a LAZY DataFrame (spark.range + a per-index fetch
    # UDF); it must be MATERIALIZED with an action or NOTHING downloads. Force it
    # (and surface how many assets the fetch reported valid) BEFORE globbing the dir.
    valid = manifest.filter(manifest["is_out_file_valid"]).count()
    staged = list_corpus_files(corpus_dir, r".*\.nc$")
    print(
        f"stage_netcdf_corpus: {len(staged)} .nc swath granule(s) staged under "
        f"{corpus_dir} ({valid} valid per manifest)",
        flush=True,
    )
    return manifest


def stage_nasanex_corpus(
    spark,
    corpus_dir: str,
    bbox: Optional[List[float]] = None,
    temporal: Optional[str] = None,
    variables: Tuple[str, ...] = ("tas",),
    partitions: Optional[int] = None,
):
    """Stage real NASA-NEX GDDP-CMIP6 regular-grid NetCDF granules into
    ``corpus_dir`` for the NetCDF RASTER reader bench (download-and-stop mode).

    Bench-only helper -- NOT product code and NOT run at import time. The human
    invokes it once (interactively / from a staging notebook) to populate the
    ``{CORPUS}/netcdf`` pool that the NetCDF reader-bench RASTER leg reads.
    NASA-NEX GDDP-CMIP6 granules are regular 0.25-degree lat/lon global grids, so
    BOTH the heavy ``netcdf_gdal`` and the light ``netcdf_gbx`` (raster mode)
    readers can enumerate them -- a fair heavy-vs-light comparison. (The S5P swath
    VECTOR leg is staged separately by ``stage_netcdf_corpus`` into
    ``{CORPUS}/netcdf-swath``.)

    Staging is DECOUPLED from ``read()``: the bench cell only globs the pool, so
    the download happens at stage time only, while the bench itself does not.

    Granules land as ``{item_id}_{asset_name}.nc`` (one file per climate variable
    per item; matching the ``.*\\.nc$`` filter the bench passes). Returns the
    download-manifest DataFrame. Prints the resulting file count so a partial or
    empty stage is never silent.
    """
    from databricks.labs.gbx.sample.nasanex import NasaNexDownloader

    # Default AOI: a modest box over the US Southwest with a short window -- enough
    # real regular-grid granules for a throughput bench without pulling the whole
    # archive. Override via bbox/temporal/variables for other AOIs.
    if bbox is None:
        bbox = [-104.5, 31.0, -101.5, 33.0]
    if temporal is None:
        temporal = "2021-01-01/2021-01-08"
    print(
        f"stage_nasanex_corpus: downloading NASA-NEX GDDP-CMIP6 grid granules to "
        f"{corpus_dir} (bbox={bbox}, temporal={temporal}, variables={variables})",
        flush=True,
    )
    manifest = NasaNexDownloader().download(
        bbox,
        corpus_dir,
        temporal=temporal,
        variables=variables,
        partitions=partitions,
        spark=spark,
    )
    # StacClient.download returns a LAZY DataFrame (spark.range + a per-index fetch
    # UDF); it must be MATERIALIZED with an action or NOTHING downloads. Force it
    # (and surface how many assets the fetch reported valid) BEFORE globbing the dir.
    valid = manifest.filter(manifest["is_out_file_valid"]).count()
    staged = list_corpus_files(corpus_dir, r".*\.nc$")
    print(
        f"stage_nasanex_corpus: {len(staged)} .nc grid granule(s) staged under "
        f"{corpus_dir} ({valid} valid per manifest)",
        flush=True,
    )
    return manifest


def _write_striped_gtiff(
    path: str, width: int, height: int, bands: int, dtype: str
) -> None:
    """Write a fully-striped (1-row-per-strip) GeoTIFF to ``path``.

    Striped layout is what large satellite sensors produce; it is the worst-case
    for the window-read path because each windowed ds.read() must seek to the
    row's strip offset instead of reading pre-tiled blocks. ``path`` must end in
    ``.tif``. Dimensions are configurable so tests can use tiny fixtures (< 1 MB)
    while the cluster profile uses VIIRS/UK-scale sizes (> 1 GB).
    """
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    data = np.zeros((bands, height, width), dtype=dtype)
    # Fill with a ramp so checksums are non-trivial.
    for b in range(bands):
        data[b] = np.arange(height * width, dtype=dtype).reshape(height, width) % 256

    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=bands,
        dtype=dtype,
        crs="EPSG:4326",
        transform=from_origin(0.0, 60.0, 0.01, 0.01),
        compress="DEFLATE",
        # Explicit STRIP layout — no tiling, no blockxsize/blockysize.
        tiled=False,
    )
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)


def _write_tiled_cog(
    path: str,
    width: int,
    height: int,
    bands: int,
    dtype: str,
    cog_blocksize: int = 64,
) -> None:
    """Write a genuine Cloud-Optimized GeoTIFF (COG) to ``path``.

    A real COG has (a) internal tiling AND (b) pre-built overview pyramid.
    This function writes a temporary plain GeoTIFF, then converts it to COG
    using the shared ``pyrx.core.analysis.cog_convert`` path — the same path
    the reader/writer COG features use — so the result is identical to what the
    product emits.

    ``cog_blocksize`` controls the overview tile size.  It must be small enough
    that at least one overview level is generated for the given ``width``/``height``
    (rule of thumb: ``max(width, height) > cog_blocksize``).  The default 64 is
    small so even tiny 128×128 unit-test fixtures produce overviews.

    ``path`` must end in ``.tif``.
    """
    import tempfile

    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.analysis import cog_convert

    data = np.zeros((bands, height, width), dtype=dtype)
    for b in range(bands):
        data[b] = np.arange(height * width, dtype=dtype).reshape(height, width) % 256

    # Write a plain GTiff first, then convert to COG in-memory.
    fd, tmp_path = tempfile.mkstemp(suffix=".tif")
    import os

    os.close(fd)
    try:
        profile = dict(
            driver="GTiff",
            width=width,
            height=height,
            count=bands,
            dtype=dtype,
            crs="EPSG:4326",
            transform=from_origin(0.0, 60.0, 0.01, 0.01),
        )
        with rasterio.open(tmp_path, "w", **profile) as ds_tmp:
            ds_tmp.write(data)
        # Convert to real COG (tiled + overviews) using the shared converter.
        with rasterio.open(tmp_path) as ds_src:
            cog_bytes = cog_convert(ds_src, "DEFLATE", cog_blocksize, "AVERAGE")
    finally:
        os.unlink(tmp_path)

    with open(path, "wb") as fh:
        fh.write(cog_bytes)


# ---------------------------------------------------------------------------
# Large-raster bench profile
# ---------------------------------------------------------------------------

#: Default corpus parameters for the full-scale cluster run (VIIRS/UK-scale).
#: Override via ``LargeRasterCorpusConfig`` in the cluster notebook.
_LARGE_RASTER_CLUSTER_DEFAULTS = {
    "width": 86400,  # VIIRS 750m global (~86400 cols)
    "height": 43200,  # VIIRS 750m global (~43200 rows)
    "bands": 1,
    "dtype": "float32",
    # Strategy sweep — the key variable under test.
    "split_strategies": ("none", "serverless", "classic", "auto"),
    # Serverless decoded budget (~512 MiB decoded) expressed as sizeInMB for the
    # legacy pure-local path that still takes a sizeInMB arg.
    "size_mib_serverless": 512,
    "size_mib_classic": 1536,
}


def _large_raster_result_row(
    *,
    run_id: str,
    env: dict,
    source_tag: str,
    raster_dtype: str,
    strategy: str,
    rows: int,
    status: str,
    note: str,
    stats=None,
    warmup: int = 0,
    corpus_size_mib: float = 0.0,
    throughput_mib_s: float = 0.0,
) -> "ResultRow":
    """Build a ResultRow for one large-raster bench leg.

    ``source_tag``    — "striped" | "tiled-cog"
    ``raster_dtype``  — the actual NumPy/rasterio dtype of the corpus raster
                        (e.g. "float32"), stored in the ``dtype`` field as intended.
    ``strategy``      — splitStrategy value: "none"|"serverless"|"classic"|"auto",
                        stored in the dedicated ``split_strategy`` optional field.
    ``corpus_size_mib`` — decoded file size in MiB; stamped into ``note`` so a
                          0-row fast read is immediately visible as wrong.
    ``throughput_mib_s`` — decoded MiB/s; stored in ``throughput_mpix_s``
                           (repurposed for this profile since mpix doesn't apply
                           to large single-file reads).
    """
    ms = stats["iter_median_ms"] if stats else 0.0
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=f"raster_read_large_{source_tag}",
        category="large_raster",
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype=raster_dtype,
        srid=0,
        rows=rows,
        nodata_frac=0.0,
        warmup_iters=(stats["warmup_iters"] if stats else warmup),
        measured_iters=(stats["measured_iters"] if stats else 0),
        iter_median_s=(ms / 1000.0),
        iter_min_s=((stats["iter_min_ms"] / 1000.0) if stats else 0.0),
        iter_p90_s=((stats["iter_p90_ms"] / 1000.0) if stats else 0.0),
        iter_total_wall_clock_s=(
            (stats["iter_total_wall_clock_ms"] / 1000.0) if stats else 0.0
        ),
        avg_wall_clock_s=((stats["avg_wall_clock_ms"] / 1000.0) if stats else 0.0),
        per_tile_avg_s=((ms / rows / 1000.0) if (ms and rows) else 0.0),
        per_tile_avg_ms=((ms / rows) if (ms and rows) else 0.0),
        # throughput_mpix_s repurposed for decoded MiB/s (mpix doesn't apply here).
        throughput_mpix_s=throughput_mib_s,
        throughput_rows_s=((rows / (ms / 1000.0)) if (ms and rows) else 0.0),
        peak_rss_mb=peak_rss_mb(),
        status=status,
        note=note,
        output_fingerprint="",
        split_strategy=strategy,
        **env,
    )


def _bench_large_raster_leg(
    spark,
    corpus_path: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    source_tag: str,
    strategy: str,
    corpus_size_mib: float,
    raster_dtype: str,
    where: str,
    env: dict,
) -> "ResultRow":
    """Time one leg of the large-raster profile.

    Reads ``corpus_path`` (a directory with one raster file) via ``raster_gbx``
    with the specified ``splitStrategy``. Times the full
    ``spark.read.format("raster_gbx").option(...).load(path).count()`` job.
    Verifies rows > 0 before recording (bench-verify-nonzero rule).

    ``corpus_size_mib`` is the decoded uncompressed size of the source file in MiB;
    it is stamped into the note so a 0-row fast read is visible. Throughput in
    MiB/s is computed from corpus_size_mib / iter_median_s (total file / one pass).
    """
    from databricks.labs.gbx.ds.register import register

    register(spark)

    def _job():
        return (
            spark.read.format("raster_gbx")
            .option("splitStrategy", strategy)
            .load(corpus_path)
            .count()
        )

    note_prefix = (
        f"{source_tag} strategy={strategy} " f"corpus={corpus_size_mib:.1f}MiB"
    )
    try:
        stats = time_iters(_job, warmup, measured)
        ms = stats["iter_median_ms"]
        # Verify rows > 0 before recording (bench-verify-nonzero rule: a 0-row
        # read is not a valid throughput measurement — it means the corpus is wrong
        # or the reader silently skipped the file).
        actual_rows = _job()
        actual_rows = int(actual_rows)
        if actual_rows == 0:
            note = f"{note_prefix} -- READ 0 ROWS (check corpus/options)"
            return _large_raster_result_row(
                run_id=run_id,
                env=env,
                source_tag=source_tag,
                raster_dtype=raster_dtype,
                strategy=strategy,
                rows=0,
                status="empty",
                note=note,
                warmup=warmup,
                corpus_size_mib=corpus_size_mib,
            )
        throughput = (corpus_size_mib / (ms / 1000.0)) if ms else 0.0
        note = f"{note_prefix} -> {actual_rows} tile(s) " f"({throughput:.1f} MiB/s)"
        return _large_raster_result_row(
            run_id=run_id,
            env=env,
            source_tag=source_tag,
            raster_dtype=raster_dtype,
            strategy=strategy,
            rows=actual_rows,
            status="ok",
            note=note,
            stats=stats,
            corpus_size_mib=corpus_size_mib,
            throughput_mib_s=throughput,
        )
    except Exception as e:  # noqa: BLE001
        return _large_raster_result_row(
            run_id=run_id,
            env=env,
            source_tag=source_tag,
            raster_dtype=raster_dtype,
            strategy=strategy,
            rows=0,
            status="error",
            note=f"{note_prefix} -- {str(e)[-400:]}",
            warmup=warmup,
            corpus_size_mib=corpus_size_mib,
        )


def run_large_raster_profile(
    spark,
    corpus_dir: str,
    run_id: str,
    warmup: int,
    measured: int,
    *,
    width: int = _LARGE_RASTER_CLUSTER_DEFAULTS["width"],
    height: int = _LARGE_RASTER_CLUSTER_DEFAULTS["height"],
    bands: int = _LARGE_RASTER_CLUSTER_DEFAULTS["bands"],
    dtype: str = _LARGE_RASTER_CLUSTER_DEFAULTS["dtype"],
    split_strategies: tuple = _LARGE_RASTER_CLUSTER_DEFAULTS["split_strategies"],
    where: str = "cluster",
) -> List[ResultRow]:
    """Large-raster bench profile: striped vs tiled-COG across splitStrategy values.

    Generates two synthetic corpora under ``corpus_dir`` — a striped GeoTIFF
    (worst-case sequential layout as produced by large satellite sensors) and a
    tiled COG (cloud-native baseline) — then times the light ``raster_gbx`` reader
    over each for every value in ``split_strategies``.

    Measurement goals:
    - **Ingest throughput** (decoded MiB/s): ``corpus_size_mib / iter_median_s``.
    - **Per-tile memory proxy**: ``peak_rss_mb`` captured after each leg (the
      Spark task memory watermark is not directly measurable from the driver in
      local mode, so RSS is the best available proxy for unit tests; on the cluster
      the executor RSS is observable via Spark metrics/Ganglia).
    - **Striped-vs-COG delta**: the ratio ``striped_median_s / cog_median_s`` for
      the same strategy — measurable from the returned rows.
    - **OOM envelope concept**: the ``strategy=serverless`` leg uses the ~512 MiB
      decoded budget; a file whose decoded size exceeds that will split into
      multiple tiles. The ``strategy=none`` leg disables splitting — if the file
      is large enough it will fail with an explicit error (the caller can read the
      OOM boundary from the ``status=error`` rows).

    Corpora are created lazily (idempotent): if the files already exist at the
    expected paths they are reused, so a cluster run that stages real VIIRS/UK
    GeoTIFFs beforehand does not regenerate them.

    For the unit test, pass small dimensions (e.g. width=512, height=512) so the
    fixture is generated quickly and the Spark job completes in seconds.
    For the full cluster run, use the class defaults (VIIRS-scale: 86400x43200).

    Args:
        spark: Active SparkSession (local or cluster).
        corpus_dir: Directory under which the two fixture sub-directories are
            created (``striped/`` and ``tiled_cog/``).
        run_id: Run-ID label stamped into every ResultRow.
        warmup: Warmup iterations per leg.
        measured: Measured iterations per leg.
        width: Raster width in pixels.
        height: Raster height in pixels.
        bands: Number of bands.
        dtype: NumPy/rasterio dtype string (e.g. ``"float32"``).
        split_strategies: Sequence of ``splitStrategy`` option values to sweep.
        where: ``env_where`` label (``"cluster"`` for cluster runs).

    Returns:
        A list of ``ResultRow`` instances, one per (source_tag, strategy) leg plus
        one ``ResultRow`` for the delta comparison (status="ok", category="large_raster",
        fn ending in "_delta").  The list length is
        ``len(split_strategies) * 2 + len(split_strategies)`` at most:
        striped × strategies + cog × strategies + delta per strategy.
    """
    import os

    env = capture_env(where)
    import numpy as np

    # Decoded size in MiB (before compression; this is what the reader must budget).
    itemsize = np.dtype(dtype).itemsize
    decoded_mib = (width * height * bands * itemsize) / (1024 * 1024)

    corpus_dir = str(corpus_dir)
    striped_dir = os.path.join(corpus_dir, "striped")
    cog_dir = os.path.join(corpus_dir, "tiled_cog")
    os.makedirs(striped_dir, exist_ok=True)
    os.makedirs(cog_dir, exist_ok=True)

    striped_path = os.path.join(striped_dir, "large_striped.tif")
    cog_path = os.path.join(cog_dir, "large_cog.tif")

    # Create fixtures lazily (idempotent).
    leg_n = len(split_strategies) * 2
    leg_i = 0
    print(
        f"[bench] large-raster profile: {width}x{height}x{bands} {dtype} "
        f"({decoded_mib:.1f} MiB decoded) — "
        f"{len(split_strategies)} strategies × 2 layouts = {leg_n} leg(s)"
    )

    if not os.path.exists(striped_path):
        print(f"  generating striped fixture -> {striped_path} ...", flush=True)
        _write_striped_gtiff(striped_path, width, height, bands, dtype)
    else:
        print(f"  reusing striped fixture at {striped_path}", flush=True)

    if not os.path.exists(cog_path):
        print(f"  generating tiled-COG fixture -> {cog_path} ...", flush=True)
        _write_tiled_cog(cog_path, width, height, bands, dtype)
    else:
        print(f"  reusing tiled-COG fixture at {cog_path}", flush=True)

    out: List[ResultRow] = []
    striped_by_strategy: dict = {}
    cog_by_strategy: dict = {}

    for strategy in split_strategies:
        # Striped leg.
        leg_i += 1
        print(
            f"[{leg_i}/{leg_n}] large-raster  striped  strategy={strategy}  ...",
            flush=True,
        )
        r_striped = _bench_large_raster_leg(
            spark,
            striped_dir,
            run_id,
            warmup,
            measured,
            source_tag="striped",
            strategy=strategy,
            corpus_size_mib=decoded_mib,
            raster_dtype=dtype,
            where=where,
            env=env,
        )
        out.append(r_striped)
        striped_by_strategy[strategy] = r_striped
        print(
            f"[{leg_i}/{leg_n}] large-raster  striped  strategy={strategy}  "
            f"{r_striped.iter_median_s * 1000:.1f}ms  rows={r_striped.rows}  "
            f"{r_striped.status}",
            flush=True,
        )

        # Tiled-COG leg.
        leg_i += 1
        print(
            f"[{leg_i}/{leg_n}] large-raster  tiled-cog  strategy={strategy}  ...",
            flush=True,
        )
        r_cog = _bench_large_raster_leg(
            spark,
            cog_dir,
            run_id,
            warmup,
            measured,
            source_tag="tiled-cog",
            strategy=strategy,
            corpus_size_mib=decoded_mib,
            raster_dtype=dtype,
            where=where,
            env=env,
        )
        out.append(r_cog)
        cog_by_strategy[strategy] = r_cog
        print(
            f"[{leg_i}/{leg_n}] large-raster  tiled-cog  strategy={strategy}  "
            f"{r_cog.iter_median_s * 1000:.1f}ms  rows={r_cog.rows}  "
            f"{r_cog.status}",
            flush=True,
        )

    # Emit one delta row per strategy so the striped-vs-COG ratio is explicit in
    # the results table without requiring the user to compute it from the raw rows.
    # The delta row is status="ok" even when one leg errored, but its note makes
    # the situation explicit (so it doesn't masquerade as a clean measurement).
    for strategy in split_strategies:
        r_s = striped_by_strategy.get(strategy)
        r_c = cog_by_strategy.get(strategy)
        if r_s is not None and r_c is not None:
            if r_s.status == "ok" and r_c.status == "ok" and r_c.iter_median_s > 0:
                delta = r_s.iter_median_s / r_c.iter_median_s
                note = (
                    f"striped/cog delta: strategy={strategy} "
                    f"ratio={delta:.2f}x "
                    f"(striped={r_s.iter_median_s * 1000:.1f}ms "
                    f"cog={r_c.iter_median_s * 1000:.1f}ms)"
                )
                delta_status = "ok"
            else:
                delta = 0.0
                note = (
                    f"striped/cog delta: strategy={strategy} "
                    f"could not compute (striped={r_s.status} cog={r_c.status})"
                )
                delta_status = (
                    "error"
                    if (r_s.status == "error" or r_c.status == "error")
                    else "empty"
                )
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn="raster_read_large_delta",
                    category="large_raster",
                    mode="spark-path",
                    tile_px=0,
                    bands=0,
                    dtype=dtype,
                    srid=0,
                    rows=r_s.rows + r_c.rows,
                    nodata_frac=0.0,
                    warmup_iters=warmup,
                    measured_iters=measured if delta_status == "ok" else 0,
                    iter_median_s=delta,
                    iter_min_s=0.0,
                    iter_p90_s=0.0,
                    iter_total_wall_clock_s=0.0,
                    avg_wall_clock_s=0.0,
                    per_tile_avg_s=0.0,
                    per_tile_avg_ms=0.0,
                    throughput_mpix_s=delta,
                    throughput_rows_s=0.0,
                    split_strategy=strategy,
                    peak_rss_mb=peak_rss_mb(),
                    status=delta_status,
                    note=note,
                    output_fingerprint="",
                    **env,
                )
            )

    # Progress summary.
    ok_rows = [
        r
        for r in out
        if r.status == "ok" and r.category == "large_raster" and "delta" not in r.fn
    ]
    print(
        f"[bench] large-raster profile done: "
        f"{len(ok_rows)} ok legs, {len(out)} total rows",
        flush=True,
    )
    return out


def run_file_write_layout_sweep(
    spark,
    *,
    fmt: str,
    source,
    target_prefix: str,
    run_id: str,
    warmup: int,
    measured: int,
    file_mode: str,
    filespace: Optional[str] = None,
    layouts: tuple = ("order", "cluster", "plain"),
    where: str = "cluster",
) -> List["ResultRow"]:
    """Run the FILE-write leg for each layout and return one ResultRow per layout.

    Loops ``layouts`` ("order"/"cluster"/"plain"), calls the format's write leg
    (``run_gtiff_file_write`` for ``fmt="gtiff"``, ``run_gpkg_file_write`` for
    ``fmt="gpkg"``) with ``layout=<L>`` and a per-layout target
    ``{target_prefix}_{L}`` (leg isolation — each layout writes its own table
    so no layout can advantage another by inheriting a prior write's on-disk
    grouping).

    For ``layout="cluster"``, runs ``OPTIMIZE <table>`` after the write inside a
    guarded ``try`` block:
    - On success: appends ``" +OPTIMIZE"`` to the row's ``note``.
    - On failure/skip: appends the skip reason to the row's ``note``.

    ``file_mode="fuse"`` legs pass ``layout`` through to the write leg (the
    DataSource writer ignores it), but still record it so the sweep row is
    complete.  The meaningful layout bite is on FILE-table writes (on-cluster
    legs).

    Args:
        spark: active SparkSession.
        fmt: ``"gtiff"`` or ``"gpkg"``.
        source: for ``"gtiff"`` — a tile DataFrame (passed as ``tile_df``);
                for ``"gpkg"`` — a path to a local/FUSE-accessible .gpkg file.
        target_prefix: path prefix; each layout writes to ``{target_prefix}_{L}``.
        run_id: benchmark run identifier label.
        warmup: number of warmup iterations.
        measured: number of measured iterations.
        file_mode: ``"fuse"``, ``"external"``, or ``"managed"``.
        filespace: filespace identifier (passed through for FILE modes).
        layouts: ordered tuple of layout names to sweep.
        where: env_where label recorded in each ResultRow.

    Returns:
        List of ResultRow — one per layout in ``layouts`` order.
    """
    results: List["ResultRow"] = []

    for layout in layouts:
        target = f"{target_prefix}_{layout}"

        if fmt == "gtiff":
            row = run_gtiff_file_write(
                spark,
                source,
                target,
                run_id,
                warmup,
                measured,
                file_mode=file_mode,
                filespace=filespace,
                layout=layout,
                where=where,
            )
        elif fmt == "gpkg":
            row = run_gpkg_file_write(
                spark,
                source,
                target,
                run_id,
                warmup,
                measured,
                file_mode=file_mode,
                filespace=filespace,
                layout=layout,
                where=where,
            )
        else:
            raise ValueError(f"run_file_write_layout_sweep: unsupported fmt={fmt!r}")

        # For the "cluster" layout, attempt OPTIMIZE on the FILE table.
        # FUSE legs have no Delta table → guard and record skip reason.
        if layout == "cluster":
            import dataclasses

            try:
                spark.sql(f"OPTIMIZE `{target}`")
                # Append "+OPTIMIZE" to the note on success.
                existing_note = row.note or ""
                row = dataclasses.replace(
                    row, note=(existing_note + " +OPTIMIZE").strip()
                )
            except Exception as exc:  # noqa: BLE001
                skip_reason = str(exc)[:160]
                existing_note = row.note or ""
                row = dataclasses.replace(
                    row,
                    note=(existing_note + f" OPTIMIZE skipped: {skip_reason}").strip(),
                )

        results.append(row)

    return results


def _print_summary(rows: List[ResultRow]) -> None:
    """Print a compact results table to stdout."""
    if not rows:
        print("(no results)")
        return
    print(
        f"\n{'file/note':<40} {'mode':<12} {'status':<8} {'median_s':>10} {'rows':>8}"
    )
    print("-" * 82)
    for r in rows:
        print(
            f"{r.note:<40} {r.mode:<12} {r.status:<8} "
            f"{r.iter_median_s:>10.4f} {r.rows:>8}"
        )
    ok = [r for r in rows if r.status == "ok"]
    if ok:
        import statistics

        med = statistics.median(r.iter_median_s for r in ok)
        print(f"\nMedian iter_median_s across {len(ok)} file(s): {med:.4f} s")


def main(argv: Optional[List[str]] = None) -> None:
    ap = argparse.ArgumentParser(prog="databricks.labs.gbx.bench.readers")
    ap.add_argument(
        "--mode",
        default="pure-local",
        choices=["pure-local", "spark-path", "both"],
        help="Benchmark mode (default: pure-local)",
    )
    ap.add_argument(
        "--corpus",
        required=True,
        help="Directory containing *.tif files to benchmark",
    )
    ap.add_argument("--run-id", default="local", help="Run ID label (default: local)")
    ap.add_argument(
        "--warmup", type=int, default=1, help="Warmup iterations (default: 1)"
    )
    ap.add_argument(
        "--measured", type=int, default=3, help="Measured iterations (default: 3)"
    )
    ap.add_argument(
        "--size-mib", type=int, default=16, help="Tile size budget in MiB (default: 16)"
    )
    ap.add_argument(
        "--out",
        default="",
        help="Output JSONL path (default: print summary only)",
    )
    ap.add_argument("--where", default="venv", help="env_where label (default: venv)")
    a = ap.parse_args(argv)

    rows: List[ResultRow] = []

    if a.mode in ("pure-local", "both"):
        files = _list_tifs(a.corpus)
        if not files:
            print(f"WARNING: no .tif/.tiff files found under {a.corpus}", flush=True)
        else:
            print(f"pure-local: {len(files)} file(s)", flush=True)
            rows += run_pure_local_reader(
                files=files,
                run_id=a.run_id,
                warmup=a.warmup,
                measured=a.measured,
                size_mib=a.size_mib,
                where=a.where,
            )

    if a.mode in ("spark-path", "both"):
        import sys

        from pyspark.sql import SparkSession

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("bench-readers")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        print(f"spark-path: corpus={a.corpus}", flush=True)
        rows += run_spark_path_reader(
            spark=spark,
            path=a.corpus,
            run_id=a.run_id,
            warmup=a.warmup,
            measured=a.measured,
            size_mib=a.size_mib,
            where=a.where,
        )

    _print_summary(rows)

    if a.out:
        from databricks.labs.gbx.bench.results import write_jsonl

        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(rows, a.out)
        print(f"wrote {len(rows)} rows -> {a.out}")


if __name__ == "__main__":
    main()
