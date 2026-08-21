"""Cluster-side helpers: persist bench rows to a Delta table + build the bench notebook."""

from __future__ import annotations

import dataclasses
from dataclasses import asdict, replace
from typing import List

from databricks.labs.gbx.bench.results import ResultRow

# Explicit on-write column order for the bench_results Delta table. The ResultRow
# dataclass field order keeps its defaults last (a dataclass constraint), but the
# table reads better with the timing/throughput metrics grouped and the per-iter
# distribution (iter_*) trailing. rows_to_dataframe .select()s to this order.
ORDER = [
    "run_event_num",
    "run_id",
    "api",
    "fn",
    "category",
    "mode",
    # Headline timing metrics sit right after `mode` for at-a-glance reading (seconds; per-
    # tile also in ms). The rest of the dims/throughput/env follow.
    "avg_wall_clock_s",
    "per_tile_avg_s",
    "per_tile_avg_ms",
    "tile_px",
    "bands",
    "dtype",
    "srid",
    "rows",
    "nodata_frac",
    "warmup_iters",
    "measured_iters",
    "throughput_mpix_s",
    "throughput_rows_s",
    "peak_rss_mb",
    "status",
    "note",
    "output_fingerprint",
    "env_arch",
    "env_cpu_model",
    "env_cpu_count",
    "env_os",
    "env_gbx_version",
    "env_gdal_version",
    "env_runtime_version",
    "env_where",
    "iter_median_s",
    "iter_min_s",
    "iter_p90_s",
    "iter_total_wall_clock_s",
    # Large-raster profile: splitStrategy value ("none"|"serverless"|"classic"|"auto").
    # None for all other profiles; the field is optional in ResultRow.
    "split_strategy",
    # Virtual-tile bench leg: which input-tile form was measured ("materialized" or "virtual").
    "input_tile",
    # Whether the fn materialized pixels on a virtual input: "deferred", "materialized", "na".
    "output_disposition",
    # Spark-path plan build time (seconds): time to call col_fn + build the DataFrame plan.
    # Isolates the plan-build cost from the actual execution time.
    "plan_s",
    # FILE-access matrix (Phase 2): mode, layout, parallelism, chunk size.
    "file_mode",
    "layout",
    "input_partitions",
    "launched_tasks",
    "slots_available",
    "chunk_size",
]

# Guard against drift: ORDER must cover exactly the ResultRow fields, no more, no less.
_RR_FIELDS = {f.name for f in dataclasses.fields(ResultRow)}
if set(ORDER) != _RR_FIELDS:
    raise RuntimeError(
        "bench.cluster.ORDER out of sync with ResultRow fields: "
        f"missing={_RR_FIELDS - set(ORDER)} extra={set(ORDER) - _RR_FIELDS}"
    )


def rows_to_dataframe(rows: List[ResultRow], spark, where: str = "cluster"):
    """Build a Spark DataFrame from ResultRows, re-tagging env_where (e.g. 'cluster').

    Columns are emitted in the explicit ``ORDER`` (not dataclass field order) so the
    Delta table column layout is the human-readable grouping, with the per-iter
    distribution (iter_*) trailing.

    Optional[str] fields (e.g. ``split_strategy``) that are all-None in the batch
    must be explicitly typed as StringType; Spark/pandas cannot infer the type of an
    all-None column. The pandas column is filled with empty string before creating the
    DataFrame so the inference path works; the value is preserved as-is for non-None
    rows (the actual strategy label), and the empty string is a safe sentinel for rows
    where the field is not applicable (all non-large-raster profiles).
    """
    import pandas as pd

    _OPTIONAL_STR_FIELDS = {"split_strategy"}

    retagged = [replace(r, env_where=where) for r in rows]
    records = [asdict(r) for r in retagged]
    # Fill all-None optional-str columns so pandas can infer "object" (string) dtype.
    for field in _OPTIONAL_STR_FIELDS:
        if all(rec.get(field) is None for rec in records):
            for rec in records:
                rec[field] = ""
    df = spark.createDataFrame(pd.DataFrame(records))
    return df.select(*ORDER)


def to_delta(rows: List[ResultRow], spark, table: str, where: str = "cluster") -> int:
    """Append ResultRows to the bench_results Delta table. Returns row count. (Cluster-only.)"""
    if not rows:
        return 0
    df = rows_to_dataframe(rows, spark, where=where)
    # mergeSchema so new ResultRow columns (e.g. iter_total/avg_wall_clock_s) append to an
    # existing bench_results table instead of failing on a schema mismatch.
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table
    )
    return len(rows)


def _remap_heavy_iter_to_seconds(d: dict) -> dict:
    """Map a heavy Scala jsonl row (MILLISECOND timing keys) onto ResultRow second-scale
    fields, in place. The Scala BenchRow still emits ms under the old key names (Scala is
    unchanged); ResultRow now stores SECONDS, so rename each ms key to its iter_*_s / *_s
    field and divide by 1000. per_tile_avg_{s,ms} are derived from the median + rows (Scala
    emits neither). The heavy run notebook calls this so the conversion is shared + testable.
    """
    for _old, _new in (
        ("median_ms", "iter_median_s"),
        ("min_ms", "iter_min_s"),
        ("p90_ms", "iter_p90_s"),
        ("total_wall_clock_ms", "iter_total_wall_clock_s"),
        ("avg_wall_clock_ms", "avg_wall_clock_s"),
    ):
        if _old in d:
            d[_new] = d.pop(_old) / 1000.0
    _n = d.get("rows") or 0
    _ms = d.get("iter_median_s", 0.0) * 1000.0  # back to the original ms median
    d["per_tile_avg_ms"] = (_ms / _n) if (_ms and _n) else 0.0
    d["per_tile_avg_s"] = (_ms / _n / 1000.0) if (_ms and _n) else 0.0
    return d


def _cell(source: str, kind: str = "code", collapsed: bool = False) -> dict:
    # collapsed: hide this cell's SOURCE by default. Sets the JupyterLab standard
    # (`jupyter.source_hidden`) plus the legacy `collapsed` flag; the Databricks notebook /
    # job-run viewer may or may not honor either (jobs render a static notebook), so this is
    # best-effort -- it cleanly collapses in Jupyter/.ipynb viewers regardless.
    meta = {}
    if collapsed:
        meta = {"jupyter": {"source_hidden": True}, "collapsed": True}
    return {
        "cell_type": kind,
        "metadata": meta,
        "outputs": [],
        "execution_count": None,
        "source": source.splitlines(keepends=True),
    }


_PREAMBLE = """import json
import os
import importlib as _importlib
import sys as _sys

# --disable-file: suppress Databricks FILE type for virtual-tile reads (FILE-off A/B leg).
# Must run before any geobrix imports that call file_supported() so the memoized
# per-session cache is never populated with True.
DISABLE_FILE = {disable_file!r}
if DISABLE_FILE:
    os.environ["GBX_DISABLE_FILE"] = "1"
    print("GBX_DISABLE_FILE=1 — FILE type disabled for this leg")

# DBR ships databricks/__init__.py that pre-sets databricks.__path__ to specific
# system directories, excluding the %pip virtual-env site-packages where
# geobrix just landed. Extend the path before any geobrix imports so that
# `databricks.labs.gbx` resolves correctly.
try:
    import databricks as _db_pkg
    for _p in _sys.path:
        _db_dir = _p + "/databricks"
        if (
            _db_dir not in _db_pkg.__path__
            and not os.path.exists(_db_dir + "/__init__.py")
            and os.path.isdir(_db_dir)
        ):
            _db_pkg.__path__.append(_db_dir)
    _importlib.invalidate_caches()
    del _p, _db_dir, _db_pkg
except Exception:
    pass
del _importlib, _sys

from databricks.labs.gbx.bench import compare, results, runner
from databricks.labs.gbx.bench import cluster as _cl
from databricks.labs.gbx.bench import manifest as _m
from databricks.labs.gbx.bench import spec as _s

CORPUS = {corpus!r}
OUT = {out_dir!r}
TABLE = {table!r}
RUN_ID = {run_id!r}
FUNCTIONS = {functions!r}
SET = {set!r}
MODES = {modes!r}
ROW_COUNTS = [int(x) for x in {row_counts!r}.split(",") if x]
WARMUP, MEASURED = {warmup}, {measured}
# Spark-path uses its own (usually smaller) iteration counts -- the N-tile sweep is the
# averaging, so 1 warm-up + 1 measured is the efficient default; pure-core keeps WARMUP/MEASURED.
SPARK_WARMUP, SPARK_MEASURED = {spark_warmup}, {spark_measured}
# Tiles per partition for the spark-path row DataFrame; 0 = auto (n / (slots*4), i.e.
# oversubscribe slots ~4x so finished slots grab pending tasks instead of idling on the
# straggler tail). Set via --override-partition-size.
PARTITION_SIZE = {partition_size}
TRUNCATE = {truncate!r}
TRUNCATE_ALL = {truncate_all!r}
RESUME = {resume!r}
# On --resume, FIX_ERRORS (default) re-runs fns whose only row is an error (e.g. after a
# code/JAR fix) by purging those error rows first; --no-fix-errors keeps them as-is.
FIX_ERRORS = {fix_errors!r}
# --redo-functions: force re-run this explicit list of fns for the selected (api, mode) by
# DELETING their existing rows first (whatever the status), so they re-run while every OTHER
# fn's rows stay. It is INDEPENDENT of the run scope (--set/--functions), so one run can
# resume-run the never-ran fns AND force-redo this named subset. Unlike --resume (runs only
# MISSING) or --truncate-* (clears broadly). CSV string of fn names ("" = redo nothing).
REDO_FUNCTIONS = {redo_functions!r}
LIGHTWEIGHT, HEAVYWEIGHT = {lightweight!r}, {heavyweight!r}
# --explain-only: build each spark-path fn's DataFrame and print/persist its physical plan
# WITHOUT timing or writing to Delta. Plans are also teed to EXPLAIN_DIR on the Volume so
# they can be harvested after the run. Diagnostic only -- no rows are produced.
EXPLAIN_ONLY = {explain_only!r}
EXPLAIN_DIR = OUT + "/explain"
# --benchmark-readers: also run the reader benchmark (raster_gbx vs gdal) on the cluster.
# --readers-only: ONLY run the reader benchmark, skip all fn benchmarks.
BENCHMARK_READERS = {benchmark_readers!r}
READERS_ONLY = {readers_only!r}
# --benchmark-pmtiles: also run the pmtiles writer benchmark (pmtiles_gbx vs pmtiles).
# --pmtiles-only: ONLY run the pmtiles benchmark, skip all fn benchmarks.
BENCHMARK_PMTILES = {benchmark_pmtiles!r}
PMTILES_ONLY = {pmtiles_only!r}
# --benchmark-vector: also run the vector reader benchmark (light *_gbx vs heavy *_ogr).
# --vector-only: ONLY run the vector reader benchmark, skip all fn benchmarks.
BENCHMARK_VECTOR = {benchmark_vector!r}
VECTOR_ONLY = {vector_only!r}
# --benchmark-mvt: also run the st_asmvt benchmark (light pyvx vs heavy vectorx).
# --mvt-only: ONLY run the MVT benchmark, skip all fn benchmarks.
BENCHMARK_MVT = {benchmark_mvt!r}
MVT_ONLY = {mvt_only!r}
# --benchmark-pmtiles-agg: also run the pmtiles_agg grouped-agg benchmark (light vs heavy).
# --pmtiles-agg-only: ONLY run the pmtiles_agg benchmark, skip all fn benchmarks.
BENCHMARK_PMTILES_AGG = {benchmark_pmtiles_agg!r}
PMTILES_AGG_ONLY = {pmtiles_agg_only!r}
# --benchmark-vector-tin: also run the TIN + legacy benchmark (light pyvx vs heavy vectorx).
# --vector-tin-only: ONLY run the TIN + legacy benchmark, skip all fn benchmarks.
BENCHMARK_VECTOR_TIN = {benchmark_vector_tin!r}
VECTOR_TIN_ONLY = {vector_tin_only!r}
# --benchmark-grid-quadbin: also run the quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
# --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip all fn benchmarks.
BENCHMARK_GRID_QUADBIN = {benchmark_grid_quadbin!r}
GRID_QUADBIN_ONLY = {grid_quadbin_only!r}
# --benchmark-grid-bng: also run the BNG grid benchmark (light pygx vs heavy gridx.bng).
# --grid-bng-only: ONLY run the BNG grid benchmark, skip all fn benchmarks.
BENCHMARK_GRID_BNG = {benchmark_grid_bng!r}
GRID_BNG_ONLY = {grid_bng_only!r}
# --benchmark-grid-custom: also run the custom grid benchmark (light pygx vs heavy gridx.custom).
# --grid-custom-only: ONLY run the custom grid benchmark, skip all fn benchmarks.
BENCHMARK_GRID_CUSTOM = {benchmark_grid_custom!r}
GRID_CUSTOM_ONLY = {grid_custom_only!r}
# --benchmark-fanout: also run the fan-out UDTF benchmark (all 8 streaming UDTFs).
# --fanout-only: ONLY run the fanout benchmark, skip all fn benchmarks.
BENCHMARK_FANOUT = {benchmark_fanout!r}
FANOUT_ONLY = {fanout_only!r}
# --benchmark-netcdf: also run the NetCDF reader benchmark (light netcdf_gbx vs heavy
# netcdf_gdal) over the same staged .nc granule pool at {{CORPUS}}/netcdf.
# --netcdf-only: ONLY run the NetCDF reader benchmark, skip all fn benchmarks.
# The corpus is staged separately (real S5P L2 CH4 granules via TropomiDownloader);
# the bench SKIPS CLEANLY when the pool is empty/missing.
BENCHMARK_NETCDF = {benchmark_netcdf!r}
NETCDF_ONLY = {netcdf_only!r}
# --benchmark-netcdf-writer: also run the NetCDF WRITER benchmark (light netcdf_gbx write
# throughput, raster + vector legs) reusing the reader-cycle corpora at {{CORPUS}}/netcdf +
# {{CORPUS}}/netcdf-swath. --netcdf-writer-only: ONLY run it, skip all fn benchmarks.
# LIGHT-ONLY: there is no heavy NetCDF writer, so this is a throughput measurement, NOT a
# heavy-vs-light comparison. Skips cleanly when the input corpus is empty/missing.
BENCHMARK_NETCDF_WRITER = {benchmark_netcdf_writer!r}
NETCDF_WRITER_ONLY = {netcdf_writer_only!r}
# --fanout-scale: dial the synthetic fan-out size (default 1.0 -> meaningful but ~couple
# minutes on ~20 workers). Larger = more output rows per function.
FANOUT_SCALE = {fanout_scale}
# --vector-scale: read the scaled 1M-seed corpus (copies dir + seed file) instead of the
# tiny 4-file corpus. Requires the scaled corpus to have been generated first via
# gbx:bench:generate-vector-corpus and staged at {{CORPUS}}/vector-scale/<fmt>/.
VECTOR_SCALE = {vector_scale!r}
WRITER_ROWS = {writer_rows}
# --vector-legs reader|writer|both : run only the reader-ingest legs, only the writer-export
# leg, or both. Lets each reader/writer be benchmarked as its own isolated cluster job (a
# struggling reader can't block the writers, and the 14M writer-source table is only
# materialized for writer runs). Default both.
VECTOR_LEGS = {vector_legs!r}
# --vector-formats csv : restrict the scaled vector run to these light formats (e.g.
# "geojson_gbx"). Empty = all four. Combined with --vector-legs and --lightweight-only/
# --heavyweight-only, this runs ONE (format x tier x leg) per job for true cold isolation.
VECTOR_FORMATS = {vector_formats!r}
# --input-tile: which form of input tile the spark-path runner receives ("materialized" or
# "virtual"). "materialized" is the classic path (bytes from binaryFile); "virtual" passes
# a path+window tile struct and triggers the virtual-tile reader code path.
INPUT_TILE = {input_tile!r}
# --grouped-file: also run the grouped FILE-amortization benchmark (rst_clip_grouped +
# pixel-op _grouped fns) over a MULTIWINDOW COG corpus, across three tile modes
# (materialized / virtual+FILE-off / virtual+FILE-on). --grouped-file-only: ONLY run it.
# --multiwindow-corpus: the corpus dir (holds cog_multiwindow_manifest.json); default is
# CORPUS + "/bench-corpus-cog-multiwindow". Skips cleanly when the manifest is absent.
BENCHMARK_GROUPED_FILE = {benchmark_grouped_file!r}
GROUPED_FILE_ONLY = {grouped_file_only!r}
MULTIWINDOW_CORPUS = {multiwindow_corpus!r} or (CORPUS + "/bench-corpus-cog-multiwindow")
# --file-matrix: sweep file_mode in ("fuse","external","managed") for GeoTIFF + GeoPackage reads.
# --file-matrix-only: ONLY run the FILE matrix, skip all fn benchmarks.
BENCHMARK_FILE_MATRIX = {benchmark_file_matrix!r}
FILE_MATRIX_ONLY = {file_matrix_only!r}
# --gpkg-chunksize: sweep chunkSize (1k/10k/100k) for GeoPackage fuse reads (fanout-invariance).
# --gpkg-chunksize-only: ONLY run the chunkSize sweep, skip all fn benchmarks.
BENCHMARK_GPKG_CHUNKSIZE = {benchmark_gpkg_chunksize!r}
GPKG_CHUNKSIZE_ONLY = {gpkg_chunksize_only!r}
# --layout-sweep: sweep GeoTIFF + GeoPackage write layouts (order/cluster/plain).
# --layout-sweep-only: ONLY run the layout sweep, skip all fn benchmarks.
BENCHMARK_LAYOUT_SWEEP = {benchmark_layout_sweep!r}
LAYOUT_SWEEP_ONLY = {layout_sweep_only!r}
# --layout-scan: compare sequential-scan cost across write layouts (requires FILE tables from
# the layout-sweep leg). --layout-scan-only: ONLY run the layout scan.
BENCHMARK_LAYOUT_SCAN = {benchmark_layout_scan!r}
LAYOUT_SCAN_ONLY = {layout_scan_only!r}
# Filespace identifier for FILE EXTERNAL/MANAGED table creation (e.g. a TBLPROPERTIES
# filespace path such as "/Volumes/catalog/schema/vol/filespace"). Empty string means no
# filespace provisioned; external/managed legs yield na_by_design on FUSE-only tiers or
# when not configured.
FILE_FILESPACE = {file_filespace!r}
# GeoPackage bench corpus base dir. Sub-dirs staged by corpus_vector.stage_gpkg_bench_corpus().
# Default: CORPUS + "/bench-corpus-gpkg".
GPKG_CORPUS = {gpkg_corpus!r} or (CORPUS + "/bench-corpus-gpkg")

os.makedirs(OUT, exist_ok=True)
# Disable AQE so it can't coalesce the spark-path repartition back toward
# defaultParallelism (~slots) -- which reintroduces the straggler idle. The runner sets an
# explicit partition count per fn; with AQE off it's respected. This is a CLASSIC-cluster
# optimization: on Serverless / Spark Connect these configs are not settable
# (CONFIG_NOT_AVAILABLE, even to GET with a default) and AQE-off does not apply -- so detect
# Connect and skip cleanly rather than attempt-and-catch (which logged a misleading
# "could not set ... CONFIG_NOT_AVAILABLE" that reads like a failure). See the serverless
# forbidden-config guidance: never attempt a restricted conf on Connect, detect and skip.
if "connect" in type(spark).__module__:
    print("AQE: skipped on Serverless/Connect (configs not settable; using platform defaults)")
else:
    # Classic: set both the Apache and the Databricks switches, then echo the effective values.
    for _ck, _cv in (
        ("spark.sql.adaptive.enabled", "false"),
        ("spark.databricks.optimizer.adaptive.enabled", "false"),
    ):
        try:
            spark.conf.set(_ck, _cv)
        except Exception as _e:
            print(f"could not set {{_ck}}: {{_e}}")
    try:
        print(
            "AQE: adaptive.enabled="
            + str(spark.conf.get("spark.sql.adaptive.enabled", "?"))
            + " databricks.adaptive="
            + str(spark.conf.get("spark.databricks.optimizer.adaptive.enabled", "?"))
        )
    except Exception as _e:
        print(f"AQE: config readback unavailable: {{_e}}")
# Reader-only runs (readers/pmtiles/vector/mvt/pmtiles-agg/tin/grid/fanout/netcdf --*-only) stage
# their OWN corpus (a reader pool, a vector corpus, synthetic in-memory rows, or -- for netcdf --
# the {{CORPUS}}/netcdf + {{CORPUS}}/netcdf-swath pools). They do NOT use the function-bench row
# pool, so requiring {{CORPUS}}/corpus.json for them would hard-fail a valid reader run on a missing
# scaffold. Read corpus.json LAZILY: only when a function-bench leg actually runs. When it is not
# read, `corpus` is None and downstream pool_size usages fall back to None (summarize accepts it).
_READER_ONLY = (
    READERS_ONLY or PMTILES_ONLY or VECTOR_ONLY or MVT_ONLY or PMTILES_AGG_ONLY
    or VECTOR_TIN_ONLY or GRID_QUADBIN_ONLY or GRID_BNG_ONLY or GRID_CUSTOM_ONLY
    or FANOUT_ONLY or NETCDF_ONLY or NETCDF_WRITER_ONLY or GROUPED_FILE_ONLY
    or FILE_MATRIX_ONLY or GPKG_CHUNKSIZE_ONLY or LAYOUT_SWEEP_ONLY or LAYOUT_SCAN_ONLY
)
corpus = None if _READER_ONLY else _m.Corpus.read(f"{{CORPUS}}/corpus.json")
# Function-bench summaries annotate results with the row-pool size; reader-only runs have no
# function pool (corpus is None), so POOL_SIZE is None and results.summarize omits that annotation.
POOL_SIZE = None if corpus is None else len(corpus.row_pool.tiles)
fnspecs = _s.select(functions=[x for x in FUNCTIONS.split(",") if x] or None, set=SET)
# --redo-functions FORCES its fns into the run scope even when they fall outside --set /
# --functions, so `--redo-functions X` actually RE-RUNS X. Purging alone (the old behavior)
# would DELETE X's rows and re-run nothing when X is not in the selected set -- a silent
# data-loss footgun. Union any redo fn missing from the base scope into BOTH fnspecs (the
# lightweight scope + the heavy _mode_names gate) AND FUNCTIONS (the csv run_heavy splits).
_redo_names = [x for x in REDO_FUNCTIONS.split(",") if x]
if _redo_names:
    _base_names = [f.name for f in fnspecs]
    _redo_missing = [x for x in _redo_names if x not in _base_names]
    if _redo_missing:
        fnspecs = list(fnspecs) + list(_s.select(functions=_redo_missing))
        FUNCTIONS = ",".join([x for x in FUNCTIONS.split(",") if x] + _redo_missing)
# Per-mode expected row counts: how many fns run in each mode. Used by --resume to tell a
# COMPLETE (tier x mode) section (already in the table for this run_id) from a partial one.
_PC_EXP = len([f for f in fnspecs if "pure-core" in f.modes])
_SP_EXP = len([f for f in fnspecs if "spark-path" in f.modes])
lw, hw, all_rows = [], [], []
"""

# Truncate up-front + define the incremental Delta sink. Kept OUT of _PREAMBLE (which
# is .format()-processed) so the runtime f-strings here use single braces. The sink
# appends each function's rows the moment that function finishes, so the run can be
# polled / queried in real time via SELECT ... WHERE run_id = RUN_ID. (With incremental
# flushing the table is truncated up-front, NOT at the end -- a late truncate would wipe
# the rows we just streamed in.)
_SINK = """
if TRUNCATE_ALL:
    # Whole-table reset: only THIS run's rows remain afterwards. Use for a clean table;
    # NOT for coexisting light+heavy run separately (the 2nd invocation would wipe the
    # 1st) -- use --truncate-results for that.
    try:
        spark.sql(f"TRUNCATE TABLE {TABLE}")
        print(f"TRUNCATED {TABLE} (whole table) -- only this run's rows will remain")
    except Exception as _e:  # table doesn't exist yet -> the first append creates it
        print(f"truncate-all skipped ({TABLE} absent): {_e}")
elif TRUNCATE:
    # Scope the clear to THIS run_id + the tier(s) this invocation writes, so the paired
    # tier of the same benchmark run is NOT wiped: a --heavyweight-only run clears only
    # its heavyweight rows for run_id and leaves the lightweight rows a separate
    # --lightweight-only run wrote (and vice versa); --modes both clears both. Other
    # run_ids are untouched. (Whole-table TRUNCATE would clobber the paired tier.)
    _apis = [a for a, on in (("lightweight", LIGHTWEIGHT), ("heavyweight", HEAVYWEIGHT)) if on]
    _in = ", ".join(f"'{a}'" for a in _apis)
    try:
        spark.sql(f"DELETE FROM {TABLE} WHERE run_id = '{RUN_ID}' AND api IN ({_in})")
        print(f"cleared prior rows: run_id={RUN_ID} api in [{_in}] (other runs/tiers kept)")
    except Exception as _e:  # table doesn't exist yet -> the first append creates it
        print(f"truncate skipped ({TABLE} absent/empty): {_e}")

if RESUME:
    print(
        f"[resume] run_id={RUN_ID}: keeping existing rows; complete (tier x mode) "
        f"sections will be loaded + skipped. NOTE: only valid on the SAME cluster "
        f"config -- spark-path timings depend on cluster size (pure-core does not)."
    )

_delta = [0]

# Monotonic per-run event counter. Continue from the run's current max so --resume keeps
# numbering where it left off (loaded rows keep their numbers; new events get higher ones).
import dataclasses as _dc


def _max_event():
    try:
        _v = spark.sql(
            f"SELECT max(run_event_num) FROM {TABLE} WHERE run_id = '{RUN_ID}'"
        ).collect()[0][0]
        return int(_v) if _v is not None else 0
    except Exception:  # column/table absent on a fresh run
        return 0


_event = [_max_event()]


def _sink(batch):
    # Stamp each row's run_event_num in execution order (the sink is the single chokepoint
    # all rows flow through, in completion order) just before the Delta append.
    _numbered = []
    for _r in batch:
        _event[0] += 1
        _numbered.append(_dc.replace(_r, run_event_num=_event[0]))
    _delta[0] += _cl.to_delta(_numbered, spark, TABLE, where="cluster")


def _done_fns(api, mode):
    # Function-granular resume: the set of fn names already in the table for this
    # (run_id, api, mode). --resume LOADS these and runs only the fns NOT in this set, so
    # a re-run completes exactly what's missing -- the unfinished spark-path fns, or a
    # single fn whose row you DELETEd to force its re-run (e.g. the fixed rst_fillnodata).
    if not RESUME:
        return set()
    _rows = spark.sql(
        f"SELECT DISTINCT fn FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}'"
    ).collect()
    return {_r[0] for _r in _rows}


def _load_section(api, mode):
    # Reconstruct ResultRows already in the table for this (api, mode), so the final
    # comparison includes the kept (skipped) fns without re-running them. Columns are the
    # ResultRow field names (the table was written from ResultRow), so map by field name;
    # missing column -> dataclass default.
    import dataclasses

    _fields = {f.name for f in dataclasses.fields(results.ResultRow)}
    _rows = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}'"
    ).collect()
    _out = []
    for _r in _rows:
        _d = _r.asDict()
        _out.append(results.ResultRow(**{_k: _d[_k] for _k in _fields if _k in _d}))
    return _out


def _purge_errors(api, mode):
    # FIX_ERRORS default: on resume, DELETE prior error rows for this (api, mode) so those
    # fns count as MISSING and get re-run (and don't leave a stale error row beside the new
    # ok row). --no-fix-errors skips this -> errored fns stay 'done' and are not retried.
    if not (RESUME and FIX_ERRORS):
        return
    _n = spark.sql(
        f"SELECT count(*) FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}' AND status='error'"
    ).collect()[0][0]
    if int(_n) > 0:
        spark.sql(
            f"DELETE FROM {TABLE} WHERE run_id='{RUN_ID}' "
            f"AND api='{api}' AND mode='{mode}' AND status='error'"
        )
        print(f"[resume] {api} {mode}: purged {int(_n)} error row(s) -> will re-run them")


def _purge_functions(api, mode):
    # --redo-functions: DELETE the named fns' rows for this (api, mode) -- any status -- so
    # they count as MISSING and re-run, while every other fn's rows stay. Independent of the
    # run scope, so a resume run also force-redoes this subset. Targeted, run_id-scoped.
    _fns = [f for f in REDO_FUNCTIONS.split(",") if f]
    if not _fns:
        return
    _in = ",".join("'" + f + "'" for f in _fns)
    _n = spark.sql(
        f"SELECT count(*) FROM {TABLE} WHERE run_id='{RUN_ID}' "
        f"AND api='{api}' AND mode='{mode}' AND fn IN ({_in})"
    ).collect()[0][0]
    if int(_n) > 0:
        spark.sql(
            f"DELETE FROM {TABLE} WHERE run_id='{RUN_ID}' "
            f"AND api='{api}' AND mode='{mode}' AND fn IN ({_in})"
        )
        print(f"[redo] {api} {mode}: purged {int(_n)} row(s) for {len(_fns)} fn(s) -> re-running")


def _show_md(title, text, path=None):
    # Render a generated summary inline in the run notebook (visible in the Databricks
    # run UI) as RENDERED markdown -- headings + GFM pipe-tables become real HTML.
    # Databricks' job-run UI does NOT render IPython.display.Markdown (that path only
    # renders in Jupyter), so convert markdown -> HTML and use displayHTML, which IS the
    # Databricks primitive for inline rich output. Fall back to IPython, then print.
    # path: when given, show the Volume location of the .md FILE as a line above the
    # rendered body so the run links to the artifact (e.g. .../bench-out/<run_id>/summary.md).
    _loc = ("\\n\\n**Summary file:** `" + path + "`") if path else ""
    _full = "### " + title + _loc + "\\n\\n" + text
    try:
        import markdown as _mdlib

        _html = _mdlib.markdown(_full, extensions=["tables", "fenced_code", "sane_lists"])
        displayHTML(_html)  # noqa: F821  (Databricks notebook global)
        return
    except Exception:
        pass
    try:
        from IPython.display import Markdown
        from IPython.display import display as _md_display

        _md_display(Markdown(_full))
        return
    except Exception:
        pass
    print("\\n===== " + title + " =====")
    if path:
        print("Summary file: " + path)
    print(text)


def show_section(api, mode, rows):
    # One (tier x mode) section: show its raw bench_results rows as an interactive table,
    # then render + persist a summary for just those rows. Each section lives in its OWN
    # notebook cell, so this output renders the moment the cell finishes -- the run UI is
    # a live, preserved view of progress (no need to download md from the Volume).
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND api = '{api}' AND mode = '{mode}'"
    )
    try:
        display(_df)
    except Exception:
        _df.show(300, truncate=False)
    _md = results.summarize(rows, pool_size=POOL_SIZE)
    _path = f"{OUT}/{api}.{mode}.summary.md"
    with open(_path, "w") as fh:
        fh.write(_md)
    _show_md(f"{api} {mode} summary -- {RUN_ID}", _md, path=_path)
"""

# Lightweight helper: pure Python/PySpark. Emitted only when --lightweight, so a
# heavyweight-only run never references run_pure_core / run_spark_path.
_LIGHT_HELPERS = """
def run_light(mode):
    # Run one lightweight mode over the selected fns, streaming each fn's rows to the Delta
    # sink as it finishes. Function-granular --resume: load the fns already in the table and
    # run ONLY the missing ones, so a re-run completes just what's left. Returns this
    # section's rows (loaded + newly run) for the section cell to summarize.
    # --explain-only is a spark-path-only diagnostic: print/persist plans, produce no rows.
    if EXPLAIN_ONLY:
        if mode != "spark-path":
            return []
        runner.run_spark_path(
            spark, CORPUS, corpus, fnspecs, RUN_ID, ROW_COUNTS, SPARK_WARMUP,
            SPARK_MEASURED, "cluster", sink=None, partition_size=PARTITION_SIZE,
            explain_only=True, explain_dir=EXPLAIN_DIR, input_tile=INPUT_TILE,
        )
        return []
    _purge_errors("lightweight", mode)
    _purge_functions("lightweight", mode)
    _done = _done_fns("lightweight", mode)
    _loaded = _load_section("lightweight", mode) if _done else []
    if _loaded:
        lw.extend(_loaded)
    # Scope the to-do to fns that actually HAVE this mode -- e.g. the *_agg aggregators are
    # spark-path-only (no pure-core form), so without this filter a pure-core resume would
    # forever report them as "missing" and call run_pure_core on them for zero rows.
    _todo = [f for f in fnspecs if f.name not in _done and mode in f.modes]
    if _loaded or _done:
        print(f"[resume] lightweight {mode}: loaded {len(_loaded)} existing fn(s), "
              f"running {len(_todo)} missing")
    _new = []
    if _todo:
        if mode == "pure-core":
            _new = runner.run_pure_core(
                CORPUS, corpus, _todo, RUN_ID, WARMUP, MEASURED, "cluster", sink=_sink
            )
        else:
            _new = runner.run_spark_path(
                spark, CORPUS, corpus, _todo, RUN_ID, ROW_COUNTS, SPARK_WARMUP, SPARK_MEASURED, "cluster", sink=_sink, partition_size=PARTITION_SIZE, input_tile=INPUT_TILE
            )
        lw.extend(_new)
    return _loaded + _new
"""

# Heavyweight helper: drives the Scala HeavyBenchMain. Emitted only when --heavyweight.
_HEAVY_HELPERS = """
def run_heavy(mode):
    # Run one heavy mode in the JVM. The JVM writes its shard to a LOCAL path one row at a
    # time (it can't write the /Volumes object-storage mount), so run HeavyBenchMain in a
    # thread and TAIL the shard, streaming each newly-flushed row to the SAME Delta sink as
    # the lightweight tier. Pure-core opens tiles via GDAL on the driver, which can't read
    # /Volumes -> stage a LOCAL corpus copy (dbutils is UC-aware) for pure-core; spark-path
    # reads the Volume directly (binaryFile, UC-aware) so it keeps CORPUS.
    # Function-granular --resume: load fns already in the table; run ONLY the missing ones
    # by passing a filtered FUNCTIONS list to the JVM (e.g. a single fn to re-run).
    _purge_errors("heavyweight", mode)
    _purge_functions("heavyweight", mode)
    _done = _done_fns("heavyweight", mode)
    _loaded = _load_section("heavyweight", mode) if _done else []
    if _loaded:
        hw.extend(_loaded)
    # Scope to fns that HAVE this mode (the *_agg aggregators are spark-path-only); otherwise
    # a pure-core resume perpetually reports the 7 aggregators as "missing" + dispatches them
    # to the JVM pure-core path for zero rows.
    _mode_names = {f.name for f in fnspecs if mode in f.modes}
    _todo_fns = [
        f for f in FUNCTIONS.split(",") if f and f not in _done and f in _mode_names
    ]
    if _loaded or _done:
        print(f"[resume] heavyweight {mode}: loaded {len(_loaded)} existing fn(s), "
              f"running {len(_todo_fns)} missing")
    if not _todo_fns:
        return _loaded
    _fns_csv = ",".join(_todo_fns)
    import threading
    import time as _time

    _out = f"/local_disk0/heavyweight.{mode}.jsonl"
    if os.path.exists(_out):
        os.remove(_out)
    _err = {}
    _root = CORPUS
    if mode == "pure-core":
        _root = "/local_disk0/bench-corpus-pc"
        if not os.path.exists(_root):
            dbutils.fs.cp(CORPUS, "file:" + _root, recurse=True)

    # Spark-path uses the (smaller) spark iteration counts; pure-core keeps WARMUP/MEASURED.
    _wu, _ms = (SPARK_WARMUP, SPARK_MEASURED) if mode == "spark-path" else (WARMUP, MEASURED)

    def _go():
        try:
            spark._jvm.com.databricks.labs.gbx.bench.HeavyBenchMain.run(
                spark._jsparkSession, CORPUS, _root, _out, _fns_csv, mode,
                ",".join(str(x) for x in ROW_COUNTS), _wu, _ms, RUN_ID)
        except Exception as _e:  # re-raised after the join
            _err["e"] = _e

    def _complete_lines(path):
        # Each append is text + "\\n" + flush + fsync, so split("\\n")[:-1] drops a
        # trailing partial line until it's complete.
        if not os.path.exists(path):
            return []
        with open(path) as _fh:
            return _fh.read().split("\\n")[:-1]

    # ms->s conversion of the heavy Scala jsonl rows uses the shared, unit-tested
    # _cl._remap_heavy_iter_to_seconds (imported in the preamble) so the cluster path and
    # the host tests exercise the SAME logic.
    _remap_iter = _cl._remap_heavy_iter_to_seconds

    _th = threading.Thread(target=_go, daemon=True)
    _th.start()
    _seen = 0
    _new = []
    while True:
        _alive = _th.is_alive()
        _lines = _complete_lines(_out)
        if len(_lines) > _seen:
            _batch = [results.ResultRow(**_remap_iter(json.loads(_l))) for _l in _lines[_seen:] if _l.strip()]
            _sink(_batch)          # interim Delta append -> pollable live
            hw.extend(_batch)
            _new.extend(_batch)
            _seen = len(_lines)
        if not _alive:             # thread done -> the read above drained the tail
            break
        _time.sleep(2)
    _th.join()
    if _err.get("e"):
        raise _err["e"]
    return _loaded + _new
"""

# One cell per (tier x mode) section. Each is emitted only when its tier+mode is selected,
# and renders its table + summary as soon as the cell completes.
_CELL_LIGHT_PURE = """# (a) Lightweight pure-core
show_section("lightweight", "pure-core", run_light("pure-core"))
"""
_CELL_HEAVY_PURE = """# (b) Heavyweight pure-core
show_section("heavyweight", "pure-core", run_heavy("pure-core"))
"""
_CELL_LIGHT_SPARK = """# (c) Lightweight spark-path
show_section("lightweight", "spark-path", run_light("spark-path"))
"""
_CELL_HEAVY_SPARK = """# (d) Heavyweight spark-path
show_section("heavyweight", "spark-path", run_heavy("spark-path"))
"""

_CELL_READERS = """# Reader benchmark: light raster_gbx vs heavy gdal (both on-cluster)
from databricks.labs.gbx.bench import readers as _rd
_rows_dir = f"{CORPUS}/rows"
_reader_rows = []
if LIGHTWEIGHT:
    _r = _rd.run_format_read(spark, _rows_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="lightweight", fmt="raster_gbx",
                             options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_r]); lw.append(_r); _reader_rows.append(_r)
if HEAVYWEIGHT:
    _r = _rd.run_format_read(spark, _rows_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="heavyweight", fmt="gdal", where="cluster")
    _sink([_r]); hw.append(_r); _reader_rows.append(_r)
# Writer benchmark: light gtiff_gbx vs heavy gtiff_gdal (same raster_gbx-read input)
import shutil as _sh
_wsrc = f"{CORPUS}/rows"
if LIGHTWEIGHT:
    _wl = "/local_disk0/bench_writer_light"
    _sh.rmtree(_wl, ignore_errors=True)
    _wr = _rd.run_format_write(spark, _wsrc, _wl, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="raster_gbx", write_fmt="gtiff_gbx",
                               options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_wr]); lw.append(_wr); _reader_rows.append(_wr)
if HEAVYWEIGHT:
    _wh = "/local_disk0/bench_writer_heavy"
    _sh.rmtree(_wh, ignore_errors=True)
    _wr = _rd.run_format_write(spark, _wsrc, _wh, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="heavyweight", read_fmt="raster_gbx", write_fmt="gtiff_gdal",
                               mode="append",  # heavy gdal writer is append-only (overwrite -> truncate error)
                               options={"filterRegex": r".*\\.tif$"}, where="cluster")
    _sink([_wr]); hw.append(_wr); _reader_rows.append(_wr)
if _reader_rows:
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category IN ('reader', 'writer')"
    )
    try:
        display(_df)
    except Exception:
        _df.show(100, truncate=False)
    _md = results.summarize(_reader_rows)
    _show_md(f"reader benchmark -- {RUN_ID}", _md)
"""

_CELL_NETCDF = """# NetCDF RASTER reader benchmark: heavy netcdf_gdal vs light netcdf_gbx (regular grids, on-cluster)
# Honest heavy-vs-light throughput over real NASA-NEX GDDP-CMIP6 granules staged at {CORPUS}/netcdf
# (download-and-stop via NasaNexDownloader -- staged separately, decoupled from read()). These are
# regular 0.25-degree lat/lon global grids, so BOTH tiers can enumerate the SAME .nc pool with
# filterRegex .*\\.nc$ (light in raster mode) -- a fair comparison. size_mib=-1 makes each grid var
# ONE tile in both tiers (matching one-tile-per-var granularity). This is a THROUGHPUT bench, NOT a
# bit-parity gate (cross-tier parity uses a small gridded fixture -- see docs/api/benchmarking.mdx).
from databricks.labs.gbx.bench import readers as _rd
import os as _os
import glob as _glob
_netcdf_dir = f"{CORPUS}/netcdf"
# GUARD: the NASA-NEX grid corpus is staged separately (download-and-stop at stage time).
# When the pool is empty/missing, SKIP CLEANLY with a clear reason rather than failing the run.
_nc_files = _rd.list_corpus_files(_netcdf_dir, r".*\\.nc$") if _os.path.isdir(_netcdf_dir) else []
if not _nc_files:
    print(
        f"NETCDF BENCH SKIPPED: no .nc granules under {_netcdf_dir}. "
        f"Stage the raster corpus first via readers.stage_nasanex_corpus(spark, '{{CORPUS}}/netcdf', ...) "
        f"(real NASA-NEX GDDP-CMIP6 regular grids from Planetary Computer, anonymous access), then "
        f"re-run with --benchmark-netcdf / --netcdf-only.",
        flush=True,
    )
else:
    print(f"NETCDF RASTER BENCH: {len(_nc_files)} .nc grid granule(s) under {_netcdf_dir}", flush=True)
    _netcdf_rows = []
    if LIGHTWEIGHT:
        _r = _rd.run_format_read(spark, _netcdf_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                 api="lightweight", fmt="netcdf_gbx",
                                 options={"filterRegex": r".*\\.nc$"}, size_mib=-1, where="cluster")
        _sink([_r]); lw.append(_r); _netcdf_rows.append(_r)
    if HEAVYWEIGHT:
        _r = _rd.run_format_read(spark, _netcdf_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                 api="heavyweight", fmt="netcdf_gdal",
                                 options={"filterRegex": r".*\\.nc$"}, size_mib=-1, where="cluster")
        _sink([_r]); hw.append(_r); _netcdf_rows.append(_r)
    if _netcdf_rows:
        _df = spark.sql(
            f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'reader' AND fn LIKE 'read_netcdf%'"
        )
        try:
            display(_df)
        except Exception:
            _df.show(100, truncate=False)
        _md = results.summarize(_netcdf_rows)
        _show_md(f"netcdf RASTER reader benchmark -- {RUN_ID}", _md)
"""

_CELL_NETCDF_SWATH = """# NetCDF VECTOR (swath) reader benchmark: light netcdf_gbx mode=vector ONLY (S5P swaths, on-cluster)
# Real Sentinel-5P L2 CH4 SWATH granules staged at {CORPUS}/netcdf-swath (download-and-stop via
# TropomiDownloader -- staged separately, decoupled from read()). S5P L2 is an irregular per-pixel
# lat/lon SWATH, so it is read in the light netcdf_gbx VECTOR mode (one point row per pixel). There
# is NO heavy swath path -- netcdf_gdal has no swath reader -- so this leg is a LIGHT-ONLY throughput
# measurement, NOT a heavy-vs-light comparison. (The heavy-vs-light comparison lives in the RASTER
# leg above, over regular-grid NASA-NEX granules.)
from databricks.labs.gbx.bench import readers as _rd
import os as _os
_swath_dir = f"{CORPUS}/netcdf-swath"
# GUARD: the S5P swath corpus is staged separately (download-and-stop at stage time).
# When the pool is empty/missing, SKIP CLEANLY with a clear reason rather than failing the run.
_swath_files = _rd.list_corpus_files(_swath_dir, r".*\\.nc$") if _os.path.isdir(_swath_dir) else []
if not _swath_files:
    print(
        f"NETCDF SWATH BENCH SKIPPED: no .nc granules under {_swath_dir}. "
        f"Stage the swath corpus first via readers.stage_netcdf_corpus(spark, '{{CORPUS}}/netcdf-swath', ...) "
        f"(real S5P L2 CH4 swaths from Planetary Computer, anonymous access), then re-run "
        f"with --benchmark-netcdf / --netcdf-only.",
        flush=True,
    )
elif not LIGHTWEIGHT:
    # Light-only leg: heavy has no swath path, so nothing to run when --heavyweight-only.
    print(
        "NETCDF SWATH BENCH SKIPPED: swath (vector) mode is light-only (heavy has no swath path); "
        "run with --lightweight to include it.",
        flush=True,
    )
else:
    print(f"NETCDF SWATH BENCH (light-only): {len(_swath_files)} .nc swath granule(s) under {_swath_dir}", flush=True)
    _swath_rows = []
    # S5P L2 stores its fields under the /PRODUCT HDF5 group; without group+variables the reader
    # opens the (empty) root group and yields zero rows. Match TropomiDownloader.read's options.
    _r = _rd.run_format_read(spark, _swath_dir, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="lightweight", fmt="netcdf_gbx",
                             options={"mode": "vector", "group": "/PRODUCT",
                                      "variables": "methane_mixing_ratio_bias_corrected,qa_value",
                                      "filterRegex": r".*\\.nc$"}, where="cluster")
    _sink([_r]); lw.append(_r); _swath_rows.append(_r)
    if _swath_rows:
        _df = spark.sql(
            f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'reader' AND fn LIKE 'read_netcdf%'"
        )
        try:
            display(_df)
        except Exception:
            _df.show(100, truncate=False)
        _md = results.summarize(_swath_rows)
        _show_md(f"netcdf VECTOR (swath, light-only) reader benchmark -- {RUN_ID}", _md)
"""

_CELL_NETCDF_WRITER = """# NetCDF WRITER benchmark: light netcdf_gbx write throughput (raster + vector legs, on-cluster)
# LIGHT-ONLY: there is NO heavy NetCDF writer (netcdf_gdal is read-only here), so this is a
# throughput measurement of the light netcdf_gbx writer, NOT a heavy-vs-light comparison -- the
# same shape as the S5P vector reader leg. Each mode runs THREE write shapes:
#   parts  (default): one .nc per row (raster) / per partition (vector)
#   single (singleFile=true): two-phase scratch+driver-merge into ONE .nc
#   merge  (merge=true keepParts=true): POST-HOC fold of the .nc parts ALREADY on disk
# Corpora:
#   RASTER leg  -> read {CORPUS}/netcdf-distinct (DISTINCT-variable SINGLE-grid corpus)
#                  -> write {CORPUS}/netcdf-out{,-single,-merge}
#   VECTOR leg  -> read {CORPUS}/netcdf-swath    (S5P swaths)
#                  -> write {CORPUS}/netcdf-swath-out{,-single,-merge}
# The RASTER corpus is distinct-variable single-grid on purpose: single+merge REQUIRE
# distinct data vars sharing one grid (they correctly ERROR on duplicate variable names),
# so the 33-same-variable NASA-NEX raster reader corpus would make single+merge fail by
# design. All three raster legs read the SAME distinct-var input for a fair comparison.
# run_format_write reads the input once via read_fmt, caches it, then times the write.format(
# write_fmt).save(...) call. It applies the SAME `options` dict to BOTH the reader and the writer,
# so mode=vector on the vector leg dispatches the vector reader (point rows) AND the vector writer.
# For the merge legs it also does a ONE-SHOT untimed parts write to seed the dir (see readers.py).
from databricks.labs.gbx.bench import readers as _rd
import os as _os
_ncw_rows = []
# --- RASTER writer leg (distinct-variable single-grid corpus: parts/single/merge comparison) ---
_ncw_in = f"{CORPUS}/netcdf-distinct"
_ncw_files = _rd.list_corpus_files(_ncw_in, r".*\\.nc$") if _os.path.isdir(_ncw_in) else []
if not LIGHTWEIGHT:
    # Light-only leg: there is no heavy netcdf writer, so nothing to run when --heavyweight-only.
    print(
        "NETCDF WRITER RASTER BENCH SKIPPED: the netcdf writer is light-only "
        "(no heavy netcdf writer); run with --lightweight to include it.",
        flush=True,
    )
elif not _ncw_files:
    print(
        f"NETCDF WRITER RASTER BENCH SKIPPED: no .nc granules under {_ncw_in}. "
        f"Stage the raster corpus first (same pool as the netcdf RASTER reader leg), then "
        f"re-run with --benchmark-netcdf-writer / --netcdf-writer-only.",
        flush=True,
    )
else:
    print(f"NETCDF WRITER RASTER BENCH (light-only): {len(_ncw_files)} .nc grid granule(s) under {_ncw_in}", flush=True)
    _wr = _rd.run_format_write(spark, _ncw_in, f"{CORPUS}/netcdf-out", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"filterRegex": r".*\\.nc$"}, where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
    # singleFile variant: same corpus, distinct out-dir + label so the parts vs single
    # rows are a real comparison in the store (label -> note "... [singleFile]").
    _wr = _rd.run_format_write(spark, _ncw_in, f"{CORPUS}/netcdf-out-single", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"filterRegex": r".*\\.nc$", "singleFile": "true"},
                               label="singleFile", where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
    # merge variant: POST-HOC fold of the parts already on disk (distinct out-dir + label).
    # keepParts=true is REQUIRED so parts survive across warmup+measured iters (run_format_write
    # seeds the dir with a one-shot untimed parts write before timing the merge).
    _wr = _rd.run_format_write(spark, _ncw_in, f"{CORPUS}/netcdf-out-merge", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"filterRegex": r".*\\.nc$", "merge": "true", "keepParts": "true"},
                               label="merge", where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
# --- VECTOR (swath) writer leg: mode=vector on read AND write ---
_ncw_sw_in = f"{CORPUS}/netcdf-swath"
_ncw_sw_files = _rd.list_corpus_files(_ncw_sw_in, r".*\\.nc$") if _os.path.isdir(_ncw_sw_in) else []
if not LIGHTWEIGHT:
    print(
        "NETCDF WRITER SWATH BENCH SKIPPED: the netcdf writer is light-only "
        "(no heavy netcdf writer); run with --lightweight to include it.",
        flush=True,
    )
elif not _ncw_sw_files:
    print(
        f"NETCDF WRITER SWATH BENCH SKIPPED: no .nc granules under {_ncw_sw_in}. "
        f"Stage the swath corpus first (same pool as the netcdf VECTOR reader leg), then "
        f"re-run with --benchmark-netcdf-writer / --netcdf-writer-only.",
        flush=True,
    )
else:
    print(f"NETCDF WRITER SWATH BENCH (light-only): {len(_ncw_sw_files)} .nc swath granule(s) under {_ncw_sw_in}", flush=True)
    # S5P fields live under the /PRODUCT group; pass group+variables so the read yields point rows
    # (the single options dict flows to BOTH the read and the vector write). Match TropomiDownloader.
    _wr = _rd.run_format_write(spark, _ncw_sw_in, f"{CORPUS}/netcdf-swath-out", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"mode": "vector", "group": "/PRODUCT",
                                                          "variables": "methane_mixing_ratio_bias_corrected,qa_value",
                                                          "filterRegex": r".*\\.nc$"}, where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
    # singleFile variant: distinct out-dir + label so parts vs single don't collide/clobber.
    _wr = _rd.run_format_write(spark, _ncw_sw_in, f"{CORPUS}/netcdf-swath-out-single", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"mode": "vector", "group": "/PRODUCT",
                                                          "variables": "methane_mixing_ratio_bias_corrected,qa_value",
                                                          "filterRegex": r".*\\.nc$", "singleFile": "true"},
                               label="singleFile", where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
    # merge variant: POST-HOC fold of the CF-DSG .nc parts already on disk (distinct out-dir).
    # keepParts=true keeps parts across warmup+measured iters; run_format_write seeds the dir first.
    _wr = _rd.run_format_write(spark, _ncw_sw_in, f"{CORPUS}/netcdf-swath-out-merge", RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               write_api="lightweight", read_fmt="netcdf_gbx", write_fmt="netcdf_gbx",
                               mode="overwrite", options={"mode": "vector", "group": "/PRODUCT",
                                                          "variables": "methane_mixing_ratio_bias_corrected,qa_value",
                                                          "filterRegex": r".*\\.nc$", "merge": "true", "keepParts": "true"},
                               label="merge", where="cluster")
    _sink([_wr]); lw.append(_wr); _ncw_rows.append(_wr)
if _ncw_rows:
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'writer' AND api = 'lightweight'"
    )
    try:
        display(_df)
    except Exception:
        _df.show(100, truncate=False)
    _md = results.summarize(_ncw_rows)
    _show_md(f"netcdf WRITER (light-only) throughput benchmark -- {RUN_ID}", _md)
"""

_CELL_PMTILES = """# PMTiles benchmark: light pmtiles_gbx vs heavy pmtiles (both on-cluster) + parity check
from databricks.labs.gbx.bench import readers as _rd
from pmtiles.reader import MemorySource, Reader as _PMReader
import pmtiles.reader as _pmr
import glob as _glob
import gzip as _gz
import os as _os
import shutil as _sh
# Both pmtiles writers are two-phase (executor-write -> driver-merge), so their
# intermediates MUST be on a filesystem shared across driver+executors. Node-local
# /local_disk0 fails the driver merge on a multi-node cluster. Use DBFS: light
# (pure-Python) via the /dbfs FUSE mount (its single-archive finalize is now
# rename-free, so sequential FUSE writes are safe); heavy (JVM) via the dbfs:
# scheme (it cannot use UC /Volumes by direct API). Parity decodes both via FUSE.
_DBFS_FUSE = "/dbfs/tmp/gbx_bench"
_os.makedirs(_DBFS_FUSE, exist_ok=True)
_pmtiles_rows = []
_pl = _ph = None
if LIGHTWEIGHT:
    _pl = _DBFS_FUSE + "/pmtiles_light.pmtiles"
    if _os.path.isdir(_pl):
        _sh.rmtree(_pl, ignore_errors=True)
    elif _os.path.exists(_pl):
        _os.remove(_pl)
    _r = _rd.run_pmtiles_write(spark, _pl, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               n_tiles=1000, shard_zoom=0, write_fmt="pmtiles_gbx",
                               where="cluster")
    _sink([_r]); lw.append(_r); _pmtiles_rows.append(_r)
if HEAVYWEIGHT:
    _ph = _DBFS_FUSE + "/pmtiles_heavy"            # FUSE path (for parity decode)
    _sh.rmtree(_ph, ignore_errors=True)
    _ph_save = "dbfs:/tmp/gbx_bench/pmtiles_heavy"  # dbfs: scheme (for the JVM writer)
    _r = _rd.run_pmtiles_write(spark, _ph_save, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                               n_tiles=1000, shard_zoom=0, write_fmt="pmtiles",
                               where="cluster")
    _sink([_r]); hw.append(_r); _pmtiles_rows.append(_r)
# Parity check: decode all tiles from both archives and compare (z,x,y) keys+bytes.
if LIGHTWEIGHT and HEAVYWEIGHT and _pl and _ph:
    # The Python pmtiles Reader gzip-decompresses directories unconditionally, but
    # PMTiles directories may be uncompressed (the heavy Scala writer uses
    # internal_compression=NONE). Patch deserialize_directory to fall back to the
    # raw bytes (re-gzip so the lib's own parser still runs) -> reads both tiers.
    _orig_dd = _pmr.deserialize_directory
    def _dd_compat(_buf):
        try:
            return _orig_dd(_buf)
        except Exception:
            return _orig_dd(_gz.compress(bytes(_buf)))
    _pmr.deserialize_directory = _dd_compat

    def _decode_any(path):
        # Return {(z,x,y): bytes} for every tile in path (file or dir of *.pmtiles).
        # Read the whole archive into memory (sequential) and use MemorySource:
        # the DBFS/Volumes paths are cloud object storage and do not support the
        # mmap/seek that MmapSource needs.
        import os as _os2
        if _os2.path.isfile(path):
            files = [path]
        else:
            files = sorted(_glob.glob(_os2.path.join(path, "**", "*.pmtiles"), recursive=True))
        tiles = {}
        for _pf in files:
            with open(_pf, "rb") as _fh:
                _data = _fh.read()
            _rdr = _PMReader(MemorySource(_data))
            # min/max zoom live in the PMTiles header (not metadata); the Reader
            # tile accessor is .get(z, x, y) (returns None when absent).
            _hdr = _rdr.header()
            _zmin = int(_hdr["min_zoom"])
            _zmax = int(_hdr["max_zoom"])
            for _z in range(_zmin, _zmax + 1):
                _side = 2 ** _z
                for _x in range(_side):
                    for _y in range(_side):
                        _b = _rdr.get(_z, _x, _y)
                        if _b is not None:
                            tiles[(_z, _x, _y)] = bytes(_b)
        return tiles
    _verdict_path = "/dbfs/tmp/gbx_bench/parity_verdict.txt"
    try:
        _lt = _decode_any(_pl)
        _ht = _decode_any(_ph)
        _lk = set(_lt.keys())
        _hk = set(_ht.keys())
        if _lk == _hk and all(_lt[_k] == _ht[_k] for _k in _lk):
            _verdict = f"PMTILES PARITY: PASS ({len(_lk)} tiles, keys+bytes equal)"
        elif _lk == _hk:
            _bad = [_k for _k in _lk if _lt[_k] != _ht[_k]]
            _verdict = (f"PMTILES PARITY: FAIL bytes differ on {len(_bad)} tile(s) "
                        f"e.g. {sorted(_bad)[:5]} ({len(_lk)} tiles)")
        else:
            _verdict = (f"PMTILES PARITY: FAIL keyset differs -- light-only "
                        f"{sorted(_lk - _hk)[:5]}, heavy-only {sorted(_hk - _lk)[:5]} "
                        f"(light={len(_lk)}, heavy={len(_hk)})")
    except Exception as _pe:
        _verdict = f"PMTILES PARITY: ERROR -- {type(_pe).__name__}: {_pe}"
    print(_verdict)
    try:
        with open(_verdict_path, "w") as _vf:
            _vf.write(_verdict)
    except Exception as _we:
        print(f"(could not write verdict file: {_we})")
    # Hard gate: a parity mismatch or decode error fails the bench run.
    assert _verdict.startswith("PMTILES PARITY: PASS"), _verdict
if _pmtiles_rows:
    _df = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'writer' "
        f"AND fn IN ('pmtiles_gbx', 'pmtiles')"
    )
    try:
        display(_df)
    except Exception:
        _df.show(100, truncate=False)
    _md = results.summarize(_pmtiles_rows)
    _show_md(f"pmtiles benchmark -- {RUN_ID}", _md)
"""

_CELL_MVT = """# MVT benchmark: light pyvx st_asmvt vs heavy vectorx st_asmvt (+ decoded-feature parity)
from databricks.labs.gbx.bench import readers as _rd
import mapbox_vector_tile as _mvt_lib
_mvt_rows = []
_mvt_light_blobs = None  # {(z,x,y): bytes} decoded from light run
_mvt_heavy_blobs = None  # {(z,x,y): bytes} decoded from heavy run
_N_FEATURES = 500
_N_TILES = 10
if LIGHTWEIGHT:
    _r = _rd.run_mvt_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                         api="lightweight", n_features=_N_FEATURES, n_tiles=_N_TILES,
                         where="cluster")
    _sink([_r]); lw.append(_r); _mvt_rows.append(_r)
    # Capture decoded blobs for parity: re-run the agg once (untimed) and collect.
    if _r.status == "ok":
        try:
            import pyspark.sql.functions as _F_mvt
            from shapely.geometry import box as _b
            from shapely import to_wkb as _wkb
            from pyspark.sql.types import (StructType, StructField, IntegerType,
                                           BinaryType, DoubleType, StringType)
            _z3 = 3
            _addrs = [(_z3, i % 8, (i // 8) % 8) for i in range(_N_TILES)]
            _rd2 = []
            for _i in range(_N_FEATURES):
                _tz, _tx, _ty = _addrs[_i % _N_TILES]
                # Mirror run_mvt_agg's coordinate spread: squares on a 16x16 grid over
                # the full [0,4096] extent so they survive the heavy MVT driver's
                # quantization (a packed band collapses to sub-pixel -> empty heavy tile).
                _slot = (_i // _N_TILES) % 256
                _cx = 128 + (_slot % 16) * 256; _cy = 128 + (_slot // 16) * 256
                _g = bytes(_wkb(_b(_cx-32, _cy-32, _cx+32, _cy+32)))
                _rd2.append((_tz, _tx, _ty, _g, _i, float(_i)*0.1, f"feat_{_i}"))
            _sch = StructType([
                StructField("z", IntegerType(), False), StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False), StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True), StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ])
            _df2 = spark.createDataFrame(_rd2, schema=_sch).select(
                "z","x","y","geom",
                _F_mvt.struct(_F_mvt.col("id"), _F_mvt.col("score"), _F_mvt.col("label")).alias("attrs"),
            ).cache(); _df2.count()
            from databricks.labs.gbx.pyvx import functions as _vx
            _vx.register(spark)
            _lrows = (_df2.groupBy("z","x","y")
                .agg(_vx.st_asmvt(_F_mvt.col("geom"), _F_mvt.col("attrs"), _F_mvt.lit("layer")).alias("mvt"))
                .collect())
            _mvt_light_blobs = {(r["z"], r["x"], r["y"]): bytes(r["mvt"]) for r in _lrows if r["mvt"]}
        except Exception as _pe:
            print(f"MVT light parity capture error: {type(_pe).__name__}: {_pe}")
if HEAVYWEIGHT:
    _r = _rd.run_mvt_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                         api="heavyweight", n_features=_N_FEATURES, n_tiles=_N_TILES,
                         where="cluster")
    _sink([_r]); hw.append(_r); _mvt_rows.append(_r)
    # Capture decoded blobs for parity.
    if _r.status == "ok":
        try:
            import pyspark.sql.functions as _F_mvth
            from shapely.geometry import box as _bh
            from shapely import to_wkb as _wkbh
            from pyspark.sql.types import (StructType, StructField, IntegerType,
                                           BinaryType, DoubleType, StringType)
            _z3h = 3
            _addrsh = [(_z3h, i % 8, (i // 8) % 8) for i in range(_N_TILES)]
            _rdh = []
            for _i in range(_N_FEATURES):
                _tz, _tx, _ty = _addrsh[_i % _N_TILES]
                # Mirror run_mvt_agg's coordinate spread: squares on a 16x16 grid over
                # the full [0,4096] extent so they survive the heavy MVT driver's
                # quantization (a packed band collapses to sub-pixel -> empty heavy tile).
                _slot = (_i // _N_TILES) % 256
                _cx = 128 + (_slot % 16) * 256; _cy = 128 + (_slot // 16) * 256
                _g = bytes(_wkbh(_bh(_cx-32, _cy-32, _cx+32, _cy+32)))
                _rdh.append((_tz, _tx, _ty, _g, _i, float(_i)*0.1, f"feat_{_i}"))
            _schh = StructType([
                StructField("z", IntegerType(), False), StructField("x", IntegerType(), False),
                StructField("y", IntegerType(), False), StructField("geom", BinaryType(), True),
                StructField("id", IntegerType(), True), StructField("score", DoubleType(), True),
                StructField("label", StringType(), True),
            ])
            _dfh = spark.createDataFrame(_rdh, schema=_schh).select(
                "z","x","y","geom",
                _F_mvth.struct(_F_mvth.col("id"), _F_mvth.col("score"), _F_mvth.col("label")).alias("attrs"),
            ).cache(); _dfh.count()
            from databricks.labs.gbx.vectorx import functions as _hx
            _hx.register(spark)
            _hrows = (_dfh.groupBy("z","x","y")
                .agg(_hx.st_asmvt(_F_mvth.col("geom"), _F_mvth.col("attrs"), _F_mvth.lit("layer")).alias("mvt"))
                .collect())
            _mvt_heavy_blobs = {(r["z"], r["x"], r["y"]): bytes(r["mvt"]) for r in _hrows if r["mvt"]}
        except Exception as _pe:
            print(f"MVT heavy parity capture error: {type(_pe).__name__}: {_pe}")
# Parity check: decode both tiers' MVT output and compare geometry+property counts.
if LIGHTWEIGHT and HEAVYWEIGHT and _mvt_light_blobs is not None and _mvt_heavy_blobs is not None:
    try:
        _lk = set(_mvt_light_blobs.keys())
        _hk = set(_mvt_heavy_blobs.keys())
        _parity_ok = True
        _parity_msg = []
        if _lk != _hk:
            _parity_ok = False
            _parity_msg.append(
                f"tile-key mismatch: light-only={sorted(_lk-_hk)[:3]} "
                f"heavy-only={sorted(_hk-_lk)[:3]} (light={len(_lk)}, heavy={len(_hk)})"
            )
        else:
            _feat_mismatches = []
            for _k in sorted(_lk):
                _ld = _mvt_lib.decode(_mvt_light_blobs[_k])
                _hd = _mvt_lib.decode(_mvt_heavy_blobs[_k])
                _ll = _ld.get("layer", {}).get("features", [])
                _hl = _hd.get("layer", {}).get("features", [])
                if len(_ll) != len(_hl):
                    _feat_mismatches.append(f"{_k}: light={len(_ll)} heavy={len(_hl)}")
                else:
                    # Order-independent: the two encoders may emit features in a different
                    # order, so compare the SET of feature ids per tile, not positionally.
                    _lids = {_f.get("properties", {}).get("id") for _f in _ll}
                    _hids = {_f.get("properties", {}).get("id") for _f in _hl}
                    if _lids != _hids:
                        _diff = sorted((_lids ^ _hids), key=lambda v: (v is None, v))[:5]
                        _feat_mismatches.append(f"{_k}: id-set differs (sym-diff {_diff})")
            if _feat_mismatches:
                _parity_ok = False
                _parity_msg.append("feature mismatches: " + "; ".join(_feat_mismatches[:5]))
        if _parity_ok:
            _verdict = f"MVT PARITY: PASS ({len(_lk)} tiles, feature counts + ids match)"
        else:
            _verdict = "MVT PARITY: FAIL -- " + "; ".join(_parity_msg)
    except Exception as _pe:
        _verdict = f"MVT PARITY: ERROR -- {type(_pe).__name__}: {_pe}"
    print(_verdict)
    assert _verdict.startswith("MVT PARITY: PASS"), _verdict
if _mvt_rows:
    _df_mvt = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'mvt'"
    )
    try:
        display(_df_mvt)
    except Exception:
        _df_mvt.show(100, truncate=False)
    _md = results.summarize(_mvt_rows)
    _show_md(f"mvt benchmark -- {RUN_ID}", _md)
"""

_CELL_PMTILES_AGG = """# PMTiles agg benchmark: light pmtiles_agg vs heavy pmtiles_agg (grouped-agg, both on-cluster)
from databricks.labs.gbx.bench import readers as _rd
_pmtiles_agg_rows = []
_PA_N_TILES = 1000
_PA_N_GROUPS = 1
if LIGHTWEIGHT:
    _r = _rd.run_pmtiles_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="lightweight", n_tiles=_PA_N_TILES, n_groups=_PA_N_GROUPS,
                             where="cluster")
    _sink([_r]); lw.append(_r); _pmtiles_agg_rows.append(_r)
if HEAVYWEIGHT:
    _r = _rd.run_pmtiles_agg(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                             api="heavyweight", n_tiles=_PA_N_TILES, n_groups=_PA_N_GROUPS,
                             where="cluster")
    _sink([_r]); hw.append(_r); _pmtiles_agg_rows.append(_r)
if _pmtiles_agg_rows:
    _df_pa = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'pmtiles_agg'"
    )
    try:
        display(_df_pa)
    except Exception:
        _df_pa.show(100, truncate=False)
    _md = results.summarize(_pmtiles_agg_rows)
    _show_md(f"pmtiles_agg benchmark -- {RUN_ID}", _md)
"""

_CELL_VECTOR_TIN = """# TIN + legacy benchmark: light pyvx vs heavy vectorx, decoded-output parity.
# 4 functions: st_legacyaswkb (legacy migration) + st_triangulate /
# st_interpolateelevationbbox / st_interpolateelevationgeom (constrained-Delaunay TIN).
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
import pyspark.sql.functions as _F_tin
_tin_rows = []
_TIN_N_ROWS = 5
_TIN_N_POINTS = 25
_LEG_N_ROWS = 1000
if LIGHTWEIGHT:
    for _fn_run in (_rd.run_legacy_aswkb, _rd.run_triangulate,
                    _rd.run_interp_bbox, _rd.run_interp_geom):
        _kw = dict(api="lightweight", where="cluster")
        if _fn_run is _rd.run_legacy_aswkb:
            _kw["n_rows"] = _LEG_N_ROWS
        else:
            _kw["n_rows"] = _TIN_N_ROWS; _kw["n_points"] = _TIN_N_POINTS
        _r = _fn_run(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, **_kw)
        _sink([_r]); lw.append(_r); _tin_rows.append(_r)
if HEAVYWEIGHT:
    for _fn_run in (_rd.run_legacy_aswkb, _rd.run_triangulate,
                    _rd.run_interp_bbox, _rd.run_interp_geom):
        _kw = dict(api="heavyweight", where="cluster")
        if _fn_run is _rd.run_legacy_aswkb:
            _kw["n_rows"] = _LEG_N_ROWS
        else:
            _kw["n_rows"] = _TIN_N_ROWS; _kw["n_points"] = _TIN_N_POINTS
        _r = _fn_run(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, **_kw)
        _sink([_r]); hw.append(_r); _tin_rows.append(_r)
# Decoded-output parity (hard gate) -- rebuild the SAME deterministic corpus the run_*
# legs timed, then collect each tier through its native surface and compare decoded output.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _verdicts = []
    # --- legacy: decoded-geometry equality (light collected BEFORE heavy registers the
    #     same SQL name; the later registration overwrites it). ---
    try:
        _ldata, _lschema = _cv.generate_legacy_structs(_LEG_N_ROWS)
        _ldf = spark.createDataFrame(_ldata, schema=_lschema)
        _ldf.createOrReplaceTempView("_leg_parity_v")
        from databricks.labs.gbx.pyvx import functions as _vx_leg
        _vx_leg.register(spark)
        _light_w = [bytes(r["w"]) for r in spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _leg_parity_v").collect()]
        from databricks.labs.gbx.vectorx.jts.legacy import functions as _hx_leg
        _hx_leg.register(spark)
        _heavy_w = [bytes(r["w"]) for r in spark.sql(
            "SELECT gbx_st_legacyaswkb(g) AS w FROM _leg_parity_v").collect()]
        _leg_ok = len(_light_w) == len(_heavy_w) and len(_light_w) > 0
        if _leg_ok:
            for _lw, _hw in zip(_light_w, _heavy_w):
                _lg = _shp_wkb.loads(_lw); _hg = _shp_wkb.loads(_hw)
                if not (_lg.equals(_hg) and _lg.has_z and _hg.has_z):
                    _leg_ok = False; break
        _v = ("LEGACY PARITY: PASS (%d geometries, decoded equality + Z)" % len(_light_w)
              if _leg_ok else "LEGACY PARITY: FAIL -- decoded geometry mismatch")
    except Exception as _pe:
        _v = "LEGACY PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("LEGACY PARITY: PASS"), _v
    # --- TIN: register both tiers (different catalog paths -> coexist). ---
    from databricks.labs.gbx.pyvx import functions as _vx_tin
    from databricks.labs.gbx.vectorx import functions as _hx_tin
    _vx_tin.register(spark); _hx_tin.register(spark)
    _tdata, _tschema = _cv.generate_tin_points(_TIN_N_ROWS, n_points=_TIN_N_POINTS)
    _tdf = spark.createDataFrame(_tdata, schema=_tschema)
    _tdf.createOrReplaceTempView("_tin_parity_v")
    def _centroid(_blob):
        _g = _shp_wkb.loads(bytes(_blob)); _c = _g.centroid
        return (round(_c.x, 6), round(_c.y, 6))
    def _unmatched(_a, _b):
        for _cx, _cy in _a:
            if not any(abs(_cx-_bx) < 1e-6 and abs(_cy-_by) < 1e-6 for _bx, _by in _b):
                return (_cx, _cy)
        return None
    # triangulate: count + centroid-set match within 1e-6.
    try:
        _lt = spark.sql("SELECT t.triangle FROM _tin_parity_v, LATERAL "
                        "gbx_st_triangulate(pts, bl, mt, st, spf, 'constrained') t").collect()
        _ht = _tdf.select(_F_tin.call_function(
            "gbx_st_triangulate", _F_tin.col("pts"), _F_tin.col("bl"), _F_tin.col("mt"),
            _F_tin.col("st"), _F_tin.col("spf"), _F_tin.lit("constrained")
        ).alias("triangle")).collect()
        _lc = sorted(_centroid(r["triangle"]) for r in _lt)
        _hc = sorted(_centroid(r["triangle"]) for r in _ht)
        _tri_ok = (len(_lt) == len(_ht) > 0 and _unmatched(_lc, _hc) is None
                   and _unmatched(_hc, _lc) is None)
        _v = ("TIN TRIANGULATE PARITY: PASS (light=heavy=%d triangles, centroids match)" % len(_lt)
              if _tri_ok else "TIN TRIANGULATE PARITY: FAIL -- light=%d heavy=%d (centroid/count mismatch)"
              % (len(_lt), len(_ht)))
    except Exception as _pe:
        _v = "TIN TRIANGULATE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN TRIANGULATE PARITY: PASS"), _v
    # interp bbox + geom: same in-hull cell keyset + per-cell |dz| < 1e-6.
    def _grid_dict(_rows, _col):
        _out = {}
        for _r in _rows:
            _g = _shp_wkb.loads(bytes(_r[_col]))
            _out[(round(_g.x, 6), round(_g.y, 6))] = _g.z
        return _out
    # bbox
    try:
        _lb = _grid_dict(spark.sql(
            "SELECT t.elevation_point AS p FROM _tin_parity_v, LATERAL "
            "gbx_st_interpolateelevationbbox(pts, bl, mt, st, spf, xmin, ymin, xmax, ymax, "
            "w, h, srid, 'constrained') t").collect(), "p")
        _hb = _grid_dict(_tdf.select(_F_tin.call_function(
            "gbx_st_interpolateelevationbbox", _F_tin.col("pts"), _F_tin.col("bl"),
            _F_tin.col("mt"), _F_tin.col("st"), _F_tin.col("spf"), _F_tin.col("xmin"),
            _F_tin.col("ymin"), _F_tin.col("xmax"), _F_tin.col("ymax"), _F_tin.col("w"),
            _F_tin.col("h"), _F_tin.col("srid"), _F_tin.lit("constrained")
        ).alias("p")).collect(), "p")
        _bb_ok = (_lb.keys() == _hb.keys() and len(_lb) > 0
                  and all(abs(_lb[_k] - _hb[_k]) < 1e-6 for _k in _lb))
        _v = ("TIN INTERP BBOX PARITY: PASS (%d cells, |dz|<1e-6)" % len(_lb)
              if _bb_ok else "TIN INTERP BBOX PARITY: FAIL -- cell/Z mismatch (light=%d heavy=%d)"
              % (len(_lb), len(_hb)))
    except Exception as _pe:
        _v = "TIN INTERP BBOX PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN INTERP BBOX PARITY: PASS"), _v
    # geom
    try:
        _lg2 = _grid_dict(spark.sql(
            "SELECT t.elevation_point AS p FROM _tin_parity_v, LATERAL "
            "gbx_st_interpolateelevationgeom(pts, bl, mt, st, spf, origin, cols, rows_n, "
            "cell_x, cell_y, 'constrained') t").collect(), "p")
        _hg2 = _grid_dict(_tdf.select(_F_tin.call_function(
            "gbx_st_interpolateelevationgeom", _F_tin.col("pts"), _F_tin.col("bl"),
            _F_tin.col("mt"), _F_tin.col("st"), _F_tin.col("spf"), _F_tin.col("origin"),
            _F_tin.col("cols"), _F_tin.col("rows_n"), _F_tin.col("cell_x"),
            _F_tin.col("cell_y"), _F_tin.lit("constrained")
        ).alias("p")).collect(), "p")
        _gg_ok = (_lg2.keys() == _hg2.keys() and len(_lg2) > 0
                  and all(abs(_lg2[_k] - _hg2[_k]) < 1e-6 for _k in _lg2))
        _v = ("TIN INTERP GEOM PARITY: PASS (%d cells, |dz|<1e-6)" % len(_lg2)
              if _gg_ok else "TIN INTERP GEOM PARITY: FAIL -- cell/Z mismatch (light=%d heavy=%d)"
              % (len(_lg2), len(_hg2)))
    except Exception as _pe:
        _v = "TIN INTERP GEOM PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("TIN INTERP GEOM PARITY: PASS"), _v
if _tin_rows:
    _df_tin = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category IN ('tin','legacy')"
    )
    try:
        display(_df_tin)
    except Exception:
        _df_tin.show(100, truncate=False)
    _md = results.summarize(_tin_rows)
    _show_md(f"TIN + legacy benchmark -- {RUN_ID}", _md)
"""

_CELL_GRID_QUADBIN = """# Quadbin grid benchmark: light pygx vs heavy gridx.quadbin, exact-output parity.
# ALL 10 quadbin functions: pointascell (scalar) / polyfill (geom->ARRAY<cell>) /
# tessellate (struct-array) / cellunion_agg (grouped aggregate) / resolution (scalar INT) /
# kring (scalar ARRAY<LONG>) / distance (scalar INT) / aswkb (scalar EWKB polygon) /
# centroid (scalar EWKB point) / cellunion (scalar ARRAY<cell>->EWKB). Both tiers expose the
# SAME gbx_quadbin_* SQL names, so light is collected BEFORE heavy re-registers (the later
# registration overwrites the UDF) -- the same ordering trick as the legacy parity cell.
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
_quadbin_rows = []
_QB_N_ROWS = 1000
_QB_PAC_RES = 12   # pointascell resolution
_QB_GEOM_RES = 8   # polyfill / tessellate resolution
_QB_AGG_RES = 8    # cellunion_agg / cellunion cell resolution
_QB_CELL_RES = 12  # resolution / kring / distance / aswkb / centroid cell resolution
_QB_KRING_K = 1    # kring radius
_QB_N_LEGS = 10    # legs per tier (keep in sync with the appends below)
if LIGHTWEIGHT:
    _quadbin_rows.append(_rd.run_quadbin_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_PAC_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_tessellate(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_cellunion_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_resolution(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_kring(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, k=_QB_KRING_K, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_distance(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_aswkb(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_centroid(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _quadbin_rows.append(_rd.run_quadbin_cellunion(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="lightweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    for _r in _quadbin_rows[-_QB_N_LEGS:]:
        _sink([_r]); lw.append(_r)
if HEAVYWEIGHT:
    _hw_qb = []
    _hw_qb.append(_rd.run_quadbin_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_PAC_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_tessellate(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_GEOM_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_cellunion_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_resolution(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_kring(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, k=_QB_KRING_K, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_distance(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_aswkb(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_centroid(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_CELL_RES, where="cluster"))
    _hw_qb.append(_rd.run_quadbin_cellunion(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api="heavyweight",
        n_rows=_QB_N_ROWS, res=_QB_AGG_RES, where="cluster"))
    for _r in _hw_qb:
        _sink([_r]); hw.append(_r); _quadbin_rows.append(_r)
# Exact-output parity (hard gate): rebuild the SAME deterministic corpora, collect each tier
# through its native SQL surface, and compare. Cells: exact equality. Decoded geometry: 1e-6.
if LIGHTWEIGHT and HEAVYWEIGHT:
    import pyspark.sql.functions as _F_qb
    _verdicts = []
    def _centroid_qb(_blob):
        _g = _shp_wkb.loads(bytes(_blob)); _c = _g.centroid
        return (round(_c.x, 6), round(_c.y, 6))
    # --- pointascell: exact cell-id equality (light BEFORE heavy: shared SQL name). ---
    try:
        _pdata, _pschema = _cv.generate_quadbin_points(_QB_N_ROWS)
        _pdf = spark.createDataFrame(_pdata, schema=_pschema)
        _pdf.createOrReplaceTempView("_qb_pac_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pac
        _gx_pac.register(spark)
        _light_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_quadbin_pointascell(lon, lat, %d) AS cell FROM _qb_pac_parity_v"
            % _QB_PAC_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_pac
        _hx_pac.register(spark)
        _heavy_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_quadbin_pointascell(lon, lat, %d) AS cell FROM _qb_pac_parity_v"
            % _QB_PAC_RES).collect()]
        _pac_ok = (len(_light_cells) == len(_heavy_cells) > 0
                   and _light_cells == _heavy_cells)
        _v = ("QUADBIN POINTASCELL PARITY: PASS (%d cells, exact id equality)" % len(_light_cells)
              if _pac_ok else "QUADBIN POINTASCELL PARITY: FAIL -- cell id mismatch")
    except Exception as _pe:
        _v = "QUADBIN POINTASCELL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN POINTASCELL PARITY: PASS"), _v
    # --- polyfill: exact per-row cell-SET equality. ---
    try:
        _gdata, _gschema = _cv.generate_quadbin_polygons(_QB_N_ROWS)
        _gdf = spark.createDataFrame(_gdata, schema=_gschema)
        _gdf.createOrReplaceTempView("_qb_geom_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pf
        _gx_pf.register(spark)
        _light_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_quadbin_polyfill(geom, %d) AS cells FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_pf
        _hx_pf.register(spark)
        _heavy_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_quadbin_polyfill(geom, %d) AS cells FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        _pf_ok = (len(_light_pf) == len(_heavy_pf) > 0 and _light_pf == _heavy_pf)
        _ncells = sum(len(c) for c in _light_pf)
        _v = ("QUADBIN POLYFILL PARITY: PASS (%d rows, %d cells, exact set equality)"
              % (len(_light_pf), _ncells)
              if _pf_ok else "QUADBIN POLYFILL PARITY: FAIL -- cell set mismatch")
    except Exception as _pe:
        _v = "QUADBIN POLYFILL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN POLYFILL PARITY: PASS"), _v
    # --- tessellate: exact (cell, centroid) set per row; cells exact, centroid within 1e-6. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ts
        _gx_ts.register(spark)
        def _chip_set(_chips):
            return sorted((int(c["cell"]), _centroid_qb(c["geom"])) for c in _chips)
        _light_ts = [_chip_set(r["chips"]) for r in spark.sql(
            "SELECT gbx_quadbin_tessellate(geom, %d) AS chips FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ts
        _hx_ts.register(spark)
        _heavy_ts = [_chip_set(r["chips"]) for r in spark.sql(
            "SELECT gbx_quadbin_tessellate(geom, %d) AS chips FROM _qb_geom_parity_v"
            % _QB_GEOM_RES).collect()]
        _ts_ok = len(_light_ts) == len(_heavy_ts) > 0
        if _ts_ok:
            for _lrow, _hrow in zip(_light_ts, _heavy_ts):
                if len(_lrow) != len(_hrow):
                    _ts_ok = False; break
                for (_lc, (_lx, _ly)), (_hc, (_hx2, _hy)) in zip(_lrow, _hrow):
                    if _lc != _hc or abs(_lx - _hx2) >= 1e-6 or abs(_ly - _hy) >= 1e-6:
                        _ts_ok = False; break
                if not _ts_ok:
                    break
        _nchips = sum(len(c) for c in _light_ts)
        _v = ("QUADBIN TESSELLATE PARITY: PASS (%d rows, %d chips, cells exact + centroid<1e-6)"
              % (len(_light_ts), _nchips)
              if _ts_ok else "QUADBIN TESSELLATE PARITY: FAIL -- chip/cell/centroid mismatch")
    except Exception as _pe:
        _v = "QUADBIN TESSELLATE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN TESSELLATE PARITY: PASS"), _v
    # --- cellunion_agg: per-group decoded-union geometry equality (within 1e-6). ---
    try:
        _adata, _aschema = _cv.generate_quadbin_cellid_arrays(_QB_N_ROWS, res=_QB_AGG_RES)
        _adf = spark.createDataFrame(_adata, schema=_aschema)
        _adf.createOrReplaceTempView("_qb_agg_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ag
        _gx_ag.register(spark)
        _light_ag = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _qb_agg_parity_v GROUP BY group").collect()}
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ag
        _hx_ag.register(spark)
        _heavy_ag = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion_agg(cell) AS u "
            "FROM _qb_agg_parity_v GROUP BY group").collect()}
        _ag_ok = (_light_ag.keys() == _heavy_ag.keys() and len(_light_ag) > 0)
        if _ag_ok:
            for _k in _light_ag:
                _lg = _shp_wkb.loads(_light_ag[_k]); _hg = _shp_wkb.loads(_heavy_ag[_k])
                # Decoded-geometry equality within tolerance. shapely union_all and JTS
                # union pick different vertex orders / multipolygon member orders, so
                # equals()/equals_exact() report False on identical coverage. The
                # geometrically meaningful 1e-6 bar is "area of disagreement < tol":
                # symmetric_difference().area collapses to floating-point noise (~1e-13)
                # when the two unions cover the same region.
                if _lg.symmetric_difference(_hg).area >= 1e-6:
                    _ag_ok = False; break
        _v = ("QUADBIN CELLUNION_AGG PARITY: PASS (%d groups, decoded union equality)"
              % len(_light_ag)
              if _ag_ok else "QUADBIN CELLUNION_AGG PARITY: FAIL -- union geometry mismatch")
    except Exception as _pe:
        _v = "QUADBIN CELLUNION_AGG PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN CELLUNION_AGG PARITY: PASS"), _v
    # --- shared single-cell corpus for the scalar cell-in legs (resolution/kring/aswkb/centroid). ---
    _cdata, _cschema = _cv.generate_quadbin_cells(_QB_N_ROWS, res=_QB_CELL_RES)
    _cdf = spark.createDataFrame(_cdata, schema=_cschema)
    _cdf.createOrReplaceTempView("_qb_cell_parity_v")
    # --- resolution: exact INT equality. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_rs
        _gx_rs.register(spark)
        _light_rs = [r["r"] for r in spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _qb_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_rs
        _hx_rs.register(spark)
        _heavy_rs = [r["r"] for r in spark.sql(
            "SELECT gbx_quadbin_resolution(cell) AS r FROM _qb_cell_parity_v").collect()]
        _rs_ok = (len(_light_rs) == len(_heavy_rs) > 0 and _light_rs == _heavy_rs)
        _v = ("QUADBIN RESOLUTION PARITY: PASS (%d cells, exact INT equality)" % len(_light_rs)
              if _rs_ok else "QUADBIN RESOLUTION PARITY: FAIL -- resolution mismatch")
    except Exception as _pe:
        _v = "QUADBIN RESOLUTION PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN RESOLUTION PARITY: PASS"), _v
    # --- kring: exact sorted cell-set per row (fixed k). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_kr
        _gx_kr.register(spark)
        _light_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_quadbin_kring(cell, %d) AS ring FROM _qb_cell_parity_v"
            % _QB_KRING_K).collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_kr
        _hx_kr.register(spark)
        _heavy_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_quadbin_kring(cell, %d) AS ring FROM _qb_cell_parity_v"
            % _QB_KRING_K).collect()]
        _kr_ok = (len(_light_kr) == len(_heavy_kr) > 0 and _light_kr == _heavy_kr)
        _nk = sum(len(c) for c in _light_kr)
        _v = ("QUADBIN KRING PARITY: PASS (%d rows, %d cells, exact set equality, k=%d)"
              % (len(_light_kr), _nk, _QB_KRING_K)
              if _kr_ok else "QUADBIN KRING PARITY: FAIL -- ring set mismatch")
    except Exception as _pe:
        _v = "QUADBIN KRING PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN KRING PARITY: PASS"), _v
    # --- distance: exact INT equality over (cell_a, cell_b) pairs. ---
    try:
        _ddata, _dschema = _cv.generate_quadbin_cell_pairs(_QB_N_ROWS, res=_QB_CELL_RES)
        _ddf = spark.createDataFrame(_ddata, schema=_dschema)
        _ddf.createOrReplaceTempView("_qb_pair_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ds
        _gx_ds.register(spark)
        _light_ds = [r["d"] for r in spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _qb_pair_parity_v").collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ds
        _hx_ds.register(spark)
        _heavy_ds = [r["d"] for r in spark.sql(
            "SELECT gbx_quadbin_distance(cell_a, cell_b) AS d FROM _qb_pair_parity_v").collect()]
        _ds_ok = (len(_light_ds) == len(_heavy_ds) > 0 and _light_ds == _heavy_ds)
        _v = ("QUADBIN DISTANCE PARITY: PASS (%d pairs, exact INT equality)" % len(_light_ds)
              if _ds_ok else "QUADBIN DISTANCE PARITY: FAIL -- distance mismatch")
    except Exception as _pe:
        _v = "QUADBIN DISTANCE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN DISTANCE PARITY: PASS"), _v
    # --- aswkb: decoded polygon within 1e-6 + SRID 4326. ---
    try:
        from shapely import from_wkb as _shp_from_wkb, get_srid as _shp_get_srid
        from databricks.labs.gbx.pygx import functions as _gx_aw
        _gx_aw.register(spark)
        _light_aw = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _qb_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_aw
        _hx_aw.register(spark)
        _heavy_aw = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_quadbin_aswkb(cell) AS g FROM _qb_cell_parity_v").collect()]
        _aw_ok = (len(_light_aw) == len(_heavy_aw) > 0)
        if _aw_ok:
            for _lb, _hb in zip(_light_aw, _heavy_aw):
                _lg = _shp_from_wkb(_lb); _hg = _shp_from_wkb(_hb)
                if (_shp_get_srid(_lg) != 4326 or _shp_get_srid(_hg) != 4326
                        or _lg.symmetric_difference(_hg).area >= 1e-6):
                    _aw_ok = False; break
        _v = ("QUADBIN ASWKB PARITY: PASS (%d cells, polygon<1e-6 + SRID 4326)" % len(_light_aw)
              if _aw_ok else "QUADBIN ASWKB PARITY: FAIL -- polygon/SRID mismatch")
    except Exception as _pe:
        _v = "QUADBIN ASWKB PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN ASWKB PARITY: PASS"), _v
    # --- centroid: decoded point within 1e-6. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ct
        _gx_ct.register(spark)
        _light_ct = [_centroid_qb(r["g"]) for r in spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _qb_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_ct
        _hx_ct.register(spark)
        _heavy_ct = [_centroid_qb(r["g"]) for r in spark.sql(
            "SELECT gbx_quadbin_centroid(cell) AS g FROM _qb_cell_parity_v").collect()]
        _ct_ok = (len(_light_ct) == len(_heavy_ct) > 0)
        if _ct_ok:
            for (_lx, _ly), (_hx2, _hy) in zip(_light_ct, _heavy_ct):
                if abs(_lx - _hx2) >= 1e-6 or abs(_ly - _hy) >= 1e-6:
                    _ct_ok = False; break
        _v = ("QUADBIN CENTROID PARITY: PASS (%d cells, point<1e-6)" % len(_light_ct)
              if _ct_ok else "QUADBIN CENTROID PARITY: FAIL -- point mismatch")
    except Exception as _pe:
        _v = "QUADBIN CENTROID PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN CENTROID PARITY: PASS"), _v
    # --- cellunion: per-group decoded-union geometry equality (sym-diff area < 1e-6). ---
    try:
        _udata, _uschema = _cv.generate_quadbin_cellid_arrays(_QB_N_ROWS, res=_QB_AGG_RES)
        _udf = spark.createDataFrame(_udata, schema=_uschema)
        _udf.createOrReplaceTempView("_qb_union_src_v")
        _uarr = spark.sql(
            "SELECT group, collect_list(cell) AS cells FROM _qb_union_src_v GROUP BY group")
        _uarr.createOrReplaceTempView("_qb_union_arr_v")
        from databricks.labs.gbx.pygx import functions as _gx_cu
        _gx_cu.register(spark)
        _light_cu = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion(cells) AS u FROM _qb_union_arr_v").collect()}
        from databricks.labs.gbx.gridx.quadbin import functions as _hx_cu
        _hx_cu.register(spark)
        _heavy_cu = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_quadbin_cellunion(cells) AS u FROM _qb_union_arr_v").collect()}
        _cu_ok = (_light_cu.keys() == _heavy_cu.keys() and len(_light_cu) > 0)
        if _cu_ok:
            for _k in _light_cu:
                _lg = _shp_wkb.loads(_light_cu[_k]); _hg = _shp_wkb.loads(_heavy_cu[_k])
                # Member-ordering-robust: shapely union_all vs JTS union differ in
                # vertex/member order; sym-diff area collapses to FP noise on equal coverage.
                if _lg.symmetric_difference(_hg).area >= 1e-6:
                    _cu_ok = False; break
        _v = ("QUADBIN CELLUNION PARITY: PASS (%d groups, decoded union equality)" % len(_light_cu)
              if _cu_ok else "QUADBIN CELLUNION PARITY: FAIL -- union geometry mismatch")
    except Exception as _pe:
        _v = "QUADBIN CELLUNION PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("QUADBIN CELLUNION PARITY: PASS"), _v
if _quadbin_rows:
    _df_qb = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'grid'"
    )
    try:
        display(_df_qb)
    except Exception:
        _df_qb.show(100, truncate=False)
    _md = results.summarize(_quadbin_rows)
    _show_md(f"quadbin grid benchmark -- {RUN_ID}", _md)
"""


_CELL_GRID_BNG = """# BNG grid benchmark: light pygx vs heavy gridx.bng, exact-output parity.
# Representative spread of BNG functions: pointascell (geom->STRING cell) /
# polyfill (geom->ARRAY<STRING>) / tessellate (chip struct-array) / kring
# (scalar STRING cell-in -> ARRAY<STRING>) / cellunion_agg (grouped aggregate over
# chip structs). Both tiers expose the SAME gbx_bng_* SQL names, so light is
# collected BEFORE heavy re-registers (the later registration overwrites the UDF)
# -- the same ordering trick as the quadbin parity cell. Inputs are EPSG:27700
# (BNG eastings/northings, not WGS84); resolutions are string keys ("1km"),
# never metres-as-Int (the BNG resolution convention).
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
_bng_rows = []
_BNG_N_ROWS = 1000
_BNG_GEOM_RES = "1km"   # pointascell / polyfill / tessellate resolution
_BNG_AGG_RES = "1km"    # cellunion_agg chip cell resolution
_BNG_CELL_RES = "1km"   # scalar cell / pair / eastnorth resolution
_BNG_KRING_K = 1        # kring / kloop radius
_BNG_KLOOP_K = 1        # kloop radius
_BNG_GEOMK_K = 1        # geomkring / geomkloop radius
_BNG_N_LEGS = 23        # legs per tier (keep in sync with the appends below)
# Order: 5 originals (pointascell, polyfill, tessellate, kring, cellunion_agg)
# then the 18 new legs: scalars, arrays, chip-struct, intersection_agg, explodes.
def _bng_legs(_api):
    # Run all 23 BNG legs for one tier, in a fixed order, returning the rows.
    _out = []
    _out.append(_rd.run_bng_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, where="cluster"))
    _out.append(_rd.run_bng_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, where="cluster"))
    _out.append(_rd.run_bng_tessellate(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, where="cluster"))
    _out.append(_rd.run_bng_kring(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, k=_BNG_KRING_K, where="cluster"))
    _out.append(_rd.run_bng_cellunion_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_AGG_RES, where="cluster"))
    # --- scalars (cell-in / pair / eastnorth) ---
    _out.append(_rd.run_bng_aswkb(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_aswkt(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_centroid(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_cellarea(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_distance(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_euclideandistance(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_eastnorthasbng(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    # --- array-returning (kloop, geomk*) ---
    _out.append(_rd.run_bng_kloop(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, k=_BNG_KLOOP_K, where="cluster"))
    _out.append(_rd.run_bng_geomkring(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, k=_BNG_GEOMK_K, where="cluster"))
    _out.append(_rd.run_bng_geomkloop(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, k=_BNG_GEOMK_K, where="cluster"))
    # --- chip-struct scalars ---
    _out.append(_rd.run_bng_cellintersection(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    _out.append(_rd.run_bng_cellunion(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, where="cluster"))
    # --- grouped intersection aggregate ---
    _out.append(_rd.run_bng_cellintersection_agg(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_AGG_RES, where="cluster"))
    # --- explode UDTFs (LATERAL SQL, tier-agnostic job) ---
    _out.append(_rd.run_bng_kringexplode(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, k=_BNG_KRING_K, where="cluster"))
    _out.append(_rd.run_bng_kloopexplode(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_CELL_RES, k=_BNG_KLOOP_K, where="cluster"))
    _out.append(_rd.run_bng_geomkringexplode(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, k=_BNG_GEOMK_K, where="cluster"))
    _out.append(_rd.run_bng_geomkloopexplode(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, k=_BNG_GEOMK_K, where="cluster"))
    _out.append(_rd.run_bng_tessellateexplode(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_BNG_N_ROWS, res=_BNG_GEOM_RES, where="cluster"))
    return _out
if LIGHTWEIGHT:
    for _r in _bng_legs("lightweight"):
        _sink([_r]); lw.append(_r); _bng_rows.append(_r)
if HEAVYWEIGHT:
    for _r in _bng_legs("heavyweight"):
        _sink([_r]); hw.append(_r); _bng_rows.append(_r)
# Exact-output parity (hard gate): rebuild the SAME deterministic corpora, collect each tier
# through its native SQL surface, and compare. Cell ids: exact equality. Decoded geometry: 1e-6.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _verdicts = []
    # --- pointascell: exact STRING cell-id equality (light BEFORE heavy: shared SQL name). ---
    try:
        _pdata, _pschema = _cv.generate_bng_points(_BNG_N_ROWS)
        _pdf = spark.createDataFrame(_pdata, schema=_pschema)
        _pdf.createOrReplaceTempView("_bng_pac_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pac
        _gx_pac.register(spark)
        _light_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_bng_pointascell(geom, '%s') AS cell FROM _bng_pac_parity_v"
            % _BNG_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_pac
        _hx_pac.register(spark)
        _heavy_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_bng_pointascell(geom, '%s') AS cell FROM _bng_pac_parity_v"
            % _BNG_GEOM_RES).collect()]
        _pac_ok = (len(_light_cells) == len(_heavy_cells) > 0
                   and _light_cells == _heavy_cells)
        _v = ("BNG POINTASCELL PARITY: PASS (%d cells, exact id equality)" % len(_light_cells)
              if _pac_ok else "BNG POINTASCELL PARITY: FAIL -- cell id mismatch")
    except Exception as _pe:
        _v = "BNG POINTASCELL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG POINTASCELL PARITY: PASS"), _v
    # --- polyfill: exact per-row cell-SET equality. ---
    try:
        _gdata, _gschema = _cv.generate_bng_polygons(_BNG_N_ROWS)
        _gdf = spark.createDataFrame(_gdata, schema=_gschema)
        _gdf.createOrReplaceTempView("_bng_geom_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pf
        _gx_pf.register(spark)
        _light_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_bng_polyfill(geom, '%s') AS cells FROM _bng_geom_parity_v"
            % _BNG_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_pf
        _hx_pf.register(spark)
        _heavy_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_bng_polyfill(geom, '%s') AS cells FROM _bng_geom_parity_v"
            % _BNG_GEOM_RES).collect()]
        _pf_ok = (len(_light_pf) == len(_heavy_pf) > 0 and _light_pf == _heavy_pf)
        _ncells = sum(len(c) for c in _light_pf)
        _v = ("BNG POLYFILL PARITY: PASS (%d rows, %d cells, exact set equality)"
              % (len(_light_pf), _ncells)
              if _pf_ok else "BNG POLYFILL PARITY: FAIL -- cell set mismatch")
    except Exception as _pe:
        _v = "BNG POLYFILL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG POLYFILL PARITY: PASS"), _v
    # --- tessellate: exact chip cellid-SET equality per row (the load-bearing field). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ts
        _gx_ts.register(spark)
        def _chip_cells(_chips):
            return sorted(c["cellid"] for c in _chips)
        _light_ts = [_chip_cells(r["chips"]) for r in spark.sql(
            "SELECT gbx_bng_tessellate(geom, '%s') AS chips FROM _bng_geom_parity_v"
            % _BNG_GEOM_RES).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_ts
        _hx_ts.register(spark)
        _heavy_ts = [_chip_cells(r["chips"]) for r in spark.sql(
            "SELECT gbx_bng_tessellate(geom, '%s') AS chips FROM _bng_geom_parity_v"
            % _BNG_GEOM_RES).collect()]
        _ts_ok = (len(_light_ts) == len(_heavy_ts) > 0 and _light_ts == _heavy_ts)
        _nchips = sum(len(c) for c in _light_ts)
        _v = ("BNG TESSELLATE PARITY: PASS (%d rows, %d chips, cell-set exact)"
              % (len(_light_ts), _nchips)
              if _ts_ok else "BNG TESSELLATE PARITY: FAIL -- chip cell-set mismatch")
    except Exception as _pe:
        _v = "BNG TESSELLATE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG TESSELLATE PARITY: PASS"), _v
    # --- kring: exact sorted cell-set per row (fixed k), shared single-cell corpus. ---
    try:
        _cdata, _cschema = _cv.generate_bng_cells(_BNG_N_ROWS, res=_BNG_CELL_RES)
        _cdf = spark.createDataFrame(_cdata, schema=_cschema)
        _cdf.createOrReplaceTempView("_bng_cell_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_kr
        _gx_kr.register(spark)
        _light_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_kring(cell, %d) AS ring FROM _bng_cell_parity_v"
            % _BNG_KRING_K).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_kr
        _hx_kr.register(spark)
        _heavy_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_kring(cell, %d) AS ring FROM _bng_cell_parity_v"
            % _BNG_KRING_K).collect()]
        _kr_ok = (len(_light_kr) == len(_heavy_kr) > 0 and _light_kr == _heavy_kr)
        _nk = sum(len(c) for c in _light_kr)
        _v = ("BNG KRING PARITY: PASS (%d rows, %d cells, exact set equality, k=%d)"
              % (len(_light_kr), _nk, _BNG_KRING_K)
              if _kr_ok else "BNG KRING PARITY: FAIL -- ring set mismatch")
    except Exception as _pe:
        _v = "BNG KRING PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG KRING PARITY: PASS"), _v
    # --- cellunion_agg: per-group decoded chip-GEOMETRY equality (sym-diff area < 1e-6).
    # Light returns BINARY (the dissolved chip geom); heavy returns STRUCT<cellid, core,
    # chip> -> decode the .chip field. Compare the load-bearing chip geometry either way. ---
    try:
        _adata, _aschema = _cv.generate_bng_chip_groups(_BNG_N_ROWS, res=_BNG_AGG_RES)
        _adf = spark.createDataFrame(_adata, schema=_aschema)
        _adf.createOrReplaceTempView("_bng_agg_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ag
        _gx_ag.register(spark)
        _light_ag = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_bng_cellunion_agg(chip) AS u "
            "FROM _bng_agg_parity_v GROUP BY group").collect() if r["u"] is not None}
        from databricks.labs.gbx.gridx.bng import functions as _hx_ag
        _hx_ag.register(spark)
        # Heavy returns STRUCT<cellid, core, chip>; the load-bearing field is .chip (BINARY).
        _heavy_ag = {r["group"]: bytes(r["u"]["chip"]) for r in spark.sql(
            "SELECT group, gbx_bng_cellunion_agg(chip) AS u "
            "FROM _bng_agg_parity_v GROUP BY group").collect()
            if r["u"] is not None and r["u"]["chip"] is not None}
        _ag_ok = (_light_ag.keys() == _heavy_ag.keys() and len(_light_ag) > 0)
        if _ag_ok:
            for _k in _light_ag:
                _lg = _shp_wkb.loads(_light_ag[_k]); _hg = _shp_wkb.loads(_heavy_ag[_k])
                # shapely union_all vs JTS union differ in vertex/member order; the
                # geometrically meaningful 1e-6 bar is "area of disagreement < tol":
                # sym-diff area collapses to FP noise on equal coverage.
                if _lg.symmetric_difference(_hg).area >= 1e-6:
                    _ag_ok = False; break
        _v = ("BNG CELLUNION_AGG PARITY: PASS (%d groups, decoded chip-geom equality)"
              % len(_light_ag)
              if _ag_ok else "BNG CELLUNION_AGG PARITY: FAIL -- chip geometry mismatch")
    except Exception as _pe:
        _v = "BNG CELLUNION_AGG PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CELLUNION_AGG PARITY: PASS"), _v
    # --- aswkb: decoded polygon sym-diff area < 1e-6 (shared single-cell corpus). ---
    try:
        _cdata, _cschema = _cv.generate_bng_cells(_BNG_N_ROWS, res=_BNG_CELL_RES)
        spark.createDataFrame(_cdata, schema=_cschema).createOrReplaceTempView(
            "_bng_cell_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_wb
        _gx_wb.register(spark)
        _light_wb = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_bng_aswkb(cell) AS g FROM _bng_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_wb
        _hx_wb.register(spark)
        _heavy_wb = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_bng_aswkb(cell) AS g FROM _bng_cell_parity_v").collect()]
        _wb_ok = len(_light_wb) == len(_heavy_wb) > 0
        if _wb_ok:
            for _lb, _hb in zip(_light_wb, _heavy_wb):
                if _shp_wkb.loads(_lb).symmetric_difference(
                        _shp_wkb.loads(_hb)).area >= 1e-6:
                    _wb_ok = False; break
        _v = ("BNG ASWKB PARITY: PASS (%d cells, decoded geom < 1e-6)" % len(_light_wb)
              if _wb_ok else "BNG ASWKB PARITY: FAIL -- geometry mismatch")
    except Exception as _pe:
        _v = "BNG ASWKB PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG ASWKB PARITY: PASS"), _v
    # --- aswkt: decoded polygon (via WKT) sym-diff area < 1e-6. ---
    try:
        from shapely import wkt as _shp_wkt
        from databricks.labs.gbx.pygx import functions as _gx_wt
        _gx_wt.register(spark)
        _light_wt = [r["g"] for r in spark.sql(
            "SELECT gbx_bng_aswkt(cell) AS g FROM _bng_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_wt
        _hx_wt.register(spark)
        _heavy_wt = [r["g"] for r in spark.sql(
            "SELECT gbx_bng_aswkt(cell) AS g FROM _bng_cell_parity_v").collect()]
        _wt_ok = len(_light_wt) == len(_heavy_wt) > 0
        if _wt_ok:
            for _lt, _ht in zip(_light_wt, _heavy_wt):
                if _shp_wkt.loads(_lt).symmetric_difference(
                        _shp_wkt.loads(_ht)).area >= 1e-6:
                    _wt_ok = False; break
        _v = ("BNG ASWKT PARITY: PASS (%d cells, decoded geom < 1e-6)" % len(_light_wt)
              if _wt_ok else "BNG ASWKT PARITY: FAIL -- geometry mismatch")
    except Exception as _pe:
        _v = "BNG ASWKT PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG ASWKT PARITY: PASS"), _v
    # --- centroid: decoded point distance < 1e-6. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ct
        _gx_ct.register(spark)
        _light_ct = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_bng_centroid(cell) AS g FROM _bng_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_ct
        _hx_ct.register(spark)
        _heavy_ct = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_bng_centroid(cell) AS g FROM _bng_cell_parity_v").collect()]
        _ct_ok = len(_light_ct) == len(_heavy_ct) > 0
        if _ct_ok:
            for _lb, _hb in zip(_light_ct, _heavy_ct):
                if _shp_wkb.loads(_lb).distance(_shp_wkb.loads(_hb)) >= 1e-6:
                    _ct_ok = False; break
        _v = ("BNG CENTROID PARITY: PASS (%d cells, point < 1e-6)" % len(_light_ct)
              if _ct_ok else "BNG CENTROID PARITY: FAIL -- point mismatch")
    except Exception as _pe:
        _v = "BNG CENTROID PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CENTROID PARITY: PASS"), _v
    # --- cellarea: exact scalar equality. ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ca
        _gx_ca.register(spark)
        _light_ca = [r["a"] for r in spark.sql(
            "SELECT gbx_bng_cellarea(cell) AS a FROM _bng_cell_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_ca
        _hx_ca.register(spark)
        _heavy_ca = [r["a"] for r in spark.sql(
            "SELECT gbx_bng_cellarea(cell) AS a FROM _bng_cell_parity_v").collect()]
        _ca_ok = (len(_light_ca) == len(_heavy_ca) > 0 and _light_ca == _heavy_ca)
        _v = ("BNG CELLAREA PARITY: PASS (%d cells, exact equality)" % len(_light_ca)
              if _ca_ok else "BNG CELLAREA PARITY: FAIL -- area mismatch")
    except Exception as _pe:
        _v = "BNG CELLAREA PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CELLAREA PARITY: PASS"), _v
    # --- distance / euclideandistance: exact scalar equality (shared pair corpus). ---
    try:
        _pdata, _pschema = _cv.generate_bng_cell_pairs(_BNG_N_ROWS, res=_BNG_CELL_RES)
        spark.createDataFrame(_pdata, schema=_pschema).createOrReplaceTempView(
            "_bng_pair_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_di
        _gx_di.register(spark)
        _light_di = [r["d"] for r in spark.sql(
            "SELECT gbx_bng_distance(cell_a, cell_b) AS d "
            "FROM _bng_pair_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_di
        _hx_di.register(spark)
        _heavy_di = [r["d"] for r in spark.sql(
            "SELECT gbx_bng_distance(cell_a, cell_b) AS d "
            "FROM _bng_pair_parity_v").collect()]
        _di_ok = (len(_light_di) == len(_heavy_di) > 0 and _light_di == _heavy_di)
        _v = ("BNG DISTANCE PARITY: PASS (%d pairs, exact equality)" % len(_light_di)
              if _di_ok else "BNG DISTANCE PARITY: FAIL -- distance mismatch")
    except Exception as _pe:
        _v = "BNG DISTANCE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG DISTANCE PARITY: PASS"), _v
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ed
        _gx_ed.register(spark)
        _light_ed = [r["d"] for r in spark.sql(
            "SELECT gbx_bng_euclideandistance(cell_a, cell_b) AS d "
            "FROM _bng_pair_parity_v").collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_ed
        _hx_ed.register(spark)
        _heavy_ed = [r["d"] for r in spark.sql(
            "SELECT gbx_bng_euclideandistance(cell_a, cell_b) AS d "
            "FROM _bng_pair_parity_v").collect()]
        _ed_ok = (len(_light_ed) == len(_heavy_ed) > 0 and _light_ed == _heavy_ed)
        _v = ("BNG EUCLIDEANDISTANCE PARITY: PASS (%d pairs, exact equality)"
              % len(_light_ed)
              if _ed_ok else "BNG EUCLIDEANDISTANCE PARITY: FAIL -- distance mismatch")
    except Exception as _pe:
        _v = "BNG EUCLIDEANDISTANCE PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG EUCLIDEANDISTANCE PARITY: PASS"), _v
    # --- eastnorthasbng: exact STRING cell-id equality. ---
    try:
        _edata, _eschema = _cv.generate_bng_eastnorth(_BNG_N_ROWS)
        spark.createDataFrame(_edata, schema=_eschema).createOrReplaceTempView(
            "_bng_eastnorth_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_en
        _gx_en.register(spark)
        _light_en = [r["cell"] for r in spark.sql(
            "SELECT gbx_bng_eastnorthasbng(e, n, '%s') AS cell "
            "FROM _bng_eastnorth_parity_v" % _BNG_CELL_RES).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_en
        _hx_en.register(spark)
        _heavy_en = [r["cell"] for r in spark.sql(
            "SELECT gbx_bng_eastnorthasbng(e, n, '%s') AS cell "
            "FROM _bng_eastnorth_parity_v" % _BNG_CELL_RES).collect()]
        _en_ok = (len(_light_en) == len(_heavy_en) > 0 and _light_en == _heavy_en)
        _v = ("BNG EASTNORTHASBNG PARITY: PASS (%d points, exact id equality)"
              % len(_light_en)
              if _en_ok else "BNG EASTNORTHASBNG PARITY: FAIL -- cell id mismatch")
    except Exception as _pe:
        _v = "BNG EASTNORTHASBNG PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG EASTNORTHASBNG PARITY: PASS"), _v
    # --- kloop: exact sorted cell-set per row (shared single-cell corpus). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_kl
        _gx_kl.register(spark)
        _light_kl = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_kloop(cell, %d) AS ring FROM _bng_cell_parity_v"
            % _BNG_KLOOP_K).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_kl
        _hx_kl.register(spark)
        _heavy_kl = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_kloop(cell, %d) AS ring FROM _bng_cell_parity_v"
            % _BNG_KLOOP_K).collect()]
        _kl_ok = (len(_light_kl) == len(_heavy_kl) > 0 and _light_kl == _heavy_kl)
        _v = ("BNG KLOOP PARITY: PASS (%d rows, exact set equality, k=%d)"
              % (len(_light_kl), _BNG_KLOOP_K)
              if _kl_ok else "BNG KLOOP PARITY: FAIL -- loop set mismatch")
    except Exception as _pe:
        _v = "BNG KLOOP PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG KLOOP PARITY: PASS"), _v
    # --- geomkring / geomkloop: exact sorted cell-set per row (shared geom corpus). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_gkr
        _gx_gkr.register(spark)
        _light_gkr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_geomkring(geom, '%s', %d) AS ring FROM _bng_geom_parity_v"
            % (_BNG_GEOM_RES, _BNG_GEOMK_K)).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_gkr
        _hx_gkr.register(spark)
        _heavy_gkr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_geomkring(geom, '%s', %d) AS ring FROM _bng_geom_parity_v"
            % (_BNG_GEOM_RES, _BNG_GEOMK_K)).collect()]
        _gkr_ok = (len(_light_gkr) == len(_heavy_gkr) > 0 and _light_gkr == _heavy_gkr)
        _v = ("BNG GEOMKRING PARITY: PASS (%d rows, exact set equality, k=%d)"
              % (len(_light_gkr), _BNG_GEOMK_K)
              if _gkr_ok else "BNG GEOMKRING PARITY: FAIL -- ring set mismatch")
    except Exception as _pe:
        _v = "BNG GEOMKRING PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG GEOMKRING PARITY: PASS"), _v
    try:
        from databricks.labs.gbx.pygx import functions as _gx_gkl
        _gx_gkl.register(spark)
        _light_gkl = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_geomkloop(geom, '%s', %d) AS ring FROM _bng_geom_parity_v"
            % (_BNG_GEOM_RES, _BNG_GEOMK_K)).collect()]
        from databricks.labs.gbx.gridx.bng import functions as _hx_gkl
        _hx_gkl.register(spark)
        _heavy_gkl = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_bng_geomkloop(geom, '%s', %d) AS ring FROM _bng_geom_parity_v"
            % (_BNG_GEOM_RES, _BNG_GEOMK_K)).collect()]
        _gkl_ok = (len(_light_gkl) == len(_heavy_gkl) > 0 and _light_gkl == _heavy_gkl)
        _v = ("BNG GEOMKLOOP PARITY: PASS (%d rows, exact set equality, k=%d)"
              % (len(_light_gkl), _BNG_GEOMK_K)
              if _gkl_ok else "BNG GEOMKLOOP PARITY: FAIL -- loop set mismatch")
    except Exception as _pe:
        _v = "BNG GEOMKLOOP PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG GEOMKLOOP PARITY: PASS"), _v
    # --- cellintersection / cellunion: decoded .chip geom sym-diff area < 1e-6
    # (same-cell chip pairs -> the whole cell either way). ---
    try:
        _cpdata, _cpschema = _cv.generate_bng_chip_pairs(_BNG_N_ROWS, res=_BNG_CELL_RES)
        spark.createDataFrame(_cpdata, schema=_cpschema).createOrReplaceTempView(
            "_bng_chippair_parity_v")
        _ci_sql = ("SELECT gbx_bng_cellintersection("
                   "struct(lid, lcore, lchip), struct(rid, rcore, rchip)).chip AS c "
                   "FROM _bng_chippair_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ci
        _gx_ci.register(spark)
        _light_ci = [bytes(r["c"]) for r in spark.sql(_ci_sql).collect()
                     if r["c"] is not None]
        from databricks.labs.gbx.gridx.bng import functions as _hx_ci
        _hx_ci.register(spark)
        _heavy_ci = [bytes(r["c"]) for r in spark.sql(_ci_sql).collect()
                     if r["c"] is not None]
        _ci_ok = len(_light_ci) == len(_heavy_ci) > 0
        if _ci_ok:
            for _lb, _hb in zip(_light_ci, _heavy_ci):
                if _shp_wkb.loads(_lb).symmetric_difference(
                        _shp_wkb.loads(_hb)).area >= 1e-6:
                    _ci_ok = False; break
        _v = ("BNG CELLINTERSECTION PARITY: PASS (%d pairs, chip geom < 1e-6)"
              % len(_light_ci)
              if _ci_ok else "BNG CELLINTERSECTION PARITY: FAIL -- chip geom mismatch")
    except Exception as _pe:
        _v = "BNG CELLINTERSECTION PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CELLINTERSECTION PARITY: PASS"), _v
    try:
        _cu_sql = ("SELECT gbx_bng_cellunion("
                   "struct(lid, lcore, lchip), struct(rid, rcore, rchip)).chip AS c "
                   "FROM _bng_chippair_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_cu
        _gx_cu.register(spark)
        _light_cu = [bytes(r["c"]) for r in spark.sql(_cu_sql).collect()
                     if r["c"] is not None]
        from databricks.labs.gbx.gridx.bng import functions as _hx_cu
        _hx_cu.register(spark)
        _heavy_cu = [bytes(r["c"]) for r in spark.sql(_cu_sql).collect()
                     if r["c"] is not None]
        _cu_ok = len(_light_cu) == len(_heavy_cu) > 0
        if _cu_ok:
            for _lb, _hb in zip(_light_cu, _heavy_cu):
                if _shp_wkb.loads(_lb).symmetric_difference(
                        _shp_wkb.loads(_hb)).area >= 1e-6:
                    _cu_ok = False; break
        _v = ("BNG CELLUNION PARITY: PASS (%d pairs, chip geom < 1e-6)"
              % len(_light_cu)
              if _cu_ok else "BNG CELLUNION PARITY: FAIL -- chip geom mismatch")
    except Exception as _pe:
        _v = "BNG CELLUNION PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CELLUNION PARITY: PASS"), _v
    # --- cellintersection_agg: per-group decoded chip-GEOMETRY equality (sym-diff
    # area < 1e-6), identical to the cellunion_agg gate over the SAME-CELL groups. ---
    try:
        _aidata, _aischema = _cv.generate_bng_chip_groups(_BNG_N_ROWS, res=_BNG_AGG_RES)
        spark.createDataFrame(_aidata, schema=_aischema).createOrReplaceTempView(
            "_bng_agg_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_ai
        _gx_ai.register(spark)
        _light_ai = {r["group"]: bytes(r["u"]) for r in spark.sql(
            "SELECT group, gbx_bng_cellintersection_agg(chip) AS u "
            "FROM _bng_agg_parity_v GROUP BY group").collect() if r["u"] is not None}
        from databricks.labs.gbx.gridx.bng import functions as _hx_ai
        _hx_ai.register(spark)
        _heavy_ai = {r["group"]: bytes(r["u"]["chip"]) for r in spark.sql(
            "SELECT group, gbx_bng_cellintersection_agg(chip) AS u "
            "FROM _bng_agg_parity_v GROUP BY group").collect()
            if r["u"] is not None and r["u"]["chip"] is not None}
        _ai_ok = (_light_ai.keys() == _heavy_ai.keys() and len(_light_ai) > 0)
        if _ai_ok:
            for _k in _light_ai:
                _lg = _shp_wkb.loads(_light_ai[_k]); _hg = _shp_wkb.loads(_heavy_ai[_k])
                if _lg.symmetric_difference(_hg).area >= 1e-6:
                    _ai_ok = False; break
        _v = ("BNG CELLINTERSECTION_AGG PARITY: PASS (%d groups, decoded chip-geom equality)"
              % len(_light_ai)
              if _ai_ok else "BNG CELLINTERSECTION_AGG PARITY: FAIL -- chip geometry mismatch")
    except Exception as _pe:
        _v = "BNG CELLINTERSECTION_AGG PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("BNG CELLINTERSECTION_AGG PARITY: PASS"), _v
    # --- *explode UDTFs: array-equivalence pairing. Light explodes via LATERAL
    # (flatten ALL rows -> sorted global cellid set); heavy explodes the equivalent
    # ARRAY function (kringexplode<->kring, kloopexplode<->kloop, geomk*explode<->
    # geomk*, tessellateexplode<->tessellate chip cellids). Compare sorted global sets.
    def _bng_explode_gate(_name, _light_lateral, _heavy_array_sql):
        try:
            from databricks.labs.gbx.pygx import functions as _gx_ex
            _gx_ex.register(spark)
            _lset = sorted(r["cellid"] for r in spark.sql(_light_lateral).collect())
            from databricks.labs.gbx.gridx.bng import functions as _hx_ex
            _hx_ex.register(spark)
            _hset = sorted(r["cellid"] for r in spark.sql(_heavy_array_sql).collect())
            _ok = (len(_lset) == len(_hset) > 0 and _lset == _hset)
            _vv = ("BNG %s PARITY: PASS (%d flat cells, global set equality)"
                   % (_name, len(_lset))
                   if _ok else "BNG %s PARITY: FAIL -- exploded set mismatch" % _name)
        except Exception as _pe:
            _vv = "BNG %s PARITY: FAIL -- %s: %s" % (_name, type(_pe).__name__, _pe)
        print(_vv); _verdicts.append(_vv)
        assert _vv.startswith("BNG %s PARITY: PASS" % _name), _vv
    # kringexplode <-> kring (single-cell corpus)
    _bng_explode_gate(
        "KRINGEXPLODE",
        "SELECT t.cellid FROM _bng_cell_parity_v, "
        "LATERAL gbx_bng_kringexplode(cell, %d) t" % _BNG_KRING_K,
        "SELECT explode(gbx_bng_kring(cell, %d)) AS cellid FROM _bng_cell_parity_v"
        % _BNG_KRING_K)
    # kloopexplode <-> kloop
    _bng_explode_gate(
        "KLOOPEXPLODE",
        "SELECT t.cellid FROM _bng_cell_parity_v, "
        "LATERAL gbx_bng_kloopexplode(cell, %d) t" % _BNG_KLOOP_K,
        "SELECT explode(gbx_bng_kloop(cell, %d)) AS cellid FROM _bng_cell_parity_v"
        % _BNG_KLOOP_K)
    # geomkringexplode <-> geomkring (geom corpus)
    _bng_explode_gate(
        "GEOMKRINGEXPLODE",
        "SELECT t.cellid FROM _bng_geom_parity_v, "
        "LATERAL gbx_bng_geomkringexplode(geom, '%s', %d) t"
        % (_BNG_GEOM_RES, _BNG_GEOMK_K),
        "SELECT explode(gbx_bng_geomkring(geom, '%s', %d)) AS cellid "
        "FROM _bng_geom_parity_v" % (_BNG_GEOM_RES, _BNG_GEOMK_K))
    # geomkloopexplode <-> geomkloop
    _bng_explode_gate(
        "GEOMKLOOPEXPLODE",
        "SELECT t.cellid FROM _bng_geom_parity_v, "
        "LATERAL gbx_bng_geomkloopexplode(geom, '%s', %d) t"
        % (_BNG_GEOM_RES, _BNG_GEOMK_K),
        "SELECT explode(gbx_bng_geomkloop(geom, '%s', %d)) AS cellid "
        "FROM _bng_geom_parity_v" % (_BNG_GEOM_RES, _BNG_GEOMK_K))
    # tessellateexplode <-> tessellate (chip cellids)
    _bng_explode_gate(
        "TESSELLATEEXPLODE",
        "SELECT t.cellid FROM _bng_geom_parity_v, "
        "LATERAL gbx_bng_tessellateexplode(geom, '%s') t" % _BNG_GEOM_RES,
        "SELECT col.cellid AS cellid FROM (SELECT explode("
        "gbx_bng_tessellate(geom, '%s')) AS col FROM _bng_geom_parity_v)"
        % _BNG_GEOM_RES)
if _bng_rows:
    _df_bng = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'grid'"
    )
    try:
        display(_df_bng)
    except Exception:
        _df_bng.show(100, truncate=False)
    _md = results.summarize(_bng_rows)
    _show_md(f"BNG grid benchmark -- {RUN_ID}", _md)
"""

_CELL_GRID_CUSTOM = """# Custom grid benchmark: light pygx vs heavy gridx.custom, exact-output parity.
# All 7 gbx_custom_* functions: grid (8 scalar args -> validated STRUCT) / pointascell
# (geom first-coord -> BIGINT cell) / polyfill (geom -> ARRAY<BIGINT>) / kring (scalar
# BIGINT cell-in -> ARRAY<BIGINT>) / cellaswkb (cell -> WKB polygon) / cellaswkt (cell ->
# WKT polygon string) / centroid (cell -> WKB point). Both tiers expose the SAME
# gbx_custom_* SQL names, so light is collected BEFORE heavy re-registers (the later
# registration overwrites the UDF) -- the same ordering trick as the quadbin/BNG parity
# cells. Every leg builds the SAME grid struct via gbx_custom_grid
# (corpus_vector.CUSTOM_GRID_SQL = 0,1000000,0,1000000,2,1000,1000,27700) so both tiers
# consume an identical STRUCT. Cell ids are BIGINT; geometry outputs are plain WKB/WKT, no
# SRID (the grid's srid is metadata only). pointascell uses the geometry's FIRST
# coordinate (heavy geom.getCoordinate), NOT the centroid (unlike BNG). gbx_custom_grid is
# the validating STRUCT constructor consumed by every other op -- benched here as a scalar
# construction leg, and parity-compared on its struct field tuple.
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from shapely import wkb as _shp_wkb
_custom_rows = []
_CUSTOM_N_ROWS = 1000
_CUSTOM_RES = 0        # pointascell / polyfill / cell resolution (res 0 -> 1000m cells)
_CUSTOM_KRING_K = 1    # kring radius
_CUSTOM_GRID_SQL = _cv.CUSTOM_GRID_SQL
_CUSTOM_N_LEGS = 7     # legs per tier (keep in sync with the appends below)
def _custom_legs(_api):
    # Run all 7 custom legs for one tier, in a fixed order, returning the rows.
    _out = []
    _out.append(_rd.run_custom_grid(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, where="cluster"))
    _out.append(_rd.run_custom_pointascell(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, where="cluster"))
    _out.append(_rd.run_custom_polyfill(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, where="cluster"))
    _out.append(_rd.run_custom_kring(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, k=_CUSTOM_KRING_K, where="cluster"))
    _out.append(_rd.run_custom_cellaswkb(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, where="cluster"))
    _out.append(_rd.run_custom_cellaswkt(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, where="cluster"))
    _out.append(_rd.run_custom_centroid(
        spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED, api=_api,
        n_rows=_CUSTOM_N_ROWS, res=_CUSTOM_RES, where="cluster"))
    return _out
if LIGHTWEIGHT:
    for _r in _custom_legs("lightweight"):
        _sink([_r]); lw.append(_r); _custom_rows.append(_r)
if HEAVYWEIGHT:
    for _r in _custom_legs("heavyweight"):
        _sink([_r]); hw.append(_r); _custom_rows.append(_r)
# Exact-output parity (hard gate): rebuild the SAME deterministic corpora, collect each tier
# through its native SQL surface, and compare. Cell ids/sets: exact equality. Decoded geometry: 1e-6.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _verdicts = []
    # --- pointascell: exact BIGINT cell-id equality (light BEFORE heavy: shared SQL name). ---
    try:
        _pdata, _pschema = _cv.generate_custom_points(_CUSTOM_N_ROWS)
        _pdf = spark.createDataFrame(_pdata, schema=_pschema)
        _pdf.createOrReplaceTempView("_custom_pac_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pac
        _gx_pac.register(spark)
        _light_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_custom_pointascell(geom, %s, %d) AS cell FROM _custom_pac_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_RES)).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_pac
        _hx_pac.register(spark)
        _heavy_cells = [r["cell"] for r in spark.sql(
            "SELECT gbx_custom_pointascell(geom, %s, %d) AS cell FROM _custom_pac_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_RES)).collect()]
        _pac_ok = (len(_light_cells) == len(_heavy_cells) > 0
                   and _light_cells == _heavy_cells)
        _v = ("CUSTOM POINTASCELL PARITY: PASS (%d cells, exact id equality)" % len(_light_cells)
              if _pac_ok else "CUSTOM POINTASCELL PARITY: FAIL -- cell id mismatch")
    except Exception as _pe:
        _v = "CUSTOM POINTASCELL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM POINTASCELL PARITY: PASS"), _v
    # --- polyfill: exact per-row cell-SET equality. ---
    try:
        _gdata, _gschema = _cv.generate_custom_polygons(_CUSTOM_N_ROWS)
        _gdf = spark.createDataFrame(_gdata, schema=_gschema)
        _gdf.createOrReplaceTempView("_custom_geom_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_pf
        _gx_pf.register(spark)
        _light_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_custom_polyfill(geom, %s, %d) AS cells FROM _custom_geom_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_RES)).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_pf
        _hx_pf.register(spark)
        _heavy_pf = [sorted(r["cells"]) for r in spark.sql(
            "SELECT gbx_custom_polyfill(geom, %s, %d) AS cells FROM _custom_geom_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_RES)).collect()]
        _pf_ok = (len(_light_pf) == len(_heavy_pf) > 0 and _light_pf == _heavy_pf)
        _ncells = sum(len(c) for c in _light_pf)
        _v = ("CUSTOM POLYFILL PARITY: PASS (%d rows, %d cells, exact set equality)"
              % (len(_light_pf), _ncells)
              if _pf_ok else "CUSTOM POLYFILL PARITY: FAIL -- cell set mismatch")
    except Exception as _pe:
        _v = "CUSTOM POLYFILL PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM POLYFILL PARITY: PASS"), _v
    # --- kring: exact sorted cell-set per row (fixed k), shared single-cell corpus. ---
    try:
        _cdata, _cschema = _cv.generate_custom_cells(_CUSTOM_N_ROWS, res=_CUSTOM_RES)
        _cdf = spark.createDataFrame(_cdata, schema=_cschema)
        _cdf.createOrReplaceTempView("_custom_cell_parity_v")
        from databricks.labs.gbx.pygx import functions as _gx_kr
        _gx_kr.register(spark)
        _light_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_custom_kring(cell, %s, %d) AS ring FROM _custom_cell_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_KRING_K)).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_kr
        _hx_kr.register(spark)
        _heavy_kr = [sorted(r["ring"]) for r in spark.sql(
            "SELECT gbx_custom_kring(cell, %s, %d) AS ring FROM _custom_cell_parity_v"
            % (_CUSTOM_GRID_SQL, _CUSTOM_KRING_K)).collect()]
        _kr_ok = (len(_light_kr) == len(_heavy_kr) > 0 and _light_kr == _heavy_kr)
        _nk = sum(len(c) for c in _light_kr)
        _v = ("CUSTOM KRING PARITY: PASS (%d rows, %d cells, exact set equality, k=%d)"
              % (len(_light_kr), _nk, _CUSTOM_KRING_K)
              if _kr_ok else "CUSTOM KRING PARITY: FAIL -- ring set mismatch")
    except Exception as _pe:
        _v = "CUSTOM KRING PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM KRING PARITY: PASS"), _v
    # --- cellaswkb: decoded polygon sym-diff area < 1e-6 (shared single-cell corpus). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_wb
        _gx_wb.register(spark)
        _light_wb = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_custom_cellaswkb(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_wb
        _hx_wb.register(spark)
        _heavy_wb = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_custom_cellaswkb(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        _wb_ok = len(_light_wb) == len(_heavy_wb) > 0
        if _wb_ok:
            for _lb, _hb in zip(_light_wb, _heavy_wb):
                if _shp_wkb.loads(_lb).symmetric_difference(
                        _shp_wkb.loads(_hb)).area >= 1e-6:
                    _wb_ok = False; break
        _v = ("CUSTOM CELLASWKB PARITY: PASS (%d cells, decoded geom < 1e-6)" % len(_light_wb)
              if _wb_ok else "CUSTOM CELLASWKB PARITY: FAIL -- geometry mismatch")
    except Exception as _pe:
        _v = "CUSTOM CELLASWKB PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM CELLASWKB PARITY: PASS"), _v
    # --- cellaswkt: decoded polygon (via WKT) sym-diff area < 1e-6 (shared cell corpus). ---
    try:
        from shapely import wkt as _shp_wkt
        from databricks.labs.gbx.pygx import functions as _gx_wt
        _gx_wt.register(spark)
        _light_wt = [r["g"] for r in spark.sql(
            "SELECT gbx_custom_cellaswkt(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_wt
        _hx_wt.register(spark)
        _heavy_wt = [r["g"] for r in spark.sql(
            "SELECT gbx_custom_cellaswkt(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        _wt_ok = len(_light_wt) == len(_heavy_wt) > 0
        if _wt_ok:
            for _lt, _ht in zip(_light_wt, _heavy_wt):
                if _shp_wkt.loads(_lt).symmetric_difference(
                        _shp_wkt.loads(_ht)).area >= 1e-6:
                    _wt_ok = False; break
        _v = ("CUSTOM CELLASWKT PARITY: PASS (%d cells, decoded geom < 1e-6)" % len(_light_wt)
              if _wt_ok else "CUSTOM CELLASWKT PARITY: FAIL -- geometry mismatch")
    except Exception as _pe:
        _v = "CUSTOM CELLASWKT PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM CELLASWKT PARITY: PASS"), _v
    # --- centroid: decoded point distance < 1e-6 (shared cell corpus). ---
    try:
        from databricks.labs.gbx.pygx import functions as _gx_ct
        _gx_ct.register(spark)
        _light_ct = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_custom_centroid(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        from databricks.labs.gbx.gridx.custom import functions as _hx_ct
        _hx_ct.register(spark)
        _heavy_ct = [bytes(r["g"]) for r in spark.sql(
            "SELECT gbx_custom_centroid(cell, %s) AS g FROM _custom_cell_parity_v"
            % _CUSTOM_GRID_SQL).collect()]
        _ct_ok = len(_light_ct) == len(_heavy_ct) > 0
        if _ct_ok:
            for _lb, _hb in zip(_light_ct, _heavy_ct):
                if _shp_wkb.loads(_lb).distance(_shp_wkb.loads(_hb)) >= 1e-6:
                    _ct_ok = False; break
        _v = ("CUSTOM CENTROID PARITY: PASS (%d cells, point < 1e-6)" % len(_light_ct)
              if _ct_ok else "CUSTOM CENTROID PARITY: FAIL -- point mismatch")
    except Exception as _pe:
        _v = "CUSTOM CENTROID PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM CENTROID PARITY: PASS"), _v
    # --- grid: exact struct field-tuple equality. gbx_custom_grid builds the SAME
    # validated grid STRUCT in both tiers, so the 8 named fields must match exactly. ---
    try:
        _GRID_FIELDS = ["bound_x_min", "bound_x_max", "bound_y_min", "bound_y_max",
                        "cell_splits", "root_cell_size_x", "root_cell_size_y", "srid"]
        def _grid_tuple(_g):
            return tuple(_g[_f] for _f in _GRID_FIELDS)
        from databricks.labs.gbx.pygx import functions as _gx_gr
        _gx_gr.register(spark)
        _light_gr = _grid_tuple(spark.sql(
            "SELECT %s AS grid" % _CUSTOM_GRID_SQL).head(1)[0]["grid"])
        from databricks.labs.gbx.gridx.custom import functions as _hx_gr
        _hx_gr.register(spark)
        _heavy_gr = _grid_tuple(spark.sql(
            "SELECT %s AS grid" % _CUSTOM_GRID_SQL).head(1)[0]["grid"])
        _gr_ok = _light_gr == _heavy_gr
        _v = ("CUSTOM GRID PARITY: PASS (struct field tuple exact: %s)" % (_light_gr,)
              if _gr_ok else "CUSTOM GRID PARITY: FAIL -- struct mismatch %s vs %s"
              % (_light_gr, _heavy_gr))
    except Exception as _pe:
        _v = "CUSTOM GRID PARITY: FAIL -- %s: %s" % (type(_pe).__name__, _pe)
    print(_v); _verdicts.append(_v)
    assert _v.startswith("CUSTOM GRID PARITY: PASS"), _v
    _show_md("Custom grid parity -- " + RUN_ID, "\\n".join("- " + _x for _x in _verdicts))
if _custom_rows:
    _df_custom = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'grid'"
    )
    try:
        display(_df_custom)
    except Exception:
        _df_custom.show(100, truncate=False)
    _md = results.summarize(_custom_rows)
    _show_md(f"Custom grid benchmark -- {RUN_ID}", _md)
"""

_CELL_FANOUT = """# Fan-out UDTF benchmark: light pyrx (streaming UDTF) vs heavy rasterx (generator/array),
# flatten-BOTH parity -- each tier's output is flattened to comparable flat rows, then the
# flat row counts are compared per function (hard gate).
from databricks.labs.gbx.bench import readers as _rd
_fanout_rows = []
_fanout_fns = list(_rd.FANOUT_FUNCTIONS)  # all 8 streaming UDTFs
# For parity: collect (fn, api, flat_output_count) to compare light vs heavy per-fn.
_fanout_counts = {}  # (fn, api) -> flat row count
for _ffn in _fanout_fns:
    if LIGHTWEIGHT:
        _r = _rd.run_fanout_udtf(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                  api="lightweight", fn=_ffn, scale=FANOUT_SCALE,
                                  where="cluster")
        _sink([_r]); lw.append(_r); _fanout_rows.append(_r)
        _fanout_counts[(_ffn, "lightweight")] = _r.rows
    if HEAVYWEIGHT:
        _r = _rd.run_fanout_udtf(spark, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                  api="heavyweight", fn=_ffn, scale=FANOUT_SCALE,
                                  where="cluster")
        _sink([_r]); hw.append(_r); _fanout_rows.append(_r)
        _fanout_counts[(_ffn, "heavyweight")] = _r.rows
# Flatten-both parity check: compare light vs heavy FLAT output row counts per fn.
if LIGHTWEIGHT and HEAVYWEIGHT:
    _parity_ok = True
    _parity_msgs = []
    for _ffn in _fanout_fns:
        _lc = _fanout_counts.get((_ffn, "lightweight"), None)
        _hc = _fanout_counts.get((_ffn, "heavyweight"), None)
        if _lc is None or _hc is None:
            continue
        if _lc == _hc and _lc > 0:
            _parity_msgs.append(f"{_ffn}: PASS (light=heavy={_lc} flat rows)")
        else:
            _parity_ok = False
            _parity_msgs.append(f"{_ffn}: FAIL light={_lc} heavy={_hc}")
    _verdict = "FANOUT PARITY: " + (
        "PASS" if _parity_ok else "FAIL"
    ) + " -- " + "; ".join(_parity_msgs)
    print(_verdict)
    assert _verdict.startswith("FANOUT PARITY: PASS"), _verdict
if _fanout_rows:
    _df_fanout = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'fanout'"
    )
    try:
        display(_df_fanout)
    except Exception:
        _df_fanout.show(100, truncate=False)
    _md = results.summarize(_fanout_rows)
    _show_md(f"fanout UDTF benchmark -- {RUN_ID}", _md)
"""

# Cell-set constants: a "fanout-only" run uses just _CELL_FANOUT.
FANOUT_ONLY = {"fanout"}
BENCHMARK_FANOUT = {"fanout"}

_CELL_GROUPED_FILE = """# Grouped FILE-amortization benchmark: rst_clip_grouped + pixel-op _grouped fns over a
# MULTIWINDOW COG corpus, across three tile modes (materialized / virtual+FILE-off /
# virtual+FILE-on). The win = grouped_tile_map amortizing one source .open() across a
# source's windows; FILE-on vs FILE-off is the amortization spread (per-tile ms). LIGHT-only
# (pure Python; no JAR). Skips cleanly when the multiwindow manifest is absent.
import os as _os
from databricks.labs.gbx.bench import grouped_file as _gf
_gfile_manifest = _gf.resolve_manifest_path(corpus_path=MULTIWINDOW_CORPUS)
if not _os.path.exists(_gfile_manifest):
    print(
        "grouped-file benchmark SKIPPED: no manifest at "
        + str(_gfile_manifest)
        + " (stage the multiwindow COG corpus first via "
        + "datagen.generate_cog_multiwindow_corpus)"
    )
else:
    _gfile_rows = _gf.run_grouped_file(
        spark,
        corpus_path=MULTIWINDOW_CORPUS,
        warmup=SPARK_WARMUP,
        measured=SPARK_MEASURED,
        run_id=RUN_ID,
        where="cluster",
    )
    _sink(_gfile_rows)
    lw.extend(_gfile_rows)
    if _gfile_rows:
        _df_gfile = spark.sql(
            f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND category = 'grouped-file'"
        )
        try:
            display(_df_gfile)
        except Exception:
            _df_gfile.show(100, truncate=False)
        _md = results.summarize(_gfile_rows)
        _show_md(f"grouped FILE-amortization benchmark -- {RUN_ID}", _md)
"""

_CELL_FILE_MATRIX = """# FILE-access matrix: GeoTIFF + GeoPackage reads across file_mode in ("fuse","external","managed").
# Isolation: each mode's DataFrame is built fresh inside its own loop iteration so no warm
# table/partition state carries from one mode to the next. GeoTIFF reuses {CORPUS}/rows
# (10k tiles, exceeds cluster slots -- saturation preserved). GeoPackage stages
# {GPKG_CORPUS}/copies_{N}/ via stage_gpkg_bench_corpus (80-copy bracket ~ cluster slots).
# external/managed yield na_by_design on FUSE-only tiers (clean skip, no error).
# managed mode requires a pre-written FILE-column table; when FILE_FILESPACE is empty the
# Volume path is passed for managed -- access='managed'+location raises ValueError ->
# na_by_design, matching FUSE-tier behavior. Each ResultRow is _sink'd immediately (serialized).
import os as _os
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
_fm_rows = []
_fm_gtiff_src = f"{CORPUS}/rows"
# Stage GeoPackage FILE matrix corpus (idempotent: existing copies_N/ dirs reused).
_fm_gpkg_dirs = _cv.stage_gpkg_bench_corpus(
    spark, GPKG_CORPUS, rows=100000, copies_ladder=(10, 80, 160)
)
# Use the ~80-slot bracket for the mode sweep; fall back to the first available dir.
_fm_gpkg_src = _fm_gpkg_dirs.get(80) or next(iter(_fm_gpkg_dirs.values()), None)
for _fm in ("fuse", "external", "managed"):
    # Isolation: managed reads require a FILE-managed table (not a raw Volume path).
    # Use a per-mode filespace sub-path when FILE_FILESPACE is set so each mode's
    # table is distinct and no layout advantage carries between modes.
    if _fm == "managed" and FILE_FILESPACE:
        _gt_src = f"{FILE_FILESPACE}/bench_fm_gtiff"
        _gp_src = f"{FILE_FILESPACE}/bench_fm_gpkg"
    else:
        # fuse/external: Volume path (FILE EXTERNAL on capable tiers; fuse auto-fallback).
        # managed without FILE_FILESPACE: Volume path -> access='managed'+location raises
        # ValueError inside run_gtiff_file_read -> na_by_design (clean skip).
        _gt_src = _fm_gtiff_src
        _gp_src = _fm_gpkg_src
    # GeoTIFF FILE read leg.
    _r = _rd.run_gtiff_file_read(
        spark, _gt_src, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
        file_mode=_fm, api="lightweight", where="cluster",
    )
    _sink([_r]); lw.append(_r); _fm_rows.append(_r)
    # GeoPackage FILE read leg (skip if corpus staging failed).
    if _gp_src:
        _r = _rd.run_gpkg_file_read(
            spark, _gp_src, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
            file_mode=_fm, chunk_size=10000, api="lightweight", where="cluster",
        )
        _sink([_r]); lw.append(_r); _fm_rows.append(_r)
if _fm_rows:
    _df_fm = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' "
        f"AND fn IN ('gtiff_file_read','gpkg_file_read')"
    )
    try:
        display(_df_fm)
    except Exception:
        _df_fm.show(100, truncate=False)
    _md = results.summarize(_fm_rows)
    _show_md(f"FILE access matrix (file_mode sweep) -- {RUN_ID}", _md)
"""

_CELL_GPKG_CHUNKSIZE = """# GeoPackage chunkSize sweep: confirms fanout-invariance (partition count stable across
# 1k/10k/100k chunkSizes). Stages {GPKG_CORPUS}/copies_N/ via stage_gpkg_bench_corpus
# (idempotent). Uses fuse mode -- chunkSize is a within-task amortization lever, NOT a
# fanout lever; partition count is file-count bound. The 80-copy bracket sizes input so
# partitions >= cluster slots (saturation). Each ResultRow is _sink'd immediately.
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
_cs_dirs = _cv.stage_gpkg_bench_corpus(
    spark, GPKG_CORPUS, rows=100000, copies_ladder=(10, 80, 160)
)
# 80-copy bracket: saturation (copies >= slots). Fall back to any staged dir.
_cs_src = _cs_dirs.get(80) or next(iter(_cs_dirs.values()), None)
_cs_rows = []
if _cs_src:
    _cs_rows = _rd.run_gpkg_chunksize_sweep(
        spark, _cs_src, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
        file_mode="fuse", chunk_sizes=(1000, 10000, 100000),
        api="lightweight", where="cluster",
    )
    for _r in _cs_rows:
        _sink([_r]); lw.append(_r)
if _cs_rows:
    _df_cs = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' AND fn = 'gpkg_file_read'"
    )
    try:
        display(_df_cs)
    except Exception:
        _df_cs.show(100, truncate=False)
    # Fanout-invariance check: partition count should be constant across chunkSizes.
    _cs_ok = [r for r in _cs_rows if r.status not in ("na_by_design", "error")]
    _cs_parts = [r.input_partitions for r in _cs_ok]
    if len(set(_cs_parts)) > 1:
        print(f"GPKG CHUNKSIZE: WARNING -- partition counts vary: {_cs_parts} (expected stable)")
    elif _cs_parts:
        print(f"GPKG CHUNKSIZE: fanout-invariant (partitions={_cs_parts[0]})")
    _md = results.summarize(_cs_rows)
    _show_md(f"GeoPackage chunkSize sweep -- {RUN_ID}", _md)
"""

_CELL_LAYOUT_SWEEP = """# FILE write layout sweep: GeoTIFF + GeoPackage across layout in ("order","cluster","plain").
# Each layout writes to its OWN isolated target (target_prefix + "_" + layout) so no layout
# benefits from a prior write's on-disk grouping. "cluster" layout runs OPTIMIZE after write.
# GeoTIFF: tile_df built from {CORPUS}/rows (raster_gbx, 10k tiles -- saturation).
# GeoPackage: stages {GPKG_CORPUS}/copies_80/ (idempotent), picks a single .gpkg as source.
# file_mode: "external" when FILE_FILESPACE is set (FILE EXTERNAL tables); "fuse" otherwise.
# na_by_design is returned for external/managed on FUSE-only tiers (clean skip).
# Each ResultRow is _sink'd immediately (serialized).
import glob as _glob
import os as _os
from databricks.labs.gbx.bench import readers as _rd
from databricks.labs.gbx.bench import corpus_vector as _cv
from databricks.labs.gbx.ds.register import register as _ds_reg
_ls_rows = []
# Build GeoTIFF tile_df for the WRITE sweep. The write leg demonstrates write + the layout
# effect; it does NOT need the full read pool. Writing all 10k tiles x 3 layouts x
# (warmup+measured) is ~60k large-blob writes + OPTIMIZE and dominated the leg wall-clock
# (~50 min) for no added signal. Cap to a bounded subset that still exceeds the cluster slot
# count (fair writer fanout), then repartition to re-spread so .limit() cannot collapse the
# write to a single partition (which would starve slots and distort the timing).
_ds_reg(spark)
_ls_gtiff_src = f"{CORPUS}/rows"
_LS_WRITE_TILES = 1000  # >> cluster task slots -> saturates; ~10x less write than the full pool
_ls_tile_df = (
    spark.read.format("raster_gbx")
    .option("filterRegex", r".*\\.tif$")
    .load(_ls_gtiff_src)
    .limit(_LS_WRITE_TILES)
    .repartition(160)
)
# file_mode for FILE tables: use "external" when filespace is provisioned; fuse otherwise.
_ls_file_mode = "external" if FILE_FILESPACE else "fuse"
_ls_filespace = FILE_FILESPACE or None
_ls_gtiff_target = (
    FILE_FILESPACE + "/bench_layout_gtiff" if FILE_FILESPACE else OUT + "/bench_layout_gtiff"
)
_gtiff_sweep = _rd.run_file_write_layout_sweep(
    spark,
    fmt="gtiff", source=_ls_tile_df, target_prefix=_ls_gtiff_target,
    run_id=RUN_ID, warmup=SPARK_WARMUP, measured=SPARK_MEASURED,
    file_mode=_ls_file_mode, filespace=_ls_filespace,
    where="cluster",
)
for _r in _gtiff_sweep:
    _sink([_r]); lw.append(_r); _ls_rows.append(_r)
# GeoPackage layout sweep: stage corpus, pick a single .gpkg as write source.
_ls_gpkg_dirs = _cv.stage_gpkg_bench_corpus(
    spark, GPKG_CORPUS, rows=100000, copies_ladder=(10, 80, 160)
)
_ls_gpkg_dir = _ls_gpkg_dirs.get(80) or next(iter(_ls_gpkg_dirs.values()), None)
if _ls_gpkg_dir:
    _ls_gpkg_files = sorted(_glob.glob(_os.path.join(_ls_gpkg_dir, "*.gpkg")))
    _ls_gpkg_src = _ls_gpkg_files[0] if _ls_gpkg_files else None
    if _ls_gpkg_src:
        _ls_gpkg_target = (
            FILE_FILESPACE + "/bench_layout_gpkg" if FILE_FILESPACE else OUT + "/bench_layout_gpkg"
        )
        _gpkg_sweep = _rd.run_file_write_layout_sweep(
            spark,
            fmt="gpkg", source=_ls_gpkg_src, target_prefix=_ls_gpkg_target,
            run_id=RUN_ID, warmup=SPARK_WARMUP, measured=SPARK_MEASURED,
            file_mode=_ls_file_mode, filespace=_ls_filespace,
            where="cluster",
        )
        for _r in _gpkg_sweep:
            _sink([_r]); lw.append(_r); _ls_rows.append(_r)
if _ls_rows:
    _df_ls = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' "
        f"AND fn IN ('gtiff_file_write','gpkg_file_write')"
    )
    try:
        display(_df_ls)
    except Exception:
        _df_ls.show(100, truncate=False)
    _md = results.summarize(_ls_rows)
    _show_md(f"FILE write layout sweep -- {RUN_ID}", _md)
"""

_CELL_LAYOUT_SCAN = """# Layout scan comparison: sequential-scan cost across write layouts (order/cluster/plain).
# Reads the FILE tables written by the layout sweep via read_file_table and times df.count().
# Covers both formats written by the sweep: GeoTIFF (bench_layout_gtiff_*) and
# GeoPackage (bench_layout_gpkg_*) -- symmetric with _CELL_LAYOUT_SWEEP.
# Skip cleanly when FILE_FILESPACE is not set (no FILE tables to scan -- fuse-mode sweep
# produces Volume dirs, not FILE tables, so scan has no applicable tables).
# Each ResultRow is _sink'd immediately (serialized).
from databricks.labs.gbx.bench import readers as _rd
_scan_rows = []
if not FILE_FILESPACE:
    print(
        "LAYOUT SCAN SKIPPED: FILE_FILESPACE not set. "
        "Run the layout sweep with a provisioned filespace to create FILE tables, "
        "then re-run with --layout-scan / --layout-scan-only.",
        flush=True,
    )
else:
    # Tables written by the layout sweep: one per (format x layout).
    # The scan compares sequential-scan cost; the sweep must have run first (same run
    # or a prior run with the same FILE_FILESPACE) so the tables exist.
    # GeoTIFF layouts
    _tables_by_layout_gtiff = {
        "order": FILE_FILESPACE + "/bench_layout_gtiff_order",
        "cluster": FILE_FILESPACE + "/bench_layout_gtiff_cluster",
        "plain": FILE_FILESPACE + "/bench_layout_gtiff_plain",
    }
    _scan_rows_gtiff = _rd.run_layout_scan_comparison(
        spark,
        tables_by_layout=_tables_by_layout_gtiff,
        run_id=RUN_ID,
        warmup=SPARK_WARMUP,
        measured=SPARK_MEASURED,
        file_mode="managed",
        where="cluster",
        include_shuffle_input=True,
    )
    for _r in _scan_rows_gtiff:
        _sink([_r]); lw.append(_r)
    _scan_rows.extend(_scan_rows_gtiff)
    # GeoPackage layouts (symmetric with the gtiff sweep above)
    _tables_by_layout_gpkg = {
        "order": FILE_FILESPACE + "/bench_layout_gpkg_order",
        "cluster": FILE_FILESPACE + "/bench_layout_gpkg_cluster",
        "plain": FILE_FILESPACE + "/bench_layout_gpkg_plain",
    }
    _scan_rows_gpkg = _rd.run_layout_scan_comparison(
        spark,
        tables_by_layout=_tables_by_layout_gpkg,
        run_id=RUN_ID,
        warmup=SPARK_WARMUP,
        measured=SPARK_MEASURED,
        file_mode="managed",
        where="cluster",
        include_shuffle_input=True,
    )
    for _r in _scan_rows_gpkg:
        _sink([_r]); lw.append(_r)
    _scan_rows.extend(_scan_rows_gpkg)
if _scan_rows:
    _df_scan = spark.sql(
        f"SELECT * FROM {TABLE} WHERE run_id = '{RUN_ID}' "
        f"AND category IN ('layout-scan','layout-shuffle-input')"
    )
    try:
        display(_df_scan)
    except Exception:
        _df_scan.show(100, truncate=False)
    _md = results.summarize(_scan_rows)
    _show_md(f"Layout scan comparison -- {RUN_ID}", _md)
"""

_CELL_VECTOR = """# Vector reader + writer benchmark: light *_gbx vs heavy *_ogr (+ row-count parity)
# Two-leg pipeline (scaled branch):
#   Leg 1 (reader): spark.read.format(fmt).load(copies/) -> Delta ingest table (forces
#     materialization of the full multi-file corpus into one queryable table per format).
#   Leg 2 (writer): read from the shared ~WRITER_ROWS Delta table -> single-file export
#     via write.format(fmt) (no coalesce; the two-phase writer merges fragments on commit).
from databricks.labs.gbx.bench import readers as _rd
if VECTOR_SCALE:
    # Scaled corpus: 1M-row seed per format.  The READ path is the copies/ directory so
    # BOTH tiers enumerate N copies and read them in parallel -- a fair all-format
    # light-vs-heavy comparison.  Shapefile and FileGDB copies are self-contained zips
    # (.shp.zip / .gdb.zip) so the heavy OGR dir-read sees each copy as one file.
    # Writer source is the shared pre-materialized Delta table (WRITER_ROWS polygons).
    _vscale_base = f"{CORPUS}/vector-scale"
    # Fresh Delta state each run: drop the writer-source + per-format ingest tables so each
    # timed ingest is a clean CREATE (not an overwrite of a prior version) and nothing lingers
    # in the catalog between repeat benchmarks. Dropped again at the end (cleanup).
    _bench_tbls = ["geospatial_docs.geobrix.bench_vec_wsrc"] + [
        f"geospatial_docs.geobrix.bench_vec_ingest_{_f}"
        for _f in (
            "geojson_gbx", "geojson_ogr", "shapefile_gbx", "shapefile_ogr",
            "gpkg_gbx", "gpkg_ogr", "file_gdb_gbx", "file_gdb_ogr",
        )
    ]
    for _t in _bench_tbls:
        spark.sql(f"DROP TABLE IF EXISTS {_t}")
    _vcases = [
        # (light_fmt, heavy_fmt, read_path, heavy_options)
        ("geojson_gbx",  "geojson_ogr",  _vscale_base + "/geojson_gbx/copies",   {"multi": "false"}),
        ("shapefile_gbx", "shapefile_ogr", _vscale_base + "/shapefile_gbx/copies", {}),
        ("gpkg_gbx",     "gpkg_ogr",     _vscale_base + "/gpkg_gbx/copies",      {}),
        # FileGDB reads the single seed.gdb.zip for BOTH tiers: the heavy OGR reader opens
        # one FileGDB datasource, not a directory of them. (light *_gbx CAN dir-read a folder
        # of .gdb.zip -- a light-only capability noted in the docs.) Single-archive keeps the
        # light-vs-heavy comparison fair and avoids the heavy dir-read error.
        ("file_gdb_gbx", "file_gdb_ogr", _vscale_base + "/file_gdb_gbx/seed.gdb.zip",  {}),
    ]
    if VECTOR_FORMATS:
        _sel = set(f.strip() for f in VECTOR_FORMATS.split(",") if f.strip())
        _vcases = [c for c in _vcases if c[0] in _sel]
    _do_read = VECTOR_LEGS in ("both", "reader")
    _do_write = VECTOR_LEGS in ("both", "writer")
    _wsrc_tbl = "geospatial_docs.geobrix.bench_vec_wsrc"
    # Materialize the writer-source Delta table once (untimed) so all writer legs share it.
    # Only for writer runs -- reader-only jobs skip the 14M materialization.
    if LIGHTWEIGHT and _do_write:
        from databricks.labs.gbx.bench.corpus_vector import generate_polygon_seed
        generate_polygon_seed(spark, WRITER_ROWS).write.format("delta").mode("overwrite").saveAsTable(_wsrc_tbl)
    _vrows = []
    for _lfmt, _hfmt, _vp, _hopts in _vcases:
        if LIGHTWEIGHT and _do_read:
            _it = f"geospatial_docs.geobrix.bench_vec_ingest_{_lfmt}"
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="lightweight", fmt=_lfmt, ingest_table=_it,
                                     where="cluster")
            _sink([_r]); lw.append(_r); _vrows.append(_r)
        # The heavy OGR FileGDB reader reads native Esri .gdb/.gdb.zip but not the GeoBrix-
        # generated .gdb.zip archive -- skip its heavy leg (FileGDB heavy-read is reported as
        # native-archive-only; light reads the generated corpus, incl. a directory of them).
        _heavy_ok = _hfmt != "file_gdb_ogr"
        if HEAVYWEIGHT and _heavy_ok and _do_read:
            _it = f"geospatial_docs.geobrix.bench_vec_ingest_{_hfmt}"
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="heavyweight", fmt=_hfmt, options=(_hopts or None),
                                     ingest_table=_it, where="cluster")
            _sink([_r]); hw.append(_r); _vrows.append(_r)
        if LIGHTWEIGHT and HEAVYWEIGHT and _heavy_ok and _do_read:
            # Non-fatal parity: a single format's mismatch/failure must NOT abort the whole
            # vector bench -- record it and continue so the other formats + the writer leg run.
            try:
                _lc = spark.read.format(_lfmt).load(_vp).count()
                _hr = spark.read.format(_hfmt)
                for _k, _v in (_hopts or {}).items():
                    _hr = _hr.option(_k, _v)
                _hc = _hr.load(_vp).count()
                print(f"VECTOR PARITY {_lfmt}: light={_lc} heavy={_hc} {'PASS' if _lc==_hc else 'FAIL'}")
            except Exception as _e:  # noqa: BLE001
                print(f"VECTOR PARITY {_lfmt}: ERROR {type(_e).__name__}: {str(_e)[:120]}")
        if LIGHTWEIGHT and _do_write:
            _w = _rd.run_vector_write(spark, _wsrc_tbl, f"{OUT}/vecwrite/{_lfmt}", RUN_ID,
                                      SPARK_WARMUP, SPARK_MEASURED, fmt=_lfmt,
                                      src_is_table=True, where="cluster")
            _sink([_w]); lw.append(_w); _vrows.append(_w)
    # Cleanup: drop the bench Delta tables so nothing lingers in the catalog between runs.
    for _t in _bench_tbls:
        spark.sql(f"DROP TABLE IF EXISTS {_t}")
else:
    _vbase = f"{CORPUS}/vector"
    # (light_fmt, heavy_fmt, read_path, seed_path, heavy_options).
    # The heavy geojson_ogr reader defaults to the GeoJSONSeq driver, so force multi=false
    # to read the standard FeatureCollection corpus file (matching the light geojson_gbx
    # GeoJSON-driver reader) for a fair comparison.
    _vcases = [
        ("geojson_gbx",  "geojson_ogr",  _vbase + "/nyc_boroughs.geojson", _vbase + "/nyc_boroughs.geojson", {"multi": "false"}),
        ("shapefile_gbx", "shapefile_ogr", _vbase + "/nyc_subway.shp.zip",  _vbase + "/nyc_subway.shp.zip",   {}),
        ("gpkg_gbx",     "gpkg_ogr",     _vbase + "/nyc_complete.gpkg",    _vbase + "/nyc_complete.gpkg",    {}),
        ("file_gdb_gbx", "file_gdb_ogr", _vbase + "/NYC_Sample.gdb.zip",   _vbase + "/NYC_Sample.gdb.zip",   {}),
    ]
    _vrows = []
    for _lfmt, _hfmt, _vp, _vseed, _hopts in _vcases:
        if LIGHTWEIGHT:
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="lightweight", fmt=_lfmt, where="cluster")
            _sink([_r]); lw.append(_r); _vrows.append(_r)
        if HEAVYWEIGHT:
            _r = _rd.run_format_read(spark, _vp, RUN_ID, SPARK_WARMUP, SPARK_MEASURED,
                                     api="heavyweight", fmt=_hfmt, options=(_hopts or None),
                                     where="cluster")
            _sink([_r]); hw.append(_r); _vrows.append(_r)
        if LIGHTWEIGHT and HEAVYWEIGHT:
            # Non-fatal parity: a single format's mismatch/failure must NOT abort the whole
            # vector bench -- record it and continue so the other formats + the writer leg run.
            try:
                _lc = spark.read.format(_lfmt).load(_vp).count()
                _hr = spark.read.format(_hfmt)
                for _k, _v in (_hopts or {}).items():
                    _hr = _hr.option(_k, _v)
                _hc = _hr.load(_vp).count()
                print(f"VECTOR PARITY {_lfmt}: light={_lc} heavy={_hc} {'PASS' if _lc==_hc else 'FAIL'}")
            except Exception as _e:  # noqa: BLE001
                print(f"VECTOR PARITY {_lfmt}: ERROR {type(_e).__name__}: {str(_e)[:120]}")
        if LIGHTWEIGHT:
            _w = _rd.run_vector_write(spark, _vseed, f"{OUT}/vecwrite/{_lfmt}", RUN_ID,
                                      SPARK_WARMUP, SPARK_MEASURED, fmt=_lfmt, where="cluster")
            _sink([_w]); lw.append(_w); _vrows.append(_w)
if _vrows:
    _md = results.summarize(_vrows)
    _show_md(f"vector reader+writer benchmark -- {RUN_ID}", _md)
"""

_EPILOGUE = """# Wrap-up: durable jsonl shards + per-tier summaries + heavy-vs-light comparison.
all_rows = lw + hw
if lw:
    results.write_jsonl(lw, f"{OUT}/lightweight.jsonl")
    with open(f"{OUT}/lightweight.summary.md", "w") as fh:
        fh.write(results.summarize(lw, pool_size=POOL_SIZE))
if hw:
    results.write_jsonl(hw, f"{OUT}/heavyweight.jsonl")
    with open(f"{OUT}/heavyweight.summary.md", "w") as fh:
        fh.write(results.summarize(hw, pool_size=POOL_SIZE))

# Rows were appended incrementally by _sink as each function finished, so there is no
# bulk append here -- delta_rows is the running count the sink accumulated.
delta_rows = _delta[0]

# Status-led result payload. Lead with a human status + counts; only surface
# `compared` (the heavy-vs-light comparison cell count) when BOTH tiers ran, so a
# lightweight-only run doesn't report a bare `compared: 0` that reads like failure.
ok = sum(1 for r in all_rows if getattr(r, "status", "ok") == "ok")
errors = sum(1 for r in all_rows if getattr(r, "status", "ok") == "error")
result = dict(
    status=("error" if errors else "success"),
    run_id=RUN_ID,
    rows=len(all_rows), ok=ok, errors=errors,
    delta_rows=delta_rows, table=TABLE, out=OUT,
)
if hw and lw:
    cells, unmatched = compare.compare_cells(hw, lw)
    compare.write_csv(cells, f"{OUT}/comparison.csv")
    _cmp_md = compare.summarize_compare(
        cells, unmatched, hw, lw, pool_size=POOL_SIZE
    )
    with open(f"{OUT}/summary.md", "w") as fh:
        fh.write(_cmp_md)
    # final render, both tiers ran: show the compare md + link to the summary.md artifact.
    _show_md(f"Heavy vs light comparison -- {RUN_ID}", _cmp_md, path=f"{OUT}/summary.md")
    result["compared"] = len(cells)
    result["summary"] = f"{OUT}/summary.md"
elif lw:
    result["summary"] = f"{OUT}/lightweight.summary.md"
else:
    result["summary"] = f"{OUT}/heavyweight.summary.md"
"""

# The notebook exit MUST be its own trailing cell. dbutils.notebook.exit() ends the run
# immediately, so when it shares a cell with the compare _show_md(), the job-run UI keeps
# only the exit value (the JSON) and DROPS that cell's displayHTML output -- which is why the
# final heavy-vs-light summary never rendered inline (only its path showed in the JSON), while
# the per-section summaries (their own cells) did. Splitting it lets the render cell complete
# and commit its HTML output before this cell exits. `result` persists across cells (shared
# notebook globals).
_EXIT = """# Emit the status-led JSON exit payload (separate cell -- see _EPILOGUE note).
dbutils.notebook.exit(json.dumps(result))
"""


def build_bench_notebook(cfg: dict) -> dict:
    sel = cfg.get("set", "core")
    functions = cfg["functions"]
    # The Scala heavy runner reads an explicit FUNCTIONS list, not the Python
    # registry. When no explicit functions are named, resolve the selected tier
    # to concrete names so the heavy path honors --set core|full too.
    if not functions:
        from databricks.labs.gbx.bench import spec as _s

        functions = ",".join(f.name for f in _s.select(set=sel))
    setup = _PREAMBLE.format(
        corpus=cfg["corpus"],
        out_dir=cfg["out_dir"],
        table=cfg["table"],
        run_id=cfg["run_id"],
        functions=functions,
        set=sel,
        modes=cfg["modes"],
        row_counts=cfg["row_counts"],
        warmup=cfg["warmup"],
        measured=cfg["measured"],
        spark_warmup=cfg.get("spark_warmup", 0),
        spark_measured=cfg.get("spark_measured", 1),
        partition_size=int(cfg.get("partition_size", 0)),
        truncate=cfg.get("truncate_results", False),
        truncate_all=cfg.get("truncate_all", False),
        resume=bool(cfg.get("resume")),
        fix_errors=bool(cfg.get("fix_errors", True)),
        redo_functions=str(cfg.get("redo_functions", "") or ""),
        lightweight=bool(cfg.get("lightweight")),
        heavyweight=bool(cfg.get("heavyweight")),
        explain_only=bool(cfg.get("explain_only")),
        benchmark_readers=bool(cfg.get("benchmark_readers")),
        readers_only=bool(cfg.get("readers_only")),
        benchmark_pmtiles=bool(cfg.get("benchmark_pmtiles")),
        pmtiles_only=bool(cfg.get("pmtiles_only")),
        benchmark_vector=bool(cfg.get("benchmark_vector")),
        vector_only=bool(cfg.get("vector_only")),
        vector_scale=bool(cfg.get("vector_scale")),
        writer_rows=int(cfg.get("writer_rows", 14000000)),
        vector_legs=str(cfg.get("vector_legs", "both")),
        vector_formats=str(cfg.get("vector_formats", "") or ""),
        benchmark_mvt=bool(cfg.get("benchmark_mvt")),
        mvt_only=bool(cfg.get("mvt_only")),
        benchmark_pmtiles_agg=bool(cfg.get("benchmark_pmtiles_agg")),
        pmtiles_agg_only=bool(cfg.get("pmtiles_agg_only")),
        benchmark_vector_tin=bool(cfg.get("benchmark_vector_tin")),
        vector_tin_only=bool(cfg.get("vector_tin_only")),
        benchmark_grid_quadbin=bool(cfg.get("benchmark_grid_quadbin")),
        grid_quadbin_only=bool(cfg.get("grid_quadbin_only")),
        benchmark_grid_bng=bool(cfg.get("benchmark_grid_bng")),
        grid_bng_only=bool(cfg.get("grid_bng_only")),
        benchmark_grid_custom=bool(cfg.get("benchmark_grid_custom")),
        grid_custom_only=bool(cfg.get("grid_custom_only")),
        benchmark_fanout=bool(cfg.get("benchmark_fanout")),
        fanout_only=bool(cfg.get("fanout_only")),
        fanout_scale=float(cfg.get("fanout_scale", 1.0)),
        benchmark_netcdf=bool(cfg.get("benchmark_netcdf")),
        netcdf_only=bool(cfg.get("netcdf_only")),
        benchmark_netcdf_writer=bool(cfg.get("benchmark_netcdf_writer")),
        netcdf_writer_only=bool(cfg.get("netcdf_writer_only")),
        input_tile=str(cfg.get("input_tile", "materialized")),
        disable_file=bool(cfg.get("disable_file", False)),
        benchmark_grouped_file=bool(cfg.get("benchmark_grouped_file")),
        grouped_file_only=bool(cfg.get("grouped_file_only")),
        multiwindow_corpus=str(cfg.get("multiwindow_corpus", "") or ""),
        benchmark_file_matrix=bool(cfg.get("file_matrix")),
        file_matrix_only=bool(cfg.get("file_matrix_only")),
        benchmark_gpkg_chunksize=bool(cfg.get("gpkg_chunksize")),
        gpkg_chunksize_only=bool(cfg.get("gpkg_chunksize_only")),
        benchmark_layout_sweep=bool(cfg.get("layout_sweep")),
        layout_sweep_only=bool(cfg.get("layout_sweep_only")),
        benchmark_layout_scan=bool(cfg.get("layout_scan")),
        layout_scan_only=bool(cfg.get("layout_scan_only")),
        file_filespace=str(cfg.get("file_filespace", "") or ""),
        gpkg_corpus=str(cfg.get("gpkg_corpus", "") or ""),
    )
    setup += (
        _SINK  # truncate up-front + define the incremental Delta sink + show_section
    )
    light = bool(cfg.get("lightweight"))
    heavy = bool(cfg.get("heavyweight"))
    modes = cfg["modes"]
    do_pure = modes in ("pure-core", "both")
    do_spark = modes in ("spark-path", "both")
    if light:
        setup += _LIGHT_HELPERS  # run_light (references run_pure_core / run_spark_path)
    if heavy:
        setup += _HEAVY_HELPERS  # run_heavy (references HeavyBenchMain)

    benchmark_readers = bool(cfg.get("benchmark_readers"))
    readers_only = bool(cfg.get("readers_only"))
    benchmark_pmtiles = bool(cfg.get("benchmark_pmtiles"))
    pmtiles_only = bool(cfg.get("pmtiles_only"))
    benchmark_vector = bool(cfg.get("benchmark_vector"))
    vector_only = bool(cfg.get("vector_only"))
    benchmark_mvt = bool(cfg.get("benchmark_mvt"))
    mvt_only = bool(cfg.get("mvt_only"))
    benchmark_pmtiles_agg = bool(cfg.get("benchmark_pmtiles_agg"))
    pmtiles_agg_only = bool(cfg.get("pmtiles_agg_only"))
    benchmark_vector_tin = bool(cfg.get("benchmark_vector_tin"))
    vector_tin_only = bool(cfg.get("vector_tin_only"))
    benchmark_grid_quadbin = bool(cfg.get("benchmark_grid_quadbin"))
    grid_quadbin_only = bool(cfg.get("grid_quadbin_only"))
    benchmark_grid_bng = bool(cfg.get("benchmark_grid_bng"))
    grid_bng_only = bool(cfg.get("grid_bng_only"))
    benchmark_grid_custom = bool(cfg.get("benchmark_grid_custom"))
    grid_custom_only = bool(cfg.get("grid_custom_only"))
    benchmark_fanout = bool(cfg.get("benchmark_fanout"))
    fanout_only = bool(cfg.get("fanout_only"))
    benchmark_netcdf = bool(cfg.get("benchmark_netcdf"))
    netcdf_only = bool(cfg.get("netcdf_only"))
    benchmark_netcdf_writer = bool(cfg.get("benchmark_netcdf_writer"))
    netcdf_writer_only = bool(cfg.get("netcdf_writer_only"))
    benchmark_grouped_file = bool(cfg.get("benchmark_grouped_file"))
    grouped_file_only = bool(cfg.get("grouped_file_only"))
    benchmark_file_matrix = bool(cfg.get("file_matrix"))
    file_matrix_only = bool(cfg.get("file_matrix_only"))
    benchmark_gpkg_chunksize = bool(cfg.get("gpkg_chunksize"))
    gpkg_chunksize_only = bool(cfg.get("gpkg_chunksize_only"))
    benchmark_layout_sweep = bool(cfg.get("layout_sweep"))
    layout_sweep_only = bool(cfg.get("layout_sweep_only"))
    benchmark_layout_scan = bool(cfg.get("layout_scan"))
    layout_scan_only = bool(cfg.get("layout_scan_only"))

    # Setup is one cell; then ONE cell per selected (tier x mode) section so each renders
    # its table + summary the moment it finishes; then the wrap-up cell. Order: pure-core
    # (light, heavy) then spark-path (light, heavy).
    # Choose the install cell based on whether this is a Serverless job or a classic cluster.
    # Serverless: %pip magic is not available in a running notebook job cell; install via
    # subprocess so we control the restart precisely. Classic: the single %pip cell is
    # preferred (ONE kernel restart, no manual dbutils.library.restartPython() needed, and
    # avoids the triple-restart failure on DBR 19.x-snapshot).
    if cfg.get("serverless"):
        _install_cell_src = (
            "import subprocess, sys\n"
            "WHL = {wheel!r}\n"
            "r = subprocess.run(\n"
            "    [sys.executable, '-m', 'pip', 'install', '--quiet', '--force-reinstall',\n"
            "     '--no-deps', WHL],\n"
            "    capture_output=True, text=True,\n"
            ")\n"
            "print('whl install rc:', r.returncode, flush=True)\n"
            "if r.returncode != 0:\n"
            "    raise RuntimeError('whl install failed: '\n"
            "                       + r.stdout[-1200:] + ' || ' + r.stderr[-1200:])\n"
            "r2 = subprocess.run(\n"
            "    [sys.executable, '-m', 'pip', 'install', '--quiet',\n"
            "     'geobrix[light-dbr19]', 'markdown'],\n"
            "    capture_output=True, text=True,\n"
            ")\n"
            "print('deps install rc:', r2.returncode, flush=True)\n"
            "if r2.returncode != 0:\n"
            "    raise RuntimeError('deps install failed: '\n"
            "                       + r2.stdout[-1200:] + ' || ' + r2.stderr[-1200:])\n"
            "print('installs done; restarting Python', flush=True)\n"
            "dbutils.library.restartPython()\n"
        ).format(wheel=cfg["wheel"])
    else:
        # Classic cluster: single %pip cell = one kernel restart; dbutils.library.restartPython()
        # is NOT called (the %pip magic already triggers it; a second restart crashes on
        # DBR 19.x-snapshot). PEP 508 URL-reference form resolves extras from the wheel file.
        # --force-reinstall is REQUIRED: the wheel version is a fixed 0.5.0 across rebuilds, so
        # without it pip treats an already-installed 0.5.0 (from a prior run on a warm cluster)
        # as satisfied and silently runs STALE geobrix code -- e.g. a freshly added bench helper
        # is missing (AttributeError). It stays a single %pip cell (one restart).
        _install_cell_src = '%pip install --quiet --force-reinstall "geobrix[light-dbr19] @ file://{wheel}" markdown'.format(
            wheel=cfg["wheel"]
        )
    cells = [
        # Ensure BOTH fresh geobrix code AND the full [light-dbr19] dep set every run.
        # [light-dbr19] is the classic DBR 19 variant of [light]: it drops the
        # mapbox-vector-tile<2.2 cap that conflicts with DBR 19's protobuf>=6.31.1.
        _cell(_install_cell_src),
        # Cmd 3 -- the big setup cell (preamble + sink + helpers). Collapsed by default so the
        # run view leads with the per-section result cells, not this wall of setup code.
        _cell(setup, collapsed=True),
    ]
    # Any "*-only" reader/writer benchmark suppresses the per-function sections (they stage
    # their own corpus and don't touch the function-bench row pool). Collapsed into one
    # membership test so adding a new *-only leg doesn't grow this function's branch count.
    _any_only = any(
        (
            readers_only,
            pmtiles_only,
            vector_only,
            mvt_only,
            pmtiles_agg_only,
            vector_tin_only,
            grid_quadbin_only,
            grid_bng_only,
            grid_custom_only,
            fanout_only,
            netcdf_only,
            netcdf_writer_only,
            grouped_file_only,
            file_matrix_only,
            gpkg_chunksize_only,
            layout_sweep_only,
            layout_scan_only,
        )
    )
    if not _any_only:
        if light and do_pure:
            cells.append(_cell(_CELL_LIGHT_PURE))
        if heavy and do_pure:
            cells.append(_cell(_CELL_HEAVY_PURE))
        if light and do_spark:
            cells.append(_cell(_CELL_LIGHT_SPARK))
        if heavy and do_spark:
            cells.append(_cell(_CELL_HEAVY_SPARK))
    # Table-driven benchmark-cell emission: (enabled, [cells]) pairs. Each reader/writer
    # benchmark is appended when its --benchmark-* OR --*-only flag is set. Kept as a loop
    # (not a chain of ifs) so adding a new benchmark leg is one row here, not a new branch in
    # this function. NetCDF reader emits TWO cells (raster + vector split); the netcdf writer
    # emits one LIGHT-ONLY writer cell (raster + vector legs inside; heavy has no netcdf writer).
    _bench_cells = [
        (benchmark_readers or readers_only, [_CELL_READERS]),
        (benchmark_netcdf or netcdf_only, [_CELL_NETCDF, _CELL_NETCDF_SWATH]),
        (benchmark_netcdf_writer or netcdf_writer_only, [_CELL_NETCDF_WRITER]),
        (benchmark_pmtiles or pmtiles_only, [_CELL_PMTILES]),
        (benchmark_vector or vector_only, [_CELL_VECTOR]),
        (benchmark_mvt or mvt_only, [_CELL_MVT]),
        (benchmark_pmtiles_agg or pmtiles_agg_only, [_CELL_PMTILES_AGG]),
        (benchmark_vector_tin or vector_tin_only, [_CELL_VECTOR_TIN]),
        (benchmark_grid_quadbin or grid_quadbin_only, [_CELL_GRID_QUADBIN]),
        (benchmark_grid_bng or grid_bng_only, [_CELL_GRID_BNG]),
        (benchmark_grid_custom or grid_custom_only, [_CELL_GRID_CUSTOM]),
        (benchmark_fanout or fanout_only, [_CELL_FANOUT]),
        (benchmark_grouped_file or grouped_file_only, [_CELL_GROUPED_FILE]),
        (benchmark_file_matrix or file_matrix_only, [_CELL_FILE_MATRIX]),
        (benchmark_gpkg_chunksize or gpkg_chunksize_only, [_CELL_GPKG_CHUNKSIZE]),
        (benchmark_layout_sweep or layout_sweep_only, [_CELL_LAYOUT_SWEEP]),
        (benchmark_layout_scan or layout_scan_only, [_CELL_LAYOUT_SCAN]),
    ]
    for _enabled, _cell_srcs in _bench_cells:
        if _enabled:
            for _src in _cell_srcs:
                cells.append(_cell(_src))
    cells.append(_cell(_EPILOGUE))
    cells.append(
        _cell(_EXIT)
    )  # exit in its OWN cell so the compare render isn't truncated
    return {"cells": cells, "metadata": {}, "nbformat": 4, "nbformat_minor": 5}
