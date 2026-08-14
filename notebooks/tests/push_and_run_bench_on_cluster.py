#!/usr/bin/env python3
"""
Push the heavy-vs-light benchmark notebook to the workspace and run it on a configured cluster.

Both APIs (heavyweight Scala/Spark + lightweight pyrx) run on the SAME cluster, giving a
true same-hardware comparison. The script builds a bench notebook (via the bench cluster
notebook builder), uploads it, and runs it as a one-off job. Results append to the
bench_results Delta table and land comparison.csv / summary.md under the out_dir on the
configured Volume.

The cluster + artifacts must be provisioned by the operator (see the installation docs):
- heavyweight: x86 DBR 17.3 or 18 LTS with the init script + bundle + geobrix wheel + the bench
  geobrix-*-tests.jar staged on a Volume (the tests.jar is attached here as a job library;
  the production fat JAR is installed by the heavyweight init script, NOT attached here).
- lightweight (incl. ARM): just the [light] wheel (installed by the notebook's %pip cell).
  On ARM clusters use --lightweight-only (heavyweight is x86-only by design).

Requires: databricks-sdk, and env config (see databricks_cluster_config.example.env).

Usage:
  1. Copy databricks_cluster_config.example.env to databricks_cluster_config.env.
  2. Set DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*.
  3. Optional: set GBX_BENCH_CORPUS, GBX_BENCH_RESULTS_TABLE, GBX_BENCH_TESTS_JAR_VOLUME_PATH,
     GBX_BUNDLE_WHEEL_VOLUME_PATH.
  4. Run: python push_and_run_bench_on_cluster.py [options]
     Options: --no-wait, --heavyweight-only, --lightweight-only, --run-id, --functions,
              --modes, --row-counts, --warmup, --measured,
              --truncate-results (clear only this run_id + this invocation's tier(s)),
              --truncate-all (empty the whole table -> only the current run remains)
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Load config from .env in same dir
TESTS_DIR = Path(__file__).resolve().parent
_env_file = TESTS_DIR / "databricks_cluster_config.env"


def _strip_invisible(s: str) -> str:
    """Remove BOM and common invisible Unicode so env-derived paths are clean."""
    s = (s or "").strip()
    for c in ("﻿", "​", "‌", "‍", "\r"):
        s = s.replace(c, "")
    return s.strip()


if _env_file.exists():
    with open(_env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if k and v and not os.environ.get(k):
                    os.environ[k] = _strip_invisible(v)


def _geobrix_version() -> str:
    """Read version from python package __init__.py (avoid heavy imports)."""
    init_py = (
        TESTS_DIR.parent.parent
        / "python"
        / "geobrix"
        / "src"
        / "databricks"
        / "labs"
        / "gbx"
        / "__init__.py"
    )
    if init_py.exists():
        with open(init_py) as f:
            for line in f:
                line = line.strip()
                if line.startswith("__version__"):
                    # __version__ = "0.4.0"
                    if "=" in line:
                        v = line.split("=", 1)[1].strip().strip("'\"").strip()
                        if v:
                            return v
    return "0.4.0"


def _arg(flag: str, default: str) -> str:
    """Read --flag <value> from argv, else default."""
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def _discover_warehouse(w):
    """First RUNNING SQL warehouse (else any) for live progress counts; None if none.
    Used only for the host-side progress heartbeat -- never required for the run."""
    try:
        whs = list(w.warehouses.list())
    except Exception:
        return None
    if not whs:
        return None

    def _st(x):
        s = getattr(x, "state", None)
        return s.value if s is not None and hasattr(s, "value") else str(s)

    running = [x for x in whs if _st(x) == "RUNNING"]
    return (running or whs)[0].id


def _count_run_rows(w, warehouse_id: str, table: str, run_id: str, apis=None) -> int:
    """COUNT(*) of THIS run + THIS invocation's tier(s) in the table; -1 on any error.
    Filters by run_id AND api so a live count is accurate even when light and heavy
    share a run_id in the same table (else it would mix both tiers). Interim rows stream
    in as each function finishes, so this climbs during the run."""
    api_clause = ""
    if apis:
        api_clause = " AND api IN (" + ", ".join(f"'{a}'" for a in apis) + ")"
    try:
        # count(*) over a small filtered table returns well under this; the wait_timeout
        # is just the cap before execute_statement would return PENDING. Keep it modest
        # so a tight poll loop isn't blocked behind a long wait.
        res = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT count(*) FROM {table} WHERE run_id = '{run_id}'{api_clause}",
            wait_timeout="10s",
        )
        data = res.result.data_array if res and res.result else None
        return int(data[0][0]) if data else 0
    except Exception:
        return -1


def _local_size_sweep_count() -> int:
    """# of size-sweep tiles in the locally-staged corpus (for pure-core expected rows).
    None if the local corpus.json can't be read."""
    try:
        local = (
            TESTS_DIR.parent.parent
            / "sample-data"
            / "Volumes"
            / "main"
            / "default"
            / "bench-corpus"
            / "corpus.json"
        )
        if not local.exists():
            return None
        with open(local) as fh:
            c = json.load(fh)
        return len(c.get("size_sweep", []))
    except Exception:
        return None


def _expected_rows(
    functions: str,
    sel: str,
    modes: str,
    row_counts: str,
    lightweight: bool,
    heavyweight: bool,
    benchmark_readers: bool = False,
    readers_only: bool = False,
    benchmark_pmtiles: bool = False,
    pmtiles_only: bool = False,
    benchmark_vector: bool = False,
    vector_only: bool = False,
) -> int:
    """Best-effort total rows this run will stream, for an 'N of EXPECTED' progress
    display. Only reliable for a LIGHTWEIGHT-ONLY run: the lightweight fn set + corpus
    are known host-side. The heavyweight fn count is decided in Scala (BenchDispatch),
    not visible here, so any run that includes heavy returns None (count shown without a
    denominator) rather than a misleading number."""
    # Reader + writer rows: 2 per tier that is active (1 read + 1 write; light + heavy = up to 4).
    reader_rows = 2 * ((1 if lightweight else 0) + (1 if heavyweight else 0))
    # PMTiles rows: 1 per active tier (1 write per tier).
    pmtiles_rows = (1 if lightweight else 0) + (1 if heavyweight else 0)
    # Vector rows: 4 formats × up to 2 tiers (1 read per format per tier).
    _n_vformats = 4
    vector_rows = _n_vformats * ((1 if lightweight else 0) + (1 if heavyweight else 0))

    if readers_only:
        return reader_rows or None

    if pmtiles_only:
        return pmtiles_rows or None

    if vector_only:
        return vector_rows or None

    if not lightweight or heavyweight:
        # heavyweight included -> fn count unknown -> return None, but add reader rows if requested
        if benchmark_readers and reader_rows:
            return None  # still unknown due to heavyweight fn count
        return None
    try:
        sys.path.insert(0, "python/geobrix/src")
        from databricks.labs.gbx.bench import spec as _spec

        fns = _spec.select(
            functions=[x for x in functions.split(",") if x] or None, set=sel
        )
    except Exception:
        return None
    rc = [int(x) for x in row_counts.split(",") if x]
    total = 0
    if modes in ("spark-path", "both"):
        total += sum(1 for f in fns if "spark-path" in f.modes) * max(1, len(rc))
    if modes in ("pure-core", "both"):
        n_size = _local_size_sweep_count()
        if n_size is None:
            return None  # can't be exact -> don't show a misleading denominator
        total += sum(1 for f in fns if "pure-core" in f.modes) * n_size
    if benchmark_readers:
        total += reader_rows
    if benchmark_pmtiles:
        total += pmtiles_rows
    if benchmark_vector:
        total += vector_rows
    return total or None


def main() -> int:
    # Force line-buffered stdout so the run URL + interim progress heartbeats appear
    # LIVE, not all at once at exit. Python block-buffers stdout when it isn't a TTY
    # (piped to a file, a log, or a background task), which otherwise hides every
    # interim print until the process ends.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    do_wait = "--no-wait" not in sys.argv
    heavyweight = "--lightweight-only" not in sys.argv
    lightweight = "--heavyweight-only" not in sys.argv
    # --explain-only: print/persist each spark-path fn's physical plan, run nothing timed.
    # It's a lightweight spark-path-only diagnostic -- force that scope so the heavy JVM
    # path (which can't .explain) and the pure-core path stay out entirely.
    explain_only = "--explain-only" in sys.argv
    if explain_only:
        heavyweight = False
        lightweight = True
    # --benchmark-readers: also run the reader benchmark (raster_gbx vs gdal) on-cluster.
    # --readers-only: ONLY run the reader benchmark, skip all fn benchmarks.
    benchmark_readers = "--benchmark-readers" in sys.argv
    readers_only = "--readers-only" in sys.argv
    # --benchmark-pmtiles: also run the pmtiles writer benchmark on-cluster.
    # --pmtiles-only: ONLY run the pmtiles benchmark, skip all fn benchmarks.
    benchmark_pmtiles = "--benchmark-pmtiles" in sys.argv
    pmtiles_only = "--pmtiles-only" in sys.argv
    # --benchmark-vector: also run the vector reader benchmark on-cluster.
    # --vector-only: ONLY run the vector reader benchmark, skip all fn benchmarks.
    # --vector-scale: read the scaled 1M-seed corpus (copies dir + seed file) instead
    #   of the tiny 4-file corpus. Requires the scaled corpus staged at
    #   {CORPUS}/vector-scale/<fmt>/. Only meaningful when benchmark_vector or vector_only.
    benchmark_vector = "--benchmark-vector" in sys.argv
    vector_only = "--vector-only" in sys.argv
    vector_scale = "--vector-scale" in sys.argv
    # --benchmark-mvt: also run the st_asmvt benchmark (light pyvx vs heavy vectorx).
    # --mvt-only: ONLY run the MVT benchmark, skip all fn benchmarks.
    benchmark_mvt = "--benchmark-mvt" in sys.argv
    mvt_only = "--mvt-only" in sys.argv
    # --benchmark-pmtiles-agg: also run the pmtiles_agg grouped-agg benchmark (light vs heavy).
    # --pmtiles-agg-only: ONLY run the pmtiles_agg benchmark, skip all fn benchmarks.
    benchmark_pmtiles_agg = "--benchmark-pmtiles-agg" in sys.argv
    pmtiles_agg_only = "--pmtiles-agg-only" in sys.argv
    # --benchmark-vector-tin: also run the TIN + legacy benchmark (light pyvx vs heavy vectorx).
    # --vector-tin-only: ONLY run the TIN + legacy benchmark, skip all fn benchmarks.
    benchmark_vector_tin = "--benchmark-vector-tin" in sys.argv
    vector_tin_only = "--vector-tin-only" in sys.argv
    # --benchmark-grid-quadbin: also run the quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
    # --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip all fn benchmarks.
    benchmark_grid_quadbin = "--benchmark-grid-quadbin" in sys.argv
    grid_quadbin_only = "--grid-quadbin-only" in sys.argv
    # --benchmark-grid-bng: also run the BNG grid benchmark (light pygx vs heavy gridx.bng).
    # --grid-bng-only: ONLY run the BNG grid benchmark, skip all fn benchmarks.
    benchmark_grid_bng = "--benchmark-grid-bng" in sys.argv
    grid_bng_only = "--grid-bng-only" in sys.argv
    # --benchmark-grid-custom: also run the custom grid benchmark (light pygx vs heavy gridx.custom).
    # --grid-custom-only: ONLY run the custom grid benchmark, skip all fn benchmarks.
    benchmark_grid_custom = "--benchmark-grid-custom" in sys.argv
    grid_custom_only = "--grid-custom-only" in sys.argv
    # --benchmark-fanout: also run the fan-out UDTF benchmark (rst_polygonize +
    #   rst_h3_rastertogridcount), light pyrx LATERAL vs heavy rasterx explode.
    # --fanout-only: ONLY run the fanout benchmark, skip all fn benchmarks.
    benchmark_fanout = "--benchmark-fanout" in sys.argv
    fanout_only = "--fanout-only" in sys.argv
    # --benchmark-netcdf: also run the NetCDF reader benchmark (light netcdf_gbx vs heavy
    #   netcdf_gdal) over the same staged .nc pool at {CORPUS}/netcdf.
    # --netcdf-only: ONLY run the NetCDF reader benchmark, skip all fn benchmarks.
    benchmark_netcdf = "--benchmark-netcdf" in sys.argv
    netcdf_only = "--netcdf-only" in sys.argv
    # --benchmark-netcdf-writer: also run the NetCDF WRITER benchmark (light netcdf_gbx write
    #   throughput, raster + vector legs) reusing {CORPUS}/netcdf + {CORPUS}/netcdf-swath.
    # --netcdf-writer-only: ONLY run the NetCDF writer benchmark, skip all fn benchmarks.
    # LIGHT-ONLY: there is no heavy NetCDF writer.
    benchmark_netcdf_writer = "--benchmark-netcdf-writer" in sys.argv
    netcdf_writer_only = "--netcdf-writer-only" in sys.argv
    # --fanout-scale F: dial the synthetic fan-out size for each function (default 1.0 ->
    #   meaningful but ~couple minutes on ~20 workers). Larger = more output rows.
    fanout_scale = float(_arg("--fanout-scale", "1.0"))
    writer_rows = int(_arg("--writer-rows", "14000000"))
    # --vector-legs reader|writer|both: run the scaled vector reader-ingest legs, the
    # writer-export leg, or both (default). Lets each be a separate isolated cluster job.
    vector_legs = _arg("--vector-legs", "both")
    # --vector-formats csv: restrict the scaled vector run to these light formats (e.g.
    # geojson_gbx). Empty = all. With --vector-legs + --lightweight-only/--heavyweight-only,
    # runs ONE (format x tier x leg) per job for cold isolation.
    vector_formats = _arg("--vector-formats", "")
    if not heavyweight and not lightweight:
        print(
            "ERROR: --heavyweight-only and --lightweight-only are mutually exclusive "
            "(nothing to run). Pass at most one.",
            file=sys.stderr,
        )
        return 2

    # --resume: keep existing rows for this run_id and skip already-complete (tier x mode)
    # sections (load them from the table instead of re-running). For picking up after a
    # timeout/crash on the SAME cluster. Mutually exclusive with the truncate modes, which
    # would wipe the rows resume relies on. NOTE: spark-path timings depend on cluster size,
    # so only resume on an unchanged cluster config (pure-core is size-independent).
    resume = "--resume" in sys.argv
    if resume and ("--truncate-all" in sys.argv or "--truncate-results" in sys.argv):
        print(
            "ERROR: --resume is mutually exclusive with --truncate-all / "
            "--truncate-results (truncate would wipe the rows resume reuses).",
            file=sys.stderr,
        )
        return 2
    # On resume, fns whose only row is an error are re-run by default (purged + retried,
    # e.g. after a code/JAR fix); --no-fix-errors keeps them as-is and skips them.
    fix_errors = "--no-fix-errors" not in sys.argv

    run_id = _arg("--run-id", "cluster")
    # Keep reader/writer benchmarks SEPARABLE from the function benchmarks: an *-only run
    # gets its own run_id suffix (unless --run-id was given explicitly) so its rows land in
    # a distinct partition of the results table instead of commingling with the function
    # rows under 'cluster'. This also makes the live "N of EXPECTED" count accurate (it
    # polls by run_id) rather than counting leftover function rows from a prior run.
    if "--run-id" not in sys.argv:
        if readers_only:
            run_id = f"{run_id}-readers"
        elif pmtiles_only:
            run_id = f"{run_id}-pmtiles"
        elif vector_only:
            run_id = f"{run_id}-vector"
        elif mvt_only:
            run_id = f"{run_id}-mvt"
        elif pmtiles_agg_only:
            run_id = f"{run_id}-pmtiles-agg"
        elif vector_tin_only:
            run_id = f"{run_id}-vector-tin"
        elif grid_quadbin_only:
            run_id = f"{run_id}-grid-quadbin"
        elif grid_bng_only:
            run_id = f"{run_id}-grid-bng"
        elif grid_custom_only:
            run_id = f"{run_id}-grid-custom"
        elif fanout_only:
            run_id = f"{run_id}-fanout"
        elif netcdf_only:
            run_id = f"{run_id}-netcdf"
        elif netcdf_writer_only:
            run_id = f"{run_id}-netcdf-writer"
    functions = _arg("--functions", "")
    sel = _arg("--set", "core")

    # --redo-functions <csv>: force re-run this explicit list of fns for the selected
    # (run_id, api, mode) by purging their existing rows first, leaving every other fn's rows
    # intact. INDEPENDENT of --set/--functions, so one run can resume the never-ran fns AND
    # force-redo this named subset (e.g. re-measure the aggregators with a new wheel while the
    # rest of the run completes normally). Exclusive with the truncates (which clear broadly).
    redo_functions = _arg("--redo-functions", "")
    if redo_functions.strip() and (
        "--truncate-all" in sys.argv or "--truncate-results" in sys.argv
    ):
        print(
            "ERROR: --redo-functions is mutually exclusive with --truncate-all / "
            "--truncate-results.",
            file=sys.stderr,
        )
        return 2
    if sel not in ("core", "full"):
        print(f"ERROR: --set must be 'core' or 'full' (got '{sel}')", file=sys.stderr)
        return 2
    modes = _arg("--modes", "both")
    row_counts = _arg("--row-counts", "10,100,1000,10000")
    # Pure-core defaults: 1 warm-up + 3 measured. One warm-up absorbs the cold cost (e.g.
    # numba JIT compiles on the FIRST call), and 3 measured gives a stable median of a fast
    # single-tile op without overpaying. Override with --warmup / --measured.
    warmup = int(_arg("--warmup", "1"))
    measured = int(_arg("--measured", "3"))
    # Spark-path iteration counts are SEPARATE from pure-core: the N-tile sweep over the whole
    # partitioned job IS the averaging, so spark-path defaults to 1 warm-up + 1 measured.
    # Override with --spark-warmup / --spark-measured.
    spark_warmup = int(_arg("--spark-warmup", "1"))
    spark_measured = int(_arg("--spark-measured", "1"))
    # Tiles per spark-path partition; 0 = auto (n / (slots*4): oversubscribe slots ~4x so
    # finished slots grab pending tasks rather than idling on the straggler tail).
    partition_size = int(_arg("--override-partition-size", "0"))
    # --input-tile materialized|virtual: input tile mode for the light spark-path leg.
    # materialized = bytes column (default, matches all prior bench runs).
    # virtual = path+window tile (tests the new virtual-tile reader path).
    input_tile = _arg("--input-tile", "materialized")
    if input_tile not in ("materialized", "virtual"):
        print(
            f"ERROR: --input-tile must be 'materialized' or 'virtual' (got '{input_tile}')",
            file=sys.stderr,
        )
        return 2
    # --disable-file: set GBX_DISABLE_FILE=1 in the notebook so virtual-tile reads
    # fall back to plain-path I/O (FILE type bypassed). Intended for the FILE-off A/B leg.
    # Only meaningful when --input-tile virtual; silently a no-op for materialized inputs.
    disable_file = "--disable-file" in sys.argv

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not (host and token) and not profile:
        print(
            "Set DATABRICKS_HOST and DATABRICKS_TOKEN, or DATABRICKS_CONFIG_PROFILE",
            file=sys.stderr,
        )
        return 2

    cluster_id = _strip_invisible(os.environ.get("CLUSTER_ID") or "")
    if not cluster_id:
        print(
            "Set CLUSTER_ID (existing cluster to run the benchmark on)", file=sys.stderr
        )
        return 2

    catalog = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_CATALOG") or "main")
    schema = _strip_invisible(os.environ.get("GBX_BUNDLE_VOLUME_SCHEMA") or "default")
    volume = _strip_invisible(
        os.environ.get("GBX_BUNDLE_VOLUME_NAME") or "geobrix_samples"
    )
    volroot = f"/Volumes/{catalog}/{schema}/{volume}"
    ver = _geobrix_version()

    # Wheel path: explicit or derived under the Volume root.
    wheel = _strip_invisible(
        os.environ.get("GBX_BUNDLE_WHEEL_VOLUME_PATH") or ""
    ).strip()
    if not wheel:
        wheel = f"{volroot}/geobrix-{ver}-py3-none-any.whl"

    corpus = (
        _strip_invisible(os.environ.get("GBX_BENCH_CORPUS") or "").strip()
        or f"{volroot}/bench-corpus"
    )
    table = (
        _strip_invisible(os.environ.get("GBX_BENCH_RESULTS_TABLE") or "").strip()
        or f"{catalog}.{schema}.bench_results"
    )
    tests_jar = (
        _strip_invisible(
            os.environ.get("GBX_BENCH_TESTS_JAR_VOLUME_PATH") or ""
        ).strip()
        or f"{volroot}/geobrix-{ver}-tests.jar"
    )
    out_dir = f"{volroot}/bench-out/{run_id}"

    cfg = dict(
        wheel=wheel,
        corpus=corpus,
        out_dir=out_dir,
        table=table,
        run_id=run_id,
        functions=functions,
        set=sel,
        modes=modes,
        row_counts=row_counts,
        warmup=warmup,
        measured=measured,
        spark_warmup=spark_warmup,
        spark_measured=spark_measured,
        partition_size=partition_size,
        heavyweight=heavyweight,
        lightweight=lightweight,
        # Two truncate modes (default: neither -> rows accumulate across runs):
        #  --truncate-results: SCOPED -- clear only this run_id + the tier(s) this
        #    invocation writes, so the paired tier / other runs survive (coexist).
        #  --truncate-all: WHOLE TABLE -- empty bench_results so ONLY the current run's
        #    rows remain. (Takes precedence if both are passed.)
        truncate_results=("--truncate-results" in sys.argv),
        truncate_all=("--truncate-all" in sys.argv),
        #  --resume: keep existing rows; function-granular (load done fns, run missing).
        resume=resume,
        #  fix_errors (default True): on resume, re-run fns whose only row is an error.
        fix_errors=fix_errors,
        #  --redo-functions: force re-run this explicit fn list (purge their rows first),
        #  independent of --set/--functions, layered on the normal resume run.
        redo_functions=redo_functions,
        #  --explain-only: print/persist spark-path physical plans, no timing/no rows.
        explain_only=explain_only,
        #  --benchmark-readers: also run reader benchmark (raster_gbx vs gdal).
        benchmark_readers=benchmark_readers,
        #  --readers-only: ONLY run the reader benchmark, skip fn benchmarks.
        readers_only=readers_only,
        #  --benchmark-pmtiles: also run pmtiles writer benchmark.
        benchmark_pmtiles=benchmark_pmtiles,
        #  --pmtiles-only: ONLY run the pmtiles benchmark, skip fn benchmarks.
        pmtiles_only=pmtiles_only,
        #  --benchmark-vector: also run vector reader benchmark (light *_gbx vs heavy *_ogr).
        benchmark_vector=benchmark_vector,
        #  --vector-only: ONLY run the vector reader benchmark, skip fn benchmarks.
        vector_only=vector_only,
        #  --vector-scale: use the scaled 1M-seed corpus instead of the tiny 4-file corpus.
        vector_scale=vector_scale,
        #  --writer-rows N: row count for the shared writer-source Delta table (default 14M).
        writer_rows=writer_rows,
        #  --vector-legs reader|writer|both: which scaled vector legs to run (default both).
        vector_legs=vector_legs,
        #  --vector-formats csv: restrict scaled vector run to these light formats (default all).
        vector_formats=vector_formats,
        #  --benchmark-mvt: also run st_asmvt benchmark (light pyvx vs heavy vectorx).
        benchmark_mvt=benchmark_mvt,
        #  --mvt-only: ONLY run the MVT benchmark, skip fn benchmarks.
        mvt_only=mvt_only,
        #  --benchmark-pmtiles-agg: also run pmtiles_agg grouped-agg benchmark (light vs heavy).
        benchmark_pmtiles_agg=benchmark_pmtiles_agg,
        #  --pmtiles-agg-only: ONLY run the pmtiles_agg benchmark, skip fn benchmarks.
        pmtiles_agg_only=pmtiles_agg_only,
        #  --benchmark-vector-tin: also run TIN + legacy benchmark (light pyvx vs heavy vectorx).
        benchmark_vector_tin=benchmark_vector_tin,
        #  --vector-tin-only: ONLY run the TIN + legacy benchmark, skip fn benchmarks.
        vector_tin_only=vector_tin_only,
        #  --benchmark-grid-quadbin: also run quadbin grid benchmark (light pygx vs heavy gridx.quadbin).
        benchmark_grid_quadbin=benchmark_grid_quadbin,
        #  --grid-quadbin-only: ONLY run the quadbin grid benchmark, skip fn benchmarks.
        grid_quadbin_only=grid_quadbin_only,
        #  --benchmark-grid-bng: also run BNG grid benchmark (light pygx vs heavy gridx.bng).
        benchmark_grid_bng=benchmark_grid_bng,
        #  --grid-bng-only: ONLY run the BNG grid benchmark, skip fn benchmarks.
        grid_bng_only=grid_bng_only,
        #  --benchmark-grid-custom: also run custom grid benchmark (light pygx vs heavy gridx.custom).
        benchmark_grid_custom=benchmark_grid_custom,
        #  --grid-custom-only: ONLY run the custom grid benchmark, skip fn benchmarks.
        grid_custom_only=grid_custom_only,
        #  --benchmark-fanout: also run fan-out UDTF benchmark (all 8 streaming UDTFs).
        benchmark_fanout=benchmark_fanout,
        #  --fanout-only: ONLY run the fanout benchmark, skip fn benchmarks.
        fanout_only=fanout_only,
        #  --fanout-scale F: dial the synthetic fan-out size (default 1.0).
        fanout_scale=fanout_scale,
        #  --benchmark-netcdf: also run NetCDF reader benchmark (light netcdf_gbx vs heavy netcdf_gdal).
        benchmark_netcdf=benchmark_netcdf,
        #  --netcdf-only: ONLY run the NetCDF reader benchmark, skip fn benchmarks.
        netcdf_only=netcdf_only,
        #  --benchmark-netcdf-writer: also run NetCDF WRITER benchmark (light-only throughput).
        benchmark_netcdf_writer=benchmark_netcdf_writer,
        #  --netcdf-writer-only: ONLY run the NetCDF writer benchmark, skip fn benchmarks.
        netcdf_writer_only=netcdf_writer_only,
        #  --input-tile materialized|virtual: input tile mode for the light spark-path leg.
        input_tile=input_tile,
        #  --disable-file: set GBX_DISABLE_FILE=1 in the notebook (FILE-off A/B leg).
        disable_file=disable_file,
    )
    if explain_only:
        # Plans are a spark-path concern only; never run the pure-core sections.
        cfg["modes"] = "spark-path"
    if pmtiles_only:
        # PMTiles writer is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if vector_only:
        # Vector reader benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if mvt_only:
        # MVT agg benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if pmtiles_agg_only:
        # PMTiles agg benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if vector_tin_only:
        # TIN + legacy benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_quadbin_only:
        # Quadbin grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_bng_only:
        # BNG grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if grid_custom_only:
        # Custom grid benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if fanout_only:
        # Fan-out UDTF benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if netcdf_only:
        # NetCDF reader benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"
    if netcdf_writer_only:
        # NetCDF writer benchmark is spark-path only; skip pure-core sections.
        cfg["modes"] = "spark-path"

    # Import the notebook builder from the repo source (this runs on the HOST, not the cluster).
    sys.path.insert(0, "python/geobrix/src")
    try:
        from databricks.labs.gbx.bench.cluster import build_bench_notebook
    except ImportError as e:
        print(
            f"Could not import build_bench_notebook from repo source (python/geobrix/src): {e}",
            file=sys.stderr,
        )
        print(
            "Run this from the repo root so 'python/geobrix/src' resolves.",
            file=sys.stderr,
        )
        return 2

    try:
        import io

        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service import compute, jobs
        from databricks.sdk.service.workspace import ImportFormat
    except ImportError:
        print("Install databricks-sdk: pip install databricks-sdk", file=sys.stderr)
        return 2

    # Pre-flight: show the operator exactly what will run before submitting.
    print("=" * 64)
    print("gbx:bench:cluster pre-flight")
    print(f"  cluster_id : {cluster_id}")
    print(f"  scope      : heavyweight={heavyweight}  lightweight={lightweight}")
    print(f"  run_id     : {run_id}")
    print(f"  functions  : {functions or f'(set={sel})'}")
    print(f"  modes      : {modes}   row_counts={row_counts}")
    print(f"  warmup/meas: {warmup}/{measured}")
    print(f"  corpus     : {corpus}")
    print(f"  table      : {table}")
    print(f"  out_dir    : {out_dir}")
    print(f"  wheel      : {wheel}")
    if heavyweight:
        print(
            f"  tests.jar  : {tests_jar}  (attached as job library; fat JAR via cluster init script)"
        )
    print("  NOTE: this submits a job to a real cluster and consumes compute.")
    print("=" * 64)

    nb = build_bench_notebook(cfg)
    nb_bytes = json.dumps(nb, indent=1).encode("utf-8")

    w = (
        WorkspaceClient(profile=profile)
        if profile
        else WorkspaceClient(host=host, token=token)
    )

    # Pre-flight: REFUSE to submit a spark-path run the corpus row pool can't fill. The
    # spark-path draws max(--row-counts) DISTINCT tiles from the pool; a smaller pool would
    # silently UNDER-FILL (report rows=max while processing fewer tiles -> misleading numbers).
    # Require pool >= the largest requested row count. (Skipped for --explain-only, which
    # builds plans and draws no tiles; pure-core uses the size-sweep, not the row pool. Also
    # skipped for the *-only reader/writer/pmtiles/vector benchmarks: those read the WHOLE pool
    # (or their own vector corpus), not the --row-counts function ladder, so the ladder max is
    # irrelevant to them -- gating on it would falsely refuse a valid 1000-tile reader run.)
    _only_run = (
        cfg.get("readers_only")
        or cfg.get("pmtiles_only")
        or cfg.get("vector_only")
        or cfg.get("mvt_only")
        or cfg.get("pmtiles_agg_only")
        or cfg.get("vector_tin_only")
        or cfg.get("grid_quadbin_only")
        or cfg.get("grid_bng_only")
        or cfg.get("grid_custom_only")
        or cfg.get("fanout_only")
        or cfg.get("netcdf_only")
    )
    if (
        cfg["modes"] in ("spark-path", "both")
        and not cfg.get("explain_only")
        and not _only_run
    ):
        _max_rc = max(
            (int(x) for x in str(cfg["row_counts"]).split(",") if x), default=0
        )
        try:
            _cj = json.loads(
                w.files.download(f"{corpus}/corpus.json")
                .contents.read()
                .decode("utf-8")
            )
            _pool = len(_cj.get("row_pool", {}).get("tiles", []))
        except Exception as _e:
            _e_str = str(_e)
            if "disabled for users without account admin" in _e_str or "API is disabled" in _e_str:
                print(
                    f"WARNING: cannot read {corpus}/corpus.json (Files API admin-gated on this workspace); "
                    f"skipping pool-size validation. Ensure --row-counts <= corpus pool size.",
                    file=sys.stderr,
                )
                _pool = _max_rc  # bypass the size check below
            else:
                print(
                    f"ERROR: cannot read {corpus}/corpus.json to validate the row pool size: {_e}",
                    file=sys.stderr,
                )
                return 2
        if _max_rc > _pool:
            print(
                f"ERROR: spark-path --row-counts max ({_max_rc}) exceeds the corpus row pool "
                f"({_pool} tiles) at {corpus}. The pool must have >= {_max_rc} tiles or the run "
                f"under-fills. Generate a larger pool (gbx:bench:gen-data --row-rows {_max_rc}, "
                f"then stage it) or lower --row-counts. Refusing to submit.",
                file=sys.stderr,
            )
            return 2

    # Notebook path: env override or per-user default.
    notebook_path_from_env = (
        _strip_invisible(os.environ.get("GBX_BENCH_RUNNER_NOTEBOOK_PATH") or "").strip()
        or None
    )
    me = w.current_user.me()
    default_path = f"/Users/{me.user_name}/geobrix_bench_runner.ipynb"
    notebook_path = notebook_path_from_env or default_path
    if not notebook_path.endswith(".ipynb"):
        notebook_path = notebook_path.rstrip("/") + ".ipynb"
    # Ensure it doesn't have the /Workspace prefix for the SDK calls.
    notebook_path = "/" + notebook_path.strip().removeprefix("/Workspace").lstrip("/")

    # Ensure parent directory exists (workspace API).
    notebook_parent = Path(notebook_path).parent
    try:
        w.workspace.mkdirs(str(notebook_parent))
    except Exception:
        pass

    print(f"Uploading bench notebook to {notebook_path}...")
    # CRITICAL: ImportFormat.JUPYTER creates a NOTEBOOK, not a FILE.
    w.workspace.upload(
        notebook_path,
        io.BytesIO(nb_bytes),
        format=ImportFormat.JUPYTER,
        overwrite=True,
    )

    # Attach the bench tests.jar only for the heavyweight leg (it carries the bench Scala classes).
    libraries = [compute.Library(jar=tests_jar)] if heavyweight else None

    print("Submitting one-off benchmark run on cluster...")
    submit_waiter = w.jobs.submit(
        run_name=f"geobrix-bench-{run_id}",
        # 6h: the full 1000-row both-run is dominated by the slow light spark-path fns
        # (the perf gap being measured). 2h timed out mid light-spark-path; 6h covers
        # light+heavy spark-path even before cluster upsizing parallelizes it.
        timeout_seconds=21600,
        tasks=[
            jobs.SubmitTask(
                task_key="run_bench",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    source=jobs.Source.WORKSPACE,
                ),
                libraries=libraries,
            )
        ],
    )

    remote_run_id = submit_waiter.run_id

    # Results stream into the table as each function finishes -> queryable immediately.
    # Surface the run URL + the exact query right after submit (don't make the operator
    # wait until the end or hunt for it).
    print("=" * 64)
    print(f"Run submitted: run_id={remote_run_id}")
    try:
        _r0 = w.jobs.get_run(remote_run_id)
        if getattr(_r0, "run_page_url", None):
            print(f"  run URL : {_r0.run_page_url}")
    except Exception:
        pass
    print(f"  results : {table}  (streaming; filter run_id = '{run_id}')")
    print(
        f"  query   : SELECT fn, mode, rows, iter_median_ms, status "
        f"FROM {table} WHERE run_id = '{run_id}' ORDER BY fn"
    )
    print("=" * 64)

    if not do_wait:
        # Fire-and-forget: submit returns a waiter; we don't call .result().
        print("Run submitted (--no-wait). Poll the table above, or the Databricks UI.")
        return 0

    # Poll the live row count as interim rows land in the table (each function appends
    # as it finishes). Use a TIGHT interval so the displayed count tracks the table
    # closely -- a coarse sleep makes the count lag far behind during the fast middle of
    # a run. Print only when it advances, plus a periodic heartbeat so a slow function
    # doesn't look like a stall. Live counts need a SQL warehouse (auto-discovered, or
    # GBX_BENCH_SQL_WAREHOUSE_ID); without one we heartbeat run state -- the table is
    # queryable regardless.
    import time

    POLL_SECS = 5
    # The tier(s) this invocation writes -> filter the live count by (run_id, api) so it
    # stays accurate when light and heavy share a run_id in the same table.
    poll_apis = [
        a
        for a, on in (("lightweight", lightweight), ("heavyweight", heavyweight))
        if on
    ]
    expected = _expected_rows(
        functions,
        sel,
        modes,
        row_counts,
        lightweight,
        heavyweight,
        benchmark_readers=benchmark_readers,
        readers_only=readers_only,
        benchmark_pmtiles=benchmark_pmtiles,
        pmtiles_only=pmtiles_only,
        benchmark_vector=benchmark_vector,
        vector_only=vector_only,
    )
    # "27 (of 83)" when we know the total this run will stream, else just "27".
    of = f" (of {expected})" if expected else ""
    warehouse_id = _strip_invisible(
        os.environ.get("GBX_BENCH_SQL_WAREHOUSE_ID") or ""
    ) or _discover_warehouse(w)
    if warehouse_id:
        tot = f" (expecting {expected} rows)" if expected else ""
        print(
            f"Waiting; polling live row count via SQL warehouse {warehouse_id} every {POLL_SECS}s{tot}..."
        )
    else:
        print(
            "Waiting; no SQL warehouse for live counts (set GBX_BENCH_SQL_WAREHOUSE_ID "
            "for them) -- heartbeating run state. The table above is queryable now."
        )
    run = None
    last_count = -1
    stale_polls = 0
    while True:
        try:
            run = w.jobs.get_run(remote_run_id)
        except Exception as e:  # transient -> keep polling
            print(f"  (poll error: {e})", file=sys.stderr)
            time.sleep(POLL_SECS)
            continue
        st = run.state
        lc = (
            st.life_cycle_state.value
            if st and st.life_cycle_state and hasattr(st.life_cycle_state, "value")
            else str(st.life_cycle_state if st else "?")
        )
        if lc in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        if warehouse_id:
            c = _count_run_rows(w, warehouse_id, table, run_id, poll_apis)
            if c > last_count:
                print(f"  [{lc}] {c}{of} rows streamed to table so far")
                last_count = c
                stale_polls = 0
            else:
                stale_polls += 1
                if stale_polls % 6 == 0:  # ~30s with no new rows -> show we're alive
                    print(
                        f"  [{lc}] still {max(last_count, 0)}{of} rows (current fn taking a while)"
                    )
        time.sleep(POLL_SECS)

    run_id_remote = remote_run_id
    state = run.state
    if state and getattr(state, "life_cycle_state", None):
        lc = (
            state.life_cycle_state.value
            if hasattr(state.life_cycle_state, "value")
            else str(state.life_cycle_state)
        )
        if lc == "TERMINATED":
            result_state = (
                (
                    state.result_state.value
                    if state.result_state and hasattr(state.result_state, "value")
                    else str(state.result_state)
                )
                if state.result_state
                else "UNKNOWN"
            )
            # Try to surface the notebook's dbutils.notebook.exit() payload.
            try:
                out = w.jobs.get_run_output(run.tasks[0].run_id) if run.tasks else None
                if (
                    out
                    and getattr(out, "notebook_output", None)
                    and out.notebook_output.result
                ):
                    print("Notebook output:", out.notebook_output.result)
            except Exception:
                pass
            if result_state == "SUCCESS":
                # Point at the summary file that actually exists for this scope:
                # both tiers -> comparison summary.md; lightweight-only ->
                # lightweight.summary.md; heavyweight-only -> heavyweight.jsonl.
                if heavyweight and lightweight:
                    summary_file = "summary.md"
                elif lightweight:
                    summary_file = "lightweight.summary.md"
                else:
                    summary_file = "heavyweight.jsonl"
                print(f"Run {run_id_remote} finished: result_state=SUCCESS.")
                print(f"Results -> table {table}")
                print(f"Summary  -> {out_dir}/{summary_file}")
                return 0
            print(
                f"Run {run_id_remote} finished with result_state={result_state}",
                file=sys.stderr,
            )
            print(
                f"Check the Delta table {table} and out_dir {out_dir} for partial results.",
                file=sys.stderr,
            )
    else:
        print(f"Run state: {state}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
