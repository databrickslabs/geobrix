"""Python benchmark runner: pure-core and spark-path timing over a corpus."""

from __future__ import annotations

import dataclasses as _dc
import platform
import statistics
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Callable, Dict, List

from databricks.labs.gbx.bench import manifest as m
from databricks.labs.gbx.bench import spec as _spec
from databricks.labs.gbx.bench import synth as _synth
from databricks.labs.gbx.bench.fingerprint import (
    fingerprint_collection,
    fingerprint_dggs_grid,
    fingerprint_dggs_grid_str,
    fingerprint_output,
    fingerprint_vector,
)
from databricks.labs.gbx.bench.results import ResultRow
from databricks.labs.gbx.bench.spec import FnSpec
from databricks.labs.gbx.pyrx import _serde

# Default groups (keys) per task for the *_agg spark-path scaling. Small so large-output
# aggregators (rst_merge_agg's union mosaic) hold few big outputs per task -> bounded worker
# memory (a 6-keys/task fan-out OOM'd merge on the cluster). --override-partition-size wins.
_AGG_KEYS_PER_TASK = 2


def _synthesized_paths(corpus_root, tile_rel_path: str, fn: str) -> List[str]:
    """Synthesize (write-once) the multi-tile input for a `tile_array` fn.

    Both engines compute the SAME output dir via ``synth.synth_dir`` (from corpus
    root + tile path + fn) and read the identical files. The pyrx runner writes
    them here; the heavy runner reads the same paths. Idempotent: re-runs reuse the
    existing files. Returns the synthesized GTiff paths in consumption order.
    """
    src = str(Path(corpus_root) / tile_rel_path)
    out_dir = _synth.synth_dir(corpus_root, tile_rel_path, _spec.synth_recipe(fn))
    return _synth.synthesize(src, _spec.synth_recipe(fn), out_dir)


def time_iters(
    fn: Callable[[], None],
    warmup: int,
    measured: int,
    warmup_fn: Callable[[], None] = None,
) -> Dict:
    """Run fn warmup+measured times; return ms distribution over measured runs.

    warmup_fn (optional): a cheaper stand-in run for the warm-up iterations only --
    e.g. the spark-path warm-up exercises one tile per executor slot instead of the
    full row count, so JVM/UDF/Python-worker spin-up isn't charged to the first
    measured iteration without paying the full-N cost on every warm-up pass. The
    MEASURED iterations always run fn.
    """
    _warm = warmup_fn or fn
    for _ in range(warmup):
        _warm()
    samples = []
    for _ in range(measured):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000.0)
    samples.sort()
    p90_idx = max(0, min(len(samples) - 1, int(round(0.9 * (len(samples) - 1)))))
    total = sum(samples)
    return {
        "warmup_iters": warmup,
        "measured_iters": measured,
        "iter_median_ms": statistics.median(samples),
        "iter_min_ms": samples[0],
        "iter_p90_ms": samples[p90_idx],
        # total wall clock across the measured iterations + its mean (avg per iter).
        "iter_total_wall_clock_ms": total,
        "avg_wall_clock_ms": (total / len(samples)) if samples else 0.0,
    }


def capture_env(where: str) -> Dict:
    try:
        import rasterio

        gdal_version = rasterio.__gdal_version__
    except Exception:  # noqa: BLE001
        gdal_version = "unknown"
    try:
        from importlib.metadata import version as _pkg_version

        gbx_version = _pkg_version("geobrix")
    except Exception:  # noqa: BLE001
        gbx_version = "unknown"
    import os

    return {
        "env_arch": platform.machine(),
        "env_cpu_model": platform.processor() or "unknown",
        "env_cpu_count": os.cpu_count() or 0,
        "env_os": platform.system(),
        "env_gbx_version": str(gbx_version),
        "env_gdal_version": str(gdal_version),
        "env_runtime_version": "py" + platform.python_version(),
        "env_where": where,
    }


def peak_rss_mb() -> float:
    try:
        import resource

        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # macOS reports bytes; Linux reports kilobytes.
        return (kb / (1024 * 1024)) if platform.system() == "Darwin" else (kb / 1024)
    except Exception:  # noqa: BLE001
        return 0.0


def _mpix(tile_px: int, bands: int, rows: int) -> float:
    return (tile_px * tile_px * bands * rows) / 1e6


def _open_ds_list(paths):
    """Open synthesized tile paths into a list of datasets under one ExitStack."""
    stack = ExitStack()
    dss = [stack.enter_context(_serde.open_tile(Path(p).read_bytes())) for p in paths]
    return stack, dss


def _load_geometry_corpus(corpus_root):
    """Load the geometry corpus (geometry.json) under the corpus root, or None.

    The geometry corpus is written alongside corpus.json at gen-data time; both
    engines read these identical WKB bytes (write-once-read-both). Returns None if
    absent so non-geometry runs over an older corpus still work.
    """
    p = Path(corpus_root) / "geometry.json"
    if not p.exists():
        return None
    return m.GeometryCorpus.read(p)


def _geometry_set_for(geom_corpus, te):
    """The GeometrySet for a tile: by source_tile path, else by matching srid.

    Geometry is generated per distinct CRS from a representative tile, so a tile
    whose own path is not a geometry source still gets the in-extent set for its
    srid (same projection, so the bounds-derived geometry is in-extent).
    """
    if geom_corpus is None:
        return None
    for gset in geom_corpus.sets.values():
        if gset.source_tile == te.path:
            return gset
    for gset in geom_corpus.sets.values():
        if gset.srid == te.srid:
            return gset
    return None


def _capture_and_call(fs, input_kind, raster, tile_path, synth_paths, geom=None):
    """Build (fingerprint, call) for a pure-core function via its input_kind adapter.

    The fingerprint is the untimed output summary (empty for timing-only fns); the
    `call` closure runs the core_fn once per timed iteration. Both branch on the
    same input_kind: "bytes" (raw raster bytes), "path" (corpus file path),
    "tile_array" (a list of open datasets from the synthesized multi-tile input),
    "geometry" (the open tile PLUS the tile's GeometrySet from the geometry
    corpus, as core_fn(ds, args, geom)), or "tile" (the default: a single opened
    DatasetReader).
    """
    if input_kind == "geometry":

        def _run_geometry(_fs=fs, _b=raster, _g=geom):
            with _serde.open_tile(_b) as ds:
                return _fs.core_fn(ds, _fs.args, _g)

        feed = None
        call = _run_geometry
    elif input_kind == "bytes":
        feed = lambda: raster  # noqa: E731
        call = lambda _b=raster, _fs=fs: _fs.core_fn(_b, _fs.args)  # noqa: E731
    elif input_kind == "path":
        feed = lambda: tile_path  # noqa: E731
        call = lambda _p=tile_path, _fs=fs: _fs.core_fn(_p, _fs.args)  # noqa: E731
    elif input_kind == "tile_array":

        def _run_array(_fs=fs, _paths=synth_paths):
            stack, dss = _open_ds_list(_paths)
            try:
                return _fs.core_fn(dss, _fs.args)
            finally:
                stack.close()

        feed = None
        call = _run_array
    else:

        def _run_tile(_fs=fs, _b=raster):
            with _serde.open_tile(_b) as ds:
                return _fs.core_fn(ds, _fs.args)

        feed = None
        call = _run_tile

    if not getattr(fs, "fingerprint", True):
        return "", call
    # bytes/path feed a value; tile/tile_array reuse the timed `call` (which
    # returns the output) for the one untimed fingerprint pass.
    out = fs.core_fn(feed(), fs.args) if feed is not None else call()
    return _fingerprint_for(fs, out), call


def _fingerprint_for(fs, out):
    """Fingerprint a core_fn output, honoring the FnSpec's fingerprint_kind.

    Most functions use ``"auto"`` -- ``fingerprint_output`` inspects the value
    (bytes -> raster, list-of-bytes -> raster_collection, list-of-scalars ->
    scalar_list, else scalar). Functions whose output shape the auto-detector
    cannot classify declare an explicit kind: ``"dggs_grid"`` (a per-band list of
    cell records the auto path would mis-read as a scalar_list), ``"vector"``, or
    ``"collection"``.
    """
    kind = getattr(fs, "fingerprint_kind", "auto")
    if kind == "dggs_grid":
        return fingerprint_dggs_grid(out)
    if kind == "dggs_grid_str":
        # BNG: STRING cell ids (OS grid refs), not Longs -- no signed-int64 fold.
        return fingerprint_dggs_grid_str(out)
    if kind == "vector":
        return fingerprint_vector(out)
    if kind == "collection":
        return fingerprint_collection(out)
    return fingerprint_output(out)


def run_pure_core(
    corpus_root,
    corpus: m.Corpus,
    fnspecs: List[FnSpec],
    run_id: str,
    warmup: int,
    measured: int,
    where: str,
    sink=None,
) -> List[ResultRow]:
    """sink: optional callable(List[ResultRow]) invoked with each function's rows as
    soon as that function finishes, so a long run is observable in real time (the
    cluster harness uses it to append rows to the Delta table incrementally)."""
    root = Path(corpus_root)
    env = capture_env(where)
    # Loaded lazily-once: geometry-input fns read the tile's GeometrySet from the
    # geometry corpus written alongside corpus.json (write-once-read-both).
    geom_corpus = _load_geometry_corpus(root)
    _leg_fns = [f for f in fnspecs if "pure-core" in f.modes]
    _leg_n = len(_leg_fns)
    print(f"[bench] lightweight pure-core: {_leg_n} fn(s) to run")
    out: List[ResultRow] = []
    _leg_i = 0
    for fs in fnspecs:
        if "pure-core" not in fs.modes:
            continue
        _leg_i += 1
        _mark = len(out)
        # Tile routing: a gb_tile fn (the BNG raster->grid / tessellate fns, which
        # reproject to EPSG:27700 and drop out-of-GB pixels) benches ONLY the
        # Great-Britain-overlapping tile (role "bng_gb") so it bins REAL cells;
        # every other fn benches the ordinary sweep tiles and SKIPS the GB tile, so
        # its tile selection is unchanged. Fall back to the full sweep if the corpus
        # predates the GB tile (older corpora carry no role="bng_gb" entry).
        _want_gb = getattr(fs, "gb_tile", False)
        _tiles = [te for te in corpus.size_sweep if (te.role == "bng_gb") == _want_gb]
        if _want_gb and not _tiles:
            _tiles = list(corpus.size_sweep)  # older corpus: no GB tile -> full sweep
        for te in _tiles:
            if te.bands < getattr(fs, "min_bands", 1):
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="pure-core",
                        tile_px=te.tile_px,
                        bands=te.bands,
                        dtype=te.dtype,
                        srid=te.srid,
                        rows=1,
                        nodata_frac=te.nodata_frac,
                        warmup_iters=warmup,
                        measured_iters=0,
                        iter_median_s=0.0,
                        iter_min_s=0.0,
                        iter_p90_s=0.0,
                        throughput_mpix_s=0.0,
                        throughput_rows_s=0.0,
                        peak_rss_mb=0.0,
                        status="na_by_design",
                        note=f"requires >= {getattr(fs, 'min_bands', 1)} bands",
                        output_fingerprint="",
                        **env,
                    )
                )
                continue
            raster = (root / te.path).read_bytes()
            tile_path = str(root / te.path)
            # The `input_kind` adapter decides what the core_fn is fed:
            #   "bytes": the raw raster bytes (reader/constructor fns whose input
            #            is content, not an open ds -- rst_tryopen/fromcontent).
            #   "path":  the corpus tile's file path (rst_fromfile).
            #   "tile_array": a LIST of open datasets, synthesized from the corpus
            #            tile (write-once-read-both) -- the multi-tile fns
            #            (rst_frombands/combineavg/merge).
            #   "tile"  (default): an opened rasterio DatasetReader -- every
            #            pre-existing function takes this path unchanged.
            input_kind = getattr(fs, "input_kind", "tile")
            # For tile_array, synthesize the multi-tile input ONCE (idempotent) and
            # reuse the same files for the fingerprint pass and every timed call, so
            # the heavy runner reads byte-identical inputs from the same paths.
            synth_paths = (
                _synthesized_paths(root, te.path, fs.name)
                if input_kind == "tile_array"
                else None
            )
            geom = (
                _geometry_set_for(geom_corpus, te) if input_kind == "geometry" else None
            )

            try:
                # Capture the untimed output fingerprint (consistency) and build the
                # timed `call` closure, both keyed off the input_kind adapter.
                fingerprint, call = _capture_and_call(
                    fs, input_kind, raster, tile_path, synth_paths, geom
                )
                stats = time_iters(call, warmup, measured)
                ms = stats["iter_median_ms"]
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="pure-core",
                        tile_px=te.tile_px,
                        bands=te.bands,
                        dtype=te.dtype,
                        srid=te.srid,
                        rows=1,
                        nodata_frac=te.nodata_frac,
                        warmup_iters=stats["warmup_iters"],
                        measured_iters=stats["measured_iters"],
                        iter_median_s=ms / 1000.0,
                        iter_min_s=stats["iter_min_ms"] / 1000.0,
                        iter_p90_s=stats["iter_p90_ms"] / 1000.0,
                        iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"]
                        / 1000.0,
                        avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
                        throughput_mpix_s=(
                            (_mpix(te.tile_px, te.bands, 1) / (ms / 1000.0))
                            if ms
                            else 0.0
                        ),
                        throughput_rows_s=(1.0 / (ms / 1000.0)) if ms else 0.0,
                        peak_rss_mb=peak_rss_mb(),
                        status="ok",
                        note="",
                        output_fingerprint=fingerprint,
                        **env,
                    )
                )
            except Exception as e:  # noqa: BLE001
                out.append(
                    ResultRow(
                        run_id=run_id,
                        api="lightweight",
                        fn=fs.name,
                        category=fs.category,
                        mode="pure-core",
                        tile_px=te.tile_px,
                        bands=te.bands,
                        dtype=te.dtype,
                        srid=te.srid,
                        rows=1,
                        nodata_frac=te.nodata_frac,
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
        # Flush this function's rows now so the run is observable in real time.
        if sink is not None and len(out) > _mark:
            try:
                sink(out[_mark:])
            except Exception:  # noqa: BLE001 — a sink failure must never abort timing
                pass
        # Per-function progress line: emitted outside the timed region so it adds no
        # timing bias.  Pick the representative "ok" row (best-available tile); fall back
        # to the first row of any status if none are ok.
        try:
            _fn_rows = out[_mark:]
            _ok_rows = [r for r in _fn_rows if r.status == "ok"]
            _rep = _ok_rows[0] if _ok_rows else (_fn_rows[0] if _fn_rows else None)
            if _rep is not None:
                _ms_str = f"{_rep.iter_median_s * 1000:.1f}ms"
                print(
                    f"[{_leg_i}/{_leg_n}] {fs.name}  lightweight pure-core"
                    f"  {_ms_str}  {_rep.status}"
                )
            else:
                print(f"[{_leg_i}/{_leg_n}] {fs.name}  lightweight pure-core  -  error")
        except Exception:  # noqa: BLE001 — progress print must never abort the leg
            pass
    return out


def _agg_result_row(fs, run_id, pool, n, env, stats, fingerprint, status, note):
    """A spark-path ResultRow for an aggregate cell (shared ok/error builder)."""
    ms = stats["iter_median_ms"] if stats else 0.0
    measured = stats["measured_iters"] if stats else 0
    warmup = stats["warmup_iters"] if stats else 0
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=fs.name,
        category=fs.category,
        mode="spark-path",
        tile_px=pool.tile_px,
        bands=pool.bands,
        dtype=pool.dtype,
        srid=(pool.tiles[0].srid if pool.tiles else 0),
        rows=n,
        nodata_frac=0.0,
        warmup_iters=warmup,
        measured_iters=measured,
        iter_median_s=ms / 1000.0,
        iter_min_s=(stats["iter_min_ms"] / 1000.0) if stats else 0.0,
        iter_p90_s=(stats["iter_p90_ms"] / 1000.0) if stats else 0.0,
        iter_total_wall_clock_s=(
            stats["iter_total_wall_clock_ms"] / 1000.0 if stats else 0.0
        ),
        avg_wall_clock_s=(stats["avg_wall_clock_ms"] / 1000.0) if stats else 0.0,
        # Headline spark-path metric: amortized wall-clock per aggregated group (iter / n),
        # reported in both seconds and milliseconds. Aggregators set this too (regular
        # spark-path rows already did) so it isn't 0.
        per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
        per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
        throughput_mpix_s=(
            (_mpix(pool.tile_px, pool.bands, n) / (ms / 1000.0)) if ms else 0.0
        ),
        throughput_rows_s=(n / (ms / 1000.0)) if ms else 0.0,
        peak_rss_mb=peak_rss_mb(),
        status=status,
        note=note,
        output_fingerprint=fingerprint,
        **env,
    )


def _tile_aggregate_df(spark, root, corpus, fs):
    """Build the (tile[, band_index]) DataFrame for a tile aggregator's group.

    The fixed CONSISTENCY group is the synthesized tiles for the function's recipe
    (write-once-read-both: the SAME bytes both engines read). combineavg over the
    aligned copies, merge over the offset copies, frombands/derivedband over the
    per-band split. Each synthesized tile becomes ONE group row; frombands rows
    additionally carry a 0-based ``band_index`` (the ascending-sort key both tiers
    rely on). Returns ``(df, has_band_index)`` -- df has columns tile[, band_index].
    """
    from pyspark.sql import functions as F

    recipe = _spec.agg_synth_recipe(fs.name)
    array_root = corpus.row_pool.tiles[0].path
    out_dir = _synth.synth_dir(root, array_root, recipe)
    paths = _synth.synthesize(str(root / array_root), recipe, out_dir)
    rows = []
    for i, p in enumerate(paths):
        d = _serde.build_tile(Path(p).read_bytes(), "GTiff", 0)
        rows.append((d["cellid"], d["raster"], d["metadata"], i))
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("raster", BinaryType(), False),
            StructField("metadata", MapType(StringType(), StringType()), True),
            StructField("band_index", IntegerType(), False),
        ]
    )
    base = spark.createDataFrame(rows, schema=schema)
    df = base.select(
        F.struct("cellid", "raster", "metadata").alias("tile"),
        F.col("band_index"),
    )
    return df, (fs.name == "rst_frombands_agg")


def _geometry_aggregate_df(spark, root, corpus, fs):
    """Build the (geom_wkb, value) DataFrame + extent constants for a geom aggregator.

    The fixed CONSISTENCY group is a slice of the source tile's GeometrySet (boxes
    for rasterize_agg, points for gridfrompoints_agg, zpoints for dtmfromgeoms_agg)
    -- the SAME WKB both engines read via geometry.json. The extent/size/srid are
    per-group constants read from the SAME source tile (so the burn/interp grid
    aligns with the heavy tier). Returns ``(df, extent_tuple)`` where df has columns
    (geom_wkb BINARY, value DOUBLE) and extent is (xmin,ymin,xmax,ymax,w,h,srid).
    """
    import rasterio
    from pyspark.sql.types import BinaryType, DoubleType, StructField, StructType

    geom_corpus = _load_geometry_corpus(root)
    if geom_corpus is None:
        raise RuntimeError("geometry.json absent; cannot run geometry aggregator")
    # The representative source tile (first row_pool tile) supplies extent + geom.
    src_rel = corpus.row_pool.tiles[0].path
    gset = None
    for g in geom_corpus.sets.values():
        if g.source_tile == src_rel:
            gset = g
            break
    if gset is None:
        gset = next(iter(geom_corpus.sets.values()))
    with rasterio.open(root / src_rel) as ds:
        left, bottom, right, top = ds.bounds
        epsg = ds.crs.to_epsg() if ds.crs is not None else None
        extent = (
            float(left),
            float(bottom),
            float(right),
            float(top),
            int(ds.width),
            int(ds.height),
            int(epsg) if epsg is not None else 0,
        )
    if fs.name == "rst_dtmfromgeoms_agg":
        pairs = [(bytes(wkb), 0.0) for wkb in gset.zpoints]
    elif fs.name == "rst_gridfrompoints_agg":
        pairs = [(bytes(wkb), float(v)) for wkb, v in gset.points]
    else:  # rst_rasterize_agg
        pairs = [(bytes(wkb), float(v)) for wkb, v in gset.boxes]
    schema = StructType(
        [
            StructField("geom_wkb", BinaryType(), False),
            StructField("value", DoubleType(), False),
        ]
    )
    df = spark.createDataFrame(pairs, schema=schema)
    return df, extent


def _h3_aggregate_df(spark, fs):
    """Build the (cellid LONG, value DOUBLE) DataFrame for the H3 rasterize agg group.

    The fixed CONSISTENCY group is the deterministic H3 cell set from
    ``spec.h3_rasterize_cells()`` (the res-9 NYC cell grid-disk'd to k=10 -- the
    SAME recipe the Scala BenchDispatch h3Aggregate branch generates, so both
    tiers burn an identical cell set). ``value`` is NULL on every row -> the
    presence-mask burn (1.0). The explicit grid rides in ``fs.args`` (passed to
    both tiers identically), not in this DataFrame. Returns a df with columns
    (cellid LONG, value DOUBLE).
    """
    from pyspark.sql.types import DoubleType, LongType, StructField, StructType

    cells = _spec.h3_rasterize_cells()
    # value=None on every row -> presence mask (heavy + light both burn 1.0).
    rows = [(int(c), None) for c in cells]
    schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("value", DoubleType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def _grid_aggregate_df(spark, fs):
    """Build the (cellid, value DOUBLE) DataFrame for a quadbin/BNG rasterize agg.

    The quadbin/BNG grid aggregators stream (cellid, value) rows from a fixed
    deterministic cell set (the SAME recipe the Scala BenchDispatch generates, so
    both tiers burn an identical cell set) and use the 2-arg auto-derive overload
    (the grid is derived from the cell set, not passed explicitly like H3). Unlike
    ``_h3_aggregate_df`` the cellid dtype varies by grid:
      - quadbin: LONG cell ids (``spec.quadbin_rasterize_cells``).
      - BNG:     STRING OS grid reference ids (``spec.bng_rasterize_cells``).
    ``value`` is NULL on every row -> presence-mask burn (1.0). Returns a df with
    columns (cellid, value DOUBLE).
    """
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    if fs.name == "rst_bng_rasterize_agg":
        cells = _spec.bng_rasterize_cells()  # STRING ids
        cid_type = StringType()
        rows = [(str(c), None) for c in cells]
    else:  # rst_quadbin_rasterize_agg: LONG ids
        cells = _spec.quadbin_rasterize_cells()
        cid_type = LongType()
        rows = [(int(c), None) for c in cells]
    schema = StructType(
        [
            StructField("cellid", cid_type, False),
            StructField("value", DoubleType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema)


def _build_input_tile(path, content, cellid, input_tile):
    """Build a V2 tile dict for the spark-path leg.

    materialized -> bytes tile (build_tile). virtual -> bytes-free tile pointing
    at the path over its whole-file window, built via the real rst_fromfile
    creation path (_fromfile_impl), so the leg dogfoods the feature. Raises on a
    virtual build failure -- NEVER silently falls back to materialized.
    """
    if input_tile == "virtual":
        from databricks.labs.gbx.pyrx.functions import _fromfile_impl

        row = _fromfile_impl(path, "GTiff", False)
        if row is None:
            raise ValueError(f"virtual tile build returned null for {path!r}")
        row["cellid"] = int(cellid)  # preserve corpus cellid for key alignment
        return row
    return _serde.build_tile(bytes(content), "GTiff", int(cellid))


def _disposition_of(fs, sample_out_tile):
    """Disposition for a fn on a virtual input. Accessors: classify by name.
    Tile-returning: inspect the sampled output tile (raster None => deferred).
    None sample => "na" (uncaptured), never a crash."""
    from databricks.labs.gbx.bench.spec import accessor_disposition

    if fs.category == "accessor":
        return accessor_disposition(fs.name, fs)
    if sample_out_tile is None:
        return "na"
    try:
        raster = sample_out_tile["raster"]
    except (KeyError, TypeError, IndexError):
        raster = None
    return "deferred" if raster is None else "materialized"


def _emit_explain(label: str, df, explain_dir: str = "") -> None:
    """Print a DataFrame's formatted physical plan under a labeled header and, when
    explain_dir is set, also persist it to {explain_dir}/{label}.explain.txt so a run's
    plans can be harvested off the Volume afterward instead of scraped from job logs.

    PySpark's df.explain() prints a JVM-returned string via Python print(), so
    redirect_stdout captures it -- we echo to the notebook AND tee to the file.
    """
    import contextlib
    import io

    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            df.explain(mode="formatted")
    except Exception as e:  # noqa: BLE001 — a plan that won't build is itself a finding
        buf.write(f"explain error: {e}\n")
    plan = buf.getvalue()
    header = f"===== EXPLAIN: {label} ====="
    print(f"\n{header}\n{plan}")
    if explain_dir:
        import os

        try:
            os.makedirs(explain_dir, exist_ok=True)
            safe = label.replace("/", "_").replace(" ", "_")
            with open(os.path.join(explain_dir, f"{safe}.explain.txt"), "w") as fh:
                fh.write(f"{header}\n{plan}")
        except Exception as e:  # noqa: BLE001 — harvesting is best-effort, never fatal
            print(f"  (could not write explain file for {label}: {e})")


def _explain_aggregate(spark, fs, group_df, agg_col, n, parts, explain_dir):
    """--explain-only for an aggregator: build the scaled groupBy plan for the largest
    N and print/persist it -- no consistency collect, no timed write. Shows the
    (non-partial) ArrowAggregatePython + the hash-partitioning Exchange + its task
    count, so the *_agg staging can be read off the plan directly without re-running a
    full timed cell.
    """
    from pyspark.sql import functions as F

    try:
        spark.conf.set("spark.sql.shuffle.partitions", str(parts))
    except Exception:  # noqa: BLE001
        pass
    keys = (
        spark.range(n).select(F.col("id").alias("key")).repartition(parts, F.col("key"))
    )
    scaled = keys.crossJoin(F.broadcast(group_df))
    plan_df = scaled.groupBy("key").agg(agg_col(scaled).alias("out"))
    _emit_explain(
        f"{fs.name} (agg, n={n}, shuffle.partitions={parts})", plan_df, explain_dir
    )


def _build_agg_group_df(spark, root, corpus, fs, kind):
    """Build a *_agg aggregator's fixed CONSISTENCY group DataFrame.

    Returns ``(group_df, has_band_index, extent)``. ``has_band_index`` is True only
    for rst_frombands_agg (its per-row ascending-sort key); ``extent`` is the
    per-group (xmin..height, srid) constants for geometry aggregators, else None.
    h3_aggregate streams (cellid, value) rows from the fixed deterministic cell set
    (the explicit grid rides in fs.args, so no extent). grid_aggregate (quadbin /
    BNG) streams (cellid, value) rows and auto-derives the grid from the cell set,
    so likewise no extent.
    """
    if kind == "tile_aggregate":
        group_df, has_band_index = _tile_aggregate_df(spark, root, corpus, fs)
        return group_df, has_band_index, None
    if kind == "h3_aggregate":
        return _h3_aggregate_df(spark, fs), False, None
    if kind == "grid_aggregate":
        return _grid_aggregate_df(spark, fs), False, None
    group_df, extent = _geometry_aggregate_df(spark, root, corpus, fs)
    return group_df, False, extent


def _run_aggregate(
    spark,
    root,
    corpus,
    fs,
    run_id,
    row_counts,
    warmup,
    measured,
    env,
    partition_size=0,
    explain_only=False,
    explain_dir="",
) -> List[ResultRow]:
    """Run a *_agg aggregator: consistency fingerprint + perf timing over groupBy.

    CONSISTENCY: build the fixed group, key every row into ONE group, run
    df.groupBy("key").agg(col_fn(...).alias("out")), COLLECT the single out tile and
    fingerprint its raster bytes. Emitted on the smallest-N row only.
    PERF: for each N in row_counts, broadcast the fixed group into N*group_size rows
    spread over a few keys and time the grouped aggregate via the noop write.
    """
    from pyspark.sql import functions as F

    pool = corpus.row_pool
    kind = fs.input_kind
    try:
        group_df, has_band_index, extent = _build_agg_group_df(
            spark, root, corpus, fs, kind
        )
    except Exception as e:  # noqa: BLE001 — surface a clean error row, don't crash
        return [
            _agg_result_row(fs, run_id, pool, n, env, None, "", "error", str(e)[:300])
            for n in sorted(row_counts)
        ]

    def _agg_col(df):
        """The aggregate Column for this fn over the (already-keyed) DataFrame df."""
        if kind == "tile_aggregate":
            if has_band_index:
                return fs.col_fn(df["tile"], fs.args, df["band_index"])
            return fs.col_fn(df["tile"], fs.args)
        if kind in ("h3_aggregate", "grid_aggregate"):
            # h3/grid aggregate: (cellid, value, args). h3 passes an explicit grid
            # in args; quadbin/BNG (grid_aggregate) auto-derive the grid from the
            # streamed cell set, so args is empty -- both take the same 3-arg col_fn.
            return fs.col_fn(df["cellid"], df["value"], fs.args)
        # geometry aggregate: (geom_wkb, value, extent_tuple, args)
        return fs.col_fn(df["geom_wkb"], df["value"], extent, fs.args)

    out: List[ResultRow] = []
    sorted_counts = sorted(row_counts)

    def _agg_parts(n):
        # Task count for the aggregate's post-shuffle stage. The groupBy injects a mandatory
        # hash-partitioning shuffle on `key` whose width is spark.sql.shuffle.partitions; we
        # set that (below) to _parts. A pre-groupBy repartition does NOT work -- it's elided
        # by the optimizer because the hash exchange supersedes it (confirmed via --explain).
        #
        # MEMORY: keys-per-task = n/_parts, and each key holds its whole group + output in the
        # worker. Large-OUTPUT aggregators (rst_merge_agg's union mosaic is ~2.25x a tile)
        # OOM'd a worker at the old ~6 keys/task (RSS measured ~94MB for 6 merges + Arrow
        # batch + JVM shuffle). So default to a SMALL keys-per-task (2) -> ~1-2 big outputs
        # in flight per task -> bounded per-task memory, still parallel (n/2 partitions over
        # the slots). --override-partition-size still wins for tuning. (Per-task memory > a
        # few extra scheduling waves for the 7 aggregators.)
        import math as _math

        _psize = (
            partition_size
            if (partition_size and partition_size > 0)
            else _AGG_KEYS_PER_TASK
        )
        return max(1, _math.ceil(n / _psize))

    # --explain-only: build the scaled groupBy plan for the largest N and print/persist it
    # -- no consistency collect, no timed write (delegated to _explain_aggregate).
    if explain_only:
        n = sorted_counts[-1]
        _explain_aggregate(spark, fs, group_df, _agg_col, n, _agg_parts(n), explain_dir)
        return []

    # --- consistency: ONE group -> ONE out tile -> raster fingerprint -----------
    fingerprint = ""
    try:
        one = group_df.withColumn("key", F.lit(0))
        collected = one.groupBy("key").agg(_agg_col(one).alias("out")).collect()
        if collected:
            tile = collected[0]["out"]
            raster = tile["raster"] if tile is not None else None
            if raster:
                fingerprint = _fingerprint_for(fs, bytes(raster))
    except Exception as e:  # noqa: BLE001
        return [
            _agg_result_row(fs, run_id, pool, n, env, None, "", "error", str(e)[:300])
            for n in sorted_counts
        ]

    # --- perf: time the scaled groupBy via the noop write -----------------------
    group_df = group_df.cache()
    group_df.count()
    for n in sorted_counts:
        # Replicate the fixed group N times across N distinct keys (one group per key), so
        # the aggregate runs N times -- the scaled distributed-aggregation timing.
        #
        # PARALLELISM: the col_fn is a non-partial Python Arrow UDAF (no map-side partial
        # aggregation -- the whole group per key lands in one post-shuffle stage). The naive
        # form `group_df.crossJoin(broadcast(keys))` broadcasts the KEYS and iterates group_df,
        # which puts the ENTIRE xN replication into group_size tasks (a 2-tile group -> 2 busy
        # tasks while every other slot idles -- the "38/40, 2 running" straggler). Instead:
        # hash-partition the N KEYS into _parts (a cheap shuffle of N longs) and BROADCAST the
        # tiny few-tile group, so the replication runs _parts-wide; and because the keys are
        # already hash-partitioned by `key` into _parts == shuffle.partitions, the groupBy's
        # own hash exchange is ELIDED -- replication + aggregate run in ONE _parts-wide stage.
        # (Confirmed via the local plan harness + --explain-only.) AQE is off (preamble) so
        # shuffle.partitions is respected.
        _parts = _agg_parts(n)
        try:
            spark.conf.set("spark.sql.shuffle.partitions", str(_parts))
        except Exception:  # noqa: BLE001
            pass
        keys = (
            spark.range(n)
            .select(F.col("id").alias("key"))
            .repartition(_parts, F.col("key"))
        )
        scaled = keys.crossJoin(F.broadcast(group_df))
        # Minimal warm-up scaled: ONE key per partition (so each post-shuffle task's
        # Python worker / Arrow UDAF spins up once) instead of the full n-key group --
        # mirrors the regular spark-path's _warm_df. For geometry aggregators _parts is
        # small (1), so this is a single-group warm-up.
        _warm_keys = (
            spark.range(_parts)
            .select(F.col("id").alias("key"))
            .repartition(_parts, F.col("key"))
        )
        _warm_scaled = _warm_keys.crossJoin(F.broadcast(group_df))
        try:

            def job(_df=scaled):
                _df.groupBy("key").agg(_agg_col(_df).alias("out")).write.format(
                    "noop"
                ).mode("overwrite").save()

            def warm(_df=_warm_scaled):
                _df.groupBy("key").agg(_agg_col(_df).alias("out")).write.format(
                    "noop"
                ).mode("overwrite").save()

            stats = time_iters(job, warmup, measured, warmup_fn=warm)
            # Emit the consistency fingerprint on the smallest-N row only.
            fp = fingerprint if n == sorted_counts[0] else ""
            out.append(_agg_result_row(fs, run_id, pool, n, env, stats, fp, "ok", ""))
        except Exception as e:  # noqa: BLE001
            out.append(
                _agg_result_row(
                    fs, run_id, pool, n, env, None, "", "error", str(e)[:300]
                )
            )
    group_df.unpersist()
    return out


def _explain_spark_path(
    spark,
    root,
    corpus,
    fnspecs,
    run_id,
    row_counts,
    warmup,
    measured,
    env,
    partition_size,
    explain_dir,
    df_all,
    nparts,
    input_col,
):
    """--explain-only for the spark-path: build each fn's spark-path DataFrame and PRINT
    its physical plan, no timing / no Delta write. Reveals the exchanges, partial-agg
    placement, and per-fn partition counts so plan issues can be read directly instead
    of inferred from the UI.
    """
    import math

    from pyspark.sql import functions as F

    for _fs in fnspecs:
        if "spark-path" not in _fs.modes:
            continue
        _k = getattr(_fs, "input_kind", "tile")
        _n = max(row_counts)
        try:
            if _k in (
                "tile_aggregate",
                "geometry_aggregate",
                "h3_aggregate",
                "grid_aggregate",
            ):
                _run_aggregate(
                    spark,
                    root,
                    corpus,
                    _fs,
                    run_id,
                    [_n],
                    warmup,
                    measured,
                    env,
                    partition_size=partition_size,
                    explain_only=True,
                    explain_dir=explain_dir,
                )
            elif getattr(_fs, "udtf", False):
                # UDTF grid fn: the spark-path is a SQL LATERAL join, not a scalar
                # column, so build + explain the LATERAL plan (the scalar col_fn
                # would raise NotImplementedError). Register the light UDTF first.
                from databricks.labs.gbx.pyrx import functions as _prx_reg

                _prx_reg.register(spark, only=[_fs.sql_name])
                _ps = (
                    partition_size
                    if (partition_size and partition_size > 0)
                    else max(1, _n // (nparts * 4))
                )
                _parts = max(1, math.ceil(_n / _ps))
                _edf = df_all.repartition(max(1, _parts), F.rand())
                _view = f"_bench_udtf_explain_{_fs.name}"
                _edf.createOrReplaceTempView(_view)
                try:
                    _emit_explain(
                        f"{_fs.name} (udtf-lateral, n={_n}, parts={_parts})",
                        spark.sql(_udtf_lateral_sql(_view, _fs)),
                        explain_dir,
                    )
                finally:
                    try:
                        spark.catalog.dropTempView(_view)
                    except Exception:  # noqa: BLE001
                        pass
            else:
                _ps = (
                    partition_size
                    if (partition_size and partition_size > 0)
                    else max(1, _n // (nparts * 4))
                )
                _parts = _n if _k == "tile_array" else max(1, math.ceil(_n / _ps))
                # _n == max(row_counts) == the cap df_all is already built at, so the cached
                # set IS the input -- no limit (which would funnel through one partition).
                # repartition by F.rand() not the tile struct: tile is the only column and
                # carries the raster bytes -- hashing it would hash megabytes/row and skew timings.
                _edf = df_all.repartition(max(1, _parts), F.rand())
                _ecol = _fs.col_fn(input_col(_fs.name, _k, _edf), _fs.args)
                _emit_explain(
                    f"{_fs.name} (kind={_k}, n={_n}, parts={_parts})",
                    _edf.select(_ecol.alias("out")),
                    explain_dir,
                )
        except Exception as _e:  # noqa: BLE001
            print(f"  explain error for {_fs.name}: {_e}")


def _sql_literal(v) -> str:
    """Render a scalar bench arg as a SQL literal for a LATERAL UDTF call.

    The grid-family UDTFs (rastertogrid* / tessellate) take numeric scalars only
    (resolution / tile_width / tile_height / overlap / min_z / max_z), but keep the
    string branch defensive so a future string-arg UDTF still renders correctly.
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def _udtf_lateral_sql(view: str, fs) -> str:
    """The timed SQL for a UDTF grid fn: SELECT ... FROM <view>, LATERAL <udtf>(...).

    The lightweight grid functions (rastertogrid* / tessellate) are registered as
    Python UDTFs, so their realistic distributed per-tile cost is a SQL LATERAL
    table-function join over the tile DataFrame -- NOT a scalar column (the pyrx
    ``prx.<fn>`` wrapper raises NotImplementedError for exactly this reason). The
    UDTF's first positional arg is the tile struct (``d.tile``); the remaining
    scalar args ride from ``fs.args`` in signature order as SQL literals. ``t.*``
    projects every emitted row so the whole per-tile cell/row fan-out is realized
    (and written to noop for timing), not short-circuited.
    """
    scalar_args = "".join(", " + _sql_literal(v) for v in fs.args.values())
    return (
        f"SELECT t.* FROM {view} AS d, "
        f"LATERAL {fs.sql_name}(d.tile{scalar_args}) AS t"
    )


def _spark_path_warmup(spark, fnspecs, pool, df_all, input_col):
    """One throwaway Spark job so JVM/Spark spin-up isn't charged to the first timed
    cell. Band-aware (mirrors the Scala HeavyRunner warm-up) + guarded so a warm-up
    failure can never abort timing. Excludes aggregate fns from the column warm-up
    (they have no _input_col form).
    """
    _spark_fns = [
        f
        for f in fnspecs
        if "spark-path" in f.modes
        # Exclude the aggregators (no _input_col column form) AND the UDTF grid fns
        # (their scalar col_fn raises NotImplementedError -- they warm up per-fn via
        # the LATERAL warm body inside _run_udtf_lateral instead).
        and not getattr(f, "udtf", False)
        and getattr(f, "input_kind", "tile")
        not in (
            "tile_aggregate",
            "geometry_aggregate",
            "h3_aggregate",
            "grid_aggregate",
        )
    ]
    _warm = next(
        (f for f in _spark_fns if getattr(f, "min_bands", 1) <= pool.bands), None
    )
    if _warm is None and _spark_fns:
        _warm = _spark_fns[0]
    if _warm is not None:
        try:
            _wc = _warm.col_fn(
                input_col(_warm.name, getattr(_warm, "input_kind", "tile")),
                _warm.args,
            )
            df_all.limit(1).select(_wc.alias("warmup")).write.format("noop").mode(
                "overwrite"
            ).save()
        except Exception:  # noqa: BLE001 — warm-up failures must never abort timing
            pass


def _run_udtf_lateral(
    spark,
    fs,
    run_id,
    row_counts,
    warmup,
    measured,
    env,
    pool,
    df_all,
    warm_df,
    max_rows,
    nparts,
    partition_size,
    math,
    F,
) -> List[ResultRow]:
    """Time a UDTF grid fn's spark-path via SQL LATERAL over the tile ladder.

    The lightweight rastertogrid* / tessellate functions are registered Python
    UDTFs; their realistic distributed per-tile cost is a SQL LATERAL table-function
    join over the tile DataFrame (``SELECT t.* FROM <view> AS d, LATERAL
    <udtf>(d.tile, <scalar args>) AS t``), NOT a scalar column -- the scalar
    ``prx.<fn>`` wrapper raises NotImplementedError. This mirrors the scalar
    spark-path cell exactly (same row ladder, same repartition sizing, same noop
    write, same warm-up-one-row-per-slot), only the timed body is the LATERAL SQL.
    Registers the per-fn view name off df_all / warm_df so ``spark.sql`` can join it.
    """
    out: List[ResultRow] = []
    # Register the cached tile DataFrames as temp views the LATERAL SQL can join.
    # Per-fn unique names so parallel/repeated calls don't collide; dropped in finally.
    _safe = fs.name
    _view = f"_bench_udtf_{_safe}"
    _warm_view = f"_bench_udtf_warm_{_safe}"
    for n in sorted(row_counts):
        # Same partition sizing as the scalar spark-path cell (oversubscribe slots).
        if partition_size and partition_size > 0:
            _psize = partition_size
        else:
            _psize = max(1, n // (nparts * 4))
        _parts = max(1, math.ceil(n / _psize))
        _src = df_all if n >= max_rows else df_all.limit(n)
        df = _src.repartition(max(1, _parts), F.rand())
        try:
            df.createOrReplaceTempView(_view)
            warm_df.createOrReplaceTempView(_warm_view)
            _sql = _udtf_lateral_sql(_view, fs)
            _warm_sql = _udtf_lateral_sql(_warm_view, fs)

            def job(_q=_sql):
                spark.sql(_q).write.format("noop").mode("overwrite").save()

            def warm(_q=_warm_sql):
                spark.sql(_q).write.format("noop").mode("overwrite").save()

            stats = time_iters(job, warmup, measured, warmup_fn=warm)
            ms = stats["iter_median_ms"]
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn=fs.name,
                    category=fs.category,
                    mode="spark-path",
                    tile_px=pool.tile_px,
                    bands=pool.bands,
                    dtype=pool.dtype,
                    srid=(pool.tiles[0].srid if pool.tiles else 0),
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
                    throughput_mpix_s=(
                        (_mpix(pool.tile_px, pool.bands, n) / (ms / 1000.0))
                        if ms
                        else 0.0
                    ),
                    throughput_rows_s=(n / (ms / 1000.0)) if ms else 0.0,
                    peak_rss_mb=peak_rss_mb(),
                    status="ok",
                    note="lateral-udtf",
                    output_fingerprint="",
                    **env,
                )
            )
        except Exception as e:  # noqa: BLE001
            out.append(
                ResultRow(
                    run_id=run_id,
                    api="lightweight",
                    fn=fs.name,
                    category=fs.category,
                    mode="spark-path",
                    tile_px=pool.tile_px,
                    bands=pool.bands,
                    dtype=pool.dtype,
                    srid=(pool.tiles[0].srid if pool.tiles else 0),
                    rows=n,
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
        finally:
            try:
                spark.catalog.dropTempView(_view)
                spark.catalog.dropTempView(_warm_view)
            except Exception:  # noqa: BLE001
                pass
    return out


def _sp_progress_line(fn_name, fn_rows, mark, leg_i, leg_n):
    """Print a per-function progress line after a spark-path fn finishes.

    Must never raise -- the outer loop catches nothing here; any print failure
    would abort the timing run.
    """
    try:
        _fn_rows = fn_rows[mark:]
        # Prefer the max-row row (headline throughput); fall back to any ok.
        _ok_rows = [r for r in _fn_rows if r.status == "ok"]
        if _ok_rows:
            _rep = max(_ok_rows, key=lambda r: r.rows)
            _ms_str = (
                f"{_rep.per_tile_avg_ms:.2f}ms/tile @{_rep.rows}r"
                if _rep.per_tile_avg_ms
                else f"{_rep.iter_median_s * 1000:.1f}ms"
            )
            _status = "ok"
        elif _fn_rows:
            _rep = _fn_rows[-1]
            _ms_str = "-"
            _status = _rep.status
        else:
            _ms_str = "-"
            _status = "error"
        print(
            f"[{leg_i}/{leg_n}] {fn_name}  lightweight spark-path"
            f"  {_ms_str}  {_status}"
        )
    except Exception:  # noqa: BLE001 — progress print must never abort the leg
        pass


def _sp_scalar_ok_row(fs, run_id, pool, n, env, stats):
    """Build a successful spark-path ResultRow for a scalar fn at row-count n."""
    ms = stats["iter_median_ms"]
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=fs.name,
        category=fs.category,
        mode="spark-path",
        tile_px=pool.tile_px,
        bands=pool.bands,
        dtype=pool.dtype,
        srid=(pool.tiles[0].srid if pool.tiles else 0),
        rows=n,
        nodata_frac=0.0,
        warmup_iters=stats["warmup_iters"],
        measured_iters=stats["measured_iters"],
        iter_median_s=ms / 1000.0,
        iter_min_s=stats["iter_min_ms"] / 1000.0,
        iter_p90_s=stats["iter_p90_ms"] / 1000.0,
        iter_total_wall_clock_s=stats["iter_total_wall_clock_ms"] / 1000.0,
        avg_wall_clock_s=stats["avg_wall_clock_ms"] / 1000.0,
        # Headline spark-path metric: amortized wall-clock per tile,
        # reported in both seconds and milliseconds.
        per_tile_avg_s=(ms / n / 1000.0) if (ms and n) else 0.0,
        per_tile_avg_ms=(ms / n) if (ms and n) else 0.0,
        throughput_mpix_s=(
            (_mpix(pool.tile_px, pool.bands, n) / (ms / 1000.0)) if ms else 0.0
        ),
        throughput_rows_s=(n / (ms / 1000.0)) if ms else 0.0,
        peak_rss_mb=peak_rss_mb(),
        status="ok",
        note="",
        output_fingerprint="",
        **env,
    )


def _sp_scalar_error_row(fs, run_id, pool, n, env, warmup_iters, e):
    """Build an error spark-path ResultRow for a scalar fn at row-count n."""
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=fs.name,
        category=fs.category,
        mode="spark-path",
        tile_px=pool.tile_px,
        bands=pool.bands,
        dtype=pool.dtype,
        srid=(pool.tiles[0].srid if pool.tiles else 0),
        rows=n,
        nodata_frac=0.0,
        warmup_iters=warmup_iters,
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


def _run_sp_scalar_fn(
    fs,
    run_id,
    pool,
    env,
    row_counts,
    warmup,
    measured,
    df_all,
    warm_df,
    max_rows,
    nparts,
    partition_size,
    input_col_fn,
    math,
    F,
):
    """Time a single scalar spark-path fn over each row-count ladder point.

    Handles partition sizing, per-n DataFrame slicing, warm-up / measured
    timing, and ResultRow building.  Extracted from ``run_spark_path`` to
    reduce that function's cyclomatic complexity.
    """
    import math as _math

    _kind = getattr(fs, "input_kind", "tile")
    rows: List[ResultRow] = []
    for n in sorted(row_counts):
        # Force the partition (task) count via repartition. AQE is disabled for the run
        # (notebook preamble) so it can't coalesce these back toward defaultParallelism
        # (~slots) -- which would reintroduce the straggler idle. (Also handles
        # limit(n<total), which GlobalLimit-collapses to ONE partition -> single-slot.)
        #
        # Partition count = ceil(n / tiles_per_partition). Default tiles/partition =
        # n / (slots * 4) -> ~4 tasks per slot (oversubscribed) so finished slots grab
        # pending tasks instead of idling through a straggler tail. --override-partition-size
        # pins tiles/partition. tile_array fns (rst_merge/combineavg/frombands) emit one
        # often-LARGER output per row from a constant broadcast array; >1 per task can OOM
        # an executor (rst_merge did), so they're hard-pinned to ONE tile/partition.
        if partition_size and partition_size > 0:
            _psize = partition_size
        else:
            _psize = max(1, n // (nparts * 4))
        _parts = n if _kind == "tile_array" else max(1, _math.ceil(n / _psize))
        # df_all is already capped to max_rows at the SOURCE (paths = tiles[:max_rows])
        # and cached, so when n == max_rows the whole cached set IS the input -- use it
        # directly. A per-fn .limit(n) would inject a GlobalLimit that collapses the
        # corpus through ONE partition (SinglePartition funnel) BEFORE the repartition,
        # serializing every tile through a single task ahead of the UDF -- paid per fn for
        # zero benefit when n already == the cached size. Only sub-max ladder points
        # (n < max_rows) still need a limit; that one funnels a smaller (cheaper) subset.
        _src = df_all if n >= max_rows else df_all.limit(n)
        # repartition by F.rand() not the tile struct (avoid hashing the raster payload).
        df = _src.repartition(max(1, _parts), F.rand())
        try:

            def job(_df=df, _fs=fs, _k=_kind):
                c = _fs.col_fn(input_col_fn(_fs.name, _k, _df), _fs.args)
                _df.select(c.alias("out")).write.format("noop").mode("overwrite").save()

            # Warm-up runs over warm_df (one row per slot), NOT the full n rows.
            def warm(_wd=warm_df, _fs=fs, _k=_kind):
                c = _fs.col_fn(input_col_fn(_fs.name, _k, _wd), _fs.args)
                _wd.select(c.alias("out")).write.format("noop").mode("overwrite").save()

            stats = time_iters(job, warmup, measured, warmup_fn=warm)
            rows.append(_sp_scalar_ok_row(fs, run_id, pool, n, env, stats))
        except Exception as e:  # noqa: BLE001
            rows.append(_sp_scalar_error_row(fs, run_id, pool, n, env, warmup, e))
    return rows


def run_spark_path(
    spark,
    corpus_root,
    corpus: m.Corpus,
    fnspecs: List[FnSpec],
    run_id: str,
    row_counts: List[int],
    warmup: int,
    measured: int,
    where: str,
    sink=None,
    partition_size: int = 0,
    explain_only: bool = False,
    explain_dir: str = "",
    input_tile: str = "materialized",
) -> List[ResultRow]:
    """Time each fn as a Spark Column over N tile rows (serialization + UDF overhead).

    explain_only: diagnostic mode -- build each fn's spark-path DataFrame and PRINT its
    physical plan (df.explain) WITHOUT executing/timing or writing to Delta. Lets you read
    where the exchanges / partial-agg stages sit + the partition counts for each function.
    explain_dir: when set (a Volume path), each plan is also written to
    {explain_dir}/{fn}.explain.txt so the plans can be harvested off the Volume afterward.

    sink: optional callable(List[ResultRow]) invoked with each function's rows the
    moment that function finishes (the cluster harness uses it to append rows to the
    Delta table incrementally, so a long run can be polled/queried in real time).

    partition_size: tiles per partition for the row DataFrame. 0 (default) auto-sizes to
    n / (slots * 4) -- i.e. ~4 tasks per slot, OVERSUBSCRIBING the slots so finished
    slots pick up pending tasks instead of sitting idle while a straggler task runs
    (matching partitions to slots wastes the cluster on the straggler tail). A positive
    value pins tiles/partition explicitly (--override-partition-size)."""
    import math

    from pyspark.sql import functions as F

    root = Path(corpus_root)
    env = capture_env(where)
    pool = corpus.row_pool

    # Build the tile DataFrame once, capped at the max row count (paths = tiles[:max_rows]),
    # then cache it. The cap lives at the SOURCE so no downstream limit funnel is needed.
    # Read the tile bytes on the EXECUTORS via spark.read.binaryFile -- the UC-aware
    # reader, the same approach the heavy tier uses (HeavyRunner.scala) -- rather than
    # decoding every tile on the driver and handing Spark a local list. A driver-side
    # createDataFrame(list-of-bytes) becomes a LocalRelation that Spark embeds in each
    # task's serialized closure; at large row counts that blows spark.rpc.message.maxSize
    # (1000 x ~4MB tiles -> a ~348MB task > the 256MB ceiling -> SparkException). Reading
    # via binaryFile distributes the I/O so the bytes never transit the driver as one
    # giant relation, and the harness scales to any row count.
    max_rows = max(row_counts)
    # Refuse to run an under-filled iteration: `pool.tiles[:max_rows]` would silently cap at
    # the pool size and report `rows=max_rows` while only that many distinct tiles were
    # processed -- a misleading measurement. Require pool >= the largest requested row count.
    if max_rows > len(pool.tiles):
        raise ValueError(
            f"spark-path needs a row pool of >= {max_rows} tiles (the largest --row-counts), "
            f"but the corpus pool has only {len(pool.tiles)}. Generate a larger pool "
            f"(gbx:bench:gen-data --row-rows {max_rows}) or lower --row-counts. "
            f"Refusing to run an under-filled iteration."
        )
    tiles = pool.tiles[:max_rows]
    paths = [str(root / te.path) for te in tiles]
    # cellid keyed by file basename: binaryFile's "path" column is a fully-qualified URI
    # (e.g. dbfs:/Volumes/...) that won't string-match the local path, but the basename is
    # stable. Tiny dict (basename -> cellid), safe to capture in the UDF closure.
    _cellid_by_base = {Path(te.path).name: int(te.cellid) for te in tiles}

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA

    @F.udf(returnType=V2_TILE_SCHEMA)
    def _to_tile(path, content):
        import os

        cid = _cellid_by_base.get(os.path.basename(path), 0)
        return _build_input_tile(path, content, cid, input_tile)

    raw = spark.read.format("binaryFile").load(paths)
    # binaryFile partitions by maxPartitionBytes (packs many small tiles per partition);
    # repartition to defaultParallelism to reproduce the previous createDataFrame layout
    # (e.g. a 12-core cluster -> 12 partitions, so 12 Spark tasks for 10 rows, 2 empty),
    # keeping per-row timing comparable to prior runs. We deliberately DON'T coalesce.
    _nparts = max(1, spark.sparkContext.defaultParallelism)
    df_all = (
        raw.select(_to_tile(F.col("path"), F.col("content")).alias("tile"))
        # repartition by F.rand() not the tile struct (hashing the raster payload would
        # hash megabytes/row and skew timings); tile is the only column here.
        .repartition(_nparts, F.rand()).cache()
    )
    df_all.count()  # materialize the cache so it isn't part of timing

    # tile_array adapter (spark-path): the multi-tile fns (rst_frombands/
    # combineavg/merge) consume an ARRAY<tile> column, not a single tile. Build a
    # CONSTANT array literal from the SAME synthesized files the pure-core path
    # writes (write-once-read-both), broadcast across every row. This times the
    # array serialization + UDF overhead, which is the point of the spark-path
    # measurement. The first row_pool tile is the representative source.
    _array_root = pool.tiles[0].path if pool.tiles else None

    def _synth_array_col(fn: str):
        """ARRAY<tile> literal column of the synthesized tiles for a tile_array fn."""
        paths = _synthesized_paths(root, _array_root, fn)
        elems = []
        for p in paths:
            d = _serde.build_tile(Path(p).read_bytes(), "GTiff", 0)
            elems.append(
                F.struct(
                    F.lit(d["cellid"]).cast("long").alias("cellid"),
                    F.lit(d["raster"]).alias("raster"),
                    F.create_map(
                        *[
                            x
                            for k, v in d["metadata"].items()
                            for x in (F.lit(k), F.lit(v))
                        ]
                    ).alias("metadata"),
                )
            )
        return F.array(*elems)

    def _input_col(fn: str, kind: str, df=None):
        """The column fed to col_fn: an array literal for tile_array, else the tile.

        ``df`` selects which DataFrame's ``tile`` column to reference -- the measured
        pass uses df_all, the warm-up pass uses the one-row-per-partition warm DF (a
        cross-DataFrame column ref would fail analysis, so each pass passes its own df).
        The tile_array literal is DataFrame-independent.
        """
        _d = df_all if df is None else df
        return _synth_array_col(fn) if kind == "tile_array" else _d["tile"]

    # Warm-up DF: exactly one tile row per partition of df_all (mapPartitions -> take 1),
    # so a warm-up pass exercises EVERY executor slot's Python worker / UDF once without
    # paying the full row-count cost. (df.limit(N) would pull all N rows from the first
    # couple of partitions, warming only a slot or two.) The measured iterations run the
    # cached df_all directly (repartitioned); only the warm-up uses this slot-spread DF.
    import itertools as _it

    _warm_df = spark.createDataFrame(
        df_all.rdd.mapPartitions(lambda _p: _it.islice(_p, 1)), df_all.schema
    ).cache()
    _warm_df.count()  # materialize so building it isn't charged to any fn's timing

    # --explain-only: build each fn's spark-path DataFrame and PRINT its physical plan,
    # no timing / no Delta write (delegated to _explain_spark_path).
    if explain_only:
        _explain_spark_path(
            spark,
            root,
            corpus,
            fnspecs,
            run_id,
            row_counts,
            warmup,
            measured,
            env,
            partition_size,
            explain_dir,
            df_all,
            _nparts,
            _input_col,
        )
        return []

    # UDTF grid fns (rastertogrid* / tessellate) are invoked via SQL LATERAL, so
    # their registered gbx_ names must exist in this session. Register JUST the light
    # UDTFs needed by this run's udtf fns (idempotent; last registration wins) so the
    # LATERAL branch below can call them. The scalar col_fn path never needs this
    # (its UDFs are module-level singletons), so register only when a udtf fn is present.
    _udtf_fns = [
        f for f in fnspecs if getattr(f, "udtf", False) and "spark-path" in f.modes
    ]
    if _udtf_fns:
        from databricks.labs.gbx.pyrx import functions as _prx_reg

        _prx_reg.register(spark, only=[f.sql_name for f in _udtf_fns])

    # Spark warm-up: one throwaway job so JVM/Spark spin-up isn't charged to the first
    # timed cell (delegated to _spark_path_warmup).
    _spark_path_warmup(spark, fnspecs, pool, df_all, _input_col)

    def _flush(rows, mark):
        # Flush a function's rows now so the run is observable in real time.
        if sink is not None and len(rows) > mark:
            try:
                sink(rows[mark:])
            except Exception:  # noqa: BLE001 — a sink failure must never abort timing
                pass

    out: List[ResultRow] = []
    # bucket A: the *_agg aggregators reduce a GROUP of rows to ONE tile via a real
    # df.groupBy(key).agg(col_fn(...)). They ride input_kind in {"tile_aggregate",
    # "geometry_aggregate"} and are handled by a dedicated aggregate harness (below)
    # that emits BOTH a consistency fingerprint (a fixed deterministic single group
    # -> one out tile -> raster fingerprint) and the perf timing (scaled groupBy).
    _agg_kinds = (
        "tile_aggregate",
        "geometry_aggregate",
        "h3_aggregate",
        "grid_aggregate",
    )
    _sp_leg_fns = [f for f in fnspecs if "spark-path" in f.modes]
    _sp_leg_n = len(_sp_leg_fns)
    print(f"[bench] lightweight spark-path: {_sp_leg_n} fn(s) to run")
    _sp_leg_i = 0

    for fs in fnspecs:
        if "spark-path" not in fs.modes:
            continue
        _sp_leg_i += 1
        _mark = len(out)
        if getattr(fs, "input_kind", "tile") in _agg_kinds:
            out += _run_aggregate(
                spark,
                root,
                corpus,
                fs,
                run_id,
                row_counts,
                warmup,
                measured,
                env,
                partition_size=partition_size,
            )
        elif getattr(fs, "udtf", False):
            # UDTF grid fns (rastertogrid* / tessellate): the lightweight impl is a
            # registered Python UDTF, so the realistic distributed per-tile cost is a
            # SQL LATERAL table-function join over the tile DataFrame -- NOT a scalar
            # column (prx.<fn> raises NotImplementedError). Time
            # `SELECT t.* FROM <view> AS d, LATERAL <udtf>(d.tile, <args>) AS t`
            # written to noop, over the same row ladder + partitioning as the scalar
            # path, so the number is comparable to the scalar spark-path cells.
            out += _run_udtf_lateral(
                spark,
                fs,
                run_id,
                row_counts,
                warmup,
                measured,
                env,
                pool,
                df_all,
                _warm_df,
                max_rows,
                _nparts,
                partition_size,
                math,
                F,
            )
        else:
            out += _run_sp_scalar_fn(
                fs,
                run_id,
                pool,
                env,
                row_counts,
                warmup,
                measured,
                df_all,
                _warm_df,
                max_rows,
                _nparts,
                partition_size,
                _input_col,
                math,
                F,
            )
        if input_tile == "virtual":
            _sample = None
            if fs.category != "accessor" and getattr(fs, "input_kind", "tile") == "tile":
                try:
                    _row1 = (
                        df_all.limit(1)
                        .select(fs.col_fn(_input_col(fs.name, "tile", df_all), fs.args).alias("out"))
                        .collect()
                    )
                    _sample = _row1[0]["out"] if _row1 else None
                except Exception as _e:  # noqa: BLE001 — finding, not a silent skip
                    print(f"[bench][QA] disposition sample failed for {fs.name}: {_e}")
            _disp = _disposition_of(fs, _sample)
            for _i in range(_mark, len(out)):
                out[_i] = _dc.replace(out[_i], input_tile="virtual", output_disposition=_disp)
        _flush(out, _mark)
        _sp_progress_line(fs.name, out, _mark, _sp_leg_i, _sp_leg_n)
    df_all.unpersist()
    return out


def main(argv=None):
    import argparse

    from databricks.labs.gbx.bench import manifest as _m
    from databricks.labs.gbx.bench import results as _r
    from databricks.labs.gbx.bench import spec as _s

    ap = argparse.ArgumentParser(prog="bench.runner")
    ap.add_argument("--corpus", required=True, help="corpus root dir (has corpus.json)")
    ap.add_argument("--out", required=True, help="output JSONL shard")
    ap.add_argument("--functions", default="")
    ap.add_argument("--categories", default="")
    ap.add_argument("--set", default="core", choices=["core", "full"])
    ap.add_argument(
        "--mode", default="both", choices=["pure-core", "spark-path", "both"]
    )
    ap.add_argument("--row-counts", default="10,100,1000,10000")
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--measured", type=int, default=5)
    ap.add_argument("--run-id", default="local")
    ap.add_argument("--where", default="venv")
    a = ap.parse_args(argv)

    corpus = _m.Corpus.read(Path(a.corpus) / "corpus.json")
    fnspecs = _s.select(
        functions=[x for x in a.functions.split(",") if x] or None,
        categories=[x for x in a.categories.split(",") if x] or None,
        set=getattr(a, "set"),
    )
    row_counts = [int(x) for x in a.row_counts.split(",") if x]
    rows: List[ResultRow] = []
    if a.mode in ("pure-core", "both"):
        rows += run_pure_core(
            a.corpus, corpus, fnspecs, a.run_id, a.warmup, a.measured, a.where
        )
    if a.mode in ("spark-path", "both"):
        import os
        import sys

        from pyspark.sql import SparkSession

        # Pin Spark workers to this interpreter (avoids PYTHON_VERSION_MISMATCH when
        # local executors would otherwise pick up a different system python).
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("bench-runner")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        rows += run_spark_path(
            spark,
            a.corpus,
            corpus,
            fnspecs,
            a.run_id,
            row_counts,
            a.warmup,
            a.measured,
            a.where,
        )
    _r.write_jsonl(rows, a.out)
    print(f"wrote {len(rows)} rows -> {a.out}")
    summary_path = (
        a.out[:-6] + ".summary.md"
        if a.out.endswith(".jsonl")
        else a.out + ".summary.md"
    )
    Path(summary_path).write_text(_r.summarize(rows))
    print(f"summary -> {summary_path}")


if __name__ == "__main__":
    main()
