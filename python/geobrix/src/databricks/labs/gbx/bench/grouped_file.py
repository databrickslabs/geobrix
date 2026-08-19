"""Grouped FILE-amortization benchmark leg (the headline virtual-tile perf win).

The scalar spark-path bench never touches ``grouped_tile_map``: it uses scalar
Column expressions and a one-window-per-file corpus, so the OPEN cost is paid
once per tile and there is nothing to amortize.  This module benchmarks the
GROUPED variants (``rst_clip_grouped`` + pixel-op ``_grouped`` fns) that route
through ``pyrx.grouped_exec.grouped_tile_map``, on a MULTIWINDOW corpus (many
windows per source COG) where grouping a source's windows into one partition
lets ONE ``.open()`` serve all of them.

Three tile modes span the amortization gradient:

- ``materialized``       — raster bytes inline in the row (``path`` null).  The
  LRU is never consulted; bytes are already present.  Baseline.
- ``virtual-file-off``   — path+window tiles, ``GBX_DISABLE_FILE=1`` so no
  ``_file_ref`` column is added and every tile opens its source per-row (FUSE).
- ``virtual-file-on``    — path+window tiles, FILE enabled: ``grouped_tile_map``
  adds a ``_file_ref`` column and the per-partition ``OpenResourceLRU`` opens
  each source once and reuses it across that source's windows.

The ``virtual-file-on`` amortization only *engages* on a FILE-capable runtime
(Databricks with ``try_to_file``); on plain local Spark ``file_supported()``
returns False and mode C degrades to the same per-tile fallback as mode B —
correct output, no open amortization.  The amortization mechanism itself is
verified locally at the ``OpenResourceLRU`` level (see the bench test), and the
end-to-end open-count win is measured on-cluster.

Connect-safe: no ``spark.sparkContext`` / ``_sc`` / ``df.rdd`` / ``_jvm`` /
``_jsc``; the FILE on/off toggle is a driver-side ``os.environ`` flag (read by
``file_supported`` before any Spark call), never ``spark.conf.set``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional

from .results import ResultRow
from .runner import capture_env, peak_rss_mb, time_iters

# The three tile modes, in amortization-gradient order.
MODES = ("materialized", "virtual-file-off", "virtual-file-on")

# Default multiwindow-corpus manifest basename (written by
# datagen.generate_cog_multiwindow_corpus).
MANIFEST_NAME = "cog_multiwindow_manifest.json"


# ---------------------------------------------------------------------------
# Grouped-fn registry
# ---------------------------------------------------------------------------
#
# Each entry maps a grouped-fn name to a callable that, given the tile
# DataFrame plus driver-side context (the clip geometry + its CRS derived from
# the corpus), returns the transformed DataFrame.  All picked fns route through
# grouped_tile_map with view="pixels" so the source OPEN cost dominates -- the
# whole point of the amortization.  rst_clip_grouped is the canonical one the
# design calls out; the other three are pixel-reading reprojection / terrain
# ops where the read is the expensive part.


def _apply_clip(prx, df, *, geom_wkb, geom_crs, **_):
    return prx.rst_clip_grouped(df, geom_wkb, clip_crs=geom_crs)


def _apply_transform(prx, df, *, target_srid, **_):
    # Reproject to WebMercator EPSG code via the generic transform fn.
    return prx.rst_transform_grouped(df, int(target_srid))


def _apply_to_webmercator(prx, df, **_):
    return prx.rst_to_webmercator_grouped(df)


def _apply_slope(prx, df, **_):
    return prx.rst_slope_grouped(df)


# name -> (apply, category-note).  category is a free label used only for the
# ResultRow.category column; it groups the leg's rows in the summary.
_GROUPED_FNS: "Dict[str, Callable]" = {
    "rst_clip_grouped": _apply_clip,
    "rst_transform_grouped": _apply_transform,
    "rst_to_webmercator_grouped": _apply_to_webmercator,
    "rst_slope_grouped": _apply_slope,
}

DEFAULT_FNS = tuple(_GROUPED_FNS.keys())


def grouped_fn_names() -> List[str]:
    """Names of the grouped fns this leg benchmarks (registry order)."""
    return list(_GROUPED_FNS.keys())


# ---------------------------------------------------------------------------
# Corpus construction
# ---------------------------------------------------------------------------


def synthesize_multiwindow_corpus(
    out_dir,
    *,
    cog_count: int = 2,
    windows_per_cog: int = 8,
    cog_px: int = 512,
    window_px: int = 128,
    srid: int = 4326,
    bands: int = 1,
    dtype: str = "float32",
    seed: int = 13,
    compress: str = "DEFLATE",
) -> Path:
    """Write a tiny K-COG x M-window corpus for the LOCAL verify.

    Delegates to ``datagen.generate_cog_multiwindow_corpus`` -- each COG is
    internally tiled (driver="COG") so windowed reads are cheap, and the
    manifest holds ``windows_per_cog`` distinct windows per source so grouping
    has something to amortize.  Returns the manifest path.
    """
    from .datagen import generate_cog_multiwindow_corpus

    return generate_cog_multiwindow_corpus(
        out_dir=out_dir,
        seed=seed,
        cog_count=cog_count,
        windows_per_cog=windows_per_cog,
        cog_px=cog_px,
        bands=bands,
        dtype=dtype,
        srid=srid,
        window_px=window_px,
        compress=compress,
    )


def resolve_manifest_path(corpus_path=None, manifest_path=None) -> Path:
    """Resolve the manifest JSON path from a corpus dir OR an explicit path.

    ``corpus_path`` may point either directly at the manifest JSON or at the
    directory that contains ``cog_multiwindow_manifest.json``.  ``manifest_path``
    takes precedence when both are given.
    """
    if manifest_path is not None:
        return Path(manifest_path)
    if corpus_path is None:
        raise ValueError("one of corpus_path / manifest_path is required")
    p = Path(corpus_path)
    if p.is_dir():
        return p / MANIFEST_NAME
    return p


def read_manifest_rows(manifest_path) -> List[dict]:
    """Load ``[{"path": <abs>, "window": [c, r, w, h]}, ...]`` from the manifest."""
    rows = json.loads(Path(manifest_path).read_text())
    out = []
    for r in rows:
        win = r["window"]
        out.append({"path": str(r["path"]), "window": [int(v) for v in win]})
    return out


def _clip_geom_from_source(src_path):
    """Derive a full-extent clip polygon (WKB) + CRS string from a source COG.

    Opens the first source raster on the driver (rasterio reads a FUSE
    ``/Volumes`` path fine) and returns a WKB box covering the whole source
    extent so ``rst_clip_grouped`` overlaps every window of every COG (the COGs
    in a multiwindow corpus share one georeferencing transform).  Serverless-safe
    -- pure rasterio, no Spark.
    """
    import rasterio
    from shapely.geometry import box

    with rasterio.open(str(src_path)) as ds:
        b = ds.bounds
        crs = ds.crs
        crs_str = None
        if crs is not None:
            epsg = crs.to_epsg()
            crs_str = f"EPSG:{epsg}" if epsg is not None else crs.to_wkt()
    poly = box(b.left, b.bottom, b.right, b.top)
    return poly.wkb, crs_str


def _window_gtiff_bytes(src_path, window) -> bytes:
    """Read one pixel window from a source COG into standalone GeoTIFF bytes.

    Used to build the ``materialized`` mode's inline raster: the same pixels a
    virtual tile would read lazily, baked into the row.
    """
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.windows import Window
    from rasterio.windows import transform as window_transform

    col_off, row_off, width, height = window
    with rasterio.open(str(src_path)) as ds:
        win = Window(col_off, row_off, width, height)
        data = ds.read(window=win)
        profile = ds.profile.copy()
        profile.update(
            driver="GTiff",
            width=width,
            height=height,
            transform=window_transform(win, ds.transform),
        )
        # COG-specific creation keys are invalid for a plain GTiff writer.
        for k in ("blocksize", "BLOCKSIZE", "overview_resampling"):
            profile.pop(k, None)
        with MemoryFile() as mf:
            with mf.open(**profile) as dst:
                dst.write(data)
            return bytes(mf.read())


def build_tile_df(spark, manifest_rows, *, mode: str):
    """Build a one-column (``tile``) DataFrame of V2 tiles for the given mode.

    - ``materialized``: ``raster`` = window GeoTIFF bytes, ``path`` null.
    - virtual modes: ``path`` + ``window`` set, ``raster`` null.

    ``cellid`` is the row index (unique, non-null).  Rows preserve manifest
    order so same-source windows are adjacent, but grouped_tile_map re-groups
    by path internally regardless.
    """
    from pyspark.sql.types import StructField, StructType

    from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

    virtual = mode != "materialized"
    tile_rows = []
    for i, r in enumerate(manifest_rows):
        path, window = r["path"], r["window"]
        if virtual:
            vt = VirtualTile(cellid=i, path=path, window=tuple(window))
        else:
            raster = _window_gtiff_bytes(path, window)
            vt = VirtualTile(cellid=i, raster=raster)
        tile_rows.append({"tile": vt.to_row()})

    schema = StructType([StructField("tile", V2_TILE_SCHEMA, nullable=False)])
    return spark.createDataFrame(tile_rows, schema=schema)


# ---------------------------------------------------------------------------
# The bench leg
# ---------------------------------------------------------------------------


def _force(df) -> None:
    """Force full grouped compute + output serialization (Connect-safe).

    A ``noop`` write runs the ``mapInPandas`` mapper AND serializes the returned
    tile struct (the produced GeoTIFF bytes), so the whole grouped pipeline is
    charged -- stronger than ``.count()``, which need not materialize the
    struct payload.
    """
    df.write.format("noop").mode("overwrite").save()


def _mode_is_virtual(mode: str) -> bool:
    return mode != "materialized"


def _set_file_env(mode: str) -> Optional[str]:
    """Set/clear GBX_DISABLE_FILE for the mode; return the prior value to restore.

    - ``virtual-file-off`` sets ``GBX_DISABLE_FILE=1`` so file_supported() is
      False (driver adds no _file_ref column -> per-tile fallback).
    - all other modes clear it so FILE engages where the runtime supports it.
    Driver-side env only -- no spark.conf (Connect-safe).
    """
    prior = os.environ.get("GBX_DISABLE_FILE")
    if mode == "virtual-file-off":
        os.environ["GBX_DISABLE_FILE"] = "1"
    else:
        os.environ.pop("GBX_DISABLE_FILE", None)
    return prior


def _restore_file_env(prior: Optional[str]) -> None:
    if prior is None:
        os.environ.pop("GBX_DISABLE_FILE", None)
    else:
        os.environ["GBX_DISABLE_FILE"] = prior


def run_grouped_file(
    spark,
    *,
    corpus_path=None,
    manifest_path=None,
    fns: Optional[List[str]] = None,
    modes=MODES,
    warmup: int = 0,
    measured: int = 1,
    run_id: Optional[str] = None,
    env: Optional[dict] = None,
    where: str = "docker",
    category: str = "grouped-file",
    progress: bool = True,
) -> List[ResultRow]:
    """Benchmark grouped fns df->df across the three tile modes.

    Emits one ResultRow per (fn, mode).  ``mode`` on the row is the canonical
    ``"spark-path"`` so the row flows through the existing summary/sink; the
    three-way tile mode is carried in ``split_strategy`` (e.g.
    ``"virtual-file-on"``) and echoed in ``note``.  ``input_tile`` is
    ``"materialized"`` for mode A and ``"virtual"`` for B/C.

    ``warmup``/``measured`` default to 0/1 (the standing spark-path bench rule).
    """
    from databricks.labs.gbx.pyrx import functions as prx

    resolved_manifest = resolve_manifest_path(corpus_path, manifest_path)
    rows = read_manifest_rows(resolved_manifest)
    if not rows:
        raise ValueError(f"empty multiwindow manifest: {resolved_manifest}")
    n = len(rows)
    n_sources = len({r["path"] for r in rows})

    fn_names = list(fns) if fns else list(DEFAULT_FNS)
    unknown = [f for f in fn_names if f not in _GROUPED_FNS]
    if unknown:
        raise ValueError(f"unknown grouped fns: {unknown}")

    if run_id is None:
        import time as _t

        run_id = f"grouped-file-{int(_t.time())}"
    if env is None:
        env = capture_env(where)

    # Driver-side clip context, derived from the first source (COGs share georef).
    geom_wkb, geom_crs = _clip_geom_from_source(rows[0]["path"])
    ctx = {"geom_wkb": geom_wkb, "geom_crs": geom_crs, "target_srid": 3857}

    out: List[ResultRow] = []
    for mode in modes:
        prior = _set_file_env(mode)
        try:
            df = build_tile_df(spark, rows, mode=mode).cache()
            df.count()  # materialize the cache OUTSIDE any fn's timing
            input_tile = "virtual" if _mode_is_virtual(mode) else "materialized"
            for fn_name in fn_names:
                apply = _GROUPED_FNS[fn_name]
                try:

                    def job(_apply=apply):
                        _force(_apply(prx, df, **ctx))

                    stats = time_iters(job, warmup, measured)
                    out.append(
                        _ok_row(
                            fn_name,
                            category,
                            run_id,
                            mode,
                            input_tile,
                            n,
                            stats,
                            env,
                        )
                    )
                    if progress:
                        _print_progress(fn_name, mode, out[-1], n_sources)
                except (
                    Exception
                ) as e:  # noqa: BLE001 - one fn/mode must not abort the leg
                    out.append(
                        _err_row(fn_name, category, run_id, mode, input_tile, n, env, e)
                    )
                    if progress:
                        print(f"  {fn_name} [{mode}] ERROR: {str(e)[:160]}", flush=True)
            df.unpersist()
        finally:
            _restore_file_env(prior)
    return out


def _ok_row(fn, category, run_id, mode, input_tile, n, stats, env) -> ResultRow:
    ms = stats["iter_median_ms"]
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=fn,
        category=category,
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
        throughput_rows_s=(n / (ms / 1000.0)) if ms else 0.0,
        peak_rss_mb=peak_rss_mb(),
        status="ok",
        note=f"grouped-file/{mode}",
        input_tile=input_tile,
        output_disposition="materialized",
        split_strategy=mode,
        **env,
    )


def _err_row(fn, category, run_id, mode, input_tile, n, env, e) -> ResultRow:
    return ResultRow(
        run_id=run_id,
        api="lightweight",
        fn=fn,
        category=category,
        mode="spark-path",
        tile_px=0,
        bands=0,
        dtype="",
        srid=0,
        rows=n,
        nodata_frac=0.0,
        warmup_iters=0,
        measured_iters=0,
        iter_median_s=0.0,
        iter_min_s=0.0,
        iter_p90_s=0.0,
        throughput_mpix_s=0.0,
        throughput_rows_s=0.0,
        peak_rss_mb=0.0,
        status="error",
        note=f"grouped-file/{mode}: {str(e)[:240]}",
        input_tile=input_tile,
        output_disposition="na",
        split_strategy=mode,
        **env,
    )


def _print_progress(fn, mode, row, n_sources) -> None:
    try:
        print(
            f"  grouped-file {fn} [{mode}]  "
            f"{row.per_tile_avg_ms:.2f}ms/tile @{row.rows}r / {n_sources}src  "
            f"{row.status}",
            flush=True,
        )
    except Exception:  # noqa: BLE001
        pass
