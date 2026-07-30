"""Benchmark result row schema, JSONL IO, and a single-API markdown summary."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class ResultRow:
    run_id: str
    api: str  # "heavyweight" | "lightweight"
    fn: str
    category: str
    mode: str  # "pure-core" | "spark-path"
    tile_px: int
    bands: int
    dtype: str
    srid: int
    rows: int
    nodata_frac: float
    warmup_iters: int
    measured_iters: int
    iter_median_s: float
    iter_min_s: float
    iter_p90_s: float
    throughput_mpix_s: float
    throughput_rows_s: float
    peak_rss_mb: float
    status: str  # "ok" | "na_by_design" | "error"
    note: str
    env_arch: str
    env_cpu_model: str
    env_cpu_count: int
    env_os: str
    env_gbx_version: str
    env_gdal_version: str
    env_runtime_version: str
    env_where: str  # "docker" | "venv" | "cluster"
    output_fingerprint: str = (
        ""  # JSON summary of the output (pure-core only); "" when not captured
    )
    # Wall clock over the measured iterations: total (sum) + avg (mean = total /
    # measured_iters), in SECONDS. Complements iter_median_s/iter_min_s/iter_p90_s.
    # Default 0.0 so error/skip rows (no timing) stay valid; OK rows pass the real
    # values from time_iters (converted ms -> s).
    iter_total_wall_clock_s: float = 0.0
    avg_wall_clock_s: float = 0.0
    # Spark-path headline metric: amortized wall-clock per tile = iter_median / rows.
    # Stable across row counts and directly comparable light-vs-heavy (same row count
    # both tiers). 0.0 for pure-core (single-tile) and error/skip rows. Reported in
    # BOTH seconds (per_tile_avg_s) and milliseconds (per_tile_avg_ms).
    per_tile_avg_s: float = 0.0
    per_tile_avg_ms: float = 0.0
    # Monotonic per-run event index, assigned at WRITE time (Delta sink) in execution
    # order so the table's first column shows what ran last + reveals per-event slowdown.
    # 0 at construction; the cluster sink stamps the real value (continues across resume).
    run_event_num: int = 0
    # splitStrategy label for the large-raster profile. None for all other profiles so
    # existing callers are unaffected (the field has a default). Carries values like
    # "none", "serverless", "classic", or "auto".
    split_strategy: Optional[str] = None


def write_jsonl(rows: List[ResultRow], path) -> None:
    with Path(path).open("w") as fh:
        for row in rows:
            fh.write(json.dumps(asdict(row)) + "\n")


# Scala BenchRow (heavyweight shard) serializes millisecond-based field names that
# don't match the canonical seconds-based ResultRow. Map ms -> s on read so the
# heavyweight shard loads into the same dataclass as the lightweight shard.
_MS_TO_S_FIELDS = {
    "median_ms": "iter_median_s",
    "min_ms": "iter_min_s",
    "p90_ms": "iter_p90_s",
    "total_wall_clock_ms": "iter_total_wall_clock_s",
    "avg_wall_clock_ms": "avg_wall_clock_s",
}

# ResultRow field names (set once for cheap membership checks on read).
_RESULTROW_FIELDS = {f.name for f in fields(ResultRow)}


def _normalize_row(d: dict) -> dict:
    """Coerce a raw JSONL record into ResultRow kwargs.

    Heavyweight (Scala BenchRow) rows carry ms-suffixed timing fields; convert
    those to the canonical seconds fields. Then drop any keys ResultRow does not
    declare (forward-compat with extra columns either writer may add).
    """
    if any(k in d for k in _MS_TO_S_FIELDS):
        for ms_key, s_key in _MS_TO_S_FIELDS.items():
            if ms_key in d:
                val = d.pop(ms_key)
                d.setdefault(s_key, (val / 1000.0) if val is not None else 0.0)
    return {k: v for k, v in d.items() if k in _RESULTROW_FIELDS}


def read_jsonl(path) -> List[ResultRow]:
    out = []
    with Path(path).open() as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(ResultRow(**_normalize_row(json.loads(line))))
    return out


def _derive_insights(rows, ok, errs, na, pure, spark) -> List[str]:
    """Data-derived, human-readable observations. Each bullet is conditional on the data."""
    out: List[str] = []
    if pure:
        s = max(pure, key=lambda r: r.iter_median_s)
        out.append(
            f"Slowest pure-core op: `{s.fn}` — {s.iter_median_s:.2f} s "
            f"at {s.tile_px}²/{s.bands}-band ({s.dtype})."
        )
        tps = [r.throughput_mpix_s for r in pure if r.throughput_mpix_s]
        if tps:
            out.append(
                f"Pure-core throughput spans {min(tps):.1f}–{max(tps):.1f} Mpix/s "
                f"across the size sweep."
            )
        byfn = defaultdict(list)
        for r in pure:
            byfn[r.fn].append(r)
        for fn, rs in byfn.items():
            if len({r.tile_px for r in rs}) >= 2:
                small = min(rs, key=lambda r: r.tile_px)
                large = max(rs, key=lambda r: r.tile_px)
                if small.iter_median_s > 0 and small.tile_px > 0:
                    t_ratio = large.iter_median_s / small.iter_median_s
                    px_ratio = (large.tile_px**2) / (small.tile_px**2)
                    rel = t_ratio / px_ratio if px_ratio else 0
                    shape = (
                        "≈linear in pixels"
                        if 0.5 <= rel <= 2.0
                        else "sub-linear" if t_ratio < px_ratio else "supra-linear"
                    )
                    out.append(
                        f"Scaling: `{fn}` pure-core {small.tile_px}²→{large.tile_px}² is "
                        f"{t_ratio:.1f}× time for {px_ratio:.0f}× pixels ({shape})."
                    )
                break
    if spark and pure:
        spark_fns = {r.fn for r in spark}
        pure_fns = {r.fn for r in pure}
        common = sorted(spark_fns & pure_fns)
        if common:
            fn = common[0]
            sp = min((r for r in spark if r.fn == fn), key=lambda r: r.rows)
            pc = min((r for r in pure if r.fn == fn), key=lambda r: r.tile_px)
            out.append(
                f"Spark-path carries Spark/UDF overhead: `{fn}` {sp.iter_median_s:.2f} s for "
                f"{sp.rows} rows (spark-path) vs {pc.iter_median_s:.4f} s for one tile (pure-core)."
            )
    if errs:
        efns = sorted({r.fn for r in errs})
        notes = sorted({r.note.split(":")[0] for r in errs if r.note})
        hint = (" — e.g. " + "; ".join(notes[:2])) if notes else ""
        shown = ", ".join(efns[:8]) + (" …" if len(efns) > 8 else "")
        out.append(
            f"⚠ {len(errs)} error row(s) across {len(efns)} fn(s): {shown}{hint}."
        )
    if na:
        out.append(f"{len(na)} row(s) N/A by design (mode/shape not applicable).")
    flagged = []
    for r in ok:
        if r.mode != "pure-core" or not r.output_fingerprint or r.nodata_frac <= 0:
            continue
        try:
            fp = json.loads(r.output_fingerprint)
        except (ValueError, TypeError):
            continue
        if fp.get("kind") == "raster":
            bands = fp.get("bands") or []
            if bands and all(b.get("nodata_count") == 0 for b in bands):
                flagged.append(r.fn)
    if flagged:
        uniq = sorted(set(flagged))
        shown = ", ".join(uniq[:6]) + (" …" if len(uniq) > 6 else "")
        out.append(
            f"🔎 Consistency: {len(flagged)} output(s) on nodata-bearing tiles report "
            f"nodata_count=0 ({shown}) — sentinels likely treated as data. "
            f"Verify NoData semantics before heavy-vs-light comparison."
        )
    return out


def _hoist_dims(items, dims):
    """Split run-constant dims out of a table into a one-line context preamble.

    ``dims`` is a list of ``(label, getter, fmt)``. A dim with a single distinct
    value across ``items`` is hoisted: its ``fmt(value)`` token goes into
    ``const_tokens`` (rendered once above the table). A dim that varies stays a
    column (returned in ``varying``).
    """
    const_tokens, varying = [], []
    for label, getter, fmt in dims:
        vals = {getter(it) for it in items}
        if len(vals) == 1:
            const_tokens.append(fmt(next(iter(vals))))
        else:
            varying.append((label, getter, fmt))
    return const_tokens, varying


def _context_line(const_tokens, items, pool_size):
    """Italic context line of hoisted dim tokens + an optional pool token.

    The pool token is appended whenever ``pool_size`` is known; if the pool is
    smaller than the largest row count in the table it gets a visible warning
    marker so a too-small corpus pool is obvious at a glance.
    """
    tokens = list(const_tokens)
    if pool_size is not None:
        # The corpus pool is the set of tiles AVAILABLE; "N tiles/iter" (the rows dim)
        # is how many are processed each iteration. Normally pool >= tiles/iter (all
        # of the per-iter set is drawn from the pool); flag the abnormal pool<rows case.
        max_rows = max((it.rows for it in items), default=0)
        if max_rows and pool_size < max_rows:
            token = f"⚠ corpus pool {pool_size} < {max_rows} tiles/iter"
        else:
            token = f"corpus pool {pool_size} tiles available"
        tokens.append(token)
    if not tokens:
        return None
    return "_" + " · ".join(tokens) + "_"


# Hoistable dimension descriptors: (label, getter, fmt). Metric columns
# (iter_median_s, throughput, note, …) are never hoisted — only these dims.
_DIM_TILE_PX = ("tile_px", lambda r: r.tile_px, lambda v: f"tile_px {v}²")
_DIM_BANDS = ("bands", lambda r: r.bands, lambda v: f"{v} bands")
_DIM_DTYPE = ("dtype", lambda r: r.dtype, lambda v: f"{v}")
_DIM_SRID = ("srid", lambda r: r.srid, lambda v: f"srid {v}")
_DIM_ROWS = ("rows", lambda r: r.rows, lambda v: f"tile scale {v}")


def summarize(rows: List[ResultRow], pool_size=None) -> str:
    """Human-friendly markdown summary: insights at the top, then status + tables."""
    if not rows:
        return "# Benchmark summary\n\n(no rows)\n"
    first = rows[0]
    ok = [r for r in rows if r.status == "ok"]
    errs = [r for r in rows if r.status == "error"]
    na = [r for r in rows if r.status == "na_by_design"]
    pure = [r for r in ok if r.mode == "pure-core"]
    spark = [r for r in ok if r.mode == "spark-path"]

    lines = [f"# GeoBrix benchmark summary — {first.api} (run: {first.run_id})", ""]
    lines.append(
        f"_Env: {first.env_arch} · {first.env_os} · gbx {first.env_gbx_version} · "
        f"GDAL {first.env_gdal_version} · where={first.env_where} · "
        f"{len({r.fn for r in rows})} functions · {len(rows)} records_"
    )
    # Run config — state the spark-path tile count explicitly (don't make the reader
    # infer it from the run_id), and make clear EVERY tile is processed each iteration.
    _sp_rows = sorted({r.rows for r in rows if r.mode == "spark-path"})
    if _sp_rows:
        _rc = ", ".join(str(x) for x in _sp_rows)
        lines.append(
            f"_**Tile scale: {_rc} tiles/iteration** (spark-path) — every tile is "
            f"processed on each timed iteration, not a sample. Pure-core = 1 tile/iter._"
        )
    # Metric legend: the two timing columns measure different things.
    lines.append(
        "_`iter_median_s` = median wall-clock (seconds) of ONE full iteration over all "
        "N tiles (whole distributed job). `per_tile_avg` = `iter_median_s` / N "
        "(amortized per tile) — the headline spark-path rate, shown in both seconds "
        "(`per_tile_avg_s`) and milliseconds (`per_tile_avg_ms`)._"
    )
    lines += ["", "## Insights", ""]
    insights = _derive_insights(rows, ok, errs, na, pure, spark)
    lines += [f"- {b}" for b in insights] if insights else ["- (no notable insights)"]
    lines += [
        "",
        "## Status",
        "",
        f"ok {len(ok)} · na_by_design {len(na)} · error {len(errs)}",
        "",
    ]

    def _emit_table(title, items, dims, metric_labels, metric_cells):
        """Render one hoist-aware table: context line, then header + rows.

        ``metric_cells(r)`` returns the list of always-column metric strings for
        a row. Dimension columns are inserted between ``fn`` and the metrics only
        when they vary across ``items``.
        """
        const_tokens, varying = _hoist_dims(items, dims)
        lines.append(f"## {title}")
        lines.append("")
        ctx = _context_line(const_tokens, items, pool_size)
        if ctx:
            lines.extend([ctx, ""])
        cols = ["fn"] + [lbl for lbl, _, _ in varying] + metric_labels
        lines.extend(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)])
        for r in items:
            cells = (
                [r.fn]
                + [str(getter(r)) for _, getter, _ in varying]
                + list(metric_cells(r))
            )
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    slow = sorted(pure, key=lambda r: r.iter_median_s, reverse=True)[:15]
    if slow:
        _emit_table(
            "Slowest pure-core functions (by iter_median_s)",
            slow,
            [_DIM_TILE_PX, _DIM_BANDS, _DIM_DTYPE, _DIM_SRID],
            ["iter_median_s", "mpix/s"],
            lambda r: [f"{r.iter_median_s:.2f}", f"{r.throughput_mpix_s:.1f}"],
        )
    if spark:
        _emit_table(
            "Spark-path (per_tile_avg = iter_median_s / tiles)",
            sorted(spark, key=lambda r: (r.fn, r.rows)),
            [_DIM_TILE_PX, _DIM_BANDS, _DIM_DTYPE, _DIM_SRID, _DIM_ROWS],
            ["iter_median_s", "per_tile_avg_s", "per_tile_avg_ms", "rows/s"],
            lambda r: [
                f"{r.iter_median_s:.2f}",
                f"{(r.iter_median_s / r.rows if r.rows else 0.0):.5f}",
                f"{(r.iter_median_s / r.rows * 1000.0 if r.rows else 0.0):.3f}",
                f"{r.throughput_rows_s:.1f}",
            ],
        )
    if errs:
        _emit_table(
            "Errors",
            errs[:30],
            [_DIM_TILE_PX, _DIM_BANDS, _DIM_SRID],
            ["note"],
            lambda r: [r.note],
        )
    return "\n".join(lines)


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(prog="bench.results")
    ap.add_argument(
        "--in", dest="inp", required=True, help="input <engine>.jsonl shard"
    )
    ap.add_argument(
        "--out",
        default=None,
        help="output .md (default: <shard-without-.jsonl>.summary.md)",
    )
    a = ap.parse_args(argv)
    rows = read_jsonl(a.inp)
    out = a.out or (
        a.inp[:-6] + ".summary.md"
        if a.inp.endswith(".jsonl")
        else a.inp + ".summary.md"
    )
    Path(out).write_text(summarize(rows))
    print(f"summary -> {out}")


if __name__ == "__main__":
    main()
