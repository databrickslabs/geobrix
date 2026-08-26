"""Cross-API compare: join heavy vs light shards into speedup + consistency.

Consistency follows the design §11 rules: parse fingerprints (never string-compare),
float-tolerant numeric agreement, dtype excluded (GDAL "Float32" vs numpy "float32"),
nodata_count reported as informational only (neighborhood ops legitimately differ on
the kernel border), and fingerprints are never expected byte-equal across APIs.
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from databricks.labs.gbx.bench import spec
from databricks.labs.gbx.bench.results import ResultRow

REL_TOL = 1e-3
# Near-zero absolute floor. `_close` uses OR-semantics (abs OR rel), so this
# floor is what saves comparisons of near-zero values where the relative test
# is meaningless: a tiny absolute diff divided by a near-zero reference blows
# past REL_TOL (e.g. rst_aspect `min` bearing ~0.0003 deg, hw-vs-light abs diff
# ~2e-4 deg -> ~0.7 relative, a pure metric artifact, NOT a divergence).
# 1e-3 absorbs float32 quantization and near-zero angular noise while staying
# two-plus orders of magnitude below the genuine divergences this suite catches
# (abs diffs >> 1e-2 on values of order 1-100). Do NOT raise REL_TOL to fix
# near-zero cases; that would mask real divergences on meaningful magnitudes.
ABS_TOL = 1e-3
_STATS = ("min", "max", "mean", "std")


def _num(x):
    if x is None:
        return None
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return float(x)
    return None


def _rel_delta(a, b) -> float:
    if a is None or b is None:
        return 0.0 if (a is None and b is None) else math.inf
    denom = max(abs(a), abs(b))
    return 0.0 if denom == 0 else abs(a - b) / denom


def _close(a, b, rel_tol: float, abs_tol: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) <= max(abs_tol, rel_tol * max(abs(a), abs(b)))


def _cmp_pairs(pairs, rel_tol, abs_tol):
    """Compare numeric (a, b) pairs -> (exact, all_close, deltas).

    Shared by every fingerprint kind: each pair is coerced via ``_num``, tested
    for bitwise equality (exact) and float-tolerant agreement (all_close), and
    its finite relative delta is collected.
    """
    exact = True
    all_close = True
    deltas = []
    for a, b in pairs:
        a, b = _num(a), _num(b)
        exact = exact and (a == b)
        all_close = all_close and _close(a, b, rel_tol, abs_tol)
        d = _rel_delta(a, b)
        if d != math.inf:
            deltas.append(d)
    return exact, all_close, deltas


def _pairs_for_kind(kind, hw, lw):
    """Numeric (a, b) pairs + nodata_count_delta for a fingerprint kind.

    Returns (pairs, ndc_delta, err) where err is a divergence note (and pairs is
    None) on a structural mismatch the caller must short-circuit (length/count).
    """
    if kind == "scalar":
        return [(hw.get("value"), lw.get("value"))], 0, None
    if kind == "scalar_list":
        av, bv = hw.get("values") or [], lw.get("values") or []
        if len(av) != len(bv):
            return None, 0, f"scalar_list length {len(av)} vs {len(bv)}"
        return list(zip(av, bv)), 0, None
    if kind == "raster":
        hb, lb = hw.get("bands") or [], lw.get("bands") or []
        if len(hb) != len(lb):
            return None, 0, f"band count {len(hb)} vs {len(lb)}"
        pairs = []
        ndc = 0
        for h, low in zip(hb, lb):
            ndc += int((h.get("nodata_count") or 0) - (low.get("nodata_count") or 0))
            pairs += [(h.get(k), low.get(k)) for k in _STATS]
        return pairs, ndc, None
    if kind == "raster_collection":
        # Tile COUNT must match exactly (a different number of output tiles is a
        # structural divergence, not a numeric one). Then compare the pooled,
        # order-independent agg stats with the same float tolerance as `raster`.
        hc, lc = hw.get("count"), lw.get("count")
        if hc != lc:
            return None, 0, f"tile count {hc} vs {lc}"
        ha, la = hw.get("agg") or {}, lw.get("agg") or {}
        return [(ha.get(k), la.get(k)) for k in _STATS], 0, None
    if kind == "dggs_grid":
        # Cell COUNT must match exactly. Then compare the order-independent agg
        # stats over per-cell measures with the same float tolerance as `raster`.
        # (cells_hash / Jaccard handled by the caller: H3/quadbin ids are
        # parity-comparable, so an identical hash means an exact cell-set match.)
        hc, lc = hw.get("count"), lw.get("count")
        if hc != lc:
            return None, 0, f"cell count {hc} vs {lc}"
        ha, la = hw.get("agg") or {}, lw.get("agg") or {}
        return [(ha.get(k), la.get(k)) for k in _STATS], 0, None
    if kind == "vector":
        # GATE on the total measure (line length / polygon area) and the
        # order-independent attr_agg in tolerance. Feature COUNT is INFORMATIONAL
        # only: two contouring engines (gdal.ContourGenerateEx vs skimage
        # marching-squares) trace the SAME iso-surfaces at the same levels but
        # split them into a different number of features (~8-10% count delta on
        # identical geometry). Count is an arbitrary segmentation artifact, not a
        # divergence signal — the caller folds the count delta into the note and
        # demotes a bitwise-exact stat match to within_tol when count differs.
        ha, la = hw.get("attr_agg") or {}, lw.get("attr_agg") or {}
        pairs = [(hw.get("measure"), lw.get("measure"))]
        pairs += [(ha.get(k), la.get(k)) for k in _STATS]
        return pairs, 0, None
    return None, 0, f"unknown kind {kind}"


def _jaccard(a_ids, b_ids):
    """Jaccard overlap |A ∩ B| / |A ∪ B| of two cell-id sets (0.0..1.0)."""
    sa, sb = set(a_ids or []), set(b_ids or [])
    union = sa | sb
    if not union:
        return 1.0
    return len(sa & sb) / len(union)


def compare_fingerprints(
    hw_fp: str, lw_fp: str, rel_tol: float = REL_TOL, abs_tol: float = ABS_TOL
) -> Tuple[str, float, int, str]:
    """Return (consistency_class, max_rel_delta, nodata_count_delta, note).

    class: "exact" | "within_tol" | "divergent" | "na".
    nodata_count_delta: sum over bands of (hw - lw) nodata_count (informational).
    """
    if not hw_fp or not lw_fp:
        return ("na", 0.0, 0, "missing fingerprint (spark-path or non-ok)")
    try:
        hw = json.loads(hw_fp)
        lw = json.loads(lw_fp)
    except (ValueError, TypeError):
        return ("divergent", math.inf, 0, "unparseable fingerprint")

    if hw.get("kind") != lw.get("kind"):
        return (
            "divergent",
            math.inf,
            0,
            f"kind mismatch {hw.get('kind')} vs {lw.get('kind')}",
        )
    kind = hw.get("kind")

    pairs, ndc_delta, err = _pairs_for_kind(kind, hw, lw)
    if pairs is None:
        return ("divergent", math.inf, 0, err)
    exact, all_close, deltas = _cmp_pairs(pairs, rel_tol, abs_tol)

    max_delta = max(deltas) if deltas else 0.0
    if exact:
        cls = "exact"
    elif all_close:
        cls = "within_tol"
    else:
        cls = "divergent"
    note = ""
    if kind == "dggs_grid":
        # H3/quadbin cell ids are parity-comparable: an identical cells_hash is an
        # exact cell-set match, so `exact` requires both the hash AND the agg to
        # agree bitwise. When the hash differs but count + agg agree within
        # tolerance, demote to within_tol and report the cell-set Jaccard overlap.
        if hw.get("cells_hash") != lw.get("cells_hash"):
            if cls == "exact":
                cls = "within_tol"
            if cls != "divergent":
                jac = _jaccard(hw.get("cell_ids"), lw.get("cell_ids"))
                note = f"cell ids differ; Jaccard overlap {jac:.2f}"
        return (cls, max_delta, 0, note)
    if kind == "vector":
        # measure + attr_agg gate (computed above). Feature COUNT is informational:
        # the two engines legitimately segment identical iso-surfaces into a
        # different number of features. When count differs, demote a bitwise-exact
        # stat match to within_tol (the geometry agrees but the segmentation does
        # not) and always fold the count delta into the note.
        hc, lc = hw.get("count"), lw.get("count")
        if hc != lc:
            if cls == "exact":
                cls = "within_tol"
            base = lc if (isinstance(lc, (int, float)) and lc) else None
            pct = f" ({(hc - lc) / base:+.1%})" if base else ""
            note = (
                f"feature count {hc} vs {lc}{pct} — informational "
                "(segmentation artifact; measure+attr gate)"
            )
        return (cls, max_delta, 0, note)
    if ndc_delta != 0:
        if cls == "divergent":
            note = (
                f"divergence likely nodata/border-handling "
                f"(nodata_count differs by {ndc_delta})"
            )
        else:
            note = f"nodata_count differs by {ndc_delta} (informational; neighborhood-op border)"
    return (cls, max_delta, ndc_delta, note)


@dataclass(frozen=True)
class CellCompare:
    fn: str
    mode: str
    tile_px: int
    bands: int
    dtype: str
    srid: int
    nodata_frac: float
    rows: int
    # Median wall-clock of one full iteration, in SECONDS (read from ResultRow's
    # iter_median_s). Field names retain the _ms suffix for CSV/store compatibility
    # with persisted records, but the stored values are seconds.
    hw_median_ms: float
    lw_median_ms: float
    speedup: float  # hw/lw; >1 => lightweight faster
    consistency: str  # exact | within_tol | divergent | na
    max_rel_delta: float
    nodata_count_delta: int
    note: str
    hw_mpix_s: float
    lw_mpix_s: float
    hw_rows_s: float
    lw_rows_s: float
    # Timing deltas, heavy - light. Positive => heavy took MORE time (heavy
    # slower / lightweight faster); negative => heavy faster.
    hw_minus_lw_ms: float
    delta_pct: float  # (hw - lw) / hw * 100; 0.0 when hw_median_ms == 0


# Token rendered for Δ% when hw_median_ms == 0 (percentage undefined).
_DELTA_PCT_NA = "n/a"


def _delta_ms(hw_median_ms: float, lw_median_ms: float) -> float:
    """Δms = heavy - light. Positive => heavy slower (lightweight faster)."""
    return hw_median_ms - lw_median_ms


def _delta_pct(hw_median_ms: float, lw_median_ms: float):
    """Δ% = (hw - lw) / hw * 100, or None when hw is 0 (undefined)."""
    if not hw_median_ms:
        return None
    return (hw_median_ms - lw_median_ms) / hw_median_ms * 100.0


def _fmt_delta_pct(hw_median_ms: float, lw_median_ms: float) -> str:
    """Render Δ% as a signed, 1-decimal string, or the guard token when hw == 0."""
    pct = _delta_pct(hw_median_ms, lw_median_ms)
    return _DELTA_PCT_NA if pct is None else f"{pct:+.1f}"


def _key(r: ResultRow):
    return (r.fn, r.mode, r.tile_px, r.bands, r.dtype, r.srid, r.nodata_frac, r.rows)


def compare_cells(hw_rows: List[ResultRow], lw_rows: List[ResultRow]):
    """Return (cells, unmatched). cells: matched CellCompare list. unmatched: (fn, side, key)."""
    hw_by = {_key(r): r for r in hw_rows}
    lw_by = {_key(r): r for r in lw_rows}
    cells: List[CellCompare] = []
    for k in sorted(set(hw_by) & set(lw_by)):
        h, lo = hw_by[k], lw_by[k]
        speedup = (h.iter_median_s / lo.iter_median_s) if lo.iter_median_s > 0 else 0.0
        if h.status == "ok" and lo.status == "ok":
            # A function may declare a looser per-fn rel_tol for an inherent
            # cross-engine algorithm spread (e.g. rst_contour's segmentation);
            # fall back to the strict global REL_TOL when it declares none.
            fn_spec = spec.REGISTRY.get(h.fn)
            fn_rel_tol = (fn_spec.rel_tol if fn_spec else None) or REL_TOL
            cls, delta, ndc, note = compare_fingerprints(
                h.output_fingerprint, lo.output_fingerprint, rel_tol=fn_rel_tol
            )
        else:
            cls, delta, ndc, note = (
                "na",
                0.0,
                0,
                f"status hw={h.status} lw={lo.status}",
            )
        cells.append(
            CellCompare(
                fn=h.fn,
                mode=h.mode,
                tile_px=h.tile_px,
                bands=h.bands,
                dtype=h.dtype,
                srid=h.srid,
                nodata_frac=h.nodata_frac,
                rows=h.rows,
                hw_median_ms=h.iter_median_s,
                lw_median_ms=lo.iter_median_s,
                speedup=speedup,
                consistency=cls,
                max_rel_delta=delta,
                nodata_count_delta=ndc,
                note=note,
                hw_mpix_s=h.throughput_mpix_s,
                lw_mpix_s=lo.throughput_mpix_s,
                hw_rows_s=h.throughput_rows_s,
                lw_rows_s=lo.throughput_rows_s,
                hw_minus_lw_ms=_delta_ms(h.iter_median_s, lo.iter_median_s),
                delta_pct=(_delta_pct(h.iter_median_s, lo.iter_median_s) or 0.0),
            )
        )
    unmatched = [
        (hw_by[k].fn, "heavyweight", k) for k in sorted(set(hw_by) - set(lw_by))
    ]
    unmatched += [
        (lw_by[k].fn, "lightweight", k) for k in sorted(set(lw_by) - set(hw_by))
    ]
    return cells, unmatched


_CSV_FIELDS = [
    "fn",
    "mode",
    "tile_px",
    "bands",
    "dtype",
    "srid",
    "nodata_frac",
    "rows",
    "hw_median_ms",
    "lw_median_ms",
    "hw_minus_lw_ms",
    "delta_pct",
    "speedup",
    "consistency",
    "max_rel_delta",
    "nodata_count_delta",
    "note",
    "hw_mpix_s",
    "lw_mpix_s",
    "hw_rows_s",
    "lw_rows_s",
]


def write_csv(cells: List[CellCompare], path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        w.writeheader()
        for cl in cells:
            w.writerow({k: getattr(cl, k) for k in _CSV_FIELDS})


def pyrx_implemented() -> "frozenset[str]":
    """Names of every `rst_*` function the lightweight pyrx API implements.

    Derived by parsing `def rst_<name>` definitions out of the pyrx
    `functions.py` so the coverage scorecard guards against a regression (a
    registered function losing its pyrx binding). Path resolved relative to the
    repo root, mirroring `spec.registered_rst()`.
    """
    root = Path(__file__).resolve()
    rel = Path("python/geobrix/src/databricks/labs/gbx/pyrx/functions.py")
    src = None
    for _ in range(12):
        c = root / rel
        if c.exists():
            src = c.read_text()
            break
        root = root.parent
    if src is None:
        # Installed without the repo tree (e.g. the wheel on a cluster): read pyrx
        # functions.py from the IMPORTED module's own location instead of the repo
        # path, mirroring spec.registered_rst's on-cluster fallback.
        from databricks.labs.gbx.pyrx import functions as _prx

        src = Path(_prx.__file__).read_text()
    names = set(re.findall(r"def (rst_[a-z0-9_]+)", src))
    return frozenset(names)


def registered_raster() -> "frozenset[str]":
    """Registered raster (`rst_*`) function names — the scope `pyrx_implemented`
    covers.

    ``spec.registered_rst()`` spans `rst_*` AND `st_*` (it is the full bench-registry
    coverage set). The `st_*` functions are pyvx/vector bindings with no pyrx (light
    *raster*) definition, so raster parity/coverage reporting must exclude them or the
    functional-parity gap would wrongly flag every `st_*` function as "no lightweight
    implementation" (they are pyvx-implemented, just not pyrx).
    """
    return frozenset(n for n in spec.registered_rst() if n.startswith("rst_"))


def coverage_block(cells) -> str:
    """Build the neutral 'Coverage & parity' markdown section.

    Reports benchmark coverage, parity counts among compared cells, performance
    win counts, the computed functional-parity gap (registered minus pyrx-
    implemented), and the registered functions not yet benchmarked.
    """
    registered = registered_raster()
    implemented = pyrx_implemented()
    n_registered = len(registered)

    benched_fns = {cl.fn for cl in cells}
    n_benched = len(benched_fns)

    # Parity over compared (non-na) cells.
    compared = [cl for cl in cells if cl.consistency != "na"]
    n_exact = sum(1 for cl in compared if cl.consistency == "exact")
    n_tol = sum(1 for cl in compared if cl.consistency == "within_tol")
    div_fns = sorted({cl.fn for cl in compared if cl.consistency == "divergent"})
    n_na = sum(1 for cl in cells if cl.consistency == "na")

    # Performance: lightweight >= heavyweight when speedup >= 1.0.
    perf = [cl for cl in cells if cl.lw_median_ms > 0 and cl.hw_median_ms > 0]
    n_lw_fast = sum(1 for cl in perf if cl.speedup >= 1.0)
    n_hw_fast = len(perf) - n_lw_fast

    # Functional parity gap: registered minus pyrx-implemented (COMPUTED).
    gap = sorted(registered - implemented)

    # Not yet covered: registered minus benchmarked.
    not_covered = sorted(registered - benched_fns)

    lines = ["## Coverage & parity", ""]
    lines.append(
        f"- **Benchmark coverage:** {n_benched} / {n_registered} registered `rst_` "
        f"functions have a benchmark cell. (This is PERF coverage; functional "
        f"coverage is separate -- see the parity gap below.)"
    )
    div_part = (" - divergent: " + ", ".join(div_fns)) if div_fns else ""
    lines.append(
        f"- **Parity:** of {len(compared)} compared cell(s) — exact {n_exact} - "
        f"within_tol {n_tol} - divergent {len(div_fns)}{div_part}. "
        f"({n_na} timing-only, not compared.)"
    )
    lines.append(
        f"- **Performance:** lightweight at least as fast (speedup ≥ 1.0) in "
        f"{n_lw_fast} of {len(perf)} compared function-cell(s); heavyweight "
        f"faster in {n_hw_fast}."
    )
    if gap:
        lines.append(
            f"- **Functional parity gap:** {len(gap)} registered `rst_` "
            f"function(s) with no lightweight implementation: " + ", ".join(gap) + "."
        )
    else:
        lines.append(
            f"- **Functional parity gap:** 0 (lightweight implements all "
            f"{n_registered} registered functions)."
        )
    if not_covered:
        lines.append(
            f"- **No comparison cell in this run:** {len(not_covered)} registered "
            f"`rst_` function(s) -- all implemented + in the bench registry, but "
            f"pure-core-only / timing-only (geometry-in, readers, metadata accessors), "
            f"so a spark-path run produces no comparison cell. Run --modes both / "
            f"pure-core to exercise them: " + ", ".join(not_covered) + "."
        )
    else:
        lines.append(
            "- **Not yet benchmarked:** 0 (every registered function has a benchmark cell)."
        )
    lines.append("")
    return "\n".join(lines)


_CONSISTENCY_RANK = {"divergent": 0, "na": 1, "within_tol": 2, "exact": 3}


def _worst_consistency(consistencies) -> str:
    """The least-agreeing consistency class across a function's cells.

    Ranks divergent < na < within_tol < exact, so a single divergent cell makes
    the whole function divergent. Empty -> "na".
    """
    if not consistencies:
        return "na"
    return min(consistencies, key=lambda c: _CONSISTENCY_RANK.get(c, 1))


def scorecard_from_store(
    root=None, specs_by_name=None, stale_only: bool = False
) -> str:
    """Aggregate the authoritative store into a neutral coverage/parity scorecard.

    Read-only over ``store.read_all(root)`` (no benchmarking). Reports benchmark
    coverage (N / 107), parity counts over compared cells (exact/within_tol/
    divergent + divergent fn names, plus timing-only na), performance win counts
    (lightweight at-least-as-fast vs heavyweight-faster), the computed functional
    parity gap (registered minus pyrx-implemented), the registered functions not
    yet covered by a store record, and a per-function table with a STALE marker
    when the function's sources changed since its record was validated.

    ``specs_by_name`` is injectable for tests; defaults to the live full registry.
    With ``stale_only=True`` only the aggregate lines + the stale/missing list
    print (the per-function table is omitted). Neutral voice — coverage, parity,
    performance, staleness; no deprecation language.
    """
    from databricks.labs.gbx.bench import spec as _spec
    from databricks.labs.gbx.bench import store as _store

    if specs_by_name is None:
        specs_by_name = {s.name: s for s in _spec.select(set="full")}

    registered = registered_raster()
    implemented = pyrx_implemented()
    n_registered = len(registered)

    records = _store.read_all(root)
    record_names = {r["fn"] for r in records}
    n_benched = len(records)

    # Flatten all cells across records for parity + performance aggregation.
    all_cells = []
    for r in records:
        for c in r.get("cells") or []:
            all_cells.append(c)

    def _cnum(c, k):
        v = c.get(k)
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    compared = [c for c in all_cells if c.get("consistency") != "na"]
    n_exact = sum(1 for c in compared if c.get("consistency") == "exact")
    n_tol = sum(1 for c in compared if c.get("consistency") == "within_tol")
    div_fns = sorted(
        {
            r["fn"]
            for r in records
            for c in (r.get("cells") or [])
            if c.get("consistency") == "divergent"
        }
    )
    n_na = sum(1 for c in all_cells if c.get("consistency") == "na")

    # Performance: lightweight at least as fast when speedup >= 1.0 (hw/lw).
    perf = [
        c
        for c in all_cells
        if (_cnum(c, "lw_median_ms") or 0) > 0 and (_cnum(c, "hw_median_ms") or 0) > 0
    ]
    n_lw_fast = sum(1 for c in perf if (_cnum(c, "speedup") or 0) >= 1.0)
    n_hw_fast = len(perf) - n_lw_fast

    gap = sorted(registered - implemented)
    not_covered = sorted(registered - record_names)

    # Per-function rows (fn, worst consistency, max delta, representative speedup,
    # short commit, STALE marker).
    rows = []
    for r in sorted(records, key=lambda r: r["fn"]):
        fn = r["fn"]
        cells = r.get("cells") or []
        worst = _worst_consistency([c.get("consistency") for c in cells])
        deltas = [
            _cnum(c, "max_rel_delta")
            for c in cells
            if _cnum(c, "max_rel_delta") is not None
        ]
        max_delta = max(deltas) if deltas else 0.0
        speedups = [_cnum(c, "speedup") for c in cells if _cnum(c, "speedup")]
        speedup = speedups[0] if speedups else 0.0
        # Representative cell for timing deltas: the same one used for speedup
        # above (first cell with a usable speedup), else the first cell.
        rep = None
        for cell in cells:
            if _cnum(cell, "speedup"):
                rep = cell
                break
        if rep is None and cells:
            rep = cells[0]
        rep_hw = (_cnum(rep, "hw_median_ms") or 0.0) if rep else 0.0
        rep_lw = (_cnum(rep, "lw_median_ms") or 0.0) if rep else 0.0
        commit = r.get("validated_commit") or ""
        short = commit.replace("dirty:", "")[:7]
        if commit.startswith("dirty:"):
            short = "dirty:" + short
        sp = specs_by_name.get(fn)
        stale = sp is None or _store.is_stale(sp, r, root=root)
        rows.append(
            {
                "fn": fn,
                "consistency": worst,
                "max_rel_delta": max_delta,
                "speedup": speedup,
                "delta_ms": _delta_ms(rep_hw, rep_lw),
                "delta_pct": _fmt_delta_pct(rep_hw, rep_lw),
                "commit": short,
                "stale": stale,
            }
        )

    # Sort divergent/stale first for visibility, then by name.
    def _row_sort_key(row):
        divergent = row["consistency"] == "divergent"
        return (not (divergent or row["stale"]), row["fn"])

    rows.sort(key=_row_sort_key)

    lines = ["# GeoBrix benchmark — store scorecard", ""]
    lines.append(
        f"- **Benchmark coverage:** {n_benched} / {n_registered} registered `rst_` "
        f"functions have a benchmark cell. (This is PERF coverage; functional "
        f"coverage is separate -- see the parity gap below.)"
    )
    div_part = (" - divergent: " + ", ".join(div_fns)) if div_fns else ""
    lines.append(
        f"- **Parity:** of {len(compared)} compared cell(s) — exact {n_exact} - "
        f"within_tol {n_tol} - divergent {len(div_fns)}{div_part}. "
        f"({n_na} timing-only, not compared.)"
    )
    lines.append(
        f"- **Performance:** lightweight at least as fast (speedup ≥ 1.0) in "
        f"{n_lw_fast} of {len(perf)} compared function-cell(s); heavyweight "
        f"faster in {n_hw_fast}."
    )
    if gap:
        lines.append(
            f"- **Functional parity gap:** {len(gap)} registered `rst_` "
            f"function(s) with no lightweight implementation: " + ", ".join(gap) + "."
        )
    else:
        lines.append(
            f"- **Functional parity gap:** 0 (lightweight implements all "
            f"{n_registered} registered functions)."
        )
    if not_covered:
        lines.append(
            f"- **Not yet covered:** {len(not_covered)} registered `rst_` "
            f"function(s) without a store record: " + ", ".join(not_covered) + "."
        )
    else:
        lines.append(
            "- **Not yet covered:** 0 (every registered function has a store record)."
        )

    stale_fns = sorted(row["fn"] for row in rows if row["stale"])
    if stale_fns:
        lines.append(
            f"- **Stale:** {len(stale_fns)} record(s) whose sources changed since "
            f"validation — re-run `gbx:bench:changed`: " + ", ".join(stale_fns) + "."
        )
    else:
        lines.append("- **Stale:** 0 (every store record is up to date).")
    lines.append("")

    if stale_only:
        return "\n".join(lines)

    lines += [
        "## Per-function",
        "",
        "_Δms/Δ% = heavy − light (heavy timing minus light); positive → heavy "
        "slower (lightweight faster), negative → heavy faster._",
        "",
        "| fn | consistency | max_rel_delta | speedup | Δms | Δ% | validated | stale |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['fn']} | {row['consistency']} | {row['max_rel_delta']:.3g} | "
            f"{row['speedup']:.2f} | {row['delta_ms']:+.3f} | {row['delta_pct']} | "
            f"{row['commit']} | {'STALE' if row['stale'] else ''} |"
        )
    lines.append("")
    return "\n".join(lines)


def _hoist_dims(items, dims):
    """Split run-constant dims out of a table into a one-line context preamble.

    ``dims`` is a list of ``(label, getter, fmt)``. A dim with a single distinct
    value across ``items`` is hoisted (its ``fmt(value)`` token rendered once
    above the table); a dim that varies stays a column.
    """
    const_tokens, varying = [], []
    for label, getter, fmt in dims:
        vals = {getter(it) for it in items}
        if len(vals) == 1:
            const_tokens.append(fmt(next(iter(vals))))
        else:
            varying.append((label, getter, fmt))
    return const_tokens, varying


def _compare_context_line(const_tokens, items, pool_size):
    """Italic context line of hoisted dim tokens + an optional pool token."""
    tokens = list(const_tokens)
    if pool_size is not None:
        max_rows = max((it.rows for it in items), default=0)
        if max_rows and pool_size < max_rows:
            token = f"⚠ corpus pool {pool_size} < {max_rows} tiles/iter"
        else:
            token = f"corpus pool {pool_size} tiles available"
        tokens.append(token)
    if not tokens:
        return None
    return "_" + " · ".join(tokens) + "_"


# Hoistable dimension descriptors over CellCompare: (label, getter, fmt).
_CMP_DIM_TILE_PX = ("tile_px", lambda c: c.tile_px, lambda v: f"tile_px {v}²")
_CMP_DIM_BANDS = ("bands", lambda c: c.bands, lambda v: f"{v} bands")
_CMP_DIM_DTYPE = ("dtype", lambda c: c.dtype, lambda v: f"{v}")
_CMP_DIM_SRID = ("srid", lambda c: c.srid, lambda v: f"srid {v}")
_CMP_DIM_ROWS = ("rows", lambda c: c.rows, lambda v: f"tile scale {v}")


def summarize_compare(cells, unmatched, hw_rows, lw_rows, pool_size=None) -> str:
    lines = ["# GeoBrix benchmark — heavy vs light comparison", "", "## Insights", ""]
    insights = []
    ok_speed = [cl for cl in cells if cl.lw_median_ms > 0 and cl.hw_median_ms > 0]
    if ok_speed:
        lw_win = max(ok_speed, key=lambda cl: cl.speedup)
        hw_win = min(ok_speed, key=lambda cl: cl.speedup)
        insights.append(
            f"Biggest lightweight win: `{lw_win.fn}` ({lw_win.mode}) — {lw_win.speedup:.1f}x "
            f"(hw {lw_win.hw_median_ms:.2f} s vs lw {lw_win.lw_median_ms:.2f} s)."
        )
        if hw_win.speedup > 0:
            insights.append(
                f"Biggest heavyweight win: `{hw_win.fn}` ({hw_win.mode}) — {1.0 / hw_win.speedup:.1f}x "
                f"(hw {hw_win.hw_median_ms:.2f} s vs lw {hw_win.lw_median_ms:.2f} s)."
            )
    pc = [cl for cl in cells if cl.consistency != "na"]
    n_exact = sum(1 for cl in pc if cl.consistency == "exact")
    n_tol = sum(1 for cl in pc if cl.consistency == "within_tol")
    div = [cl for cl in pc if cl.consistency == "divergent"]
    if pc:
        msg = (
            f"Consistency ({len(pc)} compared cells): exact {n_exact} - within-tol {n_tol} - "
            f"divergent {len(div)}"
        )
        if div:
            msg += " - divergent: " + ", ".join(sorted({cl.fn for cl in div}))
        insights.append(msg)
    ndc = sorted({cl.fn for cl in cells if cl.nodata_count_delta != 0})
    if ndc:
        insights.append(
            "NoData-count differs (informational, neighborhood-op border) for: "
            + ", ".join(ndc)
            + " - value stats still agree within tolerance; not a divergence."
        )
    if unmatched:
        ufns = sorted({u[0] for u in unmatched})
        insights.append(
            f"{len(unmatched)} unmatched cell(s) across {len(ufns)} fn(s): "
            + ", ".join(ufns[:8])
        )
    lines += [f"- {b}" for b in insights] if insights else ["- (no cells compared)"]
    lines += [""]
    lines += [
        f"_Consistency (all functions): **exact** = every stat bitwise-equal; "
        f"**within_tol** = every stat agrees to rel ≤ {REL_TOL:g} OR abs ≤ {ABS_TOL:g}; "
        f"**divergent** = neither. Per-cell max_rel_delta in comparison.csv._",
        "",
        "_Δs/Δ% = heavy − light (heavy timing minus light); positive → heavy "
        "slower (lightweight faster), negative → heavy faster._",
        "",
        "_Timing columns: `hw_iter_s`/`lw_iter_s` = median wall-clock (seconds) of ONE "
        "full iteration over all N tiles (whole distributed job). `hw_per_tile_s`/"
        "`lw_per_tile_s` = that ÷ N (amortized per tile, seconds). Same N both tiers._",
        "",
    ]
    _sp = sorted({cl.rows for cl in cells if cl.mode == "spark-path"})
    if _sp:
        _rc = ", ".join(str(x) for x in _sp)
        lines += [
            f"_**Tile scale: {_rc} tiles/iteration** (spark-path) — every tile processed "
            f"each timed iteration, not a sample. Pure-core = 1 tile/iter._",
            "",
        ]

    lines += coverage_block(cells).split("\n")

    dims = [
        _CMP_DIM_TILE_PX,
        _CMP_DIM_BANDS,
        _CMP_DIM_DTYPE,
        _CMP_DIM_SRID,
        _CMP_DIM_ROWS,
    ]
    for mode in ("pure-core", "spark-path"):
        mc = [cl for cl in cells if cl.mode == mode]
        if not mc:
            continue
        if mode == "pure-core":
            tput_labels = ["hw_mpix/s", "lw_mpix/s"]

            def tput_cells(cl):
                return [f"{cl.hw_mpix_s:.1f}", f"{cl.lw_mpix_s:.1f}"]

        else:
            # spark-path headline: amortized per-tile wall-clock (iter_median_s / rows,
            # in seconds), more interpretable + N-stable than rows/s (its inverse). Same
            # row count both tiers, so the speedup column is unchanged.
            tput_labels = ["hw_per_tile_s", "lw_per_tile_s"]

            def tput_cells(cl):
                return [
                    f"{(cl.hw_median_ms / cl.rows if cl.rows else 0.0):.5f}",
                    f"{(cl.lw_median_ms / cl.rows if cl.rows else 0.0):.5f}",
                ]

        const_tokens, varying = _hoist_dims(mc, dims)
        lines += [f"## {mode} (hw vs lw)", ""]
        ctx = _compare_context_line(const_tokens, mc, pool_size)
        if ctx:
            lines += [ctx, ""]
        # fn is always first; varying dims sit between fn and the metric columns;
        # all timing/throughput/consistency columns are always present.
        metric_labels = (
            ["hw_iter_s", "lw_iter_s", "Δs", "Δ%"]
            + tput_labels
            + ["speedup", "consistency", "note"]
        )
        cols = ["fn"] + [lbl for lbl, _, _ in varying] + metric_labels
        lines += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        for cl in sorted(mc, key=lambda c: c.speedup, reverse=True):
            row = (
                [cl.fn]
                + [str(getter(cl)) for _, getter, _ in varying]
                + [
                    f"{cl.hw_median_ms:.2f}",
                    f"{cl.lw_median_ms:.2f}",
                    f"{_delta_ms(cl.hw_median_ms, cl.lw_median_ms):+.2f}",
                    f"{_fmt_delta_pct(cl.hw_median_ms, cl.lw_median_ms)}",
                ]
                + tput_cells(cl)
                + [f"{cl.speedup:.2f}", cl.consistency, cl.note]
            )
            lines.append("| " + " | ".join(row) + " |")
        lines += [""]
    return "\n".join(lines)


def main(argv=None):
    import argparse

    from databricks.labs.gbx.bench import results as _r

    ap = argparse.ArgumentParser(prog="bench.compare")
    ap.add_argument("--heavyweight", required=True, help="heavyweight.jsonl shard")
    ap.add_argument("--lightweight", required=True, help="lightweight.jsonl shard")
    ap.add_argument(
        "--out-dir", required=True, help="dir for comparison.csv + summary.md"
    )
    a = ap.parse_args(argv)

    hw_rows = _r.read_jsonl(a.heavyweight)
    lw_rows = _r.read_jsonl(a.lightweight)
    cells, unmatched = compare_cells(hw_rows, lw_rows)
    out = Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(cells, out / "comparison.csv")
    (out / "summary.md").write_text(
        summarize_compare(cells, unmatched, hw_rows, lw_rows)
    )
    print(
        f"compared {len(cells)} cells ({len(unmatched)} unmatched) -> "
        f"{out}/comparison.csv, summary.md"
    )


if __name__ == "__main__":
    main()
