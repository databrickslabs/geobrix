# Raster-grid reducers: sum, variance, stddev — Design

**Date:** 2026-07-26
**Status:** Design (approved direction); pending plan. Sequenced AFTER the LE3c positive-area covering fix lands.
**Branch:** `issues/49` (toward v0.4.3)
**Relates:** `raster-bng-quadbin-h3-parity`, `heavy-tier-nullable-numeric-return`, `justify-by-utility-not-mosaic`, `perf-parity-light-vs-heavy`

## 1. Goal & scope

Extend the raster→grid reducer family (currently `avg`, `count`, `min`, `max`, `median`) with three
new zonal statistics, across **all three grids** (H3, quadbin, BNG) and **both tiers**:

- **`sum`** — total of valid pixel values per cell. Highest-utility, lowest-risk. Population/area totals.
- **`variance`** — population variance of valid pixels per cell. Zonal heterogeneity/texture.
- **`stddev`** — population standard deviation = `sqrt(variance)`.

That is **+9 functions** (3 reducers × 3 grids), each with a light + heavy impl:
`gbx_rst_{h3,quadbin,bng}_rastertogrid{sum,variance,stddev}`.

**Explicitly deferred (YAGNI / need a concrete use case):** `mode` and `count_distinct` — only meaningful
on categorical rasters and need a defined deterministic tie-break; revisit if land-cover demand appears.

**Sequencing:** ship `sum` first (trivial, reuses existing sum machinery), then `variance`+`stddev`
together (they share the two-pass core and the parity-pinning work). This spec covers all three; the plan
may split into `sum` and `variance`+`stddev` phases.

## 2. Grounding facts (verified 2026-07-26)

### 2.1 Extension points are clean

- **Light** `pyrx/core/gridagg.py`: `_grouped_measures(cids, vals, agg)` already branches per agg over a
  `np.unique`+`bincount` grouping; `_AGGS = ("avg","count","min","max","median")`. Adding a branch +
  extending `_AGGS` is the whole light change. `avg` already computes per-cell sums via
  `np.bincount(inv, weights=vals)` — `sum` is literally that without the `/counts`.
- **Heavy** `rasterx/expressions/grid/`: one file per reducer per grid (5 each × 3 = 15 today), each an
  `InvokedExpression` binding an `fAgg: ArrayBuffer[Double] => T` lambda and delegating to the shared
  `RST_{grid}_RasterToGrid.execute`. `avg`'s fAgg is `values.sum / values.length`. New reducers add one
  file per grid binding their fAgg. Registration in `functions.scala`; String cellID for BNG, Long for
  H3/quadbin (unchanged pattern).

### 2.2 Empty-cell semantics — inherited, no new NULL question (spec §2.6 of the parity design)

The reducer accumulator emits a cell only when ≥1 **valid** pixel lands in it. So a reducer never sees an
empty buffer — no divide-by-zero, no NULL-for-empty case. `sum`/`variance`/`stddev` of a cell with ≥1
valid pixel are always well-defined. (Single-pixel cell: `sum`=that value, `variance`=0, `stddev`=0 —
all clean under population semantics; see §3.2.)

### 2.3 Parity reality: existing numeric reducers are `within_tol`, not `exact`

Per the cluster benchmark, `avg`/`min`/`max`/`count` land at **`within_tol`** cross-tier (FP summation
order differs Scala-vs-numpy); `median` happens to be `exact`. So the parity bar for the new numeric
reducers is **`within_tol`** (relative tol as used by the existing reducers), not `exact`. This is the
honest, achievable target — do not attempt to force bit-exact.

## 3. Design

### 3.1 `sum`

- **Light** (`_grouped_measures`): `agg == "sum"` → `out = np.bincount(inv, weights=vals, minlength=uniq.size)`.
  Returns Python floats (like avg/min/max). Add `"sum"` to `_AGGS`.
- **Heavy**: `RST_{grid}_RasterToGridSum` per grid, `fAgg = (values) => values.sum` (Double).
- **Parity**: `within_tol` (same summation-order class as `avg`).
- **Schema**: `measure DoubleType` (Long/String cellID per grid, as siblings).

### 3.2 `variance` + `stddev` (the parity-critical pair)

Three decisions pinned IDENTICALLY on both tiers — this is the entire risk of the pair:

1. **Population, not sample** (`÷ n`, not `÷ (n−1)`). Rationale: raster zonal stats have the whole cell's
   pixels (a population, not a sample); numpy `np.var`/`np.std` default to population (`ddof=0`); a
   1-pixel cell gives variance 0 (clean) rather than undefined (`÷0`). Both tiers use `ddof=0` semantics.
2. **Two-pass algorithm** (numerically stable, matchable). NOT one-pass `E[x²]−E[x]²` (unstable; will
   diverge beyond `within_tol` on real data). Both tiers:
   - pass 1: per-cell mean `m = sum(vals)/n`.
   - pass 2: per-cell `variance = sum((vals − m)²) / n`.
   - Light (`_grouped_measures`): `means = bincount(inv, weights=vals)/counts`;
     `sq = (vals − means[inv])**2`; `variance = bincount(inv, weights=sq)/counts`. Vectorized, loops over
     neither pixels nor cells beyond the bincounts.
   - Heavy `fAgg`: `val m = values.sum/values.length; values.map(v => { val d = v-m; d*d }).sum / values.length`.
3. **`stddev = sqrt(variance)`** on both tiers (implement variance once, stddev as its sqrt) — guarantees
   internal consistency and that stddev tracks variance's parity.

- **Parity**: `within_tol` (two-pass + sqrt; FP). Cross-tier test asserts light var/std within relative
  tol of heavy on a real multi-value cell (NOT an empty/single-pixel-only fixture — must exercise real
  spread; recall LE3b's empty-grid trap — use a GB-overlapping tile for BNG so cells have real pixels).
- **Schema**: `measure DoubleType`.

### 3.3 What is NOT changed

- The reducer accumulator / build-from-valid-pixels contract (§2.2) — untouched; the new reducers are new
  `fAgg` bindings only.
- BNG 27700 warp, is_valid drop, String cellID rendering — inherited from the existing BNG reducer path.
- No new dependencies (numpy already present).

## 4. Tiers & parity

Both tiers together (light is not gated — the engine is grid-generic and `pygx` cell math exists). Each of
the 9 gets: heavy Scala expression + light `_grouped_measures` branch, registration, Python binding,
function-info example, bench FnSpec (mirroring the existing rastertogrid sibling — DGGS shape, `_BOTH`
modes, String fingerprint for BNG), a cross-tier `within_tol` parity test, and docs.

**Parity bar:** exact cell-SET match (deterministic grids) + measure `within_tol` (relative), vs heavy.

## 5. Surfaces to update (per repo conventions)

- Light: `pyrx/core/gridagg.py` (`_grouped_measures` branches + `_AGGS`); `pyrx/functions.py` (9 UDTF
  registrations via the rastertogrid factory + NotImplementedError-pointer wrappers; BNG uses the String
  schema/fingerprint path already added in Phase 2).
- Heavy: 9 new `RST_{grid}_RasterToGrid{Sum,Variance,Stddev}.scala`; register in `functions.scala` (+
  column wrappers); `registered_functions.txt` +9.
- Bindings/parity: `docs/tests-function-info/registered_functions.txt`, `function-info.json` via
  `gbx:docs:function-info`, `gbx:test:bindings` (total 165 → 174; RasterX 117 → 126).
- Bench: 9 light FnSpecs in `bench/spec.py` (mirror the existing rastertogrid entries; BNG →
  `dggs_grid_str` fingerprint + GB tile) + heavy `BenchDispatch.scala`; benchmarking.mdx.
- Docs: `raster-functions.mdx` (9 entries, both-tier badges), `execution-tiers.mdx`, `performance.mdx`
  (join the existing rastertogrid family), README badges, `beta-release-notes.mdx`.

## 6. Risks

- **variance/stddev algorithm drift (the main risk):** if Scala and numpy don't use the *same* two-pass +
  population formula, they diverge beyond `within_tol`. Mitigation: §3.2 pins the exact formula; the
  cross-tier test on a real-spread cell is the gate; if it lands `divergent`, fix the formula (do not
  loosen tol).
- **Empty-grid parity trap (LE3b lesson):** BNG bench/parity fixtures must overlap Great Britain so cells
  have real pixels — an empty-grid "exact/within_tol" is vacuous. Use a GB tile.
- **Scope multiplier:** 9 functions × full surface set ≈ a third of the original 9-fn phase. Sequencing
  `sum` first, then variance+stddev, keeps each unit reviewable.

## 7. Open questions

- Naming: `stddev` vs `std` vs `stdev` — use **`stddev`** (matches common SQL/Databricks `stddev` and the
  existing `_agg` naming density). Confirm at plan time.
- Whether `variance` and `stddev` are both exposed, or just `stddev` (variance = stddev²). Keeping both
  matches user expectation (some pipelines want variance directly) and the cost delta is one fAgg +
  one branch. Recommend both.
