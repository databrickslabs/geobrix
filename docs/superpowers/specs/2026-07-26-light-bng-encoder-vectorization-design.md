# Light BNG raster→grid encoder vectorization — Design

**Date:** 2026-07-26
**Status:** Design (approved direction); pending plan.
**Branch:** `issues/49` (toward v0.4.3)
**Relates:** `light-bng-rastertogrid-perf`, `perf-parity-light-vs-heavy`, `pyrx-udf-boundary-tax`,
`pyrx-vectorization-standing-check`, `pygx-light-gridx-design`

## 1. Problem & goal

**Measured (20-worker cluster, 2026-07-26):** light BNG `rastertogrid*` runs at **~0.15–0.19× vs heavy
(≈6× slower)** — ~217–254 ms/tile light vs ~37–40 ms heavy — while quadbin ≈ parity and h3 ≈ 0.6×. BNG is
the outlier.

**Root cause (verified in code):** `gridagg._bng_cells` is a **pure-Python per-pixel scalar loop**:
`[_bng.point_to_cell_id(ei, ni, res) for ei, ni in zip(e, n)]`, and `point_to_cell_id` is itself heavy
per call (several `int()`/`math.floor`/modulo + `get_quadrant` + `encode`). H3's loop is C-backed (the
`h3` lib); quadbin's `_quadbin_cells` is **vectorized numpy**. BNG is the only grid paying Python-
interpreter cost **per pixel**, over a full raster — the dominant term. (A secondary term, the whole-raster
EPSG:27700 warp, is out of scope here — see §5.)

**Goal:** eliminate the per-pixel Python cost by adding a **vectorized array encoder** so light BNG
rastertogrid timing approaches h3/quadbin (target: at least parity-class with quadbin, not 6× slower),
**with zero change to correctness/cell-ids** (bit-exact to the current scalar codec).

## 2. Grounding facts (verified 2026-07-26)

- `pygx/_bng.py` codec is **fully vectorizable** — all integer/float arithmetic: `int()`, `//`, `%`,
  `math.floor`, and `10**k` "shift" constants. `point_to_cell_id` calls `get_quadrant` (decimal-fraction
  branches → `np.where` chains) and `encode` (the `10**(...)` shifts are per-**resolution** scalars, i.e.
  constant across all pixels in one call since resolution is fixed). No per-pixel branching that can't
  become array ops.
- **Precedent:** `gridagg._quadbin_cells` is already a numpy encoder, and `pygx/_quadbin.py` exposes both
  `resolution` (scalar) and `resolution_vec` (array) — proof that a vectorized codec form is viable and
  wanted here. Note the quadbin codec keeps its scalar and vec as *separate* bodies; this spec deliberately
  goes one better (shared numpy-polymorphic core, §3) so BNG never carries two copies of the codec math.
- **Invalid-pixel handling:** `_raster_to_bng._run` gathers valid pixels via `ys, xs = np.nonzero(mask)`
  BEFORE calling `_bng_cells`, so NaN/nodata pixels never reach the encoder — the vec encoder need not
  raise-on-NaN (the scalar does; the vec path is fed clean coords). Out-of-GB cells are dropped AFTER
  encode via a scalar `_bng.is_valid` filter over every emitted cell — a second per-cell Python loop.

## 3. Design — ONE shared core, injected into both scalar and vec wrappers (single source, no copy)

**(Revised per user 2026-07-26.)** Do NOT write two reimplementations kept-in-sync-by-test. Instead
write the codec ONCE as a **duck-typed core** using only numpy-polymorphic ops (`np.floor`, `np.where`,
`//`, `%`, comparisons) that operate identically on a Python scalar and a numpy array. Both the scalar
and vec public entry points are THIN WRAPPERS over the same core — so there is literally one copy of the
math; drift is impossible **by construction**, not merely caught by a test.

Why it works: everything in `point_to_cell_id` is numpy-polymorphic arithmetic EXCEPT the Python `if`
branches, which are of two kinds:
- **branch on `resolution`** (fixed per call — divisor, n_positions, sign, the `res == -1` encode form):
  stays a plain Python `if` at the TOP of the core (not per-element), fine for both scalar and array.
- **per-element branch** (`get_quadrant`'s `e_dec < 0.5` etc.): becomes `np.where`, which is correct on a
  scalar and an array alike.

### 3.1 `_point_to_cell_id_core(e, n, resolution)` — the single shared implementation
Numpy-polymorphic body (works whether `e`/`n` are Python floats or `np.ndarray`):
- `e_int = np.floor(e).astype(...)`-equivalent — **but see §3.4/§7 on truncation:** the scalar uses
  `int(e)` (truncate toward zero). Use an int-truncation that matches `int()` for BOTH a scalar and an
  array on the BNG-positive domain (e.g. keep integer math; `np.trunc` then int cast). Document the domain.
- `e_letter = e_int // 100000`, `n_letter = n_int // 100000`.
- per-call scalars from `resolution`: `divisor`, `n_positions`, sign branch (plain `if resolution < 0`).
- `quadrant = _get_quadrant_core(resolution, e_int, n_int, divisor)` (numpy-polymorphic; the 4-way
  `np.where` chain — correct on scalar or array).
- `e_bin = np.floor((e_int % 100000) / divisor)`, `n_bin = np.floor((n_int % 100000) / divisor)`.
- encode: **accumulate in int64** (see §7 — the packed value nears float64's 2^53 exact-int ceiling at
  high resolution; must stay integer). The `res == -1` form is a plain Python `if` (per-call), each branch
  an int64 array/scalar expression.
- return the int64 result (scalar wrappers cast to Python `int`).

### 3.2 The public wrappers (thin)
- `point_to_cell_id(e: float, n: float, resolution: int) -> int`: NaN-guard (as today) → `int(_core(e, n,
  resolution))`. Behavior identical to today.
- `point_to_cell_id_vec(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray`: `_core(e, n,
  resolution).astype(int64)` (no NaN-guard needed — fed clean coords, see §2). 
- Same one-core-two-wrappers shape for `get_quadrant` / `_get_quadrant_core`.
Note: routing the SCALAR path through numpy makes a single scalar call marginally slower than pure-Python
int arithmetic — acceptable because the only hot per-pixel callers want the vec form; remaining scalar
callers (`bng_point_as_cell`, single lookups) are cold. If any cold scalar caller proves hot, revisit.

### 3.3 `is_valid_vec(cell_ids: np.ndarray) -> np.ndarray[bool]`
Same one-core treatment for the out-of-GB drop: write `is_valid`'s bounds/index checks
(0<=x<=700000, 0<=y<=1300000, letter indices in range — BNG.scala:305 equivalent) as a numpy-polymorphic
core, with `is_valid` (scalar bool) and `is_valid_vec` (bool array) as wrappers. gridagg filters emitted
cells in one array op instead of a Python loop.

### 3.4 `gridagg._bng_cells` rewired
`_bng_cells(e, n, resolution)` calls `_bng.point_to_cell_id_vec(e, n, resolution)` (no Python loop). The
caller's out-of-GB filter uses `_bng.is_valid_vec`. Everything downstream (`_grouped_measures`,
`_bng.format` at the string boundary) unchanged. `format` stays scalar (per-emitted-cell, few cells) —
optional to vectorize later, not the hot path.

## 4. Parity — the safety gate (shape changed by the shared-core design)

Shared-core makes **scalar-vs-vec drift impossible by construction** — there is one code path, so the
scalar and array forms cannot diverge. But it introduces a *different* risk the old design didn't have:
the numpy-polymorphic core is a **rewrite of the current scalar body**, so the core must reproduce the
CURRENT scalar's output exactly. The gate is therefore two tests, not a vec-vs-scalar sweep:

- **Regression: new scalar `point_to_cell_id` vs its CURRENT behavior.** Before touching the code, capture
  a baseline: run today's `point_to_cell_id` over a dense grid of EPSG:27700 (e, n) covering GB at every
  resolution (±1..±6), including quadrant-boundary coords (the res<-1 decimal-fraction branches), cell-edge
  coords, and out-of-GB coords (encode runs before `is_valid`, §7). Freeze those ids as the fixture. After
  the rewrite, assert the new `point_to_cell_id` reproduces every frozen id **exactly** (int equality). If
  a stored baseline is impractical, assert new-scalar == an inlined copy of the old scalar body over the
  same sweep in one test. Same treatment for `get_quadrant` and `is_valid`.
- **Scalar-is-vec smoke:** assert `point_to_cell_id_vec(e_arr, n_arr, r)` equals
  `[point_to_cell_id(ei, ni, r) ...]` element-for-element — cheap, and documents that the wrappers share
  the core (guards against a future wrapper-level bug, e.g. a stray `.astype` or NaN-guard divergence).
- **int64 packing (§7):** include the highest-resolution cases (res ±6, largest `n_positions`/`10**`
  shift) in the sweep so the test fails loudly if the packed id ever overflows or loses precision to
  float64. The core must accumulate in int64 — a float64 intermediate at res 6 (~10^15–10^16) silently
  rounds past 2^53.
- **End-to-end:** the existing light-vs-heavy cross-tier BNG parity test (on the London GB tile) must
  still pass — cell sets + measures unchanged after the rewire.
- **Bench re-measure (cluster, later):** confirm the speedup landed (target: BNG rastertogrid no longer
  ~6× slower — ideally parity-class with quadbin). Follow-on cluster run, not part of the code gate.

If any exact-equality test fails, the core's math diverges from the frozen scalar behavior — fix the core;
NEVER loosen to tolerance (these are integer cell ids, must be identical).

## 5. Out of scope (this cycle)
- **The 27700 warp** (secondary cost). Possible later: warp only the valid-pixel extent, or fuse. Its own
  perf item if the encoder fix doesn't close the gap enough.
- **Heavy tier** — unchanged (it's already fast; this is a light-only speed fix).
- **quadbin/h3 encoders** — unchanged (already C-backed / vectorized).
- Vectorizing `format` (string render) — few emitted cells, not the hot path.

## 6. Surfaces
- `pygx/_bng.py`: refactor `point_to_cell_id`/`get_quadrant`/`is_valid` into numpy-polymorphic shared
  cores (`_point_to_cell_id_core`, `_get_quadrant_core`, `_is_valid_core`); keep the scalar public names as
  thin wrappers; add `point_to_cell_id_vec`, `is_valid_vec` (thin array wrappers over the same cores).
- `pyrx/core/gridagg.py`: `_bng_cells` uses `point_to_cell_id_vec`; caller's out-of-GB filter uses
  `is_valid_vec`.
- Tests: `test/pygx` regression sweep (new scalar reproduces frozen current behavior) + scalar-is-vec smoke
  + int64-packing edge cases; existing gridagg BNG tests + cross-tier parity test must stay green.
- No new deps (numpy present). Light-CI: `test/pygx` already a light dir.
- No doc/badge change (no new functions; behavior identical, only faster). Benchmarking.mdx BNG numbers
  get refreshed on the next cluster bench (a follow-on, note it).

## 7. Risks
- **Core-vs-current-scalar drift** (the main risk, reshaped): shared-core kills scalar-vs-vec drift, but
  the core is a rewrite of the current scalar body, so the risk moves to "does the numpy-polymorphic core
  reproduce today's scalar exactly?" Watch: truncation semantics (`int()` truncates toward zero vs `//`
  floors — equal on the non-negative BNG domain but NOT for negatives; document/guard the domain),
  `np.floor` vs `math.floor`, float `10**k` vs int shifts. The §4 regression sweep against frozen current
  behavior is the gate.
- **int64 packing (must-fix, not just test):** the shared core MUST accumulate the packed id in int64, not
  float64. At res ±6 the packed value reaches ~10^15–10^16, past float64's exact-integer ceiling
  (2^53 ≈ 9×10^15) but well within int64 (9.2×10^18). A float64 intermediate silently rounds and produces
  wrong ids only at high resolution — the kind of bug that passes at res 3 and fails at res 6. Force int64
  dtype at the encode step; the §4 sweep includes res ±6 to catch a regression.
- **Scalar path routes through numpy:** a single scalar `point_to_cell_id` call is marginally slower than
  the old pure-Python int arithmetic (numpy scalar overhead). Acceptable — the only hot per-pixel caller
  wants the vec form; cold scalar callers (`bng_point_as_cell`, single lookups) don't matter. Noted so a
  future reviewer doesn't "fix" it by re-forking the scalar body (which would reintroduce drift).
- **Out-of-GB inputs:** encode runs BEFORE `is_valid` (cells are encoded, then filtered), so the encoder
  sees out-of-GB coords (boundary-straddle). Because scalar and vec share one core, they necessarily
  produce the same (possibly-bogus) id for those, and `is_valid_vec` drops the same set. Test with
  out-of-GB coords in the sweep anyway.
