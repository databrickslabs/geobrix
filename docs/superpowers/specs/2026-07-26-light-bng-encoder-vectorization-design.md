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
- **Precedent:** `gridagg._quadbin_cells` is already a numpy encoder **bit-exact** to the `quadbin` lib,
  and `pygx/_quadbin.py` exposes both `resolution` (scalar) and `resolution_vec` (array). This is the
  exact pattern to mirror.
- **Invalid-pixel handling:** `_raster_to_bng._run` gathers valid pixels via `ys, xs = np.nonzero(mask)`
  BEFORE calling `_bng_cells`, so NaN/nodata pixels never reach the encoder — the vec encoder need not
  raise-on-NaN (the scalar does; the vec path is fed clean coords). Out-of-GB cells are dropped AFTER
  encode via a scalar `_bng.is_valid` filter over every emitted cell — a second per-cell Python loop.

## 3. Design — two vectorized entry points IN `pygx/_bng.py` (single module, two forms)

Chosen over a numpy copy in `gridagg`: keep the codec in ONE module with a scalar + a vec entry point
(mirrors `_quadbin`'s `resolution`/`resolution_vec`), so ownership stays with the codec and the scalar
remains the parity oracle for the vec form.

### 3.1 `point_to_cell_id_vec(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray`
Array reimplementation of `point_to_cell_id`, returning `int64` cell ids. Vectorize each step:
- `e_int = e.astype(int64)`, `n_int = n.astype(int64)` (truncate-toward-zero matches the scalar `int()`
  for the BNG-positive domain — assert/document the domain).
- `e_letter = e_int // 100000`, `n_letter = n_int // 100000` (integer floor-div; the scalar uses
  `int(e_int/100000)` = truncation, equal for the non-negative BNG domain — verify at the domain edge).
- `divisor` is a per-call scalar (resolution fixed): `10**(6-abs(res)+1)` if res<0 else `10**(6-res)`.
- `quadrant` = a vectorized `get_quadrant_vec` (see 3.2).
- `n_positions` = per-call scalar.
- `e_bin = np.floor((e_int % 100000) / divisor)`, `n_bin = np.floor((n_int % 100000) / divisor)`.
- `encode` inlined array-wise: the `10**(...)` shifts are per-call scalars; compute `val` as an array
  (with the `resolution == -1` branch handled by computing both and `np.where`, or short-circuiting since
  resolution is fixed per call — a single `if resolution == -1:` at Python level is fine, NOT per-pixel).
  Return `val.astype(int64)`.
NOTE: because `resolution` is a scalar arg (fixed per call), all the `10**`/divisor/n_positions/branch-on-
sign logic stays at Python scalar level ONCE per call — only the per-pixel `e_int/n_int/e_bin/n_bin/
quadrant/val` are arrays. That's the whole speedup.

### 3.2 `get_quadrant_vec(resolution, e: np.ndarray, n: np.ndarray, divisor) -> np.ndarray`
For `resolution >= -1`: return `np.zeros(len, int)`. For `resolution < -1`: compute `e_dec`, `n_dec`
(fractional parts via `e/divisor - np.floor(...)`) and map the 4-way branch with nested `np.where`:
SW(1) if e_dec<0.5 & n_dec<0.5; NW(2) if e_dec<0.5; SE(4) if n_dec<0.5; else NE(3). Match the scalar's
exact comparison order.

### 3.3 `is_valid_vec(cell_ids: np.ndarray) -> np.ndarray[bool]`
Vectorize the out-of-GB drop (currently a scalar `is_valid` per emitted cell in the caller). Array form of
`is_valid`'s bounds/index checks (read the scalar `is_valid`, BNG.scala:305 equivalent — 0<=x<=700000,
0<=y<=1300000, letter indices in range). Used by gridagg to filter emitted cells in one array op instead
of a Python loop.

### 3.4 `gridagg._bng_cells` rewired
`_bng_cells(e, n, resolution)` calls `_bng.point_to_cell_id_vec(e, n, resolution)` (no Python loop). The
caller's out-of-GB filter uses `_bng.is_valid_vec`. Everything downstream (`_grouped_measures`,
`_bng.format` at the string boundary) unchanged. `format` stays scalar (per-emitted-cell, few cells) —
optional to vectorize later, not the hot path.

## 4. Parity — the safety gate (this is the whole risk)

The vec forms MUST be **bit-exact** to the scalar codec. Gate with tests (mirror the `_quadbin_cells`
bit-exactness test):
- **`point_to_cell_id_vec` vs `point_to_cell_id`:** over a dense grid of EPSG:27700 (e, n) covering GB at
  several resolutions (±1..±6), assert `point_to_cell_id_vec(e_arr, n_arr, r)` equals the scalar
  `[point_to_cell_id(ei, ni, r) ...]` **element-for-element** (exact int equality, not tolerance).
  Include quadrant-boundary coords (the res<-1 decimal-fraction branches) and cell-edge coords.
- **`get_quadrant_vec` vs `get_quadrant`** and **`is_valid_vec` vs `is_valid`** — same exact-equality
  sweep.
- **End-to-end:** the existing light-vs-heavy cross-tier BNG parity test (on the London GB tile) must
  still pass — cell sets + measures unchanged after the rewire (the vec path must not shift any cell id).
- **Bench re-measure (cluster, later):** confirm the speedup landed (target: BNG rastertogrid no longer
  ~6× slower — ideally parity-class with quadbin). This is a follow-on cluster run, not part of the code
  change's gate.

If any exact-equality test fails, the vec codec has a bug — fix the vec math to match the scalar; NEVER
loosen to tolerance (these are integer cell ids, must be identical).

## 5. Out of scope (this cycle)
- **The 27700 warp** (secondary cost). Possible later: warp only the valid-pixel extent, or fuse. Its own
  perf item if the encoder fix doesn't close the gap enough.
- **Heavy tier** — unchanged (it's already fast; this is a light-only speed fix).
- **quadbin/h3 encoders** — unchanged (already C-backed / vectorized).
- Vectorizing `format` (string render) — few emitted cells, not the hot path.

## 6. Surfaces
- `pygx/_bng.py`: `point_to_cell_id_vec`, `get_quadrant_vec`, `is_valid_vec` (+ keep scalars as oracle).
- `pyrx/core/gridagg.py`: `_bng_cells` uses the vec encoder; caller's out-of-GB filter uses `is_valid_vec`.
- Tests: `test/pygx` bit-exactness sweeps (vec vs scalar) for the 3 new fns; existing gridagg BNG tests +
  cross-tier parity test must stay green (no cell-id change).
- No new deps (numpy present). Light-CI: `test/pygx` already a light dir.
- No doc/badge change (no new functions; behavior identical, only faster). Benchmarking.mdx BNG numbers
  get refreshed on the next cluster bench (a follow-on, note it).

## 7. Risks
- **Vec-vs-scalar drift** (the main risk): truncation semantics (`int()` vs `//`), float `10**k` vs int,
  `np.floor` vs `math.floor` on negatives, dtype overflow of the packed id (use int64; the scalar returns
  Python int — confirm the packed value fits int64 for all BNG resolutions, esp. the largest `n_positions`
  → largest `10**` shift; if it can exceed int64, use object/uint64 carefully + test). The exact-equality
  sweep is the gate; add explicit large-resolution + edge coords.
- **Negative-coordinate / out-of-GB inputs:** the vec path is fed post-`is_valid`? No — encode happens
  BEFORE is_valid (cells are encoded, then filtered). So the vec encoder may see out-of-GB coords (e.g.
  the boundary-straddle case). It must produce the SAME (possibly-clamped/bogus) id the scalar would for
  those, so the subsequent is_valid_vec drops the same set. Test with out-of-GB coords included.
