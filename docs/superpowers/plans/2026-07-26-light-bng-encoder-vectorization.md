# Light BNG raster→grid encoder vectorization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the two per-pixel Python loops in the light BNG raster→grid path (encoder + `is_valid` filter) by adding numpy array kernels in `pygx/_bng.py`, maintained as ONE shared numpy-polymorphic core per operation that both the scalar and vector entry points wrap — so scalar-vs-vector drift is impossible by construction.

**Architecture:** Refactor `point_to_cell_id` / `get_quadrant` into numpy-polymorphic cores (`_point_to_cell_id_core`, `_get_quadrant_core`, `_encode_core`) that run identically on a Python scalar and a numpy array (via `np.trunc`/`np.floor`/`np.where`, all arithmetic in int64). Keep the existing scalar public names as thin wrappers; add `point_to_cell_id_vec`. Add a resolution-aware `is_valid_vec(cell_ids, resolution)` (single-resolution batch, so no per-cell resolution derivation). Rewire `gridagg._raster_to_bng`'s two per-pixel loops to the vector kernels.

**Tech Stack:** Python 3.12, numpy (already a dependency), pytest. Light tier only — no Scala/JAR change, no new deps, no new functions (behavior identical, only faster).

## Global Constraints

- **Bit-exact cell ids.** The refactored scalar `point_to_cell_id` / `get_quadrant` / `is_valid` must reproduce their CURRENT output exactly (integer equality, never tolerance). `point_to_cell_id_vec` / `is_valid_vec` must equal the scalar element-for-element. These are integer cell ids — any divergence is a bug.
- **int64 accumulation.** The encode step accumulates in int64, never float64. (The packed id at res ±6 is a 16-digit number < 2×10¹⁵, within float64's 2⁵³ exact-integer ceiling AND int64's 9.2×10¹⁸ — but int64 removes all doubt and matches the current pure-integer `encode`.)
- **Truncation semantics.** The scalar uses `int(e_int/100000)` (truncate toward zero) for letter indices and `math.floor(.../divisor)` (floor) for bins. The core must mirror EACH exactly (`np.trunc` vs `np.floor`) — they differ for negative (out-of-GB) coords, which the encoder DOES see (encode runs before `is_valid`).
- **Single source of truth.** No second copy of the BNG codec math. Scalar and vector forms share one core. A reviewer must not "optimize" the scalar path back into a separate body (that reintroduces drift) — the scalar routing through numpy is marginally slower and that is accepted (only cold callers use it).
- **No public-surface change beyond the two new `*_vec` functions.** `format`/`parse`/geometry/neighborhood/polyfill/tessellate are untouched. No doc/badge/registered-function change.
- **`test/pygx` and `test/pyrx` are light dirs.** Tests run under `gbx:test:python`; the affected packages must be run before push.

---

### Task 1: Shared numpy-polymorphic core + `point_to_cell_id_vec`

Refactor the scalar encoder into a shared core and add the vector entry point. This is the dominant per-pixel cost.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/_bng.py` (`get_quadrant` ~L165-180, `encode` ~L183-200, `point_to_cell_id` ~L203-220; add cores + `point_to_cell_id_vec`)
- Test: `python/geobrix/test/pygx/test_bng_encoder_vec.py` (new)

**Interfaces:**
- Consumes: nothing new (uses `numpy`, already imported? — add `import numpy as np` if absent).
- Produces:
  - `_get_quadrant_core(resolution: int, eastings, northings, divisor) -> np.int64|np.ndarray` (numpy-polymorphic)
  - `_encode_core(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution) -> np.int64|np.ndarray` (int64)
  - `_point_to_cell_id_core(e, n, resolution: int) -> np.int64|np.ndarray` (int64)
  - `point_to_cell_id(eastings: float, northings: float, resolution: int) -> int` (thin wrapper, unchanged signature/behavior)
  - `get_quadrant(resolution: int, eastings: float, northings: float, divisor: float) -> int` (thin wrapper, unchanged)
  - `point_to_cell_id_vec(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray[int64]`

- [ ] **Step 1: Write the failing regression + smoke test**

Create `python/geobrix/test/pygx/test_bng_encoder_vec.py`:

```python
"""Bit-exactness tests for the vectorized BNG encoder.

The shared numpy core is a rewrite of the current scalar codec body, so the
gate is TWO checks: (1) the refactored scalar reproduces the CURRENT behavior
over a frozen baseline (no regression), and (2) the vec form equals the scalar
element-for-element (shared core => cannot drift, but this documents it and
guards the thin wrappers). Includes res +-6 to catch any int64 overflow /
float64 precision loss, and out-of-GB / negative coords (the encoder sees them
because encode runs BEFORE is_valid).
"""

import numpy as np
import pytest

from databricks.labs.gbx.pygx import _bng

# Dense EPSG:27700 grid across GB + explicit out-of-GB / negative / boundary coords.
_EAST = np.concatenate(
    [
        np.linspace(0.0, 700000.0, 43),
        np.array([-150000.0, -1.0, 0.0, 99999.5, 100000.0, 529999.9, 530000.0, 700001.0]),
    ]
)
_NORTH = np.concatenate(
    [
        np.linspace(0.0, 1300000.0, 41),
        np.array([-250000.0, -1.0, 0.0, 179999.9, 180000.0, 1300001.0]),
    ]
)
_RESOLUTIONS = [-1, 1, -2, 2, -3, 3, -4, 4, -5, 5, -6, 6]


def _grid():
    EE, NN = np.meshgrid(_EAST, _NORTH)
    return EE.ravel(), NN.ravel()


@pytest.mark.parametrize("res", _RESOLUTIONS)
def test_point_to_cell_id_vec_equals_scalar(res):
    e, n = _grid()
    vec = _bng.point_to_cell_id_vec(e, n, res)
    assert vec.dtype == np.int64
    for ei, ni, c in zip(e, n, vec):
        expected = _bng.point_to_cell_id(float(ei), float(ni), res)
        assert int(c) == expected, f"e={ei} n={ni} res={res}: {int(c)} != {expected}"


@pytest.mark.parametrize("res", _RESOLUTIONS)
def test_scalar_matches_frozen_reference(res):
    # Reference = an inlined copy of the ORIGINAL scalar body, proving the
    # refactor introduced no regression. (Kept local so it can't drift with
    # the module under test.)
    import math

    def ref_get_quadrant(resolution, eastings, northings, divisor):
        if resolution < -1:
            e_q = eastings / divisor
            n_q = northings / divisor
            e_dec = e_q - math.floor(e_q)
            n_dec = n_q - math.floor(n_q)
            if e_dec < 0.5 and n_dec < 0.5:
                return 1
            if e_dec < 0.5:
                return 2
            if n_dec < 0.5:
                return 4
            return 3
        return 0

    def ref_encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution):
        id_placeholder = 10 ** (5 + 2 * n_positions - 2)
        e_letter_shift = 10 ** (3 + 2 * n_positions - 2)
        n_letter_shift = 10 ** (1 + 2 * n_positions - 2)
        e_shift = 10 ** n_positions
        n_shift = 10
        if resolution == -1:
            val = (id_placeholder + e_letter * e_letter_shift) / 100 + quadrant
        else:
            val = (
                id_placeholder
                + e_letter * e_letter_shift
                + n_letter * n_letter_shift
                + e_bin * e_shift
                + n_bin * n_shift
                + quadrant
            )
        return int(val)

    def ref_point_to_cell_id(eastings, northings, resolution):
        e_int = int(eastings)
        n_int = int(northings)
        e_letter = int(e_int / 100000)
        n_letter = int(n_int / 100000)
        if resolution < 0:
            divisor = 10 ** (6 - abs(resolution) + 1)
        else:
            divisor = 10 ** (6 - resolution)
        quadrant = ref_get_quadrant(resolution, e_int, n_int, divisor)
        n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
        e_bin = math.floor((e_int % 100000) / divisor)
        n_bin = math.floor((n_int % 100000) / divisor)
        return ref_encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution)

    e, n = _grid()
    for ei, ni in zip(e, n):
        assert _bng.point_to_cell_id(float(ei), float(ni), res) == ref_point_to_cell_id(
            float(ei), float(ni), res
        ), f"e={ei} n={ni} res={res}"


def test_vec_dtype_is_int64_at_high_resolution():
    # res 6 packed id ~1.x*10^15 -- must stay exact in int64, not round in float64.
    e = np.array([529999.0, 530000.0, 123456.0])
    n = np.array([179999.0, 180000.0, 654321.0])
    vec = _bng.point_to_cell_id_vec(e, n, 6)
    assert vec.dtype == np.int64
    for ei, ni, c in zip(e, n, vec):
        assert int(c) == _bng.point_to_cell_id(float(ei), float(ni), 6)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `gbx:test:python --path python/geobrix/test/pygx/test_bng_encoder_vec.py` (or `python -m pytest python/geobrix/test/pygx/test_bng_encoder_vec.py -v`)
Expected: FAIL — `AttributeError: module ... has no attribute 'point_to_cell_id_vec'`.

- [ ] **Step 3: Add the shared cores and refactor the scalars**

Ensure `import numpy as np` is present near the top of `_bng.py` (alongside `import math`).

Replace `get_quadrant`, `encode`, and `point_to_cell_id` with cores + thin wrappers:

```python
def _get_quadrant_core(resolution, eastings, northings, divisor):
    """Numpy-polymorphic quadrant (0 for res >= -1; 1/2/4/3 for res < -1).

    ``eastings``/``northings`` are int64 (scalar or array). Mirrors the scalar
    ``get_quadrant`` if-chain order via nested ``np.where``.
    """
    if resolution >= -1:
        return eastings * 0  # int64 zeros, scalar or array
    e_q = eastings / divisor
    n_q = northings / divisor
    e_dec = e_q - np.floor(e_q)
    n_dec = n_q - np.floor(n_q)
    q = np.where(
        (e_dec < 0.5) & (n_dec < 0.5),
        1,
        np.where(e_dec < 0.5, 2, np.where(n_dec < 0.5, 4, 3)),
    )
    return q.astype(np.int64)


def get_quadrant(
    resolution: int, eastings: float, northings: float, divisor: float
) -> int:
    """Scalar wrapper over :func:`_get_quadrant_core` (unchanged public behavior)."""
    return int(_get_quadrant_core(resolution, eastings, northings, divisor))


def _encode_core(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution):
    """Pack the BNG digit-id in int64 (scalar or array). Bit-exact with the
    original pure-integer ``encode`` (the res==-1 ``/100`` is always exactly
    divisible, so integer ``//100`` matches ``int(float/100)``)."""
    id_placeholder = 10 ** (5 + 2 * n_positions - 2)
    e_letter_shift = 10 ** (3 + 2 * n_positions - 2)
    n_letter_shift = 10 ** (1 + 2 * n_positions - 2)
    e_shift = 10 ** n_positions
    n_shift = 10
    if resolution == -1:
        val = (id_placeholder + e_letter * e_letter_shift) // 100 + quadrant
    else:
        val = (
            id_placeholder
            + e_letter * e_letter_shift
            + n_letter * n_letter_shift
            + e_bin * e_shift
            + n_bin * n_shift
            + quadrant
        )
    return val


def encode(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution) -> int:
    """Scalar wrapper over :func:`_encode_core` (unchanged public behavior)."""
    return int(_encode_core(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution))


def _point_to_cell_id_core(e, n, resolution: int):
    """Numpy-polymorphic BNG encoder core (int64 result; scalar or array).

    Mirrors the scalar ``point_to_cell_id`` EXACTLY, incl. truncation semantics:
    letter indices use ``int(e_int/100000)`` (truncate toward zero -> ``np.trunc``);
    bins use ``math.floor(.../divisor)`` (floor -> ``np.floor``). These differ for
    negative (out-of-GB) coords, which the encoder sees (encode runs before is_valid).
    """
    e_int = np.trunc(e).astype(np.int64)
    n_int = np.trunc(n).astype(np.int64)
    # int(e_int/100000): float division then truncate toward zero (NOT floor-div).
    e_letter = np.trunc(e_int / 100000).astype(np.int64)
    n_letter = np.trunc(n_int / 100000).astype(np.int64)
    if resolution < 0:
        divisor = 10 ** (6 - abs(resolution) + 1)
    else:
        divisor = 10 ** (6 - resolution)
    quadrant = _get_quadrant_core(resolution, e_int, n_int, divisor)
    n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
    # math.floor((e_int % 100000) / divisor): float division then floor.
    e_bin = np.floor((e_int % 100000) / divisor).astype(np.int64)
    n_bin = np.floor((n_int % 100000) / divisor).astype(np.int64)
    return _encode_core(e_letter, n_letter, e_bin, n_bin, quadrant, n_positions, resolution)


def point_to_cell_id(eastings: float, northings: float, resolution: int) -> int:
    """Scalar wrapper over :func:`_point_to_cell_id_core` (unchanged behavior)."""
    if math.isnan(eastings) or math.isnan(northings):
        raise ValueError("NaN coordinates are not supported.")
    return int(_point_to_cell_id_core(float(eastings), float(northings), resolution))


def point_to_cell_id_vec(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized BNG encoder: array of int64 cell ids from EPSG:27700 (e, n).

    Shares :func:`_point_to_cell_id_core` with the scalar ``point_to_cell_id``, so
    the two forms cannot drift. Fed clean (post-mask) coords, so no NaN guard.
    """
    e = np.asarray(e, dtype="float64")
    n = np.asarray(n, dtype="float64")
    return _point_to_cell_id_core(e, n, resolution).astype(np.int64)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest python/geobrix/test/pygx/test_bng_encoder_vec.py -v`
Expected: PASS (all parametrizations).

- [ ] **Step 5: Run the existing BNG codec/parity suites to confirm no regression in dependent callers**

Run: `python -m pytest python/geobrix/test/pygx/test_bng_codec.py python/geobrix/test/pygx/test_parity_bng.py python/geobrix/test/pygx/test_bng_polyfill.py python/geobrix/test/pygx/test_bng_neighborhood.py -v`
Expected: PASS (polyfill/k-ring/point_as_cell all route through the rewritten scalar wrapper).

- [ ] **Step 6: Lint**

Run: `gbx:lint:python --check` (or `black --check` + `flake8` on `_bng.py` and the new test)
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/_bng.py python/geobrix/test/pygx/test_bng_encoder_vec.py
git commit -F <tmpfile>   # see commit-message-hygiene: subject <=72, WHY body, Co-authored-by: Isaac
```
Subject: `perf(pygx): vectorize BNG encoder via shared numpy core`

---

### Task 2: `is_valid_vec` (resolution-aware) — vectorize the second per-pixel loop

The gridagg filter `[is_valid(int(c)) for c in cids]` is the OTHER per-pixel loop. Since the gridagg batch is single-resolution, a resolution-aware vector form avoids per-cell resolution derivation.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/_bng.py` (add `is_valid_vec` near `is_valid` ~L606-624)
- Test: `python/geobrix/test/pygx/test_bng_encoder_vec.py` (extend)

**Interfaces:**
- Consumes: `point_to_cell_id_vec` (Task 1) to generate test ids.
- Produces: `is_valid_vec(cell_ids: np.ndarray, resolution: int) -> np.ndarray[bool]` — element-for-element equal to `[is_valid(int(c)) for c in cell_ids]` for ids produced at that resolution.

- [ ] **Step 1: Write the failing test (append to `test_bng_encoder_vec.py`)**

```python
@pytest.mark.parametrize("res", _RESOLUTIONS)
def test_is_valid_vec_equals_scalar(res):
    e, n = _grid()  # includes out-of-GB coords -> some ids are invalid
    ids = _bng.point_to_cell_id_vec(e, n, res)
    vec = _bng.is_valid_vec(ids, res)
    assert vec.dtype == bool
    for c, v in zip(ids, vec):
        assert bool(v) == _bng.is_valid(int(c)), f"id={int(c)} res={res}"


def test_is_valid_vec_empty():
    assert _bng.is_valid_vec(np.array([], dtype=np.int64), 3).tolist() == []
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest python/geobrix/test/pygx/test_bng_encoder_vec.py::test_is_valid_vec_equals_scalar -v`
Expected: FAIL — no `is_valid_vec` attribute.

- [ ] **Step 3: Implement `is_valid_vec`**

Faithful vectorization of `is_valid` for a fixed known resolution. The digit
layout is fixed by `resolution`, so extract the letter/coordinate digits with
integer arithmetic (`// 10**k % 10`) instead of string slicing, then apply the
same `get_x`/`get_y` index math as arrays. Iterate the digit-position loop only
`ndigits` (<=16) times — constant, NOT per-pixel.

```python
def is_valid_vec(cell_ids: np.ndarray, resolution: int) -> np.ndarray:
    """Vectorized :func:`is_valid` for a single-resolution batch of Long ids.

    Equivalent to ``[is_valid(int(c)) for c in cell_ids]`` when every id was
    produced at ``resolution``. Extracts digit slices by integer arithmetic
    (fixed digit layout per resolution), reproducing ``get_x``/``get_y`` and the
    bounds/index checks on arrays.
    """
    cell_ids = np.asarray(cell_ids, dtype=np.int64)
    if cell_ids.size == 0:
        return np.zeros(0, dtype=bool)

    # res -1 (500km) is a DEGENERATE 4-digit id (encode divides by 100) and its
    # get_x/get_y math has k=-1 -- the generic array formula below does NOT apply.
    # 500km cells never arise at raster-tile scale, so keep parity via the scalar
    # path for this one resolution (still correct; not the hot case).
    if resolution == -1:
        return np.array(
            [is_valid(int(c)) for c in cell_ids], dtype=bool
        )  # vectorscan: ok (res -1 degenerate, non-hot)

    # For all other resolutions the digit count is fixed by the encode layout:
    #   ndigits = 4 + 2*n_positions  (leading placeholder '1' + letter + coord fields).
    n_positions = abs(resolution) if resolution >= -1 else abs(resolution) - 1
    ndigits = 4 + 2 * n_positions

    # digit(pos) with pos=0 the MOST-significant digit (matches cell_digits order).
    def digit(pos):
        p = ndigits - 1 - pos  # power of 10 for that position
        return (cell_ids // (10 ** p)) % 10

    # Two-digit letter fields: cell_digits[1:3] -> y_letter, [3:5] -> x_letter.
    y_letter = digit(1) * 10 + digit(2)
    x_letter = digit(3) * 10 + digit(4)

    edge = get_edge_size(resolution)
    k = (ndigits - 6) // 2
    quadrant = digit(ndigits - 1)

    # get_x: x_digits = digits[1:3] + digits[5:5+k]; value * edge_adj + x_offset.
    def concat_coord(letter_pair, start):
        val = letter_pair  # digits[1:3] or [3:5] already combined by caller
        for i in range(k):
            val = val * 10 + digit(start + i)
        return val

    x_val = concat_coord(x_letter, 5)  # digits[3:5] ++ digits[5:5+k]
    y_val = concat_coord(y_letter, 5 + k)  # digits[1:3] ++ digits[5+k:5+2k]
    edge_adj = np.where(quadrant > 0, 2 * edge, edge)
    x_offset = np.where((quadrant == 3) | (quadrant == 4), edge, 0)
    y_offset = np.where((quadrant == 2) | (quadrant == 3), edge, 0)
    x = x_val * edge_adj + x_offset
    y = y_val * edge_adj + y_offset

    return (
        (x >= 0)
        & (x <= 700000)
        & (y >= 0)
        & (y <= 1300000)
        & (x_letter < len(LETTER_MAP))
        & (y_letter < len(LETTER_MAP[0]))
    )
```

NOTE for the implementer: the digit-slice mapping in `get_x`/`get_y` is the
arbiter — `test_is_valid_vec_equals_scalar` (across every resolution, incl.
res −1 with `ndigits < 6`) is the gate. If a slice index is off, that test
fails loudly. Adjust `concat_coord`/`digit` indexing until green; do NOT loosen
the test. Watch the res −1 case (500km, `ndigits = 4`, `k = -1`) — if the
generic formula misbehaves there, special-case it to match the scalar exactly.

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest python/geobrix/test/pygx/test_bng_encoder_vec.py -v`
Expected: PASS (all, including the new `is_valid_vec` cases across every resolution).

- [ ] **Step 5: Lint**

Run: `gbx:lint:python --check`
Expected: clean.

- [ ] **Step 6: Commit**

Subject: `perf(pygx): add resolution-aware is_valid_vec for BNG`

---

### Task 3: Rewire `gridagg._raster_to_bng` to the vector kernels + full parity

Replace both per-pixel loops with the vector kernels and prove end-to-end parity is unchanged.

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/gridagg.py` (`_bng_cells` ~L74-85; the `is_valid` filter in `_raster_to_bng._run` ~L292-301)
- Test: existing `python/geobrix/test/pyrx/test_gridagg_bng.py`, `python/geobrix/test/pyrx/test_parity_bng_quadbin_raster_grid.py` (run, do not modify unless a real gap surfaces)

**Interfaces:**
- Consumes: `_bng.point_to_cell_id_vec`, `_bng.is_valid_vec` (Tasks 1–2).
- Produces: no signature change; `_raster_to_bng` output identical, just faster.

- [ ] **Step 1: Rewire `_bng_cells`**

Replace the scalar loop:

```python
def _bng_cells(e: np.ndarray, n: np.ndarray, resolution: int) -> np.ndarray:
    """Per-valid-pixel BNG cell ids (Long int64), fully vectorized.

    ``e``/``n`` are EPSG:27700 eastings/northings (pixel centroids of the WARPED
    raster). Delegates to ``pygx._bng.point_to_cell_id_vec`` -- the SAME shared
    numpy core the scalar ``point_to_cell_id`` wraps, so cell ids are identical.
    """
    return _bng.point_to_cell_id_vec(e, n, resolution)
```

- [ ] **Step 2: Rewire the `is_valid` filter in `_raster_to_bng._run`**

Replace the per-pixel `keep = np.array([_bng.is_valid(int(c)) for c in cids], ...)` block with:

```python
            cids = _bng_cells(e, n, resolution)
            # Drop out-of-GB pixels (is_valid) BEFORE grouping so a cell is only
            # emitted for >=1 valid, in-GB pixel (sec 2.6). Vectorized over the
            # single-resolution batch.
            keep = _bng.is_valid_vec(cids, resolution)
            cids = cids[keep]
            vals = vals[keep]
```

(Remove the now-unused `# vectorscan: ok (pygx._bng SSoT)` comments on those two sites; the SSoT is preserved via the shared core.)

- [ ] **Step 3: Run the gridagg BNG + cross-tier parity suites**

Run: `python -m pytest python/geobrix/test/pyrx/test_gridagg_bng.py python/geobrix/test/pyrx/test_parity_bng_quadbin_raster_grid.py python/geobrix/test/pyrx/test_tessellate_bng.py -v`
Expected: PASS — cell sets + measures unchanged (the vector path must not shift any cell id or drop a different set).

- [ ] **Step 4: Run the full pygx + pyrx light suites for the touched packages**

Run: `python -m pytest python/geobrix/test/pygx/ python/geobrix/test/pyrx/ -v` (in Docker if sample-data-backed tests are present: `gbx:test:python --path python/geobrix/test/pygx/` and `--path python/geobrix/test/pyrx/`)
Expected: PASS.

- [ ] **Step 5: Lint**

Run: `gbx:lint:python --check`
Expected: clean.

- [ ] **Step 6: Commit**

Subject: `perf(pyrx): use vectorized BNG kernels in raster->grid`
Body: note the ~6× light BNG rastertogrid gap this closes; a follow-on cluster bench re-measures (not part of this change's gate).

---

## Post-plan follow-ons (NOT in this plan)

- **Cluster bench re-measure** (own cycle): confirm light BNG rastertogrid is no longer ~6× slower (target: parity-class with quadbin). Update `docs/docs/api/benchmarking.mdx` BNG numbers. Per `bench-changes-update-docs` + `benchmarking-preflight-discipline`.
- **27700 warp cost** (secondary BNG perf term, spec §5): out of scope here; separate perf item if the encoder fix doesn't close the gap enough.
- quadbin (already vectorized) and h3 (already C-backed) need NO equivalent work — BNG was the sole grid paying per-pixel Python cost.

## Self-Review

- **Spec coverage:** §3.1 core → Task 1; §3.2 wrappers → Task 1; §3.3 `is_valid_vec` → Task 2; §3.4 `_bng_cells` rewire → Task 3; §4 parity gate (regression sweep + scalar-is-vec smoke + int64 res±6 + end-to-end) → Task 1 tests + Task 3 parity suites; §6 surfaces → all tasks; §7 risks (truncation, int64, scalar-through-numpy, out-of-GB) → Global Constraints + Task 1 test coverage. Covered.
- **Placeholders:** none — all code shown; `is_valid_vec` digit-index mapping explicitly gated by a cross-resolution equality test, with a res −1 caveat.
- **Type consistency:** `point_to_cell_id_vec` returns int64 (matches `_bng_cells`' historic `dtype="int64"`); `is_valid_vec` returns bool array (matches the old `keep` bool array); scalar wrappers return Python `int`/`bool` (unchanged).
