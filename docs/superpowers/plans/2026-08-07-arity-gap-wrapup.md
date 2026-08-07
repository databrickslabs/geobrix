# Light-Tier Arity-Gap Wrap-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the 7 remaining `[B]` light-tier arity gaps in `param_name_waiver.txt` — implement two genuine parity gaps (rst_xyzpyramid `rescale`, bng_tessellate `keep_core_geom`) and reclassify five already-correct streaming-UDTF explode functions with accurate docs — driving the waiver 7→5.

**Architecture:** Three independent tasks, one per bucket. Bucket A is docs/waiver-only (no behavior change — the 5 explode functions are already correct streaming UDTFs). Buckets B and C thread an existing-but-unexposed parameter through the light Python surface to reach already-supporting implementations. All pure-Python (pyrx/pygx), no Scala, no JAR.

**Tech Stack:** Python 3.12 light tier (`databricks.labs.gbx.pyrx` / `.pygx`), Spark UDF/UDTF registration, the `gbx:*` command palette inside the `geobrix-dev` Docker container.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-08-07-arity-gap-wrapup-design.md` (ratified).
- **Bucket A = NO behavior change.** The 5 bng explode functions stay as registered streaming `@udtf`s with `NotImplementedError` DataFrame-Column stubs; only their message/docstring text and waiver annotations change. They REMAIN waived. Do NOT add a `Column` form (an `explode(bng_kring(...))` form would force large-array materialization — a regression).
- **No Scala changes, no JAR, no builder changes.** All work is in the light Python tier.
- **The guard** `docs/scripts/check-param-names.py`: normal mode exit 0; `--report` ignores the waiver. After this plan, `--report` still lists the 5 Bucket-A `[B]` arity-0 lines (correct — light has no scalar Column form by design), and no longer lists xyzpyramid or bng_tessellate.
- **`function-info.json` is GENERATED** — never hand-edit; regenerate via `gbx:docs:function-info` if any registered surface changes. usageArgs derive from the Scala field, unchanged here — regen should be a no-op but confirms.
- **The frozen fixture** `docs/tests-function-info/canonical_param_names.txt` already carries the canonical targets `gbx_rst_xyzpyramid → tile, min_z, max_z, [format], [size], [resampling], [rescale]` and `gbx_bng_tessellate → geom, resolution, [keep_core_geom]`. Do NOT edit the fixture — confirm it matches.
- **Container work** (pytest, function-info regen) via `gbx:*` inside the RUNNING `geobrix-dev` container. Guard runs on HOST.
- **`git add` EXPLICIT paths only** — NEVER `git add -A` (strays: `.isaac/`, `.tmp`, `scratchpad/`, zips). Commit subject ≤72 chars + WHY body + `Co-authored-by: Isaac`.
- **No Databricks profile needed.** NEVER run `databricks auth login`.
- **heavy `bng_tessellate` default is `keep_core_geom=True`** (verified) — the light public default must match: `True`. Note `_bng.tessellate_str`'s own default is `False`; the light wrapper must pass an explicit value, not rely on the core default.

---

### Task 1 (Bucket A): Reclassify the 5 bng explode functions as documented streaming UDTFs

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/functions.py` (`_EXPLODE_HINT` ~line 930; the 5 stub docstrings ~lines 936-974)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (the 5 explode entries, lines ~51-55)

**Interfaces:**
- Consumes: nothing.
- Produces: no API/behavior change. The 5 functions still `raise NotImplementedError` on the DataFrame-Column path; only human-facing text changes. They REMAIN in the waiver.

**Context:** Verified — these 5 are NOT unimplemented. `pygx/functions.py` ships registered row-streaming `@udtf` classes (`_BngKRingExplode` etc., ~lines 380-410) registered via `s.udtf.register(...)` (~lines 699-711). The `NotImplementedError` stubs are only the pyspark DataFrame-Column entry point, intentionally absent because the array-free streaming UDTF has no clean drop-in `Column` form. This task makes the messaging accurate.

- [ ] **Step 1: Rewrite `_EXPLODE_HINT`** to state these are streaming table functions, not missing implementations.

Replace (current, ~line 930):
```python
_EXPLODE_HINT = (
    "Light BNG {name} has no Python Column form; invoke the registered UDTF via "
    "SQL LATERAL, e.g. SELECT t.* FROM <df>, LATERAL {udtf}(...) t"
)
```
with:
```python
_EXPLODE_HINT = (
    "Light BNG {name} is a streaming table function (registered UDTF {udtf}): it "
    "emits one row per cell with no array materialized, so it has no pyspark "
    "Column form by design. Invoke via SQL LATERAL, e.g. "
    "SELECT t.* FROM <df>, LATERAL {udtf}(...) t  (or spark.sql(...))."
)
```

- [ ] **Step 2: Update the 5 stub docstrings** so each names the streaming-UDTF nature (one-liners; keep the `raise NotImplementedError(_EXPLODE_HINT.format(...))` bodies exactly as-is). For each function, change the docstring line to the pattern (example for `bng_kringexplode`):

```python
def bng_kringexplode(*args, **kwargs) -> Column:
    """Streaming UDTF (SQL-LATERAL): SELECT cellid FROM gbx_bng_kringexplode(cellid, k). No Column form."""
    raise NotImplementedError(
        _EXPLODE_HINT.format(name="bng_kringexplode", udtf="gbx_bng_kringexplode")
    )
```
Apply the same docstring rewording to `bng_kloopexplode` (`... FROM gbx_bng_kloopexplode(cellid, k)`), `bng_geomkringexplode` (`... LATERAL gbx_bng_geomkringexplode(geom, res, k) t`), `bng_geomkloopexplode` (`... LATERAL gbx_bng_geomkloopexplode(geom, res, k) t`), `bng_tessellateexplode` (`... LATERAL gbx_bng_tessellateexplode(geom, res) t`), prefixing each with "Streaming UDTF (SQL-LATERAL): " and suffixing "No Column form." Do NOT change the `*args/**kwargs` signatures or the `raise` bodies.

- [ ] **Step 3: Rewrite the 5 waiver entries** in `docs/tests-function-info/param_name_waiver.txt`. Replace each `# [B] LATERAL-only light stub (raises NotImplementedError); missing explode impl, not a naming issue` with `# [B] INTENTIONAL: registered streaming UDTF (one row/cell, array-free); no pyspark Column form by design. SQL LATERAL only.` Keep the function names and column alignment. Add/keep a header note above the block: `# The 5 bng *explode fns below are PERMANENT [B] waivers — streaming UDTFs, not gaps. See spec 2026-08-07-arity-gap-wrapup.`

- [ ] **Step 4: Verify no behavior change + guard green**

Run (HOST): `python3 docs/scripts/check-param-names.py`
Expected: `check-param-names: OK` (exit 0) — the 5 stay waived, still pass.
Run (container): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/ --log arity-a-pygx.log`
Expected: green — no test asserts on the `_EXPLODE_HINT` text (if one does, it will surface here; update it to match the new wording only if it's asserting the message verbatim). Quote the summary line.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/functions.py docs/tests-function-info/param_name_waiver.txt
git commit -m "docs(pygx): reclassify bng explode fns as streaming UDTFs

The 5 bng *explode fns are registered row-streaming UDTFs (one row per
cell, no array materialized) — not unimplemented stubs. Their absent
pyspark Column form is intentional: a Column form would force array
materialization (large-k OOM + boundary tax). Reword the NotImplementedError
hint, docstrings, and waiver entries to say so. No behavior change.

Co-authored-by: Isaac"
```

---

### Task 2 (Bucket B): Add `rescale` to `rst_xyzpyramid`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (`rst_xyzpyramid` stub ~line 4569; confirm `_RstXyzPyramidUDTF.eval` ~line 4494 already has `rescale`; registration ~line 90)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (unwaive `gbx_rst_xyzpyramid`)
- Test: `python/geobrix/test/pyrx/` (add a rescale-behavior test)

**Interfaces:**
- Consumes: nothing.
- Produces: `rst_xyzpyramid` light DataFrame stub signature gains `rescale: ColLike = "auto"`; the registered UDTF `gbx_rst_xyzpyramid` accepts a 7th SQL arg `rescale`.

**Context:** Verified — `_RstXyzPyramidUDTF.eval(self, tile, min_z, max_z, format=None, size=None, resampling=None, rescale=None)` ALREADY accepts and uses `rescale` (threads to `xyz.iter_pyramid`). The only gaps: (a) the DataFrame stub signature omits `rescale` though its docstring `Args:` already documents it, and (b) the stub's LATERAL example string omits it. The sibling `rst_tilexyz` already has `rescale: ColLike = "auto"`.

- [ ] **Step 1: Write the failing test**

Add to a new file `python/geobrix/test/pyrx/test_xyzpyramid_rescale.py`. The test exercises the registered UDTF via `spark.sql` (rst_xyzpyramid is LATERAL-only) and asserts a non-"auto" rescale is accepted and produces different bytes than "auto". Use an existing sample raster fixture the pyrx tests already use (find one via `grep -rl "modis\|/Volumes\|sample" python/geobrix/test/pyrx/*.py | head` and follow that pattern). Skeleton:

```python
def test_xyzpyramid_accepts_rescale_and_alters_output(gbx_spark, sample_tile_df):
    # sample_tile_df: a 1-row DF with a `tile` struct column (reuse the pyrx conftest fixture)
    from databricks.labs.gbx.pyrx import functions as pyrx
    pyrx.register(gbx_spark)
    sample_tile_df.createOrReplaceTempView("t")
    auto = gbx_spark.sql(
        "SELECT x.bytes FROM t, LATERAL gbx_rst_xyzpyramid(t.tile, 0, 1, 'PNG', 256, 'bilinear', 'auto') x"
    ).collect()
    none = gbx_spark.sql(
        "SELECT x.bytes FROM t, LATERAL gbx_rst_xyzpyramid(t.tile, 0, 1, 'PNG', 256, 'bilinear', 'none') x"
    ).collect()
    assert auto and none
    # rescale changes 8-bit contrast encoding -> byte payloads differ
    assert [r.bytes for r in auto] != [r.bytes for r in none]
```
If the pyrx suite has no reusable `sample_tile_df` fixture, build the tile inline from a sample GeoTIFF the way the nearest existing xyz/tilexyz test does — match that test's fixture construction exactly rather than inventing one.

- [ ] **Step 2: Run it to verify it fails**

Run (container): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_xyzpyramid_rescale.py --log arity-b-fail.log`
Expected: FAIL — the SQL `gbx_rst_xyzpyramid(..., 'auto')` call has 7 args but the registered UDTF's SQL arity may reject the 7th, OR the two results are identical because rescale isn't wired to SQL. (Confirm the actual failure mode; if the UDTF already accepts 7 args in SQL and the test passes immediately, that means only the DataFrame stub signature + docstring need fixing — note that and proceed to Step 3 for the signature/doc fix, then this test guards it.)

- [ ] **Step 3: Add `rescale` to the DataFrame stub signature + LATERAL docstring**

In `rst_xyzpyramid` (~line 4569), change the signature from:
```python
def rst_xyzpyramid(
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
) -> None:
```
to add the 7th param (matching `rst_tilexyz`'s `rescale: ColLike = "auto"`):
```python
def rst_xyzpyramid(
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
    rescale: ColLike = "auto",
) -> None:
```
And in the docstring, change the LATERAL example line from
`LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling) t` to
`LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling, rescale) t`,
and the `raise NotImplementedError(...)` message's embedded SQL likewise (append `, rescale` before ` t`). The `Args:` block already documents `rescale` — leave it.

- [ ] **Step 4: Confirm the registered UDTF exposes `rescale` to SQL**

The UDTF `eval` already has `rescale=None`. Spark Python UDTFs accept trailing optional args positionally, so a 7-arg SQL call binds `rescale`. Verify by re-running the test:
Run (container): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_xyzpyramid_rescale.py --log arity-b-pass.log`
Expected: PASS (auto vs none produce different bytes). If SQL still rejects 7 args, the registration `lambda s, n, c: s.udtf.register(n, c)` is fine (UDTF arity is defined by `eval`); investigate whether `eval`'s param defaults are being honored — but do NOT change `eval`'s already-correct signature.

- [ ] **Step 5: Unwaive + regen + guard**

Remove/tombstone `gbx_rst_xyzpyramid` in `docs/tests-function-info/param_name_waiver.txt` (comment it `# Task 2 DONE: light rescale added`).
Confirm fixture already canonical: `grep gbx_rst_xyzpyramid docs/tests-function-info/canonical_param_names.txt` → `tile, min_z, max_z, [format], [size], [resampling], [rescale]` (no edit).
Run (container): `gbx:docs:function-info` (`--log arity-b-fi.log`) — regen, confirm no unexpected JSON change.
Run (HOST): `python3 docs/scripts/check-param-names.py` → OK exit 0 (xyzpyramid no longer waived, now passes Invariant B: light arity 7 ≥ heavy 7).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/functions.py python/geobrix/test/pyrx/test_xyzpyramid_rescale.py docs/tests-function-info/param_name_waiver.txt
# add function-info.json only if regen changed it
git commit -m "feat(pyrx): expose rescale on light rst_xyzpyramid

The rst_xyzpyramid UDTF already implemented rescale (threads to
xyz.iter_pyramid); only the DataFrame stub signature and LATERAL example
omitted it, so SQL callers could not pass it and the arity lagged heavy.
Add rescale=\"auto\" to the stub signature + docstring example. Unwaive.

Co-authored-by: Isaac"
```

---

### Task 3 (Bucket C): Add `keep_core_geom` to light `bng_tessellate`

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pygx/functions.py` (`bng_tessellate` Column wrapper ~line 906; `_bng_tessellate` registered UDF ~line 357)
- Modify: `docs/tests-function-info/param_name_waiver.txt` (unwaive `gbx_bng_tessellate`)
- Test: `python/geobrix/test/pygx/test_bng_tessellate.py` (add keep_core_geom-behavior test)

**Interfaces:**
- Consumes: `_bng.tessellate_str(geom, resolution, keep_core_geom=False)` — already supports the param (verified `pygx/_bng.py:636`).
- Produces: light `bng_tessellate(geom, resolution, keep_core_geom=True)` Column wrapper; registered UDF `gbx_bng_tessellate` accepts a 3rd SQL arg.

**Context:** Verified — the core `_bng.tessellate_str` already takes `keep_core_geom` and populates the chip field when True. The gap is only the light `_bng_tessellate` scalar UDF and its Column wrapper, which hardcode the 2-arg call. `keep_core_geom` controls whether a fully-interior cell's `chip` field holds its core geometry WKB (True) or null (False). Heavy default is `True`.

- [ ] **Step 1: Write the failing test**

IMPORTANT: `python/geobrix/test/pygx/test_bng_tessellate.py` is a **Spark-free** unit-test file — it tests `_bng.tessellate_str` and helpers directly with `shapely.to_wkb(box(...))`, NO `spark.sql`, NO `st_geomfromtext`. Match that pattern: test the **registered UDF callable `_bng_tessellate`** directly (it's a plain Python function), not via Spark. This avoids depending on a product built-in and keeps the file Spark-free.

Add to `python/geobrix/test/pygx/test_bng_tessellate.py` (the file already imports `from shapely import from_wkb, to_wkb` and `from shapely.geometry import box`, and `from databricks.labs.gbx.pygx import _bng`):

```python
def test_bng_tessellate_udf_keep_core_geom_toggles_interior_chip():
    from databricks.labs.gbx.pygx.functions import _bng_tessellate
    # 4.5km box on the 1km grid -> has interior (core) cells and border cells.
    geom = to_wkb(box(530000.0, 180000.0, 534500.0, 184500.0))
    res = _bng.get_resolution("1km")

    keep = _bng_tessellate(geom, res, keep_core_geom=True)
    interior_keep = [c for c in keep if c["core"]]
    assert interior_keep, "expected at least one interior (core) cell"
    assert any(c["chip"] is not None for c in interior_keep), \
        "keep_core_geom=True must populate interior-cell chip WKB"

    drop = _bng_tessellate(geom, res, keep_core_geom=False)
    interior_drop = [c for c in drop if c["core"]]
    assert all(c["chip"] is None for c in interior_drop), \
        "keep_core_geom=False must leave interior-cell chip null"

    # default matches heavy (True): interior chips populated
    default = _bng_tessellate(geom, res)
    assert any(c["chip"] is not None for c in default if c["core"]), \
        "light bng_tessellate default must be keep_core_geom=True (heavy parity)"
```
Note: the file's existing `test_tessellate_box_has_core_and_border` documents that the ARRAY form's core chip is None under the core-level `keep_core_geom=False` default — this new test asserts the light UDF's *public* default flips to True to match heavy, which is the behavior change Step 3 introduces.

- [ ] **Step 2: Run it to verify it fails**

Run (container): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/test_bng_tessellate.py --log arity-c-fail.log`
Expected: FAIL — `_bng_tessellate(geom, res, keep_core_geom=...)` raises `TypeError: _bng_tessellate() got an unexpected keyword argument 'keep_core_geom'` (the current UDF accepts only 2 positional args), so the test errors at the first `keep=` call.

- [ ] **Step 3: Thread `keep_core_geom` through the UDF and the Column wrapper**

In `_bng_tessellate` (~line 357), change from:
```python
def _bng_tessellate(geom, res):
    if geom is None or res is None:
        return None
    return [
        {"cellid": c, "core": bool(core), "chip": chip}
        for (c, core, chip) in _bng.tessellate_str(geom, _norm_res(res))
    ]
```
to accept and pass `keep_core_geom` (default True to match heavy; guard None):
```python
def _bng_tessellate(geom, res, keep_core_geom=True):
    if geom is None or res is None:
        return None
    keep = True if keep_core_geom is None else bool(keep_core_geom)
    return [
        {"cellid": c, "core": bool(core), "chip": chip}
        for (c, core, chip) in _bng.tessellate_str(geom, _norm_res(res), keep_core_geom=keep)
    ]
```
In the Column wrapper `bng_tessellate` (~line 906), change from:
```python
def bng_tessellate(geom: ColLike, resolution: ColLike) -> Column:
    """ARRAY<STRUCT<cellid:STRING, core:BOOL, chip:BINARY>> chips per cell."""
    return f.call_function("gbx_bng_tessellate", _col(geom), _col(resolution))
```
to:
```python
def bng_tessellate(
    geom: ColLike, resolution: ColLike, keep_core_geom: ColLike = True
) -> Column:
    """ARRAY<STRUCT<cellid:STRING, core:BOOL, chip:BINARY>> chips per cell.

    keep_core_geom (default True, matches heavy): when True, a fully-interior
    cell's chip holds its core-geometry WKB; when False the chip is null.
    """
    return f.call_function(
        "gbx_bng_tessellate", _col(geom), _col(resolution), _col(keep_core_geom)
    )
```
The register entry (~line 696, `s.udf.register("gbx_bng_tessellate", _bng_tessellate, ArrayType(BNG_CHIP_SCHEMA))`) needs NO change — it registers the callable, whose new 3rd param is optional.

**Behavior-change note (intended):** the default flips to `keep_core_geom=True` for the light public surface, matching heavy. A *2-arg* SQL call `gbx_bng_tessellate(g, res)` now populates interior-cell chips (previously null). Verified this does NOT break existing tests: `test_bng_tessellate.py` tests `_bng.tessellate_str` directly (unaffected), and `test_bng_udf.py`'s 2-arg SQL calls assert only `cellid` presence + downstream cellunion/intersection (never interior-chip nullness). If Step 4 surfaces an unexpected assertion on a 2-arg call's chip being null, STOP and report — do not weaken the new test to hide it.

- [ ] **Step 4: Run the test to verify it passes (and the whole pygx suite for regressions)**

Run (container): `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pygx/ --log arity-c-pass.log`
Expected: PASS — the new test passes (keep_core_geom=True populates interior chips, False leaves null, default=True), AND `test_bng_udf.py` / the rest of the pygx suite stay green (confirms the 2-arg default flip broke nothing). Quote the summary line.

- [ ] **Step 5: Unwaive + regen + guard**

Tombstone `gbx_bng_tessellate` in the waiver (`# Task 3 DONE: light keep_core_geom added`).
Confirm fixture canonical: `grep 'gbx_bng_tessellate\b' docs/tests-function-info/canonical_param_names.txt` → `geom, resolution, [keep_core_geom]` (no edit).
Run (container): `gbx:docs:function-info` (`--log arity-c-fi.log`).
Run (HOST): `python3 docs/scripts/check-param-names.py` → OK exit 0 (bng_tessellate no longer waived; light arity 3 ≥ heavy 3).

- [ ] **Step 6: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pygx/functions.py python/geobrix/test/pygx/test_bng_tessellate.py docs/tests-function-info/param_name_waiver.txt
# add function-info.json only if regen changed it
git commit -m "feat(pygx): expose keep_core_geom on light bng_tessellate

_bng.tessellate_str already supported keep_core_geom; the light UDF and
Column wrapper hardcoded the 2-arg call, so callers could not control
whether interior-cell chips carry core-geometry WKB. Thread keep_core_geom
through (default True, matching heavy). Unwaive.

Co-authored-by: Isaac"
```

---

### Task 4: Final verification — waiver at 5, guard report correct

**Files:** none (verification only).

- [ ] **Step 1: Confirm the active waiver is exactly the 5 Bucket-A explodes**

Run (HOST): `grep -vE '^\s*#|^\s*$' docs/tests-function-info/param_name_waiver.txt | sort`
Expected: exactly `gbx_bng_geomkloopexplode`, `gbx_bng_geomkringexplode`, `gbx_bng_kloopexplode`, `gbx_bng_kringexplode`, `gbx_bng_tessellateexplode`.

- [ ] **Step 2: Confirm guard normal + report**

Run (HOST): `python3 docs/scripts/check-param-names.py` → `check-param-names: OK` exit 0.
Run (HOST): `python3 docs/scripts/check-param-names.py --report` → exactly 5 `[B]` lines (the explodes, arity 0), zero `[A]` lines, no xyzpyramid, no bng_tessellate.

- [ ] **Step 3: Binding parity**

Run (container): `bash scripts/commands/gbx-test-bindings.sh --log arity-final-bindings.log` → `binding parity OK — all 180 registered functions` + `check-param-names: OK`.

- [ ] **Step 4: No commit** (verification only; if any check fails, return to the owning task).

---

## Self-Review

**Spec coverage:** Bucket A → Task 1 (reword 5 stubs + waiver, no behavior change). Bucket B → Task 2 (rescale on xyzpyramid). Bucket C → Task 3 (keep_core_geom on bng_tessellate). Waiver 7→5 verified in Task 4. All spec sections covered. ✓

**Placeholder scan:** every code step has verbatim before/after. Test skeletons carry a fallback instruction ("match the existing fixture pattern") because the exact pyrx/pygx conftest fixture name must be read at implementation time — that is a real instruction, not a placeholder, and the assertion logic is concrete. ✓

**Type consistency:** `rescale: ColLike = "auto"` matches `rst_tilexyz`. `keep_core_geom: ColLike = True` matches heavy default and threads to `_bng.tessellate_str(..., keep_core_geom=...)`. `_bng_tessellate` new param default `True` (guarded for None). No cross-task signature drift. ✓

**Ordering:** Tasks 1-3 independent (different functions); Task 4 is the aggregate gate. Any order works, but numeric order is fine.
