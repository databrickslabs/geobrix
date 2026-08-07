# Light-Tier Arity Gaps — Wrap-Up (design)

**Date:** 2026-08-07
**Status:** ratified (this doc), pending plan
**Predecessor:** `docs/superpowers/specs/2026-08-07-canonical-param-names-orphan-tail-design.md`
(the naming spec, which deliberately deferred these 7 as "feature parity, not naming").

## Goal

Close out the 7 remaining `[B]` light-tier arity gaps in
`docs/tests-function-info/param_name_waiver.txt` — the divergences where the light
Python tier exposes fewer parameters than the heavy tier. Naming was resolved by the
predecessor spec; this decides implement-vs-document for each remaining gap and executes.

## The 7 gaps (verified against source)

| Function | Gap | Bucket |
|---|---|---|
| `gbx_bng_kringexplode` | light `*args/**kwargs` Column stub (arity 0 vs 2) | A |
| `gbx_bng_kloopexplode` | same | A |
| `gbx_bng_geomkringexplode` | same (arity 0 vs 3) | A |
| `gbx_bng_geomkloopexplode` | same | A |
| `gbx_bng_tessellateexplode` | same (arity 0 vs 3) | A |
| `gbx_rst_xyzpyramid` | light missing `rescale` (arity 6 vs 7) | B |
| `gbx_bng_tessellate` | light missing `keep_core_geom` (arity 2 vs 3) | C |

## Governing finding

Ground-truth reading of the light tier reframed the work: the "gaps" are much smaller
than "unimplemented functionality." The underlying algorithms already exist; only thin
Python-surface plumbing is missing — except Bucket A, where the light tier already ships
the *correct* implementation and the missing surface is intentionally absent.

## Bucket A — the 5 bng explode functions: RATIFY as documented streaming UDTFs (no code behavior change)

**Verified facts:**
- The 5 explode functions are NOT stubs. The light tier ships **registered, row-streaming
  `@udtf` classes** (`_BngKRingExplode`, `_BngKLoopExplode`, `_BngGeomKRingExplode`,
  `_BngGeomKLoopExplode`, `_BngTessellateExplode`) in `pygx/functions.py` (~lines 366-410),
  registered via `s.udtf.register(...)` (~lines 699-711). Each `eval` **`yield`s one row
  per cell** — the pure-Python analog of heavy's `CollectionGenerator`. No array is
  materialized. Works JAR-less on Serverless via SQL LATERAL.
- The ONLY thing absent is the **pyspark DataFrame-API `Column` form**
  (`bng_kringexplode(col, k)`), which currently `raise NotImplementedError`.

**Ruling (user-approved):** keep these 5 permanently waived, but **reclassify** — from
"unimplemented gap" to "intentional streaming-UDTF boundary." Rationale:
- The array-free streaming solution already exists and is the *correct* primitive for the
  "avoid large arrays" goal (see the large-array backlog thread).
- Spark's Python DataFrame API has no clean way to invoke a streaming UDTF as a drop-in
  `Column` in `.select()`. The only Column-shaped alternative —
  `explode(bng_kring(...))` — reintroduces the exact large-array materialization + UDF
  boundary tax the streaming UDTF avoids. So a DataFrame `Column` form would be a
  *regression*, not a feature.
- Do NOT relax the guard's Invariant B to force these to arity-0 (the guard correctly
  models scalar Column arity; a special case would obscure real gaps).

**Work (no behavior change):**
1. Rewrite the 5 `NotImplementedError` messages + docstrings: from "Light BNG {name} has no
   Python Column form" to a clear statement that these are **streaming table functions** —
   "Light {name} is a streaming UDTF (one row per cell, no array materialized). Invoke via
   SQL LATERAL: `SELECT t.* FROM <df>, LATERAL gbx_{name}(...) t`, or `spark.sql(...)`."
2. Rewrite their `param_name_waiver.txt` entries: from "LATERAL-only light stub (missing
   explode impl, not a naming issue)" to "intentional: registered streaming UDTF; no
   pyspark Column form by design (array-free — a Column form would force array
   materialization). Invoke via SQL LATERAL."
3. No test change (no behavior change). These stay in the waiver.

## Bucket B — `rst_xyzpyramid`: thread `rescale` onto the DataFrame stub

**Verified facts:**
- The registered UDTF `_RstXyzPyramidUDTF.eval` (`pyrx/functions.py:4494`) **already accepts
  and uses `rescale`** — it threads to `xyz.iter_pyramid(...)`, and its `rsc = rescale if
  rescale is not None else "auto"` line is present. So the render path is complete.
- The DataFrame entry point `rst_xyzpyramid(...)` (`pyrx/functions.py:4569`) is itself a
  `NotImplementedError` LATERAL-redirect stub (like Bucket A), and its **signature omits
  `rescale`** while its docstring's `Args:` block **already documents `rescale`** (drift).
  The sibling `rst_tilexyz` already carries `rescale: ColLike = "auto"`.

**Work:**
1. Add `rescale: ColLike = "auto"` to the `rst_xyzpyramid` stub signature (matching
   `rst_tilexyz`).
2. Add `rescale` to the LATERAL example string in the docstring:
   `... gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling, rescale) t`.
3. Confirm the registered UDTF's `register` binding exposes the `rescale` arg to SQL callers
   (the UDTF `eval` already has the param; verify the registration/`returnType` path passes
   it through).
4. Fixture line `gbx_rst_xyzpyramid` → `tile, min_z, max_z, [format], [size], [resampling], [rescale]`
   (this is already the canonical target in `canonical_param_names.txt`).
5. Unwaive `gbx_rst_xyzpyramid`.

**Test:** a light test asserting that a non-default `rescale` (e.g. an explicit `(min,max)`
pair or `"none"`) is accepted and alters the rendered tile bytes vs `"auto"`. Because
`rst_xyzpyramid` is LATERAL-only, exercise via `spark.sql` on the registered UDTF (Docker;
xyzpyramid renders — needs the full env).

## Bucket C — `bng_tessellate`: pass `keep_core_geom` through the light wrapper

**Verified facts:**
- The core `_bng.tessellate_str(geom, resolution, keep_core_geom=False)`
  (`pygx/_bng.py:636`) **already takes `keep_core_geom`** and populates the chip field
  accordingly (line ~595/620).
- The light `bng_tessellate(geom, resolution)` Column wrapper (`pygx/functions.py:906`, a
  scalar `s.udf.register` UDF) does not accept or pass `keep_core_geom`.
- Heavy `bng_tessellate(geom, resolution, keep_core_geom=True)` — note heavy default is
  `True`; the light core default is `False`. **Align the light public default to `True`**
  to match heavy (the guard/fixture models the SQL surface; heavy is canonical).

**Work:**
1. Add `keep_core_geom: ColLike = True` to light `bng_tessellate` signature.
2. Thread it through the registered scalar UDF to `_bng.tessellate_str(..., keep_core_geom=...)`.
3. Fixture line `gbx_bng_tessellate` → `geom, resolution, [keep_core_geom]` (already the
   canonical target).
4. Unwaive `gbx_bng_tessellate`.

**Test:** a light test asserting `keep_core_geom=True` populates the `chip` field of the
returned `ARRAY<STRUCT<cellid,core,chip>>` for a fully-interior cell, while `False` leaves
it null (matches the `_bng.tessellate_str` contract).

## Guard / fixture interaction

- After Buckets B and C, `gbx_rst_xyzpyramid` and `gbx_bng_tessellate` leave the waiver;
  the guard's Invariant B no longer reports them (light arity now ≥ heavy).
- Bucket A's 5 stay waived. `--report` will still list their 5 `[B]` arity-0 lines — that is
  correct and expected (light has no scalar Column form by design).
- Net waiver: **7 → 5** (all 5 remaining are the intentional streaming-UDTF explodes, with
  accurate documentation).

## Out of scope (recorded, not done here)

- **The array-returning scalar functions** (`bng_kring`/`bng_kloop`/`bng_geomkring`/
  `bng_tessellate` non-explode, quadbin/h3 kring, `bng_polyfill`) still materialize the full
  array per row and OOM at scale. Generalizing row-streaming to those is a separate
  architectural workstream (its own brainstorm) — see the large-array backlog memory. This
  spec does NOT touch them.
- The pre-existing 8 heavy RST_Clip/RST_V2RoundTrip test failures are unrelated and tracked
  separately.

## Outcome

Waiver 7 → 5. Two genuine parity gaps closed (xyzpyramid `rescale`, tessellate
`keep_core_geom`). Five explode functions reclassified from "unimplemented" to accurately
documented intentional streaming-UDTF boundary — no behavior change, no regression, honest
about the tier's array-free design.
