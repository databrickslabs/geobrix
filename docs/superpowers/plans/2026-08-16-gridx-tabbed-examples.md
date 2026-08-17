# GridX Tabbed Function Examples — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every GridX function in `docs/docs/api/gridx-functions.mdx` the same per-function 4-tab code examples (SQL / Python-light / Python-heavy / Scala) that RasterX and VectorX already have, backed by real executable doc-tests.

**Architecture:** Reuse the SHIPPED rails unchanged — the `FunctionExamples` MDX component (reads `function-info.json` `bindings[]`), the per-tier doc-test example convention (`<base>_python_light_example` / `_python_heavy_example` / `_scala_example` / `_sql_example` + each `_output`), the Setup+Conventions page template, and the `--filter` scoping added during the VectorX pass. One example per function shared across all four tabs; labeled variations only where a tier genuinely diverges. No mechanism changes.

**Tech Stack:** Docusaurus `FunctionExamples.js`; Python doc-tests under `docs/tests/python/api/` run in `geobrix-dev` Docker via `gbx:test:python-docs`; Scala string-constant examples in `docs/tests/scala/api/ScalaApiExamples.scala`; `docs/scripts/generate-function-info.py` → `function-info.json`.

**Spec / authoritative standard (the plan argues from these — executors read them):**
- `docs/tests/python/api/README-tabbed-examples.md` — the checked-in authoring standard.
- Memories: `tabbed-docs-authoring-conventions`, `rasterx-tier-divergence-doc-treatment`, `docs-example-guards`, `bng-explode-light-is-streaming-udtf`, `pygx-light-gridx-design`.
- The completed VectorX plan `docs/superpowers/plans/2026-08-16-vectorx-tabbed-examples.md` is the proven template; this plan mirrors it.

## Global Constraints

- **Fixtures MUST be real committed data or inline literals** — never `/Volumes` placeholders. GridX inputs are cell-id strings, eastings/northings, lon/lat, and geometries → use inline literals in the fixture helpers (like VectorX's `array(unhex(...))` / EWKT). Verify non-degenerate.
- **ONE example per function, shared across all present tabs** (same fixture + operation + args), expressed in each tier's language.
- **Tab + output consistency is a per-function review gate:** all present tabs show the SAME example; a value identical across tiers renders identically (same truncation + annotation); genuine tier differences are LABELED (`:::note`/`:::warning`), never fabricated or hidden.
- **★ RECURRING BUG (bit VectorX T2 & T3): the SQL tab MUST show the DIRECT named-function invocation returning its REAL type — NEVER wrap it in `length()`, `st_astext()`, an accessor, or any function to manufacture a clean scalar.** A cell-id-array-returning fn shows the array; a geometry-returning fn shows `... (WKB binary)`; a double-returning fn shows the real number.
- **Uniform output representation** (same convention as VectorX, stated once in Setup/Conventions): cell IDs shown in full when short (BNG strings like `'TQ1234'`, quadbin longs); geometry `[E]WKB` → `... (WKB binary)`/`(EWKB binary)`; `[E]WKT` → full if short else type+leading-coords + `...`; arrays of cell IDs → show a few then `...` if long; doubles → real values; ~60-char cell cap; identical-across-tier values render identically with the same annotation.
- **BNG domain rules (CLAUDE.md):** BNG resolution is integer **±1..±6** (1=100km…6=1m; negatives=quadrants) or string keys (`"1km"`, `"100m"`) — NEVER metres-as-Int (e.g. `1000`). `bng_pointascell` expects **BNG eastings/northings (EPSG:27700)**, not WGS84 — use BNG coords, e.g. `POINT(530000 180000)` (central London). `gbx_bng_cellarea` returns **square kilometres**. BNG examples need Great-Britain coords/cell-ids.
- **The 5 BNG `*explode` UDTFs** (`bng_geomkloopexplode`, `bng_geomkringexplode`, `bng_kloopexplode`, `bng_kringexplode`, `bng_tessellateexplode`) are **registered streaming UDTFs with NO pyspark Column form (permanent `[B]` waiver)** — SQL LATERAL only. Their Python-light tab shows the `spark.sql("SELECT t.* FROM v, LATERAL gbx_bng_Xexplode(...) t")` form; there is no light/heavy Column example. DESCRIBE FUNCTION is heavy-only.
- **quadbin**: `quadbin_pointascell` expects WGS84 lon/lat + a resolution (0–26); cell IDs are longs. **custom**: `custom_grid`/`custom_pointascell` take grid-definition params — use the probe's real args.
- **Verification is ALWAYS pinpointed to the affected function(s) — NEVER the full suite.** Every run is `gbx:test:python-docs --path api/ --filter '<the task's functions>'` (`-k` runs ONLY matching tests; whole-dir collection just keeps `_fixtures` resolving) **+ `gbx:test:docs-examples`** (host-only guard: ALL tabs' width/completeness/annotation consistency across every package = the cross-effect/regression net for the shared `function-info.json` regen). This applies to EVERY task including T0's baseline (`--filter 'bng_ or quadbin_ or custom_'`, GridX only) and T8's finalize (`--filter 'bng_ or quadbin_ or custom_'`, all 40 GridX fns — NOT the full ~1148-test api/ suite). The guard, not a full pytest run, is the cross-package regression check. Extract failures ANSI-stripped: `sed '/short test summary/,$p' <log> | sed 's/\x1b\[[0-9;]*m//g' | grep '^FAILED'`. NEVER inline-progress grep. NEVER run bare `--path api/` without `--filter`.
- **A `ds/`/package change is NOT expected here (docs-only)** — but if any package source is touched, the affected `test/**` unit suite (not just doc-tests) must run.
- **Port 3000 is the user's** — build/`3001` only. **Never edit user hand-edits** (git status; scoped git add). Never run two agents on the same file concurrently — author families sequentially where they share `gridx_functions.py` / `ScalaApiExamples.scala` / the mdx / `function-info.json`. Docs Python isn't covered by `gbx:lint:python`; still black-format touched files in Docker. Commit subjects ≤72 + WHY + `Co-authored-by: Isaac`.

---

## The 40 GridX functions, grouped for authoring

| Task | Family / batch | Functions |
|---|---|---|
| T2 | BNG codec + accessors | `bng_aswkb`, `bng_aswkt`, `bng_cellarea`, `bng_centroid`, `bng_eastnorthasbng`, `bng_pointascell` |
| T3 | BNG relations/distance | `bng_distance`, `bng_euclideandistance`, `bng_cellintersection`, `bng_cellunion` |
| T4 | BNG neighborhood/fill | `bng_kring`, `bng_kloop`, `bng_geomkring`, `bng_geomkloop`, `bng_polyfill`, `bng_tessellate` |
| T5 | BNG aggregators + explode UDTFs | `bng_cellintersection_agg`, `bng_cellunion_agg`, + the 5 `*explode` (SQL-LATERAL-only) |
| T6 | quadbin (10) | `quadbin_aswkb`, `quadbin_centroid`, `quadbin_distance`, `quadbin_kring`, `quadbin_pointascell`, `quadbin_polyfill`, `quadbin_resolution`, `quadbin_tessellate`, `quadbin_cellunion`, `quadbin_cellunion_agg` |
| T7 | custom (7) | `custom_grid`, `custom_pointascell`, `custom_cellaswkb`, `custom_cellaswkt`, `custom_centroid`, `custom_polyfill`, `custom_kring` |

## File Structure

- **Create** `docs/tests/python/api/gridx_functions_python_light.py` (+ `test_gridx_functions_python_light.py`) — the `pygx` per-function light examples.
- **Modify** `docs/tests/python/api/gridx_functions.py` — add `<base>_python_heavy_example` + `_output` per function (T0 confirms if it's currently per-fn or setup-only).
- **Modify** `docs/tests/python/api/gridx_functions_sql.py` — ensure every function has a real `<base>_sql_example()` + `_output`.
- **Modify** `docs/tests/scala/api/ScalaApiExamples.scala` — add `val <base>_scala_example` per function (compile+non-empty string constants).
- **Modify** `docs/docs/api/gridx-functions.mdx` — Setup+Conventions; convert each `CodeFromTest` (47 today) to `<FunctionExamples>`; alphabetize within families.
- **Modify** `docs/tests/python/api/_fixtures.py` — add GridX fixture helpers (BNG cell-ids/e-n, quadbin lon/lat + cell-ids, custom grid params, geometries) as inline literals.
- **Modify** `docs/scripts/generate-function-info.py` — the `_TIER_SCANS` light glob is already `*_python_light.py` (from VectorX T1) so it will catch `gridx_functions_python_light.py`; CONFIRM in T1, and add a heavy scan entry for `gridx_functions.py` if not already covered.
- **Regenerate** `function-info.json` after each task so `bindings[]` picks up new tabs.

---

### Task 0: Baseline + probe all 40 functions (foundation — no authoring)

**Files:** none modified (writes a probe-facts note to `prompts/documentation/2026-08-16-gridx-tabbing-probe.md`).

**Interfaces:**
- Produces: per function — invocation form per tier (scalar `df.select(...)` vs UDTF `SELECT t.* FROM v, LATERAL gbx_fn(...) t` vs heavy CollectionGenerator `LATERAL VIEW`), return type (cell-id string/long / WKB / double / array / struct), a real in-domain fixture + args, and the real output value; plus the current `--path api/` GridX pass/fail baseline.

- [ ] **Step 1:** Baseline — `bash scripts/commands/gbx-test-python-docs.sh --path api/ --filter 'bng_ or quadbin_ or custom_' --skip-build --log gx-baseline.log` (GridX-only, not the full suite); ANSI-strip failures; record the GridX passed/failed count and any `bng_`/`quadbin_`/`custom_` failures TODAY.
- [ ] **Step 2:** Inspect `gridx_functions_sql.py` + `gridx_functions.py`: per-function coverage (which of the 40 have real `*_sql_example()` / `*_python_heavy_example()`, vs setup-only).
- [ ] **Step 3:** Probe each of the 40 live (delete probes after): light invocation form (Column vs UDTF/`NotImplementedError`), heavy + Scala form, return type, a real in-domain fixture + args giving NON-DEGENERATE output, and the real output value. BNG: use BNG e/n (EPSG:27700, e.g. 530000 180000) + integer/string resolutions per CLAUDE.md. The 5 `*explode`: confirm SQL-LATERAL-only (no Column). quadbin: WGS84 lon/lat + resolution. custom: real grid params. **Trust a full `--path api/` run over standalone probes where they disagree** (registration state).
- [ ] **Step 4:** Decide per function: tiers AGREE (one example) or DIVERGE (label the variation) — e.g. heavy scalar-array vs light UDTF invocation-form split (two `;`-separated SQL, heavy first, + `_sql_variant`); light-only params; the explode UDTFs (SQL-only).
- [ ] **Step 5:** Record all facts in `prompts/documentation/2026-08-16-gridx-tabbing-probe.md`. No commit (scratch).

### Task 1: Setup + Conventions + fixtures + scaffold

**Files:** `docs/docs/api/gridx-functions.mdx` (Setup+Conventions), `docs/tests/python/api/_fixtures.py` (GridX loaders), `docs/tests/python/api/test_fixtures_helpers.py` (assert non-degenerate), create `gridx_functions_python_light.py` + `test_gridx_functions_python_light.py` scaffolds, `docs/scripts/generate-function-info.py` (confirm/extend `_TIER_SCANS` for gridx heavy), regenerate `function-info.json`.

**Interfaces:**
- Produces: fixture loaders (e.g. `bng_cells_df`, `bng_points_df`, `quadbin_cells_df`, `quadbin_points_df`, `custom_grid_df`, `gridx_geom_df` + heavy equivalents) + their Setup views; the Conventions section (canonical fixtures, tab-reading, output-representation convention, BNG-domain notes, explode-UDTF SQL-LATERAL note); the light-example scaffold.

- [ ] **Step 1:** Write fixture-helper tests asserting each new GridX loader returns real, non-degenerate data (valid cell ids, in-GB BNG coords, valid geometries).
- [ ] **Step 2:** `gbx:test:python-docs --path api/ --filter '<gridx fixture test names>' --skip-build` → confirm the new fixture tests FAIL (loaders absent).
- [ ] **Step 3:** Implement the loaders in `_fixtures.py` from inline literals (BNG cell-id strings + e/n; quadbin lon/lat + cell longs; custom grid params; geometries). Confirm `generate-function-info.py` `_TIER_SCANS` catches `gridx_functions_python_light.py` (light glob `*_python_light.py`) and has a `python-heavy` scan for `gridx_functions.py`; extend if needed. Create the two scaffold files.
- [ ] **Step 4:** Add the Setup + Conventions section to `gridx-functions.mdx` (copy the VectorX/RasterX template shape): placeholder→view→backs table; "assumes library installed"; tab-reading; the uniform output-representation convention; the BNG-domain rules; the explode-UDTF SQL-LATERAL-only note.
- [ ] **Step 5:** Regenerate `function-info.json`; `gbx:test:python-docs --path api/ --filter '<fixture tests>' --skip-build` fixture tests PASS; `gbx:test:docs-examples` guard passes; `gbx:docs:build` renders.
- [ ] **Step 6:** Commit (scoped): `_fixtures.py` + test + mdx + generate-function-info.py + scaffolds + function-info.json.

### Task 2: BNG codec + accessors (`bng_aswkb`, `bng_aswkt`, `bng_cellarea`, `bng_centroid`, `bng_eastnorthasbng`, `bng_pointascell`)

**Files:** append to `gridx_functions_python_light.py` + test; `gridx_functions.py`; `gridx_functions_sql.py`; `ScalaApiExamples.scala`; `gridx-functions.mdx` (convert 6 blocks to `FunctionExamples`); regenerate `function-info.json`.

**Interfaces:** consumes T0 probe facts (per-fn form/args/output) + T1 fixtures. Produces the 6 functions' 4-tab example fns + outputs.

- [ ] **Step 1:** Write light example tests for the 6 using the BNG fixtures + T0's real outputs. Assert REAL values: `bng_cellarea` → the real sq-km number; `bng_aswkt` → the real WKT (short → full); `bng_aswkb`/`bng_centroid` → non-empty WKB (`... (WKB binary)`); `bng_eastnorthasbng`/`bng_pointascell` → the real cell-id string.
- [ ] **Step 2:** `--path api/ --filter 'bng_aswkb or bng_aswkt or bng_cellarea or bng_centroid or bng_eastnorthasbng or bng_pointascell'` → confirm FAIL (examples absent).
- [ ] **Step 3:** Author the 4-tab examples — ONE shared example per fn; SQL tab DIRECT (no wrap); BNG e/n (EPSG:27700) + valid resolution per CLAUDE.md; output-representation convention.
- [ ] **Step 4:** Regenerate `function-info.json`; convert the 6 mdx blocks to `<FunctionExamples>`.
- [ ] **Step 5:** filtered `--path api/` green (ANSI-strip + reconcile vs T0 baseline); **Step 6:** `gbx:test:docs-examples` + black + `gbx:docs:build`; **Step 7:** commit (scoped).

### Task 3: BNG relations/distance (`bng_distance`, `bng_euclideandistance`, `bng_cellintersection`, `bng_cellunion`)

Same 7-step shape as Task 2, `--filter 'bng_distance or bng_euclideandistance or bng_cellintersection or bng_cellunion'`. Real outputs: distances → real numbers (note units from the probe); cellintersection/cellunion → cell-id(s)/array or WKB per the probe. SQL tab DIRECT, no wrap.

### Task 4: BNG neighborhood/fill (`bng_kring`, `bng_kloop`, `bng_geomkring`, `bng_geomkloop`, `bng_polyfill`, `bng_tessellate`)

Same shape, `--filter 'bng_kring or bng_kloop or bng_geomkring or bng_geomkloop or bng_polyfill or bng_tessellate'`. These return arrays of cell IDs (or WKB / tiles); probe the invocation form (scalar-array vs UDTF) and label any heavy-scalar-array vs light-UDTF divergence (two `;`-separated SQL, heavy first, + `_sql_variant`) per `rasterx-tier-divergence-doc-treatment`. Array output shown per the convention (a few cells then `...`).

### Task 5: BNG aggregators + explode UDTFs (`bng_cellintersection_agg`, `bng_cellunion_agg`, `bng_geomkloopexplode`, `bng_geomkringexplode`, `bng_kloopexplode`, `bng_kringexplode`, `bng_tessellateexplode`)

Same shape, `--filter 'bng_cellintersection_agg or bng_cellunion_agg or explode'`. Aggregators: `.groupBy().agg()` form; grouped-agg return-type divergence if any (BINARY-in-light-SQL vs struct — check per `light-agg-struct-return-convention`). **The 5 `*explode` are SQL-LATERAL-only (permanent `[B]` waiver): author ONLY the SQL tab (`SELECT t.* FROM v, LATERAL gbx_bng_Xexplode(...) t`) and the Python-light tab as the same `spark.sql(...)` LATERAL call; heavy-Python + Scala Column tabs are legitimately absent** (function-info `bindings` reflects this; the tab renders "not available in this tier"). Do NOT fabricate a Column form for them.

### Task 6: quadbin (all 10)

Same shape (may split into 6a/6b if a single dispatch is too large — implementer's call, but keep sequential on the shared files). `--filter 'quadbin_'`. `quadbin_pointascell` uses WGS84 lon/lat + resolution 0–26; `quadbin_resolution` → int; `quadbin_aswkb`/`centroid` → WKB; `quadbin_kring`/`polyfill` → cell arrays (probe form); `quadbin_tessellate` → cells/tiles; `quadbin_cellunion(_agg)` → geometry/agg. SQL tab DIRECT. Label divergences.

### Task 7: custom (all 7)

Same shape, `--filter 'custom_'`. `custom_grid`/`custom_pointascell` take grid-definition params (use the probe's real args); `custom_cellaswkb`/`cellaswkt`/`centroid` → WKB/WKT/point; `custom_polyfill`/`kring` → cell arrays. SQL tab DIRECT. Note the custom-grid-param fixture.

### Task 8: Finalize — convert residuals, alphabetize, ONE full verify

**Files:** `gridx-functions.mdx`, `function-info.json`.

- [ ] **Step 1:** `grep -nE "CodeFromTest" docs/docs/api/gridx-functions.mdx` → convert any residual per-function blocks (should be 0 after T2–T7).
- [ ] **Step 2:** Alphabetize `###` headings WITHIN each family section (pure reorder; heading set unchanged; non-function sections untouched).
- [ ] **Step 3:** Regenerate `function-info.json`; run `gbx:test:python-docs --path api/ --filter 'bng_ or quadbin_ or custom_'` (ALL 40 GridX functions — the affected set, NOT the full ~1148-test suite) — GREEN vs the T0 GridX baseline. ANSI-strip + reconcile. The `docs-examples` guard (next step) is the cross-package regression net, so no full-suite pytest run is needed.
- [ ] **Step 4:** `gbx:test:docs-examples` + `gbx:docs:build` clean.
- [ ] **Step 5:** Commit (scoped: mdx + function-info.json).

---

## Self-Review

**Spec coverage:** All 40 `gbx_{bng,quadbin,custom}_*` functions are assigned (T2 6 + T3 4 + T4 6 + T5 7 + T6 10 + T7 7 = 40). T0 probe front-loads invocation forms (the VectorX rework-avoider). T1 foundation + T8 finalize mirror VectorX. Mechanism (`FunctionExamples`/`function-info`/`--filter`) reused unchanged.

**Placeholder scan:** Per-function example *values* come from T0's live probe against real fixtures (the doc-tests-are-the-source contract), not pre-baked — each task specifies files, recipe, fixture, divergence treatment, and exact `--filter` verification.

**Type/name consistency:** Naming follows the generator's scan convention (`<base>_python_light_example` etc.; `<base>` = spark name minus `gbx_` prefix). Fixture loaders from T1 are consumed by T2–T7. Verification (`--filter` + guard) identical across tasks.

**Known risks carried from VectorX:** (1) the recurring SQL-wrap bug — pinned as a Global Constraint + every task's SQL step; (2) invocation-form surprises (scalar vs UDTF vs CollectionGenerator) — pinned as T0 probe outputs; (3) BNG domain rules (EPSG:27700, resolution ±1..±6, cellarea sq-km) — pinned as a Global Constraint; (4) the 5 explode UDTFs are SQL-only — pinned in T5 + Global Constraints.
