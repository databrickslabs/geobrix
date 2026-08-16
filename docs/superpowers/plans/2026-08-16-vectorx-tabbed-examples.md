# VectorX Tabbed Function Examples — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every VectorX function in `docs/docs/api/vectorx-functions.mdx` the same per-function 4-tab code examples (SQL / Python-light / Python-heavy / Scala) that RasterX already has, backed by real executable doc-tests.

**Architecture:** Reuse the SHIPPED RasterX rails unchanged — the `FunctionExamples` MDX component (reads `function-info.json` `bindings[]` to decide which tabs render), the per-tier doc-test example convention (`<base>_python_light_example` / `_python_heavy_example` / `_scala_example` / `_sql_example` + each `<base>_..._example_output`), and the Setup+Conventions page template. Author one shared example per function across all four tabs; add labeled variations only where a tier genuinely diverges. No mechanism changes.

**Tech Stack:** Docusaurus (`Tabs`/`TabItem`, `FunctionExamples.js`), Python doc-tests under `docs/tests/python/api/` run in the `geobrix-dev` Docker container via `gbx:test:python-docs`, Scala string-constant examples in `docs/tests/scala/api/ScalaApiExamples.scala`, `docs/scripts/generate-function-info.py` → `function-info.json`.

**Spec / authoritative standard (the plan argues from these — executors read them):**
- `docs/tests/python/api/README-tabbed-examples.md` — the checked-in authoring standard.
- `docs/superpowers/specs/2026-08-10-rasterx-tabbed-examples-corrected-design.md` — the corrected design.
- Tier-divergence rules (memory `rasterx-tier-divergence-doc-treatment`): ONE example when tiers agree; TWO labeled variations when they diverge — never fabricate or hide one.
- Guards (memory `docs-example-guards`): `gbx:test:docs-examples` + QC `docs-examples` check.

## Global Constraints

- **Fixtures MUST be real committed data**, never the `/Volumes` placeholders (all-NoData/flat → degenerate examples + false failures). Commit vector fixtures under `src/test/resources/binary/` if a suitable one is absent. Verify fixture data is non-degenerate before authoring.
- **ONE example per function, shared across all present tabs** (same fixture + operation + args), expressed in each tier's language. Never wrap a function to change its result type to manufacture a clean scalar.
- **`transformcrs` example coordinates MUST be inside the target CRS `area_of_use`** — the heavy+light domain check (`2f788d9c`) NULLs out-of-domain input. Mirror `docs/tests/python/api/../pyvx/test_crs_parity.py` (e.g. POINT(15 51) in EPSG:32633).
- **CRS-family SQL returns BINARY** (memory `vectorx-crs-family-decisions`); geometry output shown as `...` + `(WKB binary)` shorthand + note.
- **Verification is `gbx:test:python-docs --path api/` (WHOLE dir)** — a single file fails on `_fixtures` sibling import. Extract failures from the ANSI-stripped short summary and reconcile the count: `sed '/short test summary/,$p' <log> | sed 's/\x1b\[[0-9;]*m//g' | grep '^FAILED'`. NEVER trust inline-progress grep.
- **Port 3000 is the user's.** Do NOT restart it. Verify via the doc-test suite + `gbx:docs:build` (static). If a live preview is needed, use `--port 3001`.
- **Never edit the user's uncommitted hand-edits.** `git status` before/after each task; scoped `git add` of only this task's files; verify commit scope is clean.
- **Never run two agents on the same file concurrently.** Author families sequentially where they share `vectorx_functions.py` / `ScalaApiExamples.scala` / the mdx.
- Docs Python is NOT covered by `gbx:lint:python`; still black-format touched files in Docker (CI black 26.3.1) to keep source clean.
- Commit subjects ≤72 chars + WHY body + `Co-authored-by: Isaac`.

---

## The 9 VectorX functions, grouped for authoring

| Family | Functions | Fixture | Notes |
|---|---|---|---|
| CRS | `st_crs`, `st_setcrs`, `st_transformcrs` | a WKT/WKB geometry with an SRID | in-domain coords; SQL returns BINARY; `st_crs` is an accessor (string) |
| MVT | `st_asmvt`, `st_asmvt_pyramid` | tile-local polygon in `[0,extent]` | tile-local contract (memory `mvt-tile-local-contract`); returns MVT BINARY |
| TIN / elevation | `st_triangulate`, `st_interpolateelevationbbox`, `st_interpolateelevationgeom` | `src/test/resources/binary/elevation/sd46_dtm_point.shp` (+ `sd46_dtm_breakline.shp`) | `conforming` triangulation is heavy-only → divergence note |
| Legacy | `st_legacyaswkb` | a legacy-Mosaic-encoded geometry | migration to WKB |

## File Structure

- **Create** `docs/tests/python/api/vectorx_functions_python_light.py` — the `pyvx` per-function light examples (`<base>_python_light_example` + `_output`). One file (VectorX is small; no per-family split needed).
- **Create** `docs/tests/python/api/test_vectorx_functions_python_light.py` — executes each light example, asserts real output.
- **Modify** `docs/tests/python/api/vectorx_functions.py` — add `<base>_python_heavy_example` + `_output` per function (verify in T0 whether this file is currently per-fn or setup-only).
- **Modify** `docs/tests/python/api/vectorx_functions_sql.py` — ensure every function has a real `<base>_sql_example()` + `_output` (SQL source exists; confirm coverage in T0).
- **Modify** `docs/tests/scala/api/ScalaApiExamples.scala` — add `val <base>_scala_example` per function (string constants; compile+non-empty only).
- **Modify** `docs/docs/api/vectorx-functions.mdx` — add Setup+Conventions; convert each `CodeFromTest` block to `<FunctionExamples .../>`; alphabetize headings within families.
- **Modify** `docs/tests/python/api/_fixtures.py` — add VectorX fixture loaders if absent (geometry/point-set/tile-local-polygon builders); commit any new binary fixture under `src/test/resources/binary/`.
- **Regenerate** `src/main/resources/com/databricks/labs/gbx/function-info.json` (Docker) after each family so `bindings[]` picks up new tabs.

---

### Task 0: Baseline + probe all 9 functions (foundation — no authoring)

**Files:** none modified (produces a probe-facts note for later tasks; may write scratch under `prompts/documentation/`).

**Interfaces:**
- Produces: for each of the 9 functions — its invocation form per tier (scalar `df.select(...)` vs UDTF `SELECT t.* FROM v, LATERAL gbx_fn(...) t`), return type (BINARY / string / tile / array), a real in-bounds fixture + args, and the real output value; plus the current `--path api/` VectorX pass/fail baseline.

- [ ] **Step 1: Establish the current VectorX doc-test baseline.** Run `bash scripts/commands/gbx-test-python-docs.sh --path api/ --skip-build --log vx-baseline.log`, then extract failures ANSI-stripped: `sed '/short test summary/,$p' test-logs/vx-baseline.log | sed 's/\x1b\[[0-9;]*m//g' | grep '^FAILED'`. Record which (if any) `st_*` tests fail today. Expected: the ledger's 2 `st_transformcrs` SQL failures are likely resolved by this session's CRS work (`2f788d9c`/`e4553a52`/`5ea462d4`) — CONFIRM.
- [ ] **Step 2: Inspect the existing example sources.** Read `docs/tests/python/api/vectorx_functions_sql.py` and `vectorx_functions.py`: does each of the 9 functions already have a `*_sql_example()` / `*_python_heavy_example()`, or are these setup-only? Record per-function coverage.
- [ ] **Step 3: Probe each function live** (in-container, delete probes after). For each of the 9: confirm the light `pyvx` invocation form (Column vs UDTF/`NotImplementedError`), the heavy shim + Scala invocation, the return type, a real fixture + args that produce non-degenerate output, and capture the real output value. For `st_transformcrs` use in-`area_of_use` coords. For MVT confirm tile-local input. For TIN use the committed point/breakline shapefiles (or a committed point set). **Trust a full `--path api/` run over standalone `--skip-build` probes where they disagree.**
- [ ] **Step 4: Decide divergences.** For each function record whether tiers agree (one example) or diverge (label the variations): e.g. `st_triangulate` `conforming` mode heavy-only (→ `:::note`), any CRS-family BINARY-vs-other, any UDTF invocation-form split (→ two `;`-separated SQL, heavy first, + `_sql_variant`).
- [ ] **Step 5: Record the facts** in `prompts/documentation/2026-08-16-vectorx-tabbing-probe.md` (gitignored scratch) so T1–T5 consume verified ground truth. No commit (scratch).

### Task 1: Setup + Conventions section + fixtures

**Files:**
- Modify: `docs/docs/api/vectorx-functions.mdx` (add Setup + Conventions after the tier-availability intro, before the first family)
- Modify: `docs/tests/python/api/_fixtures.py` (add VectorX fixture loaders)
- Create (if needed): a committed vector fixture under `src/test/resources/binary/` (only if T0 found none suitable)
- Test: `docs/tests/python/api/test_fixtures_helpers.py` (assert the new loaders return non-degenerate data)

**Interfaces:**
- Produces: fixture loaders (e.g. `vector_geom_df(spark)`, `point_set_df(spark)`, `mvt_polygon_df(spark)` + heavy equivalents) returning DataFrames with the conventional column names the examples use; the Conventions section defining canonical fixtures + how to read the 4 tabs + the output/`(WKB binary)` convention + the single clickable "v2 Tile"/CRS pointer.

- [ ] **Step 1:** Write/extend fixture-helper tests asserting each new loader returns real, non-degenerate geometry/point data (row count > 0, geometries valid, elevations non-flat for the point set).
- [ ] **Step 2:** Run `gbx:test:python-docs --path api/ --skip-build --log vx-fixtures.log`; confirm the new fixture tests FAIL (loaders absent).
- [ ] **Step 3:** Implement the loaders in `_fixtures.py` reusing committed fixtures (point/breakline shapefiles; a small in-line WKB geometry for CRS/legacy; a tile-local polygon for MVT). Commit a new binary fixture under `src/test/resources/binary/` only if none fits.
- [ ] **Step 4:** Add the Setup + Conventions section to `vectorx-functions.mdx`, copying the RasterX template shape (placeholder→view→backs table; "assumes library installed" linking installation.mdx; tab-reading + output conventions; CRS-family BINARY note).
- [ ] **Step 5:** Run `gbx:test:python-docs --path api/ --skip-build --log vx-fixtures2.log`; ANSI-strip + reconcile; fixture tests PASS, no new failures vs the T0 baseline.
- [ ] **Step 6:** `gbx:docs:build` (static) to confirm the Conventions MDX renders; do NOT touch port 3000.
- [ ] **Step 7:** Commit (scoped): `_fixtures.py` + test + mdx (+ any committed fixture). `git status` clean of user hand-edits.

### Task 2: CRS family — `st_crs`, `st_setcrs`, `st_transformcrs`

**Files:**
- Create/append: `vectorx_functions_python_light.py` (3 light examples + outputs), `test_vectorx_functions_python_light.py`
- Modify: `vectorx_functions.py` (3 heavy examples), `vectorx_functions_sql.py` (confirm/author 3 SQL), `ScalaApiExamples.scala` (3 Scala), `vectorx-functions.mdx` (3 `FunctionExamples` blocks), `function-info.json` (regen)

**Interfaces:**
- Consumes: T0 probe facts (invocation form, in-domain coords, BINARY return); T1 fixture loaders.
- Produces: `st_crs_*`, `st_setcrs_*`, `st_transformcrs_*` example fns + outputs across all present tiers.

- [ ] **Step 1:** Write the light example test(s) for the 3 CRS functions using the in-domain fixture; assert the real output (string SRID/CRS for `st_crs`; BINARY WKB for `st_setcrs`/`st_transformcrs`, in-domain → non-null).
- [ ] **Step 2:** Run `--path api/` ; confirm the new CRS tests FAIL (examples absent).
- [ ] **Step 3:** Author the 4-tab examples (light/heavy/Scala/SQL) — ONE shared example per function per the canonical standard; in-domain coords; `(WKB binary)` output shorthand + note; SQL returns BINARY. Add a `:::note` for any real tier divergence found in T0.
- [ ] **Step 4:** Regenerate `function-info.json` (Docker) so `bindings[]` includes the new tabs; convert the 3 mdx blocks to `<FunctionExamples name="st_..." .../>`.
- [ ] **Step 5:** Run `--path api/`; ANSI-strip + reconcile; all 3 CRS functions PASS across authored tiers, zero new failures.
- [ ] **Step 6:** `gbx:test:docs-examples` (guard: ASCII tables + tab completeness + annotation consistency) passes. black-format touched files in Docker. `gbx:docs:build` renders.
- [ ] **Step 7:** Commit (scoped, all CRS-family files + regenerated function-info).

### Task 3: MVT family — `st_asmvt`, `st_asmvt_pyramid`

**Files:** append to the same example/test/mdx/scala/sql/function-info files (author sequentially — same shared files as T2).

- [ ] **Step 1:** Light example test for both MVT functions using the tile-local `[0,extent]` polygon fixture; assert non-empty MVT BINARY.
- [ ] **Step 2:** Run `--path api/`; confirm FAIL.
- [ ] **Step 3:** Author 4-tab examples; MVT output = BINARY (`...` + `(MVT binary)` note); tile-local input per the contract; note `st_asmvt_pyramid`'s zoom args.
- [ ] **Step 4:** Regenerate function-info; convert the 2 mdx blocks to `FunctionExamples`.
- [ ] **Step 5:** `--path api/` green (ANSI-strip + reconcile); **Step 6:** docs-examples guard + black + build; **Step 7:** commit (scoped).

### Task 4: TIN / elevation — `st_triangulate`, `st_interpolateelevationbbox`, `st_interpolateelevationgeom`

**Files:** same shared example/test/mdx/scala/sql/function-info files.

- [ ] **Step 1:** Light example tests using the committed point (+breakline) fixture; assert non-degenerate result (triangulate → geometry/rows; interpolate → real elevations). Probe invocation form in T0 (scalar vs UDTF) and use the correct form.
- [ ] **Step 2:** Run `--path api/`; confirm FAIL.
- [ ] **Step 3:** Author 4-tab examples; add the `:::note Lightweight-only`/divergence callout for `conforming` triangulation being heavy-only (per T0); ensure the SAME point set + args across tabs.
- [ ] **Step 4:** Regenerate function-info; convert the 3 mdx blocks.
- [ ] **Step 5:** `--path api/` green; **Step 6:** guard + black + build; **Step 7:** commit (scoped).

### Task 5: Legacy — `st_legacyaswkb`

**Files:** same shared files.

- [ ] **Step 1:** Light example test converting a legacy-encoded geometry → WKB; assert real WKB bytes.
- [ ] **Step 2:** Run `--path api/`; confirm FAIL.
- [ ] **Step 3:** Author 4-tab example (note the legacy-input fixture).
- [ ] **Step 4:** Regenerate function-info; convert the mdx block.
- [ ] **Step 5:** `--path api/` green; **Step 6:** guard + black + build; **Step 7:** commit (scoped).

### Task 6: Finalize — convert residual blocks, alphabetize, full verify

**Files:** `vectorx-functions.mdx` (+ any residual `CodeFromTest`→`FunctionExamples`), `function-info.json`.

- [ ] **Step 1:** Grep `vectorx-functions.mdx` for any remaining `CodeFromTest` (should be 0 after T2–T5); convert stragglers.
- [ ] **Step 2:** Alphabetize `###` headings WITHIN each family section (script it, dry-run diff = pure reorder, heading set unchanged).
- [ ] **Step 3:** Regenerate function-info; run the FULL `gbx:test:python-docs --path api/` (no --skip-build once, to catch build-coupled issues); ANSI-strip + reconcile — target: all 9 `st_*` functions pass across authored tiers, zero new failures vs the T0 baseline.
- [ ] **Step 4:** `gbx:test:docs-examples` + `gbx:docs:build` clean.
- [ ] **Step 5:** Optionally run `gbx:test:pyvx` on the affected `python/geobrix/test/pyvx/**` (memory `docs-cycle-verification-discipline`: docs-tests exercise the example surface, not the committed unit tests).
- [ ] **Step 6:** Commit (scoped: mdx + function-info).

---

## Self-Review

**Spec coverage:** All 9 `gbx_st_*` functions are assigned to a family task (T2 CRS ×3, T3 MVT ×2, T4 TIN/elevation ×3, T5 legacy ×1 = 9). Setup+Conventions (T1) and finalize/alphabetize (T6) mirror the RasterX template. The `FunctionExamples`/`function-info` mechanism is reused unchanged (no mechanism task needed — it shipped with RasterX).

**Placeholder scan:** Per-function example *values* (outputs, exact coords) are intentionally produced by the T0 live probe + authored against real fixtures, not pre-baked — this is the doc-tests-are-the-source contract, not a placeholder. Every task specifies exact files, the authoring recipe, the fixture, the divergence treatment, and exact verification commands.

**Type/name consistency:** Example naming follows the generator's exact scan convention (`<base>_python_light_example` etc., `<base> = spark name minus gbx_ prefix`). Fixture loader names introduced in T1 are consumed by T2–T5. Verification command (`--path api/` whole-dir + ANSI-stripped reconcile) is identical across tasks.

**Known risk carried from RasterX:** invocation-form surprises (scalar vs UDTF) and out-of-domain CRS coords — both are pinned as explicit T0 probe outputs feeding the authoring tasks, which is the mitigation for the late-probing rework that cost RasterX.
