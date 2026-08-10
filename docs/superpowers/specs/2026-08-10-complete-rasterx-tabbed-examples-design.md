# Complete RasterX Tabbed Function Examples — design

**Date:** 2026-08-10
**Status:** ratified decisions + default calls (this doc), pending user review → plan
**Scope:** docs only — finish tabbing the RasterX API page. Follow-on to the shipped
tabbed-docs mechanism (`2026-08-09-tabbed-function-docs-design.md` / `-plan.md`, both DONE +
APPROVE-FOR-MERGE). GridX/VectorX are separate later passes.

## Goal

Render every RasterX function's example on `docs/docs/api/raster-functions.mdx` as the
4-tab block (SQL default / Python-light / Python-heavy blue / Scala blue) the shipped
mechanism provides — by authoring real executable per-tier example doc-tests for the ~125
remaining RasterX functions (130 total − 5 proof-subset done) and wiring each MDX block to
`<FunctionExamples>`. The mechanism (`FunctionExamples` wrapper, `generate-function-info.py`
bindings text-scan, `gbx-example-lang` CSS) is built and reused AS-IS — no mechanism changes.

## The simplifying finding (tier-availability matrix)

A per-function tier-availability audit (all 130 RasterX `gbx_rst_*`/`gbx_h3_*` names ×
4 surfaces) collapsed the hard part:

- **SQL: 130/130** (already populated by the generator — net-zero work; it's the default tab).
- **python-light: 130/130** — every function has a light binding. BUT **33 are UDTF-only**
  (no Column-returning form; grid tessellation/aggregators, tile generators, one vector) —
  their light form is a table function, not a Column call.
- **python-heavy: 130/130** — every function has a heavy shim binding.
- **scala: 129/130** — the ONLY genuine gap is `rst_fromfile` (executor JVM can't read the
  `/Volumes` FUSE mount; documented in `functions.scala`, issue #34). Its Scala tab shows the
  data-driven "Not available in this tier" note (already the case — `rst_fromfile` is one of
  the 5 done).

So "author only real tiers" is ≈ "author all four for everyone," with exactly one Scala gap
and the 33 UDTF-only light forms as the only real nuance. Only the **3 non-SQL tiers** per
function are net-new. The matrix is CONFIRMED per-row against `pyrx/functions.py`,
`rasterx/functions.py`, and `rasterx/functions.scala`; it is the plan's authoring reference so
implementers never guess a tier.

## Locked decisions

- **Idiomatic-per-language tabs; SQL stays in the SQL tab (user ruling).** The Python tabs
  contain Python, the Scala tab contains Scala. Raw SQL never appears as the default content of
  a non-SQL tab.
- **UDTF-only light form → Python-native invocation (decision 3).** For the 33 UDTF-only
  functions, the Python-light example invokes the light UDTF via the **DataFrame-native Python
  API** (the registered `@udtf` called so it returns a DataFrame/rows), NOT a Column call it
  doesn't have. `spark.sql(... LATERAL ...)` is the fallback ONLY where no DataFrame-native
  Python form exists for that UDTF — never raw SQL as the default Python content. The plan
  verifies the native form per UDTF family before authoring.
- **Doc-tests are the documentation source.** Every example is REAL, executes with REAL
  assertions against real sample data (`/Volumes/main/geobrix_samples`), run via `gbx:test:*-docs`
  in Docker. No mocking, no stubs, no structure-only tests.
- **Never fabricate an example for a nonexistent binding.** The one Scala gap (`rst_fromfile`)
  renders the note; if authoring reveals any OTHER genuinely-missing binding not in the matrix,
  surface it as a finding — don't invent an example.

## Code-indicators compatibility (user requirement)

The docs have an opt-in **code-indicators** overlay (`CodeIndicatorToggle`, default off,
`localStorage.showCodeIndicators`; `CodeFromTest.getIndicatorConfig()`). When on, each example
renders a validation badge. The **green "Fully Validated: tested at compile-time" 🔗 badge**
(the "checkmark") is shown when `effectiveValidationLevel === "tested"`, which
`CodeFromTest` derives (lines ~349-366) as: has a `source`/`testFile`/`functionName` prop AND
the path is NOT under `integration/` or `tests-dbr/` (those downgrade to 🧪 Integration /
⚡ Databricks-Required). `rst_fromfile` (already tabbed) shows the green badge — the reference.

**Requirement, bakeable into authoring rules:** every authored tab must earn the green
checkmark, matching `rst_fromfile`. Concretely:
1. `FunctionExamples` already passes `source` + `testFile` + `functionName` per tab (that's why
   `rst_fromfile` is compatible) — the plan reuses it unchanged, so each present tab is `tested`.
2. **All per-tier example doc-test files MUST live under `docs/tests/...`, NOT under any
   `integration/` or `tests-dbr/` path** — otherwise the badge downgrades off the green
   checkmark. The plan asserts this for every new example file/path.
3. Absent-binding tabs (the note) correctly show NO badge — that's fine; the requirement is that
   every PRESENT example earns the checkmark.
Acceptance includes toggling code-indicators on in the visual spot-check and confirming the
green 🔗 badge on all present tabs of a converted function.

## Batching strategy — by function family

One implementer subagent per family; each batch shares fixtures + is independently
testable/reviewable. Families (net-new counts approximate, per the matrix):

| Batch | Family | ~count | Notes |
|---|---|---|---|
| A | Accessors | ~20 | width/height/bandcount/srid/statistics/metadata — full-stack, simplest; validate the rhythm first |
| B | Tile operations | ~25 | clip/retile/reproject/resample/filters |
| C | Band math & indices | ~9 | mapalgebra/derivedband/combineavg/NDVI/EVI |
| D | Aggregators | ~10 | `*_agg` — GROUP BY shape |
| E | Terrain | ~7 | slope/aspect/hillshade/curvature |
| F | Coordinate transforms | ~6 | raster↔world |
| G | Generators/UDTFs | ~33 | the decision-3 family — light = Python-native UDTF invocation; largest + most nuanced, LAST |

`rst_avg`/`rst_boundingbox`/`rst_numbands`/`rst_width`/`rst_fromfile` already done. Batch G is
deliberately last so the earlier full-stack batches validate the fixture/authoring rhythm before
the UDTF-invocation nuance.

## Per-function authoring contract

For each function, author the net-new tiers matching the generator's scan naming EXACTLY:
- `def <base>_python_light_example(spark)` in a per-family light file (e.g.
  `rasterx_accessors_python_light.py`), idiomatic pyrx (DataFrame-native UDTF call for UDTF-only).
- `def <base>_python_heavy_example(spark)` appended to the heavy shim example file.
- `val <base>_scala_example` (+ `_output`) in `ScalaApiExamples.scala` (skip only `rst_fromfile`).
- Each with a `<base>_..._example_output` constant; each asserts a REAL value.
- **Shared fixtures per family:** one setup helper + sample-raster selection reused across the
  family's examples (mirrors the existing `*_sql_example` harness) — not N independent spark setups.

MDX: replace each function's single `<CodeFromTest language="sql">` with
`<FunctionExamples name="rst_X" .../>`, reusing the shipped props (sql/pythonLight/pythonHeavy/
scala + `*Source` paths). All example files under `docs/tests/` (not integration/tests-dbr) so
the checkmark holds.

## Fixture strategy (default call — the one open design choice)

**Per-family fixtures** (recommended): each family selects the sample raster that best
demonstrates it (terrain → a DEM; band-math → multiband; h3-tessellation → something griddable),
mirroring how the existing SQL examples already vary their inputs. Alternative — one shared raster
across all families — is simpler but produces worse examples (a slope example on a flat 4×3 test
raster is uninformative). Recommendation stands unless the user prefers the single-raster route.

## Verification cadence (per batch)

1. `gbx:test:python-docs --suite api` + `gbx:test:scala-docs` (Docker) — the batch's new examples
   pass with real assertions.
2. `gbx:docs:function-info` regenerates → bindings check confirms the batch's functions now show
   their authored tiers (gap → note).
3. `gbx:lint:python --check` (Docker) on the batch's files.
4. ONE `gbx:docs:build` at each batch's end (self-cleans `.docusaurus`; stops port 3000 → the
   orchestrator restarts the user's 3000 server after each batch, not per function) + a non-3000
   visual spot-check of one converted function WITH code-indicators toggled on (confirm green 🔗
   on all present tabs).
5. Task review per batch; final whole-branch review after all batches.

## Testing philosophy

Doc-tests ARE the verification (repo doctrine). Real code, real assertions, real sample data, no
mocking. Cross-tier consistency (light vs heavy `rst_avg` agree) is a soft expectation; per-tier
correctness + the bindings regeneration + the clean build are the hard gates. The family reviewer
checks examples are real (not stubs), idiomatic-per-language (no SQL bleeding into Python/Scala
tabs), and checkmark-compatible (non-integration path).

## Out of scope

- The mechanism itself (`FunctionExamples`, `generate-function-info.py`, `gbx-example-lang` CSS —
  done, reuse as-is, do not modify).
- GridX and VectorX pages (their own follow-on passes after RasterX is complete).
- Any product/binding code change (docs-only; a genuinely-missing binding is a surfaced finding,
  never a fabricated example).
- The right-sidebar/DESCRIBE FUNCTION surface (unchanged).

## Known cost

Large grind: ~125 functions × up to 3 net-new tiers = a few hundred real doc-tests across 7
family batches. Mechanical-but-careful, low-risk, fully reviewable per batch. Realistically
several subagent cycles.

## Outcome

The RasterX API page renders every function as a 4-tab example (SQL default / Python-light /
Python-heavy blue / Scala blue), all sourced from executable doc-tests, every present tab
earning the green code-indicators checkmark, the one genuine Scala gap (`rst_fromfile`) showing
the data-driven note. RasterX becomes the first fully-tabbed package; GridX and VectorX follow
the same batched pattern later, reusing the same mechanism.
