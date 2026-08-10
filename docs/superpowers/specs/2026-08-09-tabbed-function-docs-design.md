# Per-function tabbed code examples — design

**Date:** 2026-08-09
**Status:** ratified decisions + default calls (this doc), pending user review → plan
**Scope:** docs only — the per-function example blocks in `docs/docs/api/*-functions.mdx`,
their source doc-tests under `docs/tests/`, the `generate-function-info.py` pipeline, and the
`CodeFromTest` render component. No product (Scala/Python binding) code changes.

## Goal

Render each function's example as **tabbed code**, one tab per binding surface, side by side —
so a reader sees the SQL, light-Python, heavy-Python, and Scala form of the same operation in
one place and can switch between them. Today each function shows a single SQL example
(`<CodeFromTest language="sql" .../>`); this makes that one-of-four the default tab of a four-tab
block.

## Locked decisions (from the user, not open)

- **Tab order, fixed:** **SQL** → **Python (light)** → **Python (heavy)** → **Scala (heavy)**.
- **SQL is the default tab** (first, pre-selected) AND the default surface shown in the
  right-sidebar for functions.
- **The two heavy tabs get the blue background**, reusing the EXISTING tier CSS tokens (do not
  invent colors): active-tab `#2545b3`, content panel tint `#f5f8ff` / border-left
  `3px solid #2545b3`, with the existing dark-mode variant (`#6b8cff` /
  `rgba(107,140,255,0.08)`). SQL and Python-light tabs stay plain. Net 4-tab visual: SQL +
  Python(light) plain; Python(heavy) + Scala(heavy) blue-tinted.
- **Tests are the documentation source** (repo doctrine): every tab's example comes from an
  executable doc-test, never hand-authored MDX.

## Default calls (mine — the review gate; override any of these)

- **Authoring model → extend the single-source pipeline, per-tier example functions.** Add
  per-tier doc-test conventions (`*_python_light_example()`, `*_python_heavy_example()`,
  `*_scala_example()`) alongside the existing `*_sql_example()`, extend
  `generate-function-info.py` to discover them, and have the renderer pull each tab from its
  tier's example. Rationale: don't reinvent the rails — the SQL pipeline already works this way
  and is 100% populated; this is the smallest-surface extension, and it keeps "tests are the
  doc source" true for all four tabs. (Rejected alternative: one multi-tier doc-test per
  function that emits all four — more magic in one function, harder to run/verify per tier.)
- **Absent bindings → render a short "Not available in this tier" note in the tab, don't drop
  the tab.** The tab order stays fixed and visible; a legitimately-absent binding shows a
  one-line note instead of code. Rationale: a fixed tab set is more legible than tabs that
  appear/disappear per function, and a visible "absent" note dovetails with binding-parity —
  it makes a missing binding obvious in the rendered docs, which is a feature, not a bug.
- **Absence is data-driven, not renderer-guessed.** Extend `function-info.json` (during
  generation) with a per-function `bindings` set (which of sql/python-light/python-heavy/scala
  have an example) so the renderer decides deterministically which tabs show code vs. the note.
  Rationale: the renderer must not infer absence from a failed import or a missing file — that
  silently hides real gaps; a generated metadata field is explicit and testable.

## Constraints & context (verified by recon 2026-08-09; shape, not line numbers)

- **The tab framework + blue CSS already exist.** Docusaurus `Tabs`/`TabItem` with
  `groupId="gbx-tier"` and the `.gbx-tier-tabs` rules in `docs/src/css/custom.css` already color
  the heavy tab `#2545b3` and tint the panel `#f5f8ff` (+ dark-mode `:has()` variant). Existing
  2-tab lightweight/heavyweight usage: `quick-start.mdx`, `installation.mdx`,
  `raster-functions.mdx` (with the `<Tier both/>` badge from `docs/src/components/Tier.js`).
  The 4-tab scheme reuses these tokens, targeting the 3rd+4th tab children.
- **Render mechanism today:** `api/*-functions.mdx` renders ONE example per function via a
  `<CodeFromTest language="sql" source=... functionName=... code={...} />` component
  (`docs/src/components/CodeFromTest.js`) — it takes raw-loaded doc-test source + a `functionName`
  prop, extracts that function's `return """..."""` body, and renders a Docusaurus `<CodeBlock>`.
- **SQL example pipeline is 100% populated:** `*_sql_example()` functions in
  `docs/tests/python/api/{rasterx,gridx,vectorx,pmtiles}_functions_sql.py` →
  `generate-function-info.py` (discovers only `*_sql_example()` today; extracts the first
  `SELECT` containing the function prefix) → `src/main/resources/.../function-info.json`
  (keys `examples` + `usage_args` only — **no tier/binding metadata**). Generation is fail-loud
  if a registered function lacks a SQL example.
- **Per-tier example functions do NOT exist yet.** Light-Python (`pyrx_functions.py`),
  heavy-Python (`rasterx_functions.py`), and Scala (`ScalaApiExamples.scala`) doc-tests have
  only module-level setup examples, not per-function snippets. This is the bulk of the work.
- **Legitimately-absent bindings are real** and the design must handle them: `rst_polygonize`
  light = UDTF/LATERAL-only (no Column form); `rst_fromfile` light-only (executor JVM can't read
  `/Volumes` FUSE); OGR readers + conforming-triangulation heavy-only; DESCRIBE FUNCTION
  heavy-only; `bng *explode` fns SQL-LATERAL-only.

## Components

### 1. Per-tier doc-test conventions (the new example sources)
- Establish `*_python_light_example()`, `*_python_heavy_example()`, `*_scala_example()` naming,
  mirroring the existing `*_sql_example()`. Light/heavy Python live in
  `docs/tests/python/api/`; Scala in `docs/tests/scala/api/`.
- Like the SQL examples, these are REAL executable doc-tests with assertions against real sample
  data — not illustrative snippets. (Whether every function needs all four immediately, or the
  feature ships incrementally per package/family with the note filling gaps, is a plan-sizing
  question — see Rollout.)

### 2. `generate-function-info.py` — discover per-tier examples + emit binding metadata
- Extend the module/discovery list to find the three new conventions in addition to
  `*_sql_example()`.
- For each registered function, emit a `bindings` field recording which tiers produced an
  example. This is the single source of truth the renderer reads to decide code-vs-note per tab.
- Keep the existing SQL fail-loud behavior; the three new tiers are NOT fail-loud initially
  (a function legitimately lacks some), which is exactly why the `bindings` metadata exists.

### 3. `CodeFromTest` / a wrapper — render the 4-tab block
- Wrap the four per-tier examples in a `<Tabs groupId="gbx-example-lang">` (a DISTINCT groupId
  from the page-level `gbx-tier`, so per-example language selection doesn't fight the page's
  tier toggle) with four `<TabItem>` children in the fixed order.
- SQL default/first. The two heavy `<TabItem>`s carry the blue styling via CSS targeting the
  3rd+4th children of `.gbx-example-lang` (mirroring the existing `gbx-tier` nth-child rules).
- A tab whose `bindings` entry is absent renders the "Not available in this tier" note instead
  of a `<CodeFromTest>`.
- Prefer extending `CodeFromTest` (or a thin `<FunctionExamples>` wrapper over it) rather than
  hand-writing four `<CodeFromTest>` + a `<Tabs>` in every function's MDX by hand — the MDX
  should stay a one-liner per function.

### 4. CSS
- Add `.gbx-example-lang` tab rules reusing the existing `#2545b3` / `#f5f8ff` tokens, targeting
  the 3rd (Python-heavy) and 4th (Scala-heavy) tabs. No new colors; extend, don't duplicate.

## Testing / acceptance

- Doc-tests for the new per-tier examples execute and assert (run via the `gbx:test:*-docs`
  commands in Docker; they need the full env + sample data). SQL examples keep passing.
- `generate-function-info.py` regenerates `function-info.json` clean, now with `bindings` per
  function; the SQL fail-loud check still holds.
- The docs build clean (`gbx:docs:build` / `npm run build`, no broken-link warnings), and — the
  standing lesson from this session — the build step cleans up `docs/.docusaurus`/`docs/build`
  so port 3000 stays usable.
- Visual: a spot-check page shows 4 tabs in the fixed order, SQL default, the two heavy tabs
  blue-tinted, an absent-binding function showing the note in the right tab. (Rendered via
  `gbx:docs:dev` on a NON-3000 port for the agent; 3000 is the user's.)
- No internal-vocabulary leak in any rendered example/prose (QC internals-leak check).

## Rollout (plan-sizing note, not a decision yet)

The per-tier example authoring is the bulk of the effort (potentially hundreds of functions ×
3 new tiers). The plan should decide whether to: (a) build the full rails + metadata +
renderer first and land examples incrementally per package (RasterX → GridX → VectorX), with
the "Not available"/"example pending" note covering un-authored tabs; or (b) require all four
up front. Recommendation: (a) — ship the mechanism + one fully-tabbed package as proof, then
fill the rest, so the feature is visible and testable early without blocking on writing every
example at once.

## Out of scope

- Changing any product binding (Scala/Python) — this is docs-only. If the tabs reveal a genuinely
  missing binding that SHOULD exist, that's a separate product task, surfaced (not silently
  hidden) by the "Not available in this tier" note.
- The right-sidebar/DESCRIBE FUNCTION surface beyond making SQL its default (DESCRIBE FUNCTION is
  heavy-only and already single-source from `function-info.json`).
- Retitling or restructuring the API pages beyond the per-function example block.

## Outcome

Every function's example renders as a fixed four-tab block — SQL (default) / Python-light /
Python-heavy (blue) / Scala-heavy (blue) — sourced entirely from executable doc-tests, with a
generated `bindings` metadata field driving deterministic "Not available in this tier" notes for
legitimately-absent surfaces. Built on the existing tab framework and tier CSS; the new work is
the per-tier example conventions, the generator extension, the binding metadata, and the render
wrapper.
