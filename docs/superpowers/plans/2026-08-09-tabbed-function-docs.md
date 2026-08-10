# Per-Function Tabbed Code Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render each function's example on the API docs pages as a fixed four-tab block — SQL (default) / Python (light) / Python (heavy, blue) / Scala (heavy, blue) — sourced entirely from executable doc-tests, with legitimately-absent bindings shown as a "Not available in this tier" note.

**Architecture:** Extend the existing single-source pipeline (`*_sql_example()` doc-test → `generate-function-info.py` → `function-info.json` → `CodeFromTest` render) with three new per-tier example conventions and a generated per-function `bindings` metadata field. A thin `<FunctionExamples>` MDX wrapper reads `bindings` (from the generated JSON), renders a `<Tabs groupId="gbx-example-lang">` delegating each present tab to the existing `CodeFromTest`, and shows the note for absent tabs. Ship the mechanism + convert the RasterX page as proof; other packages fill in incrementally with the note covering un-authored tabs.

**Tech Stack:** Docusaurus (React/MDX), `@theme/Tabs` + `@theme/TabItem`, webpack `raw-loader`, the `CodeFromTest` component, Python `generate-function-info.py`, doc-tests in `docs/tests/python/` + `docs/tests/scala/`. Run in the `geobrix-dev` Docker container via the `gbx:*` palette.

## Global Constraints

- **Tab order is fixed:** SQL → Python (light) → Python (heavy) → Scala (heavy). SQL is the default (first, pre-selected) tab.
- **The two heavy tabs (3rd + 4th) get the EXISTING blue tier styling** — reuse tokens from `docs/src/css/custom.css`: active-tab `#2545b3`, panel tint `#f5f8ff`, border-left `3px solid #2545b3`, dark-mode variant `#6b8cff` / `rgba(107,140,255,0.08)`. Do NOT invent colors. SQL + Python-light tabs stay plain.
- **Use a DISTINCT `groupId="gbx-example-lang"`** for the per-example language tabs — NOT the page's `gbx-tier` groupId (a shared groupId would make language selection fight the page's tier toggle).
- **Tests are the documentation source.** Every tab's example is an executable doc-test; never hand-author example code in MDX.
- **Absence is data-driven.** The renderer decides code-vs-note from a generated per-function `bindings` field in `function-info.json` — NEVER by catching a failed extraction / missing function.
- **Per-tier example PRESENCE is detected by TEXT-SCAN, not execution.** Python/Scala example functions take `spark` and have side effects; they cannot be called at generate time (no Spark) the way SQL examples (which just `return` a string) can. `generate-function-info.py` detects `def <name>_python_light_example`/`def <name>_python_heavy_example`/`val <name>_scala_example` by scanning the source text.
- **Docs-only.** No product (Scala/Python binding) code changes. If a tab reveals a genuinely-missing binding that SHOULD exist, that is a separate product task, surfaced (not hidden) by the note.
- **A docs build must clean `docs/.docusaurus` + `docs/build`** so port 3000 stays usable for the user (standing lesson — a stale cache breaks the user's `gbx:docs:dev` on 3000).
- **Port 3000 is the user's.** Any agent dev-server/preview runs on a NON-3000 port (e.g. 3001).
- **Doc-tests run in Docker via `gbx:test:python-docs` (SQL/api via `--suite api`) and `gbx:test:scala-docs`; docs build via `gbx:docs:build`.** Never ad-hoc npm/pytest/mvn.
- **Commit hygiene:** subjects ≤72 chars, WHY in body, end with `Co-authored-by: Isaac`; single plain `git commit` (never `-n`/`--no-verify`).

## File Structure

**Create:**
- `docs/src/components/FunctionExamples.js` — the 4-tab wrapper. Props: `name` (function base name, e.g. `rst_avg`), and the four raw-loaded tier sources (`sql`, `pythonLight`, `pythonHeavy`, `scala`). Reads `bindings[name]` from the imported `function-info.json`; renders `<Tabs groupId="gbx-example-lang">` with 4 `<TabItem>`s in fixed order, each delegating to `<CodeFromTest>` (for a present tier) or a "Not available in this tier" note (absent).
- `docs/tests/python/api/rasterx_functions_python_light.py` — per-function `*_python_light_example(spark)` doc-tests (pyrx light tier) for the RasterX proof subset.
- `docs/tests/python/api/test_rasterx_functions_python_light.py` — executes/asserts the above.
- Per-function `*_python_heavy_example(spark)` added to the existing `docs/tests/python/api/rasterx_functions.py` (heavy shim; currently setup-only).
- Per-function `val <name>_scala_example` added to `docs/tests/scala/api/ScalaApiExamples.scala` (currently setup-only) + assertions in its doc-test harness.

**Modify:**
- `docs/scripts/generate-function-info.py` — add per-tier example discovery (text-scan) + emit a `bindings` array per function into `function-info.json`.
- `docs/docs/api/raster-functions.mdx` — replace each function's single `<CodeFromTest language="sql" .../>` with `<FunctionExamples name="..." sql={rasterxSqlCode} pythonLight={pyrxCode} pythonHeavy={rasterxCode} scala={rasterxScalaCode} />`; add the raw-loader import for the new light-example source.
- `docs/src/css/custom.css` — add `.gbx-example-lang` rules (blue for 3rd + 4th tabs), reusing existing tokens.
- `scripts/commands/gbx-docs-build.sh` — add a `docs/.docusaurus` + `docs/build` cleanup step (so 3000 stays usable).

**Verify-only (no change expected, lock with a test):**
- `src/main/scala/com/databricks/labs/gbx/expressions/FunctionInfoLoader.scala` — confirmed to ignore unknown keys (`.has()` checks); the new `bindings` key must not break it.
- `docs/tests-function-info/test_function_info_coverage.py` — enforces non-empty `examples`; must stay green with `bindings` added.

---

### Task 1: Docs-build cleans stale `.docusaurus`/`build`

**Files:**
- Modify: `scripts/commands/gbx-docs-build.sh`

**Interfaces:**
- Produces: a `gbx:docs:build` that removes `docs/.docusaurus` and `docs/build` before building, so a stale cache can't break the user's `gbx:docs:dev` on port 3000. Every later task's build check depends on this.

**Why first:** the standing lesson from this session is that a docs build leaving a stale `.docusaurus` cache breaks the user's 3000 server. Fixing the build command once means every later build step in this plan is self-cleaning.

- [ ] **Step 1: Read the current build script** to find where `npm run build` is invoked.

Run: `bash scripts/commands/gbx-docs-build.sh --help` and read `scripts/commands/gbx-docs-build.sh`.
Expected: locate the `npm run build` line and the `PROJECT_ROOT`/docs-dir resolution.

- [ ] **Step 2: Add a cache-clean step immediately before the build**

Insert before the `npm run build` invocation (adjust the docs-dir variable to match the script's own):

```bash
# Clear stale Docusaurus cache + prior build so a corrupt cache can't break a
# subsequent `gbx:docs:dev` (the user's port-3000 server). Both are gitignored.
rm -rf "$DOCS_DIR/.docusaurus" "$DOCS_DIR/build"
```

- [ ] **Step 3: Run the build to confirm it still succeeds and cleans**

Run: `bash scripts/commands/gbx-docs-build.sh --log verify-docs-build.log` (in Docker per the script; do NOT bind port 3000)
Expected: build succeeds; `docs/.docusaurus` and `docs/build` are regenerated fresh (the `rm -rf` ran, then the build recreated `build`).

- [ ] **Step 4: Commit**

```bash
git add scripts/commands/gbx-docs-build.sh
git commit -m "fix(docs): gbx:docs:build clears stale .docusaurus/build cache"
```

---

### Task 2: `.gbx-example-lang` blue styling for the two heavy tabs

**Files:**
- Modify: `docs/src/css/custom.css`

**Interfaces:**
- Produces: CSS that colors the 3rd (Python-heavy) and 4th (Scala-heavy) tabs of a `.gbx-example-lang` tab group with the existing blue tier tokens, leaving tabs 1 (SQL) + 2 (Python-light) plain. Consumed visually by the `FunctionExamples` wrapper (Task 4), which sets `className="gbx-example-lang-tabs"`.

**Interfaces note:** the wrapper (Task 4) renders `<Tabs groupId="gbx-example-lang" className="gbx-example-lang-tabs">`. This task styles that class. Reuse the EXACT tokens already in this file for `.gbx-tier-tabs` — do not invent colors.

- [ ] **Step 1: Read the existing `.gbx-tier-tabs` rules** in `docs/src/css/custom.css` to copy the exact selectors + tokens (active-tab `#2545b3`, panel `#f5f8ff`, border-left `3px solid #2545b3`, dark-mode `#6b8cff` / `rgba(107,140,255,0.08)`, the `:has()` dark variant).

- [ ] **Step 2: Add the parallel `.gbx-example-lang-tabs` rules** targeting the 3rd + 4th tab children

Mirror the `.gbx-tier-tabs` block, but target `nth-child(3)` AND `nth-child(4)` (the two heavy tabs) instead of `nth-child(2)`:

```css
/* Per-example language tabs: SQL(1) + Python-light(2) plain; Python-heavy(3) +
   Scala-heavy(4) get the blue heavyweight tint — same tokens as .gbx-tier-tabs. */
.gbx-example-lang-tabs.tabs .tabs__item:nth-child(3).tabs__item--active,
.gbx-example-lang-tabs.tabs .tabs__item:nth-child(4).tabs__item--active {
  color: #2545b3;
  border-bottom-color: #2545b3;
}
.gbx-example-lang-tabs:has(.tabs__item:nth-child(3).tabs__item--active) .tabs__content,
.gbx-example-lang-tabs:has(.tabs__item:nth-child(4).tabs__item--active) .tabs__content {
  border-left: 3px solid #2545b3;
  background: #f5f8ff;
  padding: 0.75rem 1rem;
  border-radius: 0 4px 4px 0;
}
```

Add the dark-mode variant mirroring the existing `[data-theme='dark']` / `#6b8cff` / `rgba(107,140,255,0.08)` block for `.gbx-tier-tabs`, adapted to `nth-child(3)`+`nth-child(4)`. Copy the real selectors from Step 1 — match them exactly, only changing the class name and child indices.

- [ ] **Step 3: Verify the CSS compiles in a build**

Run: `bash scripts/commands/gbx-docs-build.sh --log verify-css-build.log` (Docker; non-3000)
Expected: build succeeds, no CSS/SCSS errors. (Visual confirmation happens in Task 6 once the wrapper renders real tabs.)

- [ ] **Step 4: Commit**

```bash
git add docs/src/css/custom.css
git commit -m "feat(docs): blue styling for heavy tabs in gbx-example-lang groups"
```

---

### Task 3: Generator emits per-function `bindings` metadata

**Files:**
- Modify: `docs/scripts/generate-function-info.py`
- Test: `docs/tests-function-info/test_function_bindings.py` (new)

**Interfaces:**
- Consumes: the existing `MODULES` list + `_collect_from_module` machinery (unchanged behavior for SQL examples).
- Produces: each function's entry in `function-info.json` gains a `"bindings"` array — a subset of `["sql","python-light","python-heavy","scala"]` recording which tiers have an example for that function. `"sql"` is present whenever the function has a non-empty `examples` (today's behavior). The other three are present when a text-scan finds a correspondingly-named example function/val (see below). The `FunctionExamples` wrapper (Task 4) reads this to decide code-vs-note per tab.

**Detection is TEXT-SCAN, not execution** — Python/Scala example functions take `spark` and cannot be called at generate time. Scan the raw source of the tier's doc-test file for the example symbol.

**Naming derivation (mirror the SQL convention):** for a registered spark name like `gbx_rst_avg` with local prefix `rst_`, the base is `rst_avg`; the tier example symbols are:
- SQL: `def rst_avg_sql_example` (already handled)
- Python light: `def rst_avg_python_light_example` in `rasterx_functions_python_light.py`
- Python heavy: `def rst_avg_python_heavy_example` in `rasterx_functions.py`
- Scala: `val rst_avg_scala_example` in `ScalaApiExamples.scala`

- [ ] **Step 1: Write the failing test**

```python
# docs/tests-function-info/test_function_bindings.py
import json
from pathlib import Path

FUNCTION_INFO = (
    Path(__file__).resolve().parents[1]
    / "src/main/resources/com/databricks/labs/gbx/function-info.json"
)


def _load():
    data = json.loads(FUNCTION_INFO.read_text())
    return data.get("functions", data)


def test_every_function_has_a_bindings_list_including_sql():
    fns = _load()
    checked = 0
    for name, entry in fns.items():
        if name.startswith("_"):
            continue
        assert "bindings" in entry, f"{name} missing bindings"
        assert isinstance(entry["bindings"], list)
        # Any function with a non-empty example must advertise the sql binding.
        if entry.get("examples"):
            assert "sql" in entry["bindings"], f"{name} has examples but no sql binding"
        checked += 1
    assert checked > 0


def test_bindings_values_are_from_the_known_set():
    known = {"sql", "python-light", "python-heavy", "scala"}
    for name, entry in _load().items():
        if name.startswith("_"):
            continue
        for b in entry.get("bindings", []):
            assert b in known, f"{name} has unknown binding {b!r}"
```

- [ ] **Step 2: Run to verify it fails**

Run: `gbx:test:function-info` (or `gbx:test:python --path docs/tests-function-info/test_function_bindings.py`)
Expected: FAIL — no `bindings` key exists yet.

- [ ] **Step 3: Add per-tier text-scan + emit `bindings`**

In `generate-function-info.py`, add a helper that, given a tier's source-file path and the set of base names, returns which bases have an example symbol. Scan text (do NOT import/call):

```python
import re

# tier -> (doc-test source path relative to docs/, symbol template, binding label)
_TIER_SCANS = [
    ("tests/python/api/rasterx_functions_python_light.py", "def {base}_python_light_example", "python-light"),
    ("tests/python/api/rasterx_functions.py",              "def {base}_python_heavy_example",  "python-heavy"),
    ("tests/scala/api/ScalaApiExamples.scala",             "val {base}_scala_example",         "scala"),
]

def _scan_tier_bindings(docs_root: Path, base_for_spark: dict) -> dict:
    """spark_name -> set of tier labels whose example symbol is present in source text.
    base_for_spark maps spark_name (gbx_rst_avg) -> base (rst_avg)."""
    found = {name: set() for name in base_for_spark}
    for rel, template, label in _TIER_SCANS:
        path = docs_root / rel
        text = path.read_text() if path.exists() else ""
        for spark_name, base in base_for_spark.items():
            symbol = template.format(base=base)
            # word-boundary-ish: the symbol followed by '(' (Python def) or ':'/whitespace (Scala val)
            if re.search(re.escape(symbol) + r"\s*[(:]", text):
                found[spark_name].add(label)
    return found
```

Then, where each function entry is finalized (the write section ~line 336-386), compute `bindings`:
- start `bindings = []`;
- if the entry has non-empty `examples`, append `"sql"`;
- extend with the sorted tier labels from `_scan_tier_bindings` for that spark name;
- set `entry["bindings"] = bindings` in the fixed tab order (sql, python-light, python-heavy, scala).

Derive `base_for_spark` from the same `MODULES` prefix math already used (`spark_name` with `spark_prefix` replaced back to `local_prefix`), so the base matches the SQL convention exactly.

- [ ] **Step 4: Regenerate and run the test**

Run: `gbx:docs:function-info` (regenerates function-info.json), then `gbx:test:python --path docs/tests-function-info/test_function_bindings.py`
Expected: PASS. Also run the existing `docs/tests-function-info/test_function_info_coverage.py` — must STILL pass (the non-empty-`examples` invariant is untouched; `bindings` is additive).

- [ ] **Step 5: Confirm the Scala loader is unaffected**

Run: `gbx:test:scala --suite 'com.databricks.labs.gbx.expressions.*FunctionInfo*'` (or the suite covering `FunctionInfoLoader`); if none exists, spot-read `FunctionInfoLoader.scala` to reconfirm it only reads `examples`/`usage_args`/`description` via `.has()` and ignores `bindings`.
Expected: green / confirmed — the new key is ignored by the heavy loader.

- [ ] **Step 6: Commit**

```bash
git add docs/scripts/generate-function-info.py docs/tests-function-info/test_function_bindings.py src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "feat(docs): generate per-function bindings metadata in function-info.json"
```

---

### Task 4: `<FunctionExamples>` 4-tab wrapper component

**Files:**
- Create: `docs/src/components/FunctionExamples.js`

**Interfaces:**
- Consumes: `CodeFromTest` (existing default export at `@site/src/components/CodeFromTest`), `Tabs`/`TabItem` (`@theme/Tabs`, `@theme/TabItem`), and `function-info.json` imported for the `bindings` lookup.
- Produces: `export default function FunctionExamples({ name, sql, pythonLight, pythonHeavy, scala, sqlSource, pythonLightSource, pythonHeavySource, scalaSource, testFile })`. `name` is the function base (e.g. `rst_avg`). The four `*` props are raw-loaded source strings for each tier's doc-test file; the `*Source` props are the repo-relative source paths `CodeFromTest` needs (mirroring how the SQL block passes `source=...`). Renders a `<Tabs groupId="gbx-example-lang" className="gbx-example-lang-tabs">` with exactly four `<TabItem>`s in fixed order (SQL default). A tier present in `bindings[name]` delegates to `<CodeFromTest>` with that tier's example function name; an absent tier renders the note.

**Interfaces note (example-function naming the wrapper passes to CodeFromTest):** SQL → `${name}_sql_example` (language `sql`); Python-light → `${name}_python_light_example` (language `python`); Python-heavy → `${name}_python_heavy_example` (language `python`); Scala → `${name}_scala_example` (language `scala`). Output constants follow the `${...}_output` convention CodeFromTest already supports via `outputConstant`.

- [ ] **Step 1: Write the component**

```jsx
// docs/src/components/FunctionExamples.js
import React from 'react';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeFromTest from '@site/src/components/CodeFromTest';
import functionInfo from '@site/../src/main/resources/com/databricks/labs/gbx/function-info.json';

// Fixed tab order. label = tab text; key = binding label in function-info.json;
// lang = CodeFromTest language; suffix = example-function name suffix.
const TABS = [
  { key: 'sql',          label: 'SQL',            lang: 'sql',    suffix: '_sql_example' },
  { key: 'python-light', label: 'Python (light)', lang: 'python', suffix: '_python_light_example' },
  { key: 'python-heavy', label: 'Python (heavy)', lang: 'python', suffix: '_python_heavy_example' },
  { key: 'scala',        label: 'Scala',          lang: 'scala',  suffix: '_scala_example' },
];

function bindingsFor(name) {
  const fns = functionInfo.functions || functionInfo;
  const entry = fns[name] || fns['gbx_' + name] || {};
  return new Set(entry.bindings || []);
}

export default function FunctionExamples(props) {
  const { name, testFile } = props;
  const present = bindingsFor(name);
  const codeByKey = {
    'sql': props.sql, 'python-light': props.pythonLight,
    'python-heavy': props.pythonHeavy, 'scala': props.scala,
  };
  const sourceByKey = {
    'sql': props.sqlSource, 'python-light': props.pythonLightSource,
    'python-heavy': props.pythonHeavySource, 'scala': props.scalaSource,
  };
  return (
    <Tabs groupId="gbx-example-lang" className="gbx-example-lang-tabs">
      {TABS.map((t) => (
        <TabItem key={t.key} value={t.key} label={t.label} default={t.key === 'sql'}>
          {present.has(t.key) && codeByKey[t.key] ? (
            <CodeFromTest
              language={t.lang}
              code={codeByKey[t.key]}
              source={sourceByKey[t.key]}
              testFile={testFile}
              functionName={name + t.suffix}
              outputConstant={name + t.suffix + '_output'}
            />
          ) : (
            <p><em>Not available in this tier.</em></p>
          )}
        </TabItem>
      ))}
    </Tabs>
  );
}
```

**Note on the JSON import path:** verify the relative import resolves from `docs/src/components/` to the repo `src/main/resources/.../function-info.json`. If Docusaurus/webpack rejects reaching outside `docs/`, fall back to copying/symlinking the JSON under `docs/src/` during the build, OR import the `bindings` via a small generated `docs/src/function-bindings.json` that `generate-function-info.py` also writes (decide in Step 2 based on what the build accepts). The import MUST resolve at build time — a runtime fetch is not acceptable.

- [ ] **Step 2: Verify the import resolves in a build**

Run: `bash scripts/commands/gbx-docs-build.sh --log verify-wrapper-build.log` (Docker; non-3000) after adding a single throwaway `<FunctionExamples .../>` usage on a scratch page (or rely on Task 5's first real usage).
Expected: build succeeds and the JSON import resolves. If it fails on the cross-dir import, apply the Step-1 fallback (generated `docs/src/function-bindings.json`) and adjust the import + the generator (Task 3) to also write it; re-run.

- [ ] **Step 3: Commit**

```bash
git add docs/src/components/FunctionExamples.js
# (+ docs/src/function-bindings.json and the generator tweak IF the fallback was needed)
git commit -m "feat(docs): FunctionExamples 4-tab per-function example wrapper"
```

---

### Task 5: Author RasterX per-tier example doc-tests (proof subset)

**Files:**
- Create: `docs/tests/python/api/rasterx_functions_python_light.py` + `docs/tests/python/api/test_rasterx_functions_python_light.py`
- Modify: `docs/tests/python/api/rasterx_functions.py` (add `*_python_heavy_example` fns) + its test file
- Modify: `docs/tests/scala/api/ScalaApiExamples.scala` (add `val *_scala_example`) + its doc-test harness

**Interfaces:**
- Produces: per-function tier examples for a PROOF SUBSET of RasterX functions — pick ~3-5 functions that genuinely have all four bindings (e.g. `rst_avg`, `rst_boundingbox`, `rst_bandcount` — verify each has light+heavy Python + Scala forms) PLUS at least one function that legitimately LACKS a tier (e.g. `rst_fromfile` = light-only, or `rst_polygonize` = light UDTF/no-Column) to exercise the "Not available" note. Each example is a real executable doc-test asserting against sample data, following the existing `*_sql_example` + `*_output` conventions.

**Interfaces note:** naming MUST match Task 3's scan templates exactly — `def <base>_python_light_example(spark)`, `def <base>_python_heavy_example(spark)`, `val <base>_scala_example`. Each with a matching `<base>_..._example_output` constant.

- [ ] **Step 1: Pick the proof subset + confirm bindings.** Read `docs/docs/api/raster-functions.mdx` + the pyrx/rasterx `functions.py` to choose 3-5 functions present in all four tiers and 1 with a genuine gap. Write the list into the task's commit message later.

- [ ] **Step 2: Write the light-Python examples + their failing test**

Mirror the SQL doc-test shape. Example:

```python
# docs/tests/python/api/rasterx_functions_python_light.py
def rst_avg_python_light_example(spark):
    """Per-band average pixel values via the lightweight pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx
    rx.register(spark)
    df = spark.read.format("gdal").load(SAMPLE_RASTER_PATH)  # or the pyrx setup helper
    return df.select(rx.rst_avg("tile").alias("band_averages"))

rst_avg_python_light_example_output = """
[array of per-band means]
"""
```

Test file asserts the example runs and returns the expected shape (real assertion, not just "no error"), following `test_rasterx_functions_sql.py`'s harness (fixtures for spark + sample rasters).

- [ ] **Step 3: Run the light test to green**

Run: `gbx:test:python-docs --suite api` (Docker; needs sample data)
Expected: the new light-example tests pass.

- [ ] **Step 4: Add the heavy-Python `*_python_heavy_example` fns** to `rasterx_functions.py` (+ assertions in its test file), same shape but through the heavy `databricks.labs.gbx.rasterx` shim. Run `gbx:test:python-docs --suite api` → green.

- [ ] **Step 5: Add the Scala `val *_scala_example`** to `ScalaApiExamples.scala` (+ assertions in the Scala doc-test harness, following `RasterxExamplesDocTest`'s `test("...")` pattern). Run `gbx:test:scala-docs` → green.

- [ ] **Step 6: Regenerate bindings + confirm the gap function**

Run: `gbx:docs:function-info` then `gbx:test:python --path docs/tests-function-info/test_function_bindings.py`
Expected: the proof-subset functions now show their authored tiers in `bindings`; the deliberately-gapped function shows only the tiers it actually has (proving the note will render for the missing one).

- [ ] **Step 7: Commit**

```bash
git add docs/tests/python/api/rasterx_functions_python_light.py docs/tests/python/api/test_rasterx_functions_python_light.py docs/tests/python/api/rasterx_functions.py docs/tests/scala/api/ScalaApiExamples.scala src/main/resources/com/databricks/labs/gbx/function-info.json
# + the scala/python heavy test files touched
git commit -m "test(docs): per-tier example doc-tests for RasterX proof subset"
```

---

### Task 6: Wire the RasterX page to `<FunctionExamples>` + verify end-to-end

**Files:**
- Modify: `docs/docs/api/raster-functions.mdx`

**Interfaces:**
- Consumes: `FunctionExamples` (Task 4), the RasterX per-tier examples (Task 5), the `bindings` metadata (Task 3). The raw-loader imports for `rasterxSqlCode`, `pyrxCode`, `rasterxCode`, `rasterxScalaCode` already exist at the top of the file; ADD one for the new light-example source (`rasterx_functions_python_light.py`).

- [ ] **Step 1: Add the raw-loader import** for the new light-example source at the top of `raster-functions.mdx`:

```mdx
import rasterxLightCode from '!!raw-loader!../../tests/python/api/rasterx_functions_python_light.py';
```

- [ ] **Step 2: Replace the proof-subset functions' single SQL block with `<FunctionExamples>`**

For each proof-subset function, replace its `<CodeFromTest language="sql" .../>` line with:

```mdx
<FunctionExamples
  name="rst_avg"
  sql={rasterxSqlCode}                sqlSource="docs/tests/python/api/rasterx_functions_sql.py"
  pythonLight={rasterxLightCode}      pythonLightSource="docs/tests/python/api/rasterx_functions_python_light.py"
  pythonHeavy={rasterxCode}           pythonHeavySource="docs/tests/python/api/rasterx_functions.py"
  scala={rasterxScalaCode}            scalaSource="docs/tests/scala/api/ScalaApiExamples.scala"
  testFile="docs/tests/python/api/test_rasterx_functions_sql.py"
/>
```

(Import `FunctionExamples` at the top: `import FunctionExamples from '@site/src/components/FunctionExamples';`.) Do the proof subset only; the rest of the page keeps its current single-SQL blocks until a later incremental pass.

- [ ] **Step 3: Build the docs**

Run: `bash scripts/commands/gbx-docs-build.sh --log verify-rasterx-tabs.log` (Docker; non-3000)
Expected: clean build, no broken-link/MDX warnings.

- [ ] **Step 4: Visual spot-check on a NON-3000 port**

Run: `bash scripts/commands/gbx-docs-dev.sh --port 3001 --log verify-tabs-dev.log` (NEVER 3000 — that's the user's). Load `http://localhost:3001/…/api/raster-functions` and confirm: 4 tabs in order SQL/Python(light)/Python(heavy)/Scala; SQL selected by default; tabs 3+4 blue-tinted; the gapped function shows "Not available in this tier" in the missing tab. Then STOP that dev server (`gbx:docs:stop` or kill the 3001 pid) so it doesn't linger.

- [ ] **Step 5: Internals-leak + full doc-test green**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/raster-functions.mdx` (expect empty), then `gbx:test:python-docs --suite api` + `gbx:test:scala-docs` (expect green).

- [ ] **Step 6: Commit**

```bash
git add docs/docs/api/raster-functions.mdx
git commit -m "docs(rasterx): render proof-subset functions as 4-tab examples"
```

---

## Self-Review

**1. Spec coverage.** Tab framework + blue CSS → T2 (reuses existing tokens, distinct `gbx-example-lang` groupId per constraint). Per-tier example conventions → T5 (light/heavy Python + Scala, mirroring `*_sql_example`). Generator extension + `bindings` metadata → T3 (text-scan, not execution — the key correctness decision). Data-driven absence → T3 (`bindings`) consumed by T4 (`FunctionExamples` renders note when absent). Render wrapper (thin, MDX stays ~one line) → T4. SQL default + fixed order → T4 (`default` on the SQL TabItem, fixed TABS array). Docs-build 3000 hygiene → T1. Incremental rollout (mechanism + RasterX proof) → T5/T6 do RasterX only; other packages deferred. FunctionInfoLoader safety + coverage test → verified in T3 steps 4-5. No gap.

**2. Placeholder scan.** Each code step carries real code. Two flagged implementer decisions (NOT placeholders): T4 Step-1 JSON-import path may need the `docs/src/function-bindings.json` fallback if webpack rejects the cross-dir import (explicit fallback given); T5 Step-1 requires choosing the proof subset by reading the real binding availability (a real task, exact-values-at-implementation). Both are genuine "verify against source" instructions with a defined resolution, not vague TODOs.

**3. Type/name consistency.** The example-symbol naming is consistent across T3 (scan templates), T4 (suffixes the wrapper passes to CodeFromTest), and T5 (the authored functions): `<base>_sql_example` / `<base>_python_light_example` / `<base>_python_heavy_example` / `<base>_scala_example`, each with `_output`. The `bindings` labels (`sql`/`python-light`/`python-heavy`/`scala`) match between T3 (emit), T4 (`TABS[].key` lookup), and the test in T3. `groupId="gbx-example-lang"` + `className="gbx-example-lang-tabs"` match between T2 (CSS) and T4 (component).

**Open item for the implementer (verify before coding):** the `function-info.json` cross-directory webpack import in T4 (fallback defined if it fails), and the exact proof-subset function list in T5 (must confirm real four-tier availability + one genuine gap against source).
