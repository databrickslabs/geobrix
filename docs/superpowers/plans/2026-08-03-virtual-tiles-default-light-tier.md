# Virtual tiles as the light-tier default — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. Phases B (examples) and C (benchmarking) require EXECUTION (Serverless / bench cluster) and staged artifacts — the controller paces those with the wheel-staging + cluster discipline; they are not fully subagent-autonomous.

**Goal:** Make `virtualTiles=true` the default for all light-tier raster readers (`raster_gbx` / `gtiff_gbx` / `cog_gbx`), document + orient it as the new front door, prove ship-readiness by executing the examples on the new default, and capture a `light (virtual)` column in the Benchmarking comparison.

**Architecture:** All three readers inherit one `RasterGbxReader.__init__` (`python/geobrix/src/databricks/labs/gbx/ds/raster.py:684–723`) that already parses the full windowing option surface (`virtualTiles`, `tileSize`, `overlapPercent`, `clipPolygons`, `clipCrs`, `windows`, `splitStrategy`, `sizeInMB`, `filterRegex`). The default flip is one line at `:723`; everything else is tests, docs, example validation, and benchmarking.

**Tech Stack:** Python 3.12 (light tier, no JAR), pytest (light tests run in Docker via `gbx:test:python`; doc-tests via `gbx:test:python-docs`), Docusaurus MDX, the bench harness (`bench/readers.py`) + bench cluster. Reader tests run in the `geobrix-dev` container.

## Global Constraints

- **`virtualTiles` default becomes `true` for the three RasterGbxReader-based readers** (raster_gbx, gtiff_gbx, cog_gbx). `virtualTiles=false` must still materialize on all three. **NetCDF (`netcdf_gbx`) is unchanged** — its raster path stays materialized (documented exception); do not touch `netcdf.py`'s `partitions()`.
- **This is a breaking behavior change** — flag it in the v0.5.0 release notes; a virtual tile handed to a heavy function raises the existing `VirtualTileException` (unchanged).
- **No option plumbing** — the option surface is already unified; do NOT add per-reader option parsing. The only reader-code change is the default value.
- **No heavy-tier change; no re-benchmarking of materialized-light or heavy** (reuse existing numbers).
- **Docs voice:** no internal vocabulary (no "wave N"/"inc N"/"increment N") in `docs/docs/`; no mention of the removed checkpoint/path-tile machinery. Heavy-tier boundary stated present-tense.
- **Docs-are-tests:** any executable doc snippet lives in `docs/tests/python/...` and is imported via `!!raw-loader!`; it must execute (`gbx:test:python-docs`).
- **Real numbers only** in benchmarking (from actual runs / the perf memory: ~100 B virtual row vs 148–527 KB materialized, ~1,400–5,000× smaller); never invent timings.
- **Subagents run only affected suites** ([[subagents-run-only-affected-tests]]); the controller owns broad gates.

---

## File Structure

**Phase A (code + tests + docs):**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py:723` (the default).
- Modify/add tests under `python/geobrix/test/ds/` (default-behavior tests + any materialized-assuming tests that need `virtualTiles=false`).
- Grep/update doc-tests under `docs/tests/python/` that read a light raster and expect bytes.
- Modify docs: `docs/docs/readers/raster.mdx`, `cog.mdx`, `geotiff.mdx` (option table + admonition), `netcdf.mdx` (exception note), `overview.mdx` (orienting callout), `docs/docs/readers-writers.mdx` (orienting callout), `docs/docs/api/virtual-tiles.mdx` (fix line ~92), `docs/docs/quick-start.mdx` (+ its doc-test), `docs/docs/beta-release-notes.mdx` (breaking-change note).

**Phase B (examples):** `notebooks/examples/{eo-series/03,eo-series/04,helios/03,vapor-eyes/02,vapor-eyes/03,xview/Clipping}.ipynb` + mirrored `docs/docs/notebooks/*.mdx`.

**Phase C (benchmarking):** `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (+ functions bench), `docs/docs/api/benchmarking.mdx`.

---

# PHASE A — code + tests + docs + quickstart

### Task A1: Flip the default + reader-default tests

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/raster.py:723`
- Test: `python/geobrix/test/ds/test_raster_virtual.py` (extend) or a new focused test

**Interfaces:**
- `RasterGbxReader.emit_virtual` now defaults `True` (from `options.get("virtualTiles", "true")`).

- [ ] **Step 1: Write the failing/locking default-behavior tests**

Add tests (run in Docker; use the existing `test_raster_virtual.py` fixtures/helpers as the pattern — it already builds tiled/striped rasters). For EACH of `raster_gbx`, `gtiff_gbx`, `cog_gbx`:
```python
def test_<reader>_defaults_to_virtual(tiled_path, spark):
    # NO virtualTiles option -> should now be virtual (raster null, path+window set)
    row = spark.read.format("<reader>").load(str(tiled_path)).collect()[0]["tile"]
    assert row["raster"] is None, "light reader must default to virtual (bytes-free)"
    assert row["path"] is not None and row["window"] is not None

def test_<reader>_virtualtiles_false_materializes(tiled_path, spark):
    row = spark.read.format("<reader>").option("virtualTiles", "false").load(str(tiled_path)).collect()[0]["tile"]
    assert row["raster"] is not None, "virtualTiles=false must still materialize bytes"
```
Plus one asserting `netcdf_gbx` raster still materializes by default (unchanged).

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/ds/test_raster_virtual.py --log a1-red.log`
Expected: the `defaults_to_virtual` tests FAIL (default is still `false` → raster present).

- [ ] **Step 3: Flip the default**

In `raster.py:723`: `self.emit_virtual = str(options.get("virtualTiles", "true")).lower() == "true"`.

- [ ] **Step 4: Run to verify pass**

Run: `gbx:test:python --path python/geobrix/test/ds/test_raster_virtual.py --log a1.log`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/ds/raster.py python/geobrix/test/ds/test_raster_virtual.py
git commit -m "feat(ds): light raster readers default to virtual tiles

raster_gbx / gtiff_gbx / cog_gbx now default virtualTiles=true (bytes-free
virtual tiles); virtualTiles=false still materializes. netcdf_gbx raster
is unchanged (materialized). Breaking behavior change for pipelines that
consumed raster bytes.

Co-authored-by: Isaac"
```

---

### Task A2: Fix tests + doc-tests that assumed materialized-by-default

**Files:**
- Modify: reader/ds tests under `python/geobrix/test/ds/` and any others that read a light raster without `virtualTiles` and expect bytes
- Modify: doc-tests under `docs/tests/python/` that do the same

**Interfaces:** none (test-only).

- [ ] **Step 1: Find the affected tests**

Run:
```bash
grep -rn "format(\"raster_gbx\")\|format(\"gtiff_gbx\")\|format(\"cog_gbx\")\|format('raster_gbx')\|format('gtiff_gbx')\|format('cog_gbx')" python/geobrix/test/ docs/tests/python/
```
For each hit, determine: does it read WITHOUT `virtualTiles` and then (a) assert `raster` bytes present, (b) pass the tile to a heavy `rasterx` function, or (c) feed a bytes-expecting step? Those break under the new default.

- [ ] **Step 2: Classify + fix each**

- A test that specifically verifies MATERIALIZED behavior → add `.option("virtualTiles", "false")` (keep testing materialized explicitly).
- A test where materialization is incidental but bytes are needed downstream → add a `materialize` step, or `.option("virtualTiles","false")` if simpler and the test isn't about virtual.
- A test already passing `virtualTiles=true` → unaffected, leave.
Do NOT weaken assertions; make the test's intent explicit.

- [ ] **Step 3: Run the affected suites (scoped)**

Run only the suites you touched, e.g. `gbx:test:python --path python/geobrix/test/ds/ --log a2-ds.log`, and the doc-test suite `gbx:test:python-docs --log a2-docs.log`.
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/test/ docs/tests/python/
git commit -m "test: pin materialized-expecting reader tests to virtualTiles=false

Co-authored-by: Isaac"
```

---

### Task A3: Reader docs — document `virtualTiles` (default true) + windowing surface + admonition

**Files:**
- Modify: `docs/docs/readers/raster.mdx`, `docs/docs/readers/cog.mdx`, `docs/docs/readers/geotiff.mdx`, `docs/docs/readers/netcdf.mdx`

**Interfaces:** none (docs).

- [ ] **Step 1: Add the `virtualTiles` row to each option table**

In `raster.mdx`, `cog.mdx`, `geotiff.mdx` option tables, add as the FIRST row:
```markdown
| `virtualTiles` | `"true"` | **Default.** Emit bytes-free **virtual tiles** — each row carries the source `path` + pixel `window` instead of raster bytes; pixels are read lazily when an operation needs them (the ingest-OOM-dissolving default for the light tier). Set `"false"` to materialize raster bytes into each row. See [Virtual Tiles](../api/virtual-tiles). |
```
Ensure all three tables show the same shared windowing option set (they already do in raster.mdx; verify cog.mdx/geotiff.mdx match — the recon confirmed the surface is shared).

- [ ] **Step 2: Add a "default changed" admonition to each**

Mirror the existing `splitStrategy` admonition style:
```markdown
:::note Virtual tiles are the default
Light raster readers now emit **virtual tiles** by default (`virtualTiles=true`) — bytes-free
`(path, window)` references that read pixels lazily. Previously the reader materialized raster
bytes into every row. To restore materialized reads, pass `.option("virtualTiles", "false")`.
A virtual tile passed to a heavyweight function must be materialized first — see
[Virtual Tiles](../api/virtual-tiles).
:::
```

- [ ] **Step 3: NetCDF exception note**

In `netcdf.mdx`, add a short note that the NetCDF raster reader materializes tiles (multidimensional per-variable reads); virtual-tile support for NetCDF is not part of this release.

- [ ] **Step 4: Voice + commit**

Run `grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/readers/` (must be empty).
```bash
git add docs/docs/readers/raster.mdx docs/docs/readers/cog.mdx docs/docs/readers/geotiff.mdx docs/docs/readers/netcdf.mdx
git commit -m "docs(readers): document virtualTiles (now default) + windowing surface

Co-authored-by: Isaac"
```

---

### Task A4: Orienting content (Readers Overview + Readers & Writers) + virtual-tiles page fix + quickstart + release note

**Files:**
- Modify: `docs/docs/readers/overview.mdx`, `docs/docs/readers-writers.mdx`, `docs/docs/api/virtual-tiles.mdx`, `docs/docs/quick-start.mdx`, `docs/tests/python/quickstart/examples.py` (if snippet changes), `docs/docs/beta-release-notes.mdx`

**Interfaces:** none (docs).

- [ ] **Step 1: Readers Overview orienting callout**

In `docs/docs/readers/overview.mdx`, in the Lightweight tab near the top (after the "no JAR, no init script" line, before the "Why this scales" tip), add:
```markdown
:::tip Loading into virtual tiles — the default
The lightweight raster readers (`raster_gbx`, `gtiff_gbx`, `cog_gbx`) load data as **virtual tiles**
by default — bytes-free `(path, window)` references that read pixels lazily, so a multi-gigabyte
raster fans into tiles without out-of-memory. This is the front door to the virtual-tile model; see
**[Virtual Tiles](../api/virtual-tiles)**. Pass `.option("virtualTiles", "false")` for materialized
(bytes-in-row) reads.
:::
```

- [ ] **Step 2: Readers & Writers overview callout**

In `docs/docs/readers-writers.mdx`, near the Readers section intro, add a one-line orienting note: light raster readers default to virtual tiles (link to Virtual Tiles), with the `virtualTiles=false` opt-out.

- [ ] **Step 3: Fix virtual-tiles.mdx prospective wording**

In `docs/docs/api/virtual-tiles.mdx` (~line 92, the "virtual by default in the lightweight tier" lifecycle text), confirm it now reads as present-tense fact (it becomes accurate with the flip). Adjust any wording that still implied virtual was opt-in.

- [ ] **Step 4: Quickstart**

In `docs/docs/quick-start.mdx`, at the `READ_GEOTIFF_LIGHT` section, add a one-line note that the lightweight reader returns **virtual tiles** by default (bytes-free) and link Virtual Tiles; show `.option("virtualTiles","false")` (or a `materialize`) for when bytes are needed. If the runnable snippet in `docs/tests/python/quickstart/examples.py` changes, keep it doc-test-sourced and ensure it executes.

- [ ] **Step 5: Release-notes breaking change**

In `docs/docs/beta-release-notes.mdx` v0.5.0 section, add a bullet: light raster readers now default to virtual tiles (breaking behavior change; `virtualTiles=false` to opt out). Keep the flat bullet style; link Virtual Tiles.

- [ ] **Step 6: Voice + docs build + commit**

Run `grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/` (empty) and `bash scripts/commands/gbx-docs-static-build.sh --log a4-build.log` (compiles, no broken links). If the quickstart snippet changed: `gbx:test:python-docs --log a4-docs.log`.
```bash
git add docs/docs/readers/overview.mdx docs/docs/readers-writers.mdx docs/docs/api/virtual-tiles.mdx docs/docs/quick-start.mdx docs/tests/python/quickstart/examples.py docs/docs/beta-release-notes.mdx
git commit -m "docs: orient virtual-tiles default in overviews + quickstart + release notes

Co-authored-by: Isaac"
```

---

### Task A5: Phase-A gate (light reader + doc-test suites)

**Files:** none (verification).

- [ ] **Step 1: Run the reader + ds suites**

Run: `gbx:test:python --path python/geobrix/test/ds/ --log a5-ds.log`. Expected: PASS (default flip + fixed materialized-assuming tests).

- [ ] **Step 2: Broader light suite for fallout**

Run: `gbx:test:python --path python/geobrix/test/pyrx/ --log a5-pyrx.log` (catches any pyrx function test that read a raster and assumed bytes). Fix any materialized-assuming ones per Task A2's rule.

- [ ] **Step 3: Doc-test + docs build**

Run: `gbx:test:python-docs --log a5-docs.log` and `bash scripts/commands/gbx-docs-static-build.sh --log a5-build.log`. Expected: PASS, clean links.

- [ ] **Step 4: Commit any gate fixes**

```bash
git add -A python/geobrix/test/ docs/
git commit -m "test: reader-default fallout fixes for the virtual-tiles default

Co-authored-by: Isaac"
```

---

# PHASE B — examples: validate / minimal-fix / surface (EXECUTION)

> Needs a staged 0.5.0 wheel + Serverless. Controller stages the wheel (per [[bench-wheel-path-divergence]] / [[whl-change-rebuild-and-stage]]) and fires notebooks via the runner ([[fire-serverless-jobs-directly]]). Per-notebook, not one big run.

### Task B1: Validate + fix + surface each raster example

**Files:** `notebooks/examples/eo-series/03*.ipynb`, `eo-series/04*.ipynb`, `helios/03*.ipynb`, `vapor-eyes/02*.ipynb`, `vapor-eyes/03*.ipynb`, `xview/Clipping*.ipynb`; mirror narrative to `docs/docs/notebooks/*.mdx`.

- [ ] **Step 1: Stage the fresh 0.5.0 wheel to the EXAMPLE path**

Rebuild + stage the `0.5.0` wheel to **`/Volumes/geospatial_docs/geobrix/sample-data`** — the exact path the example notebooks' `%pip` cells install from (NOT only the artifact volume; see [[bench-wheel-path-divergence]] for the two-path trap). Verify the staged copy is fresh (grep a 0.5.0 marker + confirm size/mtime) before running any notebook. Confirm the version is `0.5.0`.

- [ ] **Step 1b: Bump each notebook's "Last Modified" header to today**

For every notebook touched, update its top "Last Modified" entry to **"August 03, 2026"** (in the same edit stroke as the code/narrative change; [[announce-altered-notebooks]]).

- [ ] **Step 2: Per notebook — run on the new default, fix only breaks**

For each notebook (one at a time): run on Serverless with the staged wheel. If it breaks because a now-virtual tile hits a heavy function or a bytes-expecting step, add the minimal `materialize()` (or `.option("virtualTiles","false")` where the example specifically wants bytes). Where the notebook is a natural place to show the virtual-tile behavior, add a brief markdown callout ([[notebook-narrative-tracks-code]] — update the section narrative in the same stroke as any code change; [[announce-altered-notebooks]] — name every notebook touched). Do NOT rewrite working examples.

- [ ] **Step 3: Confirm green + record**

Each notebook must run clean on the new default (the ship-readiness gate). Record the run per [[bench-run-give-summary-link]] discipline where a run link exists.

- [ ] **Step 4: Mirror narrative to the docs notebook pages + commit**

Update the corresponding `docs/docs/notebooks/*.mdx` where narrative changed. Commit per notebook or in a coherent batch:
```bash
git add notebooks/examples/... docs/docs/notebooks/...
git commit -m "examples: validate on virtual-tile default; materialize at heavy boundaries; surface concept

Co-authored-by: Isaac"
```

---

# PHASE C — benchmarking (EXECUTION)

> Needs staged 0.5.0 wheel + the bench cluster. Follow [[benchmarking-preflight-discipline]], [[cluster-bench-setup]], [[bench-wheel-path-divergence]], [[check-cluster-contention-correctly]], [[stop-clusters-you-start]].

### Task C1: Add a virtual leg to the bench harness

**Files:** `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (+ the functions bench module)

- [ ] **Step 1: Reader virtual leg**

Add a virtual reader measurement — a `run_format_read(..., extra_options={"virtualTiles":"true"})` path (or a `run_virtual_reader`) that times `spark.read.format(fmt).option("virtualTiles","true").load(path).count()` for the light readers. Keep the existing materialized path untouched (reused numbers).

- [ ] **Step 2: Tile-emitting functions virtual leg**

For the tile-emitting `rst_*` functions in the functions bench, add a virtual-output measurement (the function's default/auto output on a virtual input, or `virtualize_dir` where applicable). Do not re-run materialized.

- [ ] **Step 3: Unit-test the harness additions (local, no cluster)**

Run the bench unit tests scoped: `gbx:test:python --path python/geobrix/test/bench/ --log c1-bench.log`. Expected: PASS (harness constructs the virtual leg correctly).

- [ ] **Step 4: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/
git commit -m "bench: add virtual-tile reader + tile-function measurement legs

Co-authored-by: Isaac"
```

### Task C2: Run the virtual benchmarks on the cluster

**Files:** none (produces numbers).

- [ ] **Step 1: Pre-flight (7-point) + stage wheel + confirm cluster**

Per [[benchmarking-preflight-discipline]]: fresh 0.5.0 wheel staged to the bench path AND verified (grep marker on the staged copy), cluster RUNNING + libs INSTALLED, no contention on the bench cluster by cluster_id.

- [ ] **Step 2: Run readers + tile-emitting functions in virtual mode**

Run the virtual leg for all light readers + all tile-emitting functions at the standard scale ([[bench-1000-scale-only-now]], [[bench-iter-defaults]]). Verify rows>0 before trusting timings ([[bench-verify-nonzero-before-reporting]]). Give the run summary link ([[bench-run-give-summary-link]]).

- [ ] **Step 3: Capture the numbers** for the doc table (Task C3). Stop any cluster the controller started ([[stop-clusters-you-start]]).

### Task C3: Benchmarking doc — add the `light (virtual)` column

**Files:** `docs/docs/api/benchmarking.mdx`

- [ ] **Step 1: Extend the comparison table**

Add a `light (virtual)` column (marked **NEW DEFAULT**) to the readers/writers + functions comparison tables, populated from the Task C2 numbers. Keep the existing `light (materialized)` + `heavy` columns and their numbers unchanged (reused). Structure: `Operation | rows | light (virtual — default) | light (materialized) | heavy | ratio/notes`.

- [ ] **Step 2: Prose**

Add prose: virtual tiles are the new light-tier default; the primary win is ingest row-size (~100 B vs 148–527 KB, ~1,400–5,000× smaller) — the OOM-dissolving property — with a per-op lazy-read cost quantified from the run. Replace the "planned" placeholder note if the virtual profile now exists.

- [ ] **Step 3: Voice + build + commit**

Run the voice grep + `gbx-docs-static-build.sh`. 
```bash
git add docs/docs/api/benchmarking.mdx
git commit -m "docs(benchmarking): add light (virtual, new default) column + prose

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:**
- One-line default flip for all 3 readers + tests → A1; materialized-assuming test fallout → A2, A5. ✓
- NetCDF unchanged/documented → A1 test + A3 Step 3. ✓
- Reader docs: `virtualTiles` row (default true) + windowing surface + admonition → A3. ✓
- Orienting content (Readers Overview + Readers & Writers) + virtual-tiles page fix + quickstart + release-note breaking change → A4. ✓
- Examples validated-by-execution + minimal fix + surface → B1. ✓
- Benchmarking `light (virtual)` column + harness + cluster runs, reuse materialized/heavy → C1–C3. ✓
- Breaking-change framing, voice, docs-are-tests, no heavy change, no re-bench of materialized/heavy → Global Constraints. ✓

**Placeholder scan:** exact file:line for the flip; the doc rows/admonitions are given verbatim; the test bodies are concrete. The example fixes (B1) are intentionally "fix only what breaks on execution" — the break set can't be known until run, so the rule (materialize at heavy/bytes boundaries) is the spec, execution is the discovery.

**Type/consistency:** `emit_virtual`/`virtualTiles` naming consistent; the three readers named consistently; the option-table row + admonition reuse the existing `raster.mdx` table/admonition format verbatim.

**Sequencing:** Phase A is self-contained (code+tests+docs, no cluster) and must land first — B and C both depend on the flipped default + a staged 0.5.0 wheel. B (examples) and C (bench) are execution phases the controller paces with wheel-staging + cluster discipline; they can run in either order after A, but both need the fresh wheel staged.
