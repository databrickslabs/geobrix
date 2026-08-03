# Virtual Tiles capstone — hero diagram + docs page — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. NOTE: the diagram tasks are VISUAL and ITERATIVE — the human partner eyeballs the rendered PNG between the draft and polish passes; the controller must surface the render to the user, not fully auto-approve it.

**Goal:** Deliver the capstone for the v2 virtual-tile arc: a polished, slide-reusable hero diagram of the virtual-tile lifecycle, and a dedicated RasterX "Virtual Tiles" docs page (hero at top), with the Large Rasters page trimmed to summary+link.

**Architecture:** A new Python SVG generator under `resources/images/generators/` emits an SVG (reusing the palette + helper primitives from `rasterx-tile-structure.py`), rasterized to PNG via Chrome-headless (the established repo pipeline). A new MDX page `docs/docs/api/virtual-tiles.mdx` embeds the PNG (relative path, Docusaurus bundles it) and carries the concept prose; it is wired into the RasterX sidebar category. Large Rasters' virtual-tile concept content collapses to a summary+link. Docs + diagram only — NO Scala/Python behavior change.

**Tech Stack:** Python 3 (stdlib only — dataclasses/textwrap, matching the precedent generator), Chrome-headless for PNG rasterization, Docusaurus MDX. No new runtime deps.

## Global Constraints

- **Docs + diagram ONLY.** No product code (Scala/Python `src`) changes; no new registered functions; binding parity untouched.
- **User-facing voice.** No internal planning vocabulary in `docs/docs/` — no "wave N"/"inc N"/"increment N", no subagent/dispatch references. Quick check: `grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/` prints nothing.
- **Silent removal honored.** NO mention anywhere (docs or diagram) of the removed heavy-tier checkpoint / path-tile machinery; no "deprecated"/"no longer supported" about it.
- **Heavy-tier boundary stated present-tense, not permanent.** "A virtual tile passed to a heavyweight function raises a clear materialize-first error" — factual present tense; do NOT frame it as a permanent architectural truth (a future file type may change it).
- **Doc-test discipline (docs-are-tests).** Any EXECUTABLE code snippet on the page must live in `docs/tests/python/...` and be imported via `!!raw-loader!`; no inline unverified code. This is a concept/overview page — prefer prose + the diagram; reuse existing doc-test snippets; add a new one only if genuinely needed (with a real assertion).
- **Diagram fidelity (from the spec):** 4-stage left→right lifecycle (SOURCE → LOAD → OPERATE → WRITE); virtual/materialized is a tile-column BADGE, not a swimlane; COG carries the overview/pyramid glyph (NO separate "tiled+overviews" box); sources are usable as-is AND optionally optimizable (prepare→COG inner loop labeled "optional"); readers are the primary LOAD glyph; the windowed/tiled/parallel read pattern is visible; the bytes-free reference-rows-vs-bytes contrast is the centerpiece with real numbers; every OPERATE op shows an available "→ virtual (virtualize_dir)" branch (no op drawn materialize-only); the light↔heavy bridge callout is OMITTED; dark/light legible; stands alone for slides.
- **Real numbers (from the perf runs, [[light-virtual-tiling-by-reference]]):** virtual reference row ≈100 B vs materialized 148–527 KB (~1400–5000× smaller rows); materialized read ~12–24× faster/op; striped strip-inflation ~570× payload vs tiled for the same window. Use these; do not invent figures.

---

## File Structure

- Create: `resources/images/generators/virtual-tiles-lifecycle.py` — the SVG generator (reuses helper/palette patterns from `rasterx-tile-structure.py`).
- Create (generated): `resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg` + `.png`.
- Create: `docs/docs/api/virtual-tiles.mdx` — the new RasterX page.
- Modify: `docs/sidebars.js` — add `api/virtual-tiles` to the RasterX category after `api/large-rasters`.
- Modify: `docs/docs/api/large-rasters.mdx` — trim virtual-tile concept content to summary+link.
- (Only if a runnable snippet is needed) Create/extend: `docs/tests/python/api/<...>.py` doc-test source.

---

### Task 1: Build the hero-diagram SVG generator (structural draft)

**Files:**
- Create: `resources/images/generators/virtual-tiles-lifecycle.py`
- Generated: `resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg`

**Interfaces:**
- Produces: `render() -> str` (SVG string) and a `__main__` that writes the SVG to the default path (mirror `rasterx-tile-structure.py`'s `__main__`). Reuse the palette constants + `esc`/`text`/`mono`/`card`/`chip`/`top_stripe` helper style; add stage/glyph helpers as needed.

- [ ] **Step 1: Scaffold the generator from the precedent**

Copy the module skeleton conventions from `resources/images/generators/rasterx-tile-structure.py`: the module docstring with the re-render command, the palette block (`C_INK`, `C_BORDER`, accent/tint pairs), the SVG string helpers (`esc`, `text`, `mono`, `card`, `top_stripe`, `chip`), and the `__main__` writer. Set `CANVAS_W`/`PAD` for a wide hero (landscape, e.g. `CANVAS_W = 1600`, height ~ 900 — tune during render). Docstring must include the exact Chrome-headless render command (see Task 2) and the output path.

- [ ] **Step 2: Model the 4 stages as data (dataclasses)**

Define the diagram content as data (like the precedent's `Field`/`FIELDS`): a `Stage` and the per-stage items.
- SOURCE items: `striped GeoTIFF`, `tiled GeoTIFF`, `COG` (overview/pyramid glyph), `NetCDF`, `tabular` — each with a short label + glyph id.
- The prepare→COG inner loop (a labeled curved arrow, "optional: optimize", `prepare_cogs`).
- LOAD: reader glyph as primary; the fan-out-to-N-partitions/executors motif; v2 tile columns with two badge states (VIRTUAL = bytes-free path+window, light-only; MATERIALIZED = raster bytes). The reference-rows-vs-bytes contrast with the real numbers (≈100 B vs 148–527 KB).
- OPERATE items: `rst_clip`, `rst_transform`, `rst_merge`/mosaic, `rst_slope` — each label + glyph; each with an auto-default output shape AND a visible "→ virtual (virtualize_dir)" branch (the always-can-virtualize property).
- WRITE: `writers → file` (COG/GeoTIFF/NetCDF), `Databricks SQL → save to table`.

- [ ] **Step 3: Implement glyphs + stage renderers**

Write small `glyph_*(cx, cy, color, tint)` functions (mirror `glyph_hex`/`glyph_grid`/`glyph_kv` in the precedent): striped (horizontal bands), tiled (grid), COG (grid + stacked pyramid + small cloud), netcdf (layered cube), tabular (table w/ a highlighted tile column), reader (arrow-into-columns), virtual-tile badge (dashed/hollow + path·window), materialized-tile badge (filled + bytes), the four op glyphs, writer-file, and sql-table. Compose the left→right layout with connecting arrows; badge state threads b→c→d.

- [ ] **Step 4: Render the SVG (draft) and self-check it is well-formed**

Run: `python3 resources/images/generators/virtual-tiles-lifecycle.py`
Expected: writes `resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg`; prints `wrote <path>`. Sanity: the file starts with `<svg` and ends with `</svg>`, no unescaped `&`/`<` in text (the `esc()` helper handles it — confirm text goes through it).

- [ ] **Step 5: Commit the generator + draft SVG**

```bash
git add resources/images/generators/virtual-tiles-lifecycle.py resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg
git commit -m "feat(docs): virtual-tile lifecycle hero diagram generator (draft SVG)

Co-authored-by: Isaac"
```

---

### Task 2: Rasterize to PNG and surface the draft to the human partner

**Files:**
- Generated: `resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png`

**Interfaces:** none (asset only).

- [ ] **Step 1: Rasterize via Chrome-headless (2x scale)**

Run (macOS Chrome path, matching the precedent):
```bash
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --headless --disable-gpu --hide-scrollbars --force-device-scale-factor=2 \
  --window-size=1600,900 \
  --screenshot=resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png \
  resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg
```
(Match `--window-size` to the SVG canvas.) Then auto-crop whitespace with the PIL snippet from `example-diagrams.py`'s docstring.

- [ ] **Step 2: Surface the render to the human partner (HARD checkpoint)**

This is a VISUAL artifact. The controller MUST present the rendered PNG to the human partner (read/attach the image) and get a react-and-iterate response BEFORE polish. Do NOT auto-approve the draft. Capture the user's visual feedback as the input to Task 3. If dispatched via subagents, the controller surfaces the image itself — a subagent cannot judge "compelling."

- [ ] **Step 3: Commit the draft PNG**

```bash
git add resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png
git commit -m "feat(docs): render virtual-tile lifecycle hero diagram (draft PNG)

Co-authored-by: Isaac"
```

---

### Task 3: Polish the diagram per human feedback (iterate to final)

**Files:**
- Modify: `resources/images/generators/virtual-tiles-lifecycle.py`
- Re-generated: the `.svg` + `.png`

**Interfaces:** none.

- [ ] **Step 1: Apply the human partner's visual feedback**

Adjust layout/spacing/color/labels/glyphs in the generator per the feedback from Task 2 Step 2. Keep all spec-fidelity constraints (COG-collapsed, always-can-virtualize, bytes-free centerpiece, no light↔heavy callout, dark/light legible). Iterate: edit generator → re-render SVG+PNG → re-surface to the user → repeat until the user approves. (Each iteration re-runs Task 1 Step 4 + Task 2 Step 1.)

- [ ] **Step 2: Final render + verify legibility**

Re-run the SVG generator and the Chrome rasterize + crop. Confirm: text is legible at slide scale, dark and light backgrounds both readable (the palette uses ink-on-white cards — confirm it reads on a dark slide too, or provide the guidance that it sits on a white card), real numbers present and correct, no internal vocabulary in any label.

- [ ] **Step 3: Commit the final diagram**

```bash
git add resources/images/generators/virtual-tiles-lifecycle.py resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png
git commit -m "feat(docs): polish virtual-tile lifecycle hero diagram (final)

Co-authored-by: Isaac"
```

---

### Task 4: Write the Virtual Tiles docs page

**Files:**
- Create: `docs/docs/api/virtual-tiles.mdx`
- (If a runnable snippet is needed) Create/extend a `docs/tests/python/api/` doc-test source

**Interfaces:** none (docs).

- [ ] **Step 1: Author the page**

Create `docs/docs/api/virtual-tiles.mdx` with front-matter (title "Virtual Tiles", a `sidebar_position` consistent with the RasterX category). Structure per the spec:
1. Hero image at top: `![Virtual-tile lifecycle — prepare, read as bytes-free virtual tiles, operate, materialize/write](../../../resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png)` (relative path, same pattern as `tile-structure.mdx:21`).
2. **What a virtual tile is** — v2 struct `(cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)`; virtual = raster null + path/window; materialized = raster bytes; reference-vs-instruction principle.
3. **Why it matters** — OOM-dissolving ingest (real numbers: ≈100 B rows vs 148–527 KB; ~1400–5000× smaller; striped ~570× strip-inflation); windowed/tiled/parallel reads across executors, no driver collect.
4. **Lifecycle** — prepare (optional COG standardization) → read virtual → operate → materialize; mirror the diagram.
5. **Reader selection surface** — `virtualTiles`, `clipPolygons`/`windows`/`clipCrs` (JSON-list over `.option()`), `tileSize`/`overlapPercent`; link reader pages for full option detail (don't duplicate).
6. **Operating on tiles** — `virtualize_dir`/`virtualize_prefix`/`materialize`; the always-can-virtualize point; LINK the three-bucket taxonomy on Execution Tiers (don't duplicate).
7. **Tiers** — "lightweight tier is for light (virtual) raster tiles; heavyweight tier is for heavy (binary) raster tiles"; heavy accepts v1+v2 materialized, emits v2, virtual→clear materialize-first error (present tense, not permanent).
8. **See also** — Large Rasters, Execution Tiers, Readers/Writers overviews.

- [ ] **Step 2: Doc-test discipline for any executable snippet**

If the page includes any runnable code (e.g. a `spark.read.format(...).option("virtualTiles","true")` example), source it from a doc-test file via `!!raw-loader!` (pattern: `tile-structure.mdx:5-6`), and ensure that test executes with a real assertion (`gbx:test:python-docs` covers it). Prefer prose + linking existing examples; add new test code only if warranted. If no runnable snippet is added, note that explicitly (concept page = prose + diagram).

- [ ] **Step 3: Voice + build sanity**

Run: `grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/api/virtual-tiles.mdx ; echo "exit:$?"` — no matches. Also grep the new page for accidental "checkpoint"/"path-tile"/"deprecated"/"no longer supported" — none.

- [ ] **Step 4: Commit the page**

```bash
git add docs/docs/api/virtual-tiles.mdx docs/tests/python/api/ 2>/dev/null; git add docs/docs/api/virtual-tiles.mdx
git commit -m "docs: add RasterX Virtual Tiles concept page with hero diagram

Co-authored-by: Isaac"
```

---

### Task 5: Wire into the sidebar + trim Large Rasters

**Files:**
- Modify: `docs/sidebars.js`
- Modify: `docs/docs/api/large-rasters.mdx`

**Interfaces:** none.

- [ ] **Step 1: Add the page to the RasterX sidebar category**

In `docs/sidebars.js`, the RasterX category `items` are `['api/rasterio-distributed', 'api/large-rasters', 'api/h3-raster-tessellation']`. Insert `'api/virtual-tiles'` immediately after `'api/large-rasters'`:
```js
items: [
  'api/rasterio-distributed',
  'api/large-rasters',
  'api/virtual-tiles',
  'api/h3-raster-tessellation',
],
```

- [ ] **Step 2: Trim Large Rasters' virtual-tile concept content to summary+link**

In `docs/docs/api/large-rasters.mdx`, the sections `### Windows and virtual tiles` (~line 57) and `## Reading COGs and virtual tiles` (~line 180) carry conceptual virtual-tile material. Collapse the CONCEPT explanation to a short summary paragraph that links to the new page (e.g. "GeoBrix reads large rasters as **virtual tiles** — bytes-free references materialized on demand. See **[Virtual Tiles](./virtual-tiles)** for the full model and lifecycle."). KEEP the how-to depth that is specific to Large Rasters (format striping details, `prepare_cogs` usage, reader options, memory footprint). Do NOT delete the how-to; only de-duplicate the concept prose now owned by the new page. Update the `## See also` to point to Virtual Tiles.

- [ ] **Step 3: Verify no broken cross-links + voice**

Grep both files for the link targets resolve (`./virtual-tiles` from an `api/` page → `api/virtual-tiles.mdx` exists). Run the voice grep over `docs/docs/` (must be clean).

- [ ] **Step 4: Commit**

```bash
git add docs/sidebars.js docs/docs/api/large-rasters.mdx
git commit -m "docs: wire Virtual Tiles into RasterX sidebar; trim Large Rasters to link

Co-authored-by: Isaac"
```

---

### Task 6: Docs build + doc-test gate

**Files:** none (verification).

- [ ] **Step 1: Static docs build (catches broken MDX / links / missing image)**

Run: `bash scripts/commands/gbx-docs-static-build.sh` (or `gbx:docs:static-build`). Expected: build succeeds; the new page renders; the hero PNG is bundled (no missing-asset error); no broken-link errors for `./virtual-tiles` or the trimmed Large Rasters links.

- [ ] **Step 2: Doc-test suite IF a runnable snippet was added**

If Task 4 added executable code to the doc-test tree, run `bash scripts/commands/gbx-test-python-docs.sh --log capstone-docs.log` and confirm it passes. If no runnable snippet was added (pure concept page), state that and skip.

- [ ] **Step 3: Final voice + silent-removal sweep**

Run:
```bash
grep -rn -iE "wave [0-9]+|inc [0-9]+|increment [0-9]+" docs/docs/ ; echo "voice-exit:$?"
grep -rn -i "no longer supported\|deprecated" docs/docs/api/virtual-tiles.mdx docs/docs/api/large-rasters.mdx ; echo "silent-exit:$?"
```
Expected: voice grep prints nothing; the silent grep finds no NEW checkpoint/path-tile-removal framing on the two touched pages.

- [ ] **Step 4: (no commit — verification only; if the build surfaced a fix, commit it)**

If the build revealed a fix (broken link, image path), make the minimal fix and commit it with a clear message. Otherwise nothing to commit.

---

## Self-Review

**Spec coverage:**
- Hero diagram via repo SVG generator pipeline, draft-first-then-polish, human-in-loop → Tasks 1–3. ✓
- Diagram fidelity (4-stage, COG-collapsed, badge-not-lane, prepare-optional, readers-primary, windowed/parallel, bytes-free centerpiece + real numbers, always-can-virtualize, no light↔heavy callout, dark/light, standalone) → Global Constraints + Task 1 Steps 2–3 + Task 3. ✓
- Virtual Tiles page (8-section structure, hero at top) → Task 4. ✓
- Sidebar wiring + Large Rasters trim to summary+link → Task 5. ✓
- Doc-test discipline / voice / silent-removal / present-tense heavy boundary → Global Constraints + Tasks 4/6. ✓
- Docs + diagram only, no product code → Global Constraints; no task touches `src`. ✓

**Placeholder scan:** the generator task references the exact precedent file + helpers to reuse (no "write a diagram" hand-wave); the render command is concrete; the MDX embed path + the sidebar edit are exact. The one genuinely open-ended part — visual polish — is intentionally an iterative human-in-loop loop (Task 3), not a placeholder.

**Type/consistency:** the hero PNG path is identical in the generator output, the render command, and the MDX embed (`resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png`). The page id `api/virtual-tiles` is consistent across the file path, the sidebar entry, and the `./virtual-tiles` cross-links.

**Execution note:** Tasks 1–3 (diagram) are visual/iterative and REQUIRE the human partner to eyeball renders — the controller surfaces the PNG and loops on feedback; these are not fully subagent-autonomous. Tasks 4–6 (page/sidebar/trim/build) are standard and subagent-friendly.
