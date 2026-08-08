# "Beta Release Notes" → "Release Notes" Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the docs "Beta Release Notes" page to "Release Notes" (title + URL slug), remove user-facing "beta" mentions (keeping the API-stability caveat), redirect the old URL, clarify that PMTiles read/write is a lightweight-tier capability, and ensure no internal doc links to the old beta URL.

**Architecture:** Docs-only, no Scala/Python source. Task 1 does the structural rename (file move, slug, sidebar, redirect plugin + lockfile, config) and repoints the non-doc references that would break (QC config, release-notes checker, CLAUDE.md). Task 2 is the user-facing content sweep (de-beta prose + PMTiles tier-clarity) and the final acceptance checks. Split here because a reviewer could accept the structural rename while rejecting a content reword, or vice versa.

**Tech Stack:** Docusaurus 3.x (`@docusaurus/core@^3.1.0`), `@docusaurus/plugin-client-redirects`, npm (`npm ci` in CI). Docs live under `docs/`.

## Global Constraints

- **CI installs docs deps with `npm ci`** (`.github/workflows/deploy-docs.yml:58`, `package-geobrix-artifacts.yml:232`) — strict lockfile install; `package.json` and `package-lock.json` MUST agree or CI fails. Any dependency add updates BOTH.
- Use `@docusaurus/plugin-client-redirects@^3.1.0` (match `@docusaurus/core@^3.1.0`).
- **Keep the API-stability caveat** ("APIs may change to stabilize; no function aliases") — only drop the "Beta" label, not the warning.
- `onBrokenLinks: 'warn'` (`docusaurus.config.js:27`) — a broken internal link does NOT fail the build; acceptance greps the build log for broken-link warnings.
- **Do NOT touch:** `docs/superpowers/plans|specs/*` (historical), the git branch `beta/0.5.0`, `developers.mdx` (`beta/0.4.0` branch-name examples).
- **PMTiles: NO code change.** `PMTiles_Table.scala:41` still raises "read not supported" for the heavyweight path; the lightweight `pmtiles_gbx` reader IS supported. This is a docs-wording fix only.
- **Hard acceptance:** the string `beta-release-notes` survives in exactly ONE place — the redirect `from:` in `docusaurus.config.js`.
- `git add` EXPLICIT paths only — NEVER `git add -A` (strays: `.isaac/`, `.tmp`, `scratchpad/`, zips). Commit ≤72-char subject + WHY body + `Co-authored-by: Isaac`.
- No Databricks profile needed. NEVER run `databricks auth login`.

---

### Task 1: Structural rename — file move, slug, sidebar, redirect plugin, non-doc refs

**Files:**
- Rename: `docs/docs/beta-release-notes.mdx` → `docs/docs/release-notes.mdx` (via `git mv`)
- Modify: `docs/docs/release-notes.mdx` (frontmatter `title` + H1 only in this task)
- Modify: `docs/sidebars.js` (line 22)
- Modify: `docs/docusaurus.config.js` (plugins array — add redirect)
- Modify: `docs/package.json` (add dep) + `docs/package-lock.json` (regenerate)
- Modify: `docs/scripts/check-release-notes-functions.py` (lines 14, 35)
- Modify: `.claude/qc-judge/config.json` (line 2)
- Modify: `CLAUDE.md` (lines 7, 22)

**Interfaces:**
- Produces: the page served at `/release-notes` (doc id `release-notes`); old `/beta-release-notes` redirects to it. Task 2 relies on the file being at `release-notes.mdx` and the `#whats-new-in-v030` anchor still existing (heading unchanged, only H1 text changes).

- [ ] **Step 1: Move the file**

```bash
git mv docs/docs/beta-release-notes.mdx docs/docs/release-notes.mdx
```

- [ ] **Step 2: Rename title + H1 in the moved file**

In `docs/docs/release-notes.mdx`: frontmatter line 3 `title: Beta Release Notes` → `title: Release Notes`; H1 line 6 `# Beta Release Notes` → `# Release Notes`. (Leave the body prose for Task 2.)

- [ ] **Step 3: Update the sidebar id**

In `docs/sidebars.js` line 22: `'beta-release-notes',` → `'release-notes',`.

- [ ] **Step 4: Add the redirect plugin dependency + lockfile**

Add to `docs/package.json` `dependencies` (alphabetical position near the other `@docusaurus/*` entries):
```json
"@docusaurus/plugin-client-redirects": "^3.1.0",
```
Then regenerate the lockfile so `npm ci` will succeed:
```bash
cd docs && npm install
```
This must update `docs/package-lock.json` to include the new package. **If `npm install` cannot reach the registry** (corp proxy unavailable in this environment), STOP and report that the lockfile could not be generated — do NOT hand-edit `package-lock.json` (a malformed lockfile breaks `npm ci` worse than a missing dep). The rest of the task can proceed only once the lockfile is consistent.

- [ ] **Step 5: Register the redirect in docusaurus.config.js**

In `docs/docusaurus.config.js`, the `plugins:` array currently ends with:
```js
    // Convert absolute paths to relative in HTML/CSS so static zip works when opening index.html from any folder (file://)
    ...(process.env.DOCS_STATIC_ZIP === '1' ? ['@someok/docusaurus-plugin-relative-paths'] : []),
  ],
```
Add the redirect plugin as a new entry in that array (before the closing `],`):
```js
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          { from: '/beta-release-notes', to: '/release-notes' },
        ],
      },
    ],
```

- [ ] **Step 6: Repoint the non-doc references (must stay valid)**

- `docs/scripts/check-release-notes-functions.py`: line 35 `RELEASE_NOTES = REPO_ROOT / "docs/docs/beta-release-notes.mdx"` → `"docs/docs/release-notes.mdx"`; line 14 docstring mention `docs/docs/beta-release-notes.mdx` → `docs/docs/release-notes.mdx`.
- `.claude/qc-judge/config.json` line 2: `"release_notes_path": "docs/docs/beta-release-notes.mdx",` → `"docs/docs/release-notes.mdx",`.
- `CLAUDE.md` line 7 (`See \`docs/docs/beta-release-notes.mdx\` for breaking changes.`) and line 22 (`\`release_notes_path\` points at \`docs/docs/beta-release-notes.mdx\``): both `beta-release-notes.mdx` → `release-notes.mdx`.

- [ ] **Step 7: Build the docs to validate the rename + redirect**

Run: `cd docs && npm run build 2>&1 | tee /tmp/docs-build-task1.log`
Expected: build succeeds. Because `onBrokenLinks: 'warn'`, ALSO grep the log:
`grep -iE "broken link|release-notes" /tmp/docs-build-task1.log` — there will still be broken-link warnings from `security.mdx` / `raster-functions.mdx` / `CHANGELOG` until Task 2 fixes those links; that is expected at THIS step (Task 1 didn't touch those link bodies). What MUST hold now: the build completes, `docs/build/release-notes/index.html` exists, and `docs/build/beta-release-notes/index.html` exists as a redirect stub to `/release-notes`.
Verify the redirect stub: `grep -i "release-notes" docs/build/beta-release-notes/index.html` (should show a meta-refresh / canonical to `/release-notes`).

- [ ] **Step 8: Commit**

```bash
git add docs/docs/release-notes.mdx docs/sidebars.js docs/docusaurus.config.js \
        docs/package.json docs/package-lock.json \
        docs/scripts/check-release-notes-functions.py .claude/qc-judge/config.json CLAUDE.md
git commit -m "docs: rename Beta Release Notes page to Release Notes (slug + redirect)

git mv beta-release-notes.mdx -> release-notes.mdx; title/H1 -> Release
Notes; sidebar id updated. Add @docusaurus/plugin-client-redirects
(package.json + lockfile) redirecting /beta-release-notes -> /release-notes
for old links. Repoint the QC release_notes_path, the release-notes checker,
and CLAUDE.md references. Content de-beta follows in the next commit.

Co-authored-by: Isaac"
```
Note: `git mv` stages the rename; the moved file's title/H1 edit is part of the same `git add`.

---

### Task 2: Content sweep — de-beta prose, fix links, PMTiles tier-clarity, acceptance

**Files:**
- Modify: `docs/docs/release-notes.mdx` (body prose: lines ~12, ~242, ~295)
- Modify: `docs/docs/security.mdx` (lines 192-195)
- Modify: `docs/docs/limitations.mdx` (lines 10, 16)
- Modify: `docs/docs/api/raster-functions.mdx` (line 72 link)
- Modify: `docs/docs/readers/pmtiles.mdx` (line 28 prose)
- Modify: `CHANGELOG.md` (line 3 link)
- Modify: `docs/docs/writers/pmtiles.mdx` (line 266)
- Modify: `docs/docs/api/pmtiles-functions.mdx` (line 230)
- Modify: `docs/docs/writers/overview.mdx` (line 255)

**Interfaces:**
- Consumes: the page now at `docs/docs/release-notes.mdx` served at `/release-notes` (Task 1); the `#whats-new-in-v030` anchor still valid.
- Produces: zero `beta-release-notes` references outside the redirect config; no beta-status prose; PMTiles read/write clarified as lightweight-capable.

- [ ] **Step 1: De-beta the release-notes.mdx body**

- Line ~12, replace:
  `This page tracks **API and naming changes** since the GeoBrix project started. After the project is approved, formal release notes will take over; until then, use this as the single place to look up what changed and why.`
  with:
  `This page tracks **API and naming changes** across GeoBrix releases — the single place to look up what changed and why.`
- Line ~242, replace:
  `Reader renames above are planned for 0.2.0. Beta (0.1.x) may still expose the baseline names in some contexts.`
  with:
  `Reader renames above landed in 0.2.0; earlier 0.1.x releases may still expose the baseline names in some contexts.`
- Line ~295, replace the bullet:
  `- **After approval:** Move content into formal release notes (e.g. per-version sections) and keep this page for historical beta-only changes, or retire it.`
  with:
  `- **Housekeeping:** Keep per-version sections here as the canonical change log; prune superseded interim notes as versions settle.`

- [ ] **Step 2: De-beta security.mdx (KEEP the caveat) + fix the link**

Lines 192-195, replace:
```
GeoBrix is **Beta** — APIs may break to stabilize, and there are no function
aliases. Pin the exact wheel and JAR version in your cluster configuration
and only bump deliberately. See the
[Beta Release Notes](./beta-release-notes) for the change list per version.
```
with:
```
GeoBrix APIs may change to stabilize, and there are no function aliases —
one canonical name per function. Pin the exact wheel and JAR version in your
cluster configuration and only bump deliberately. See the
[Release Notes](./release-notes) for the change list per version.
```

- [ ] **Step 3: De-beta limitations.mdx**

- Line 10: `GeoBrix Beta has some known limitations that will be addressed in future releases.` → `GeoBrix has some known limitations that will be addressed in future releases.`
- Line 16: `The Beta does not yet support Databricks Spatial Types directly but is standardized to WKB or WKT where geometries are involved.` → `GeoBrix does not yet support Databricks Spatial Types directly but is standardized to WKB or WKT where geometries are involved.`

- [ ] **Step 4: Fix the raster-functions.mdx link (text + slug + anchor preserved)**

Line 72: `See [Beta Release Notes](../beta-release-notes#whats-new-in-v030) for the v0.3.0 correctness fix` → `See [Release Notes](../release-notes#whats-new-in-v030) for the v0.3.0 correctness fix`.

- [ ] **Step 5: Reword pmtiles.mdx:28 prose (drop the backtick-slug)**

Line 28 currently: `` `beta-release-notes` note that PMTiles "read is not yet supported" refers to the `` (part of a sentence spanning lines 27-31). Reword the clause to drop the slug and name the page: change `` `beta-release-notes` note that PMTiles "read is not yet supported" refers to the `` → `the Release Notes' note that heavyweight PMTiles read "is not yet supported" refers to the`. (Keep the rest of the sentence: "…refers to the **heavyweight** `spark.read.format("pmtiles")` path — the lightweight `pmtiles_gbx` reader documented here **is** a supported read path.")

- [ ] **Step 6: Fix CHANGELOG.md link**

Line 3: `See [Beta Release Notes](docs/docs/beta-release-notes.mdx) for API and naming changes.` → `See [Release Notes](docs/docs/release-notes.mdx) for API and naming changes.`

- [ ] **Step 7: PMTiles tier-clarity — writers/pmtiles.mdx:266**

Replace:
`Reading PMTiles is not supported in GeoBrix 0.4.0 — \`spark.read.format("pmtiles")\` raises a friendly "Reading PMTiles archives is not supported in GeoBrix 0.4.0" error. Use one of the client libraries instead:`
with:
`Reading PMTiles through the **heavyweight** DataSource is not supported — \`spark.read.format("pmtiles")\` raises a friendly error. To read PMTiles **within GeoBrix**, use the lightweight [\`pmtiles_gbx\` reader](../readers/pmtiles), which reads tiles from an existing archive or builds a mosaic pyramid from rasters. For browser/inspection use, the client libraries also work:`

- [ ] **Step 8: PMTiles tier-clarity — api/pmtiles-functions.mdx:230**

Replace:
`- **No read path.** \`spark.read.format("pmtiles")\` raises a friendly "Reading PMTiles archives is not supported in GeoBrix 0.4.0" error — use one of the JS / Python pmtiles client libraries for read access.`
with:
`- **No heavyweight read path.** \`spark.read.format("pmtiles")\` raises a friendly error. Read within GeoBrix via the lightweight [\`pmtiles_gbx\` reader](../readers/pmtiles); the JS / Python pmtiles client libraries also work for external read access.`

- [ ] **Step 9: PMTiles tier-clarity — writers/overview.mdx:255**

Replace:
`- **Output path:** the final \`.pmtiles\` file, not a directory. Read support is not implemented in 0.4.0.`
with:
`- **Output path:** the final \`.pmtiles\` file, not a directory. The heavyweight DataSource is write-only; read PMTiles via the lightweight \`pmtiles_gbx\` reader.`

- [ ] **Step 10: Acceptance — slug-leak, beta-word, PMTiles, build**

Run each; all must pass:
```bash
# (1) slug survives ONLY in the redirect config → exactly 1 hit
grep -rn "beta-release-notes" docs/docs/ docs/sidebars.js docs/docusaurus.config.js CHANGELOG.md CLAUDE.md docs/scripts/ .claude/
#   expect: exactly one line — docusaurus.config.js  from: '/beta-release-notes'

# (2) no beta-STATUS claims in user-facing docs
grep -rniE "\bbeta\b" docs/docs/
#   expect: no beta-status prose (developers.mdx branch-name examples live OUTSIDE this grep's
#   concern only if they appear — if 'beta/0.x' branch examples show, they are allowed; judge each)

# (3) PMTiles notes steer to the lightweight reader; no stale 0.4.0 in the read notes
grep -rniE "pmtiles.*not supported|read.*not (yet )?(supported|implemented)" docs/docs/
grep -rn "0.4.0" docs/docs/writers/pmtiles.mdx docs/docs/api/pmtiles-functions.mdx docs/docs/writers/overview.mdx
#   expect: each read-note mentions pmtiles_gbx / lightweight; no 0.4.0 in the three read notes

# (4) build clean of broken-link warnings for the renamed page
cd docs && npm run build 2>&1 | tee /tmp/docs-build-task2.log; cd ..
grep -iE "broken link" /tmp/docs-build-task2.log
#   expect: no broken-link warning naming release-notes or beta-release-notes
```
For (2): the only acceptable `\bbeta\b` hits are none in `docs/docs/`. If `developers.mdx` shows `beta/0.4.0` (a branch example), that is out of scope per the spec — leave it, note it. For (1): if any hit other than the redirect config appears, fix it before committing.

- [ ] **Step 11: Commit**

```bash
git add docs/docs/release-notes.mdx docs/docs/security.mdx docs/docs/limitations.mdx \
        docs/docs/api/raster-functions.mdx docs/docs/readers/pmtiles.mdx CHANGELOG.md \
        docs/docs/writers/pmtiles.mdx docs/docs/api/pmtiles-functions.mdx docs/docs/writers/overview.mdx
git commit -m "docs: de-beta user-facing docs; clarify PMTiles is light-tier read/write

Remove 'beta' status wording (keep the API-stability caveat), relabel and
repoint all Release Notes links to /release-notes (zero links to the old
beta slug), and reword the three PMTiles 'read not supported' notes to scope
the limit to the heavyweight DataSource and steer to the lightweight
pmtiles_gbx reader (drops stale 0.4.0 pins). Docs-only, no code change.

Co-authored-by: Isaac"
```

---

## Self-Review

**Spec coverage:** Component 1 (page identity) → Task 1 Steps 1-3. Component 2 (redirect) → Task 1 Steps 4-5, verified Step 7. Component 3 (de-beta content) → Task 2 Steps 1-6. Component 3b (PMTiles clarity) → Task 2 Steps 7-9. Component 4 (non-doc refs) → Task 1 Step 6. All 6 acceptance checks → Task 2 Step 10 (+ Task 1 Step 7 for the redirect stub). All spec sections covered. ✓

**Placeholder scan:** every reword has verbatim before/after. The `npm install` step has an explicit "STOP and report if the registry is unreachable" branch rather than a vague fallback. No TBD/TODO. ✓

**Type/consistency:** the slug `release-notes` is used identically across sidebar id, all link paths, redirect `to:`, and non-doc ref paths. The `#whats-new-in-v030` anchor is preserved (H1 text change doesn't alter the `## What's new in v0.3.0` heading that generates it — confirm the anchor heading is untouched). Link label "Release Notes" used consistently. ✓

**Ordering:** Task 1 must precede Task 2 (Task 2's links point at the moved file; the redirect + slug must exist first). Task 1 Step 7's build will show *expected* residual broken-link warnings (from the not-yet-fixed links in security/raster-functions/CHANGELOG) — that is called out so the implementer doesn't chase them; Task 2 Step 10's build must be clean.
