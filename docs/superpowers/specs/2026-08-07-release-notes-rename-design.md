# "Beta Release Notes" → "Release Notes" Rename + De-Beta (design)

**Date:** 2026-08-07
**Status:** ratified (this doc), pending plan

## Goal

Rename the "Beta Release Notes" docs page to "Release Notes" (display title/heading AND
URL slug `beta-release-notes` → `release-notes`), remove user-facing "beta" mentions across
the docs (keeping the API-stability caveat), and preserve old external/bookmarked links via
a build-time redirect. Hard rule: **no internal doc reference links to the beta URL** — the
string `beta-release-notes` survives in exactly one place, the redirect config.

## Constraints

- **CI installs docs deps with `npm ci`** (`.github/workflows/deploy-docs.yml:58`,
  `package-geobrix-artifacts.yml:232`), which installs strictly from `package-lock.json` and
  FAILS if `package.json` and the lockfile disagree. Any new dependency must update BOTH
  `docs/package.json` and `docs/package-lock.json` in lockstep.
- Docusaurus core is `@docusaurus/core@^3.1.0` — use `@docusaurus/plugin-client-redirects@^3.x`
  (matching major).
- Keep the **API-stability caveat** ("APIs may change to stabilize, no function aliases") —
  it is a true, still-relevant engineering policy independent of the "beta" brand.
- Do NOT touch: `docs/superpowers/plans|specs/*` (historical, describe past work, not
  user-facing, not linked), the git branch `beta/0.5.0`, `developers.mdx` (`beta/0.4.0` is a
  branch-name example, not a beta-status claim).
- User-facing docs voice rules still apply (no wave/internal vocabulary — QC `internals-leak`).

## Components

### 1. Page identity (title + slug)
- `git mv docs/docs/beta-release-notes.mdx docs/docs/release-notes.mdx` (doc id + URL become
  `release-notes`; no explicit `slug:` frontmatter exists, so the filename drives the URL).
- Frontmatter `title: Beta Release Notes` → `title: Release Notes`.
- H1 `# Beta Release Notes` → `# Release Notes`.
- `docs/sidebars.js:22`: `'beta-release-notes'` → `'release-notes'`.

### 2. Old-URL redirect (external/historic links only)
- Add `@docusaurus/plugin-client-redirects` (`^3.x`) to `docs/package.json` `dependencies`
  AND regenerate/update `docs/package-lock.json` so `npm ci` succeeds. Install routes through
  the corp npm proxy (`db-npm-proxy`) per repo norms; run `npm install` in the container/host
  to produce a consistent lockfile.
- Register in `docs/docusaurus.config.js` `plugins`:
  ```js
  ['@docusaurus/plugin-client-redirects', { redirects: [{ from: '/beta-release-notes', to: '/release-notes' }] }]
  ```
- This emits a build-time redirect stub at `/beta-release-notes` → `/release-notes`. Invisible
  in nav. This is the ONLY permitted surviving occurrence of the `beta-release-notes` string.

### 3. De-beta the content (keep the stability caveat)
- **release-notes.mdx body:**
  - Line ~12: drop "since the GeoBrix project started / After the project is approved, formal
    release notes will take over" framing → "This page tracks **API and naming changes** across
    GeoBrix releases — the single place to look up what changed and why."
  - Line ~242: drop the "Beta (0.1.x) may still expose…" clause (reword to just the 0.2.0
    rename note without the beta framing).
  - Line ~295 ("**After approval:** Move content into formal release notes … historical
    beta-only changes"): reword to drop "approval"/"beta-only" (e.g. a neutral "per-version
    sections" maintenance note, or remove the bullet if it no longer applies).
- **security.mdx:192** — drop the "Beta" label, KEEP the caveat:
  "GeoBrix APIs may change to stabilize, and there are no function aliases — one canonical
  name per function." Line 195: link text "Beta Release Notes" → "Release Notes", path
  `./beta-release-notes` → `./release-notes`.
- **limitations.mdx:10** — "GeoBrix Beta has some known limitations…" → "GeoBrix has some
  known limitations…". Line 16: "The Beta does not yet support…" → "GeoBrix does not yet
  support…".
- **api/raster-functions.mdx:72** — link text "Beta Release Notes" → "Release Notes", URL
  `../beta-release-notes#whats-new-in-v030` → `../release-notes#whats-new-in-v030` (anchor
  preserved).
- **readers/pmtiles.mdx:28** — this is PROSE, not a markdown link: it names the page in
  backticks as `` `beta-release-notes` note that PMTiles "read is not yet supported" ``.
  Reword to name the page normally: "the **Release Notes** page's note that PMTiles read…"
  (drop the backtick-slug entirely — no link, no slug string).
- **CHANGELOG.md:3** — link text "Beta Release Notes" → "Release Notes", path
  `docs/docs/beta-release-notes.mdx` → `docs/docs/release-notes.mdx`.

### 3b. PMTiles read/write tier-clarity (content fix, folds into the same docs sweep)

Purpose: make sure users understand the **lightweight tier has PMTiles readers/writers where
the heavyweight tier does not**. NO code change — verified `PMTiles_Table.scala:41` still
raises "Reading PMTiles archives is not supported"; the lightweight `pmtiles_gbx` reader IS a
supported read path (already documented in `readers/pmtiles.mdx` and release-notes). The
issue is three notes that state "read not supported" flatly, without qualifying it's the
HEAVYWEIGHT path and steering to the lightweight reader. Reword each to: (a) scope the
limitation to heavyweight `spark.read.format("pmtiles")`, (b) point to the lightweight
`pmtiles_gbx` reader as the supported in-GeoBrix read path, (c) keep the JS/Python client
mention as an additional option. Also fix the stale "0.4.0" → "0.5.0" version in these notes.

- **writers/pmtiles.mdx:266** ("Reading PMTiles is not supported in GeoBrix 0.4.0 …") — reword
  to: heavyweight `spark.read.format("pmtiles")` is not supported; for reading in GeoBrix, use
  the lightweight [`pmtiles_gbx` reader](../readers/pmtiles) (reads tiles from an archive or
  builds a mosaic from rasters); the JS/Python pmtiles clients remain an option for
  browser/inspection use. Drop the "0.4.0" version pin.
- **api/pmtiles-functions.mdx:230** ("**No read path.** …") — reword: no HEAVYWEIGHT read path
  (`spark.read.format("pmtiles")` raises); use the lightweight `pmtiles_gbx` reader for reading
  within GeoBrix, or the JS/Python clients externally. Drop "0.4.0".
- **writers/overview.mdx:255** ("Read support is not implemented in 0.4.0.") — reword: the
  heavyweight PMTiles DataSource is write-only; read via the lightweight `pmtiles_gbx` reader.
  Drop "0.4.0".
- (`readers-writers.mdx:28` already shows PMTiles as `pmtiles_gbx` / "— (light-only)" — correct,
  no change.)

This component is docs-only and independent of the beta rename; it is bundled here because it
touches the same docs sweep and the same `pmtiles.mdx` file.

### 4. Non-doc live refs (must stay valid; not user-facing)
- `docs/scripts/check-release-notes-functions.py:14,35` — `RELEASE_NOTES` path +
  docstring → `docs/docs/release-notes.mdx`.
- `.claude/qc-judge/config.json:2` — `release_notes_path` →
  `docs/docs/release-notes.mdx` (else the QC release-notes-current check reads a missing file).
- `CLAUDE.md:7,22` — two path mentions → `docs/docs/release-notes.mdx`.

## Testing / acceptance

1. **Slug-leak check (hard):**
   `grep -rn "beta-release-notes" docs/docs/ docs/sidebars.js docs/docusaurus.config.js CHANGELOG.md CLAUDE.md docs/scripts/ .claude/`
   → exactly ONE hit: the redirect `from: '/beta-release-notes'` in docusaurus.config.js.
2. **Beta-word check:** `grep -rniE "\bbeta\b" docs/docs/` → no beta-STATUS claims remain
   (a stray legitimate non-status use, if any, is judged individually; expect none).
3. **Docs build:** `cd docs && npm run build` succeeds — validates the redirect plugin config
   and lockfile consistency (`npm ci` path). NOTE: `onBrokenLinks: 'warn'` (config line 27),
   so a broken internal link does NOT fail the build — it only prints a warning. Therefore the
   build step must **capture stdout and assert zero "Broken link" warnings** mentioning
   `release-notes` or `beta-release-notes` (grep the build log), not rely on a non-zero exit.
   The slug-leak check (#1) is the primary guard; this build-log grep is the secondary net.
4. **QC config sanity:** `.claude/qc-judge/config.json` `release_notes_path` points at an
   existing file.
5. **Redirect present:** after build, `docs/build/beta-release-notes/index.html` exists and
   redirects to `/release-notes` (the plugin's stub).
6. **PMTiles tier-clarity:** each of the three reworded notes (writers/pmtiles.mdx,
   api/pmtiles-functions.mdx, writers/overview.mdx) now (a) scopes the "no read" limitation to
   the heavyweight path and (b) names the lightweight `pmtiles_gbx` reader as the supported
   in-GeoBrix read path. `grep -rniE "pmtiles.*not supported|read.*not (yet )?(supported|implemented)" docs/docs/`
   shows no note that fails to steer to the lightweight reader. No "0.4.0" version pin remains
   in these three notes (`grep -rn "0.4.0" docs/docs/writers/pmtiles.mdx docs/docs/api/pmtiles-functions.mdx docs/docs/writers/overview.mdx` → none in the read notes).

## Out of scope

- Renaming the git branch or any `beta/*` reference in `developers.mdx` (branch-name examples).
- Rewriting `docs/superpowers/plans|specs/*` (historical design docs; may reference the old
  filename as a record of past work).
- Any content change beyond removing beta-status framing (no restructuring of the release
  notes themselves).

## Outcome

The page is titled and served as "Release Notes" at `/release-notes`; the old
`/beta-release-notes` URL redirects there for anyone with a bookmark; no doc links to the old
URL; user-facing docs no longer describe GeoBrix as "beta" while retaining the honest
API-stability caveat.
