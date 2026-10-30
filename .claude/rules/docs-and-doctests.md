---
paths:
  - "docs/docs/**"
  - "docs/tests/**"
  - "docs/scripts/**"
---

# Docs & doc-tests conventions

## Doc tests are the documentation source (single source of truth)

Tests ARE the documentation source, not validators of it. Docs import code from tests via webpack raw-loader.

- Code lives in `docs/tests/python/` and `docs/tests/scala/`.
- MDX imports via: `import code from '!!raw-loader!../../tests/python/module/file.py';` (from `docs/docs/<subdir>/`).
- Tests **must execute real code with real assertions** — not just check structure or compilation. Use real sample data from `/Volumes/main/geobrix_samples/geobrix-examples/{nyc,london}/`.
- Run doc tests in Docker via `gbx:test:*-docs` commands. Doc tests **only run in Docker** (need full env + sample data).
- Do not mock Spark, GeoBrix, or file I/O. Mock only external APIs / very expensive ops / flaky deps.
- Doc-test iteration: **run per-package with its own log, narrow to failing test node IDs, rerun only those until green** — don't retest passing packages.
- Package-source changes need the unit suite, not just doc-tests: a change to `.../{pyrx,pyvx,pygx}/functions.py` must be verified with `gbx:test:pyrx` (etc.) on the affected `python/geobrix/test/**` files.

## User-facing docs voice (no internal vocabulary)

Anything under `docs/docs/` is read by end users. Never leak internal release-planning vocabulary.

| ❌ Don't write | ✅ Write instead |
|---|---|
| "Composes with `gbx_pmtiles_agg` (Wave N)" | "Composes with `gbx_pmtiles_agg`" |
| "the Wave N aggregator" | "the aggregator" or `gbx_st_asmvt` |
| references to internal subagents or dispatch sequencing | reference behavior, not the process |

**Wave numbers** are legitimate only in: `.superpowers/prompts/features/*.md`, dispatch prompts, git commit messages, `.superpowers/input/` (all internal/gitignored).

Quick check before merging: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null` should print nothing. The QC judge enforces this via the `internals-leak` check.

## Concise, clear prose (STE-lite)

Docs stay in user narrative voice. Apply these writing rules from ASD-STE100 (Simplified
Technical English). They are the rules only, not its controlled dictionary.

- **Sentence length.** A procedure step has at most 20 words; a description has at most 25. Split
  longer sentences. A table cell or a code comment is not a sentence.
- **One instruction per sentence.** Write procedures in the imperative: "Install the env wheel."
  Do not write "You will want to install".
- **Active voice.** Name who or what acts: "The reader skips the file", not "The file is skipped".
- **One topic per paragraph,** at most 6 sentences. Use a list for 3 or more parallel items.
- **Say the result first, then the reason.** Cut how it was found, review history and internal
  mechanics that do not change what the user does.
- **One term per concept.** Use the term list below; do not alternate synonyms for variety.
- **Keep every fact a user acts on:** a breaking change, a changed default, a new parameter, a
  refusal and its fix, a version pin. Concise means fewer words, not fewer facts.

Term list (start; extend it when you settle a term):

| Use | Not |
|---|---|
| lightweight / heavyweight (API, function, reader) | light tier, heavy tier, light-tier |
| env wheel (`+env5`, `+env6`) | runtime wheel, flavor wheel |
| tile (the GeoBrix struct) | chip, raster struct (except where the API name says "chip") |
| Volume (UC Volume) | volume, UC volume path |
| Serverless | serverless compute, SL |
| dedicated / standard (access mode) | single user, shared, USER_ISOLATION (except in a quoted setting) |

## Release notes layout

- `release-notes.mdx` is the **summary** page: the current-version banner (`gbx:versions:bump`
  edits its "Current version:" string) and, per release, a few key-feature bullets that link to
  the detail.
- The **detail** lives on one subpage per series: `release-notes-0-6.mdx` for 0.6.x, and
  `release-notes-0-5-and-earlier.mdx` for v0.5.2 and every earlier release.
- A new change goes on its series subpage. Add a bullet on the summary page only for a key
  feature or a breaking change.
- A new series gets a new subpage, wired into `sidebars.js` and listed in
  `docs/scripts/check-release-notes-functions.py` and the exclusion list in
  `scripts/commands/gbx-versions-bump.sh`.

## MDX gotchas

- **Bare angle-bracket types** in MDX PROSE (`STRUCT<…>`, `Array<…>`) are parsed as JSX and CRASH the build — backtick them.
- `onBrokenLinks=warn` LIES (build succeeds with broken links). Grep the build output for `broken links found`; use `--strict` to FAIL.
- **New `docs/docs/**.mdx` pages MUST be added to `sidebars.js` in the same stroke.**
- Reserve port 3000 for the user; agents use `--port 3001`. It's OK to stop `gbx:docs:dev` for a build, but ALWAYS restart it.
- Function-count badges (README) + `intro.mdx` counts are HARDCODED — refresh on function add/remove + release.
- Docs example guards (QC) check ASCII output tables + tab completeness; bindings are auto-derived.

## Docs deploy

Docs deploy is **main-only**. `npm ci` ECONNRESET usually = stray npm-proxy hosts in `package-lock.json`.
