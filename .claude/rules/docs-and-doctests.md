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

## MDX gotchas

- **Bare angle-bracket types** in MDX PROSE (`STRUCT<…>`, `Array<…>`) are parsed as JSX and CRASH the build — backtick them.
- `onBrokenLinks=warn` LIES (build succeeds with broken links). Grep the build output for `broken links found`; use `--strict` to FAIL.
- **New `docs/docs/**.mdx` pages MUST be added to `sidebars.js` in the same stroke.**
- Reserve port 3000 for the user; agents use `--port 3001`. It's OK to stop `gbx:docs:dev` for a build, but ALWAYS restart it.
- Function-count badges (README) + `intro.mdx` counts are HARDCODED — refresh on function add/remove + release.
- Docs example guards (QC) check ASCII output tables + tab completeness; bindings are auto-derived.

## Docs deploy

Docs deploy is **main-only**. `npm ci` ECONNRESET usually = stray npm-proxy hosts in `package-lock.json`.
