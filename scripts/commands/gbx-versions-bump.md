# Bump the GeoBrix release version

Bumps the library's own release version (e.g. `0.5.0` → `0.5.1`) across every source-of-truth, artifact-filename, banner, and diagram-pill reference in one pass.

**This is not `gbx:versions:audit`.** That command tracks pinned Python *tooling* (pip/numpy/pyspark) and never touches the library version. This one changes the library's version and nothing else.

It applies only the **safe, targeted** replacements, then **reports** every remaining bare occurrence of the old number so you can judge the historically-scoped ones ("introduced in 0.5.0", "Requires GeoBrix 0.5.0+") that must stay true after the bump. It never rewrites those, never touches lock files or the standalone `genie_map` app version, and does not regenerate the committed diagram PNGs (that needs a headless-Chrome screenshot of the SVG — the command prints the exact manual step).

## Usage

```bash
bash scripts/commands/gbx-versions-bump.sh --to <NEW> [OPTIONS]
```

## Options

- `--to <NEW>` — Target version, e.g. `0.5.1` (required).
- `--from <OLD>` — Source version. Auto-detected from `pom.xml` if omitted.
- `--dry-run` — Show what would change; edit nothing.
- `--log <path>` — Write output to a log file.
- `--help` — Display help.

## Examples

```bash
# Preview the 0.5.0 -> 0.5.1 bump without editing
gbx:versions:bump --to 0.5.1 --dry-run

# Apply it (auto-detects the current version from pom.xml)
gbx:versions:bump --to 0.5.1

# Be explicit about both ends
gbx:versions:bump --from 0.5.0 --to 0.5.1
```

## What it changes (safe, targeted)

1. **Authoritative definitions** — the source of truth:
   - `pom.xml` `<version>` (Scala/Maven + JAR name; the release workflow parses VERSION from the built JAR name).
   - `python/geobrix/src/databricks/labs/gbx/__init__.py` `__version__` (the wheel version; `pyproject.toml` reads it via `version = {attr = ...}` and has no literal).
   - `docs/package.json` `"version"` (the Docusaurus site).
2. **Artifact filenames** — repo-wide `geobrix-<VER>-py3-none-any.whl` and `geobrix-<VER>-jar-with-dependencies.jar` (docs install examples, JAR-path-pinned conftests/parity tests, notebook `%pip` cells). These break at runtime if stale.
3. **Version banners** — `Current version: <VER>` in `docs/docs/release-notes.mdx`, `Current version **<VER>**` in `CLAUDE.md`.
4. **Diagram pills** — the `v<VER>` pill string in the `resources/images/generators/*.py` sources and the committed `.svg` bytes.

## What it will NOT touch (reported for you to judge)

- **Historically-scoped prose** — "introduced in `<OLD>`", "Requires GeoBrix `<OLD>`+", "capabilities introduced in `<OLD>`". These state *when* a feature landed and stay true after a bump; bumping them would be factually wrong. Listed under "Residual bare occurrences" for review.
- **Lock files** (`package-lock.json`, `pnpm-lock.yaml`), the standalone `genie_map` app version (`examples/apps/genie_map/package.json`), and third-party dep pins that happen to share the number — all false positives.
- **The committed diagram PNGs** — the docs site displays the `.png`, not the `.svg`. After the SVG pill text is bumped, re-render each PNG from its SVG (headless-Chrome screenshot per the generator header). A version bump alone leaves stale PNGs on the site. The command prints this reminder.

## Verify after applying

1. `pom.xml`, `__init__.py`, and `docs/package.json` all read the new version.
2. `git grep -n "<OLD>-py3-none-any.whl\|<OLD>-jar-with-dependencies"` returns nothing.
3. `git grep -n "Current version: <OLD>"` returns nothing.
4. Build sanity: the wheel builds as `dblabs_geobrix-<NEW>-*.whl`, the JAR as `geobrix-<NEW>-jar-with-dependencies.jar`.
5. Run affected tests: function-info, bindings/parity, and any JAR-path-pinned conftest or parity test.

## Release flow (after the bump PR merges to `main`)

```bash
git tag -a v<NEW> <merge-sha> -m "GeoBrix v<NEW> (Beta)" && git push origin v<NEW>
gh release create v<NEW> --latest --title "GeoBrix v<NEW> (Beta)" --notes "..."
gh workflow run "package geobrix artifacts" -f ref=v<NEW> -f attach_to_tag=v<NEW>
```

Builds and attaches six assets (JAR, wheel, docs zip, GDAL tarball + `.sha256`, GDAL init script); docs deploy auto-triggers on the docs-touching merge to `main`. Run as `mjohns-databricks` (`gh auth switch --user mjohns-databricks`).

## Notes

- The safe replacements are idempotent — re-running with the same `--to` is a no-op once applied.
- `--dry-run` prints the same plan and residual report without editing, so you can review the blast radius first.
