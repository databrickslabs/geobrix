# Docs Build (Dev-Server-Aware)

Verify the docs build (MDX compiles, internal links resolve) via `npm run build`, **safely cycling a running `gbx:docs:dev` server around the build** so it isn't left corrupted.

## Usage

```bash
bash scripts/commands/gbx-docs-build.sh [OPTIONS]
```

## Options

- `--strict` - Fail (exit non-zero) if Docusaurus reports **broken links**. Use for a green validation gate (pre-push / CI). Default is **permissive**: `onBrokenLinks='warn'` means broken links are reported but the build still exits 0, so the routine build + dev-server-restart flow isn't blocked by a stray broken link. Under `--strict` the dev server is still restarted (port 3000 is never left down); only the exit code fails.
- `--no-restart` - Do not restart the dev server after building (it is still stopped first if running)
- `--log <path>` - Write output to log file (filename → `test-logs/<name>`)
- `--help` - Display help message

## When to use

- Use this to check that docs changes compile — especially in an agent/automation session — instead of a bare `npm run build`.
- `docusaurus build` and `docusaurus start` share the same `docs/.docusaurus/` cache (`registry.js`, `client-manifest.json`, ...). Running a build while a `gbx:docs:dev` server is up rewrites that cache out from under it, so its routes render stale or blank until restarted. This command detects a running dev server (via `/tmp/docusaurus-<port>.pid`), stops it for the build, then restarts it on the same port — so you end on a healthy, freshly-cached server.
- The same hazard applies to `gbx:docs:static-build`, `gbx:docs:serve-local`, and `gbx:docs:restart` (all run a host-side build): stop your dev server before running those, or use this command for a plain compile check.

## Examples

```bash
# Verify docs compile; a running dev server is stopped, built, and restarted (permissive)
bash scripts/commands/gbx-docs-build.sh

# Strict validation gate — fail on any broken link (e.g. before a push)
bash scripts/commands/gbx-docs-build.sh --strict

# CI-style strict check without bringing a dev server back up
bash scripts/commands/gbx-docs-build.sh --strict --no-restart
```

## Notes

- If no dev server is running, this just builds.
- Runs on the host (Docusaurus's node/npm runtime is host-side; the container has no node).
