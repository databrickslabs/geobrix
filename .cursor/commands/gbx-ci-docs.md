# CI Documentation Tests menu

Runs the documentation-tests CI menu: run doc tests locally (Python/Scala in Docker), check doc-test CI status, trigger doc tests in CI, watch runs, or fetch logs.

## Usage

```bash
bash .cursor/commands/gbx-ci-docs.sh [command] [language]
```

## Commands (pass as first arg)

- `local [python|scala|all]` — Run doc tests locally in Docker (default: all)
- `python` — Run Python doc tests only
- `scala` — Run Scala doc tests only
- `status` — Check CI status for doc tests
- `trigger` — Trigger doc tests in CI
- `watch` — Watch latest doc test run
- `logs` — Fetch doc test CI logs
- `help` — Show script help

With no args, the script shows an interactive menu.

## Note

For running doc tests directly (non-interactive), use `gbx:test:docs` or `gbx:test:python-docs` / `gbx:test:scala-docs`.
