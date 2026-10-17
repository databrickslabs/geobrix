# Run Python Unit Tests

Runs Python unit tests (non-documentation tests) using pytest.

With **no `--path`** it runs the whole suite as CI-style **phases** (separate pytest processes, separate logs, per-phase summary) because the heavy and light tiers cannot share one pytest process. With `--path` it runs exactly the paths you give in a single pytest process.

## Usage

```bash
bash scripts/commands/gbx-test-python.sh [OPTIONS]
```

## Options

- `--path <path>` - Run specific test file or directory (repeatable). Single pytest run, no phasing. Cross-tier paths in one call can still hit the heavy/light process conflict; pass one tier per call.
- `-k <expr>` - Pytest keyword filter (e.g. `"merge or fileName"`)
- `--log <path>` - Write output to log file (supports filename, relative, or absolute path). In phased mode this is the combined log; each phase also gets `<path minus .log>-<phase>.log`.
- `--with-integration` - Include `@pytest.mark.integration` tests (network downloads, slow). Excluded by default.
- `--markers <expr>` - Override marker filter with a custom pytest expression (e.g. `"not slow"`). Disables the default `not integration` filter.
- `--help` - Display help message

## Phased default run (no `--path`)

| Phase | What runs | Where the list comes from |
|---|---|---|
| `heavy` | `test/` minus every light dir (`pytest test --ignore=test/pyrx ...`) | `LIGHT_IGNORES` in `.github/actions/python_build/action.yml` |
| `light` | the light dirs in one process (`pytest test/pyrx test/ds ...`) | the `pytest test/... -m ...` line in `.github/actions/pyrx_build/action.yml` |

- The lists are **read from the CI action files**, not copied into the script, so the command cannot drift from CI. If those lines change shape the command stops with exit 2 and a message (update `load_ci_phases()` in `gbx-test-python.sh`).
- Each phase runs from `python/geobrix` (like CI) with `-m "not integration"` (unless `--with-integration`/`--markers`), `-k` and `-rfE` (failing test ids are listed in the summary).
- Per-phase logs: `test-logs/python-tests-<phase>.log` by default, or `<--log minus .log>-<phase>.log`.
- After the last phase a summary prints each phase's pytest result line and, for failing phases, the FAILED/ERROR test ids (first 25; full list in the log).
- Exit code is non-zero if **any** phase fails (a phase that selects no tests under `-k` is not a failure).
- CI-only extras are not mirrored: heavy `--cov`, `python -m build`, and the fresh hash-pinned install.

## Default marker filter

By default the script runs with `-m "not integration"`, matching CI's `python_build` action. This excludes `python/geobrix/test/sample/test_sample_bundle.py::test_run_*_bundle_returns_dict_shape`, which download hundreds of MB of sample data.

Opt in with `--with-integration` (drops the filter entirely) or `--markers <expr>` (replaces it with your own expression).

## Examples

```bash
# Everything, as CI-style phases (heavy, light) with a per-phase summary
bash scripts/commands/gbx-test-python.sh

# Include integration tests (network downloads)
bash scripts/commands/gbx-test-python.sh --with-integration

# Run specific test file
bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/rasterx/test_operations.py

# Run with logging
bash scripts/commands/gbx-test-python.sh --log python-tests.log

# Custom marker expression (overrides the default)
bash scripts/commands/gbx-test-python.sh --markers "not slow"
```

## Test Location

- **Source**: `python/geobrix/test/`

## Notes

- Runs inside Docker container `geobrix-dev`
- Excludes documentation tests (use `gbx-test-python-docs` for those)
- Uses pytest with verbose output
- Default log location: `test-logs/` (if filename only provided)
- Tier rules and the CI lists: see the docstring in `python/geobrix/test/conftest.py` (`_LIGHT_TEST_DIRS`)
