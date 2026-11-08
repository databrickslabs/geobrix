# gbx:test:mutation-gate

Check that a slimmer test selection still catches what the old one catches. It mutates a family's kernel modules with `mutmut` 3.8.0 in a throwaway copy of the tree. It runs both selections on the host and prints PASS or FAIL.

## Usage

`bash scripts/commands/gbx-test-mutation-gate.sh --family <name> --modules <a,b> --tests "<old>" --slim-tests "<new>" [OPTIONS]`

## Options

- `--family <name>` — names the work dir and the default output dir.
- `--modules <a,b,...>` — kernel modules under `src/databricks/labs/gbx`, for example `pygx/_bng,pygx/_quadbin`.
- `--tests "<paths>"` — the old tests, as pytest args relative to `python/geobrix`, in one quoted string. `--ignore=...` entries are allowed.
- `--slim-tests "<paths>"` — the slim tests, same form.
- `--oracle-tables <T1,T2>` — sets `GBX_ORACLE_TABLES` for the family harness. Default: all tables.
- `--out <dir>` — output dir. Default: `test-logs/mutation-gate/<family>`.
- `--max-children <n>` — mutmut workers. Default: 10. Lower it when two gates run at once.
- `--seed <n>` — sample seed. Default: 31.
- `--old-codes <file>` — reuse the `K_old.json` of an earlier run, so the next slim candidate costs one run, not two. Every run writes `K_old.fingerprint` beside it (the kernel modules, the old selection and the old test files); a reuse whose fingerprint differs is refused with a `Fix:`. A kill set from before fingerprints is accepted unchecked.
- `--old-ref <git-ref>` — run the old selection on the tests and the source as they were at that ref, not on the current tree. The oracle rows live in the source, so the old run sees the old rows too. Use it when the fold edits or deletes files in place. The old versions come back, deleted files too, and `conftest.py` and helpers as they were. Files added since are absent from the old run. The slim run uses the current tree. The kernel modules must be the same at the ref and in the tree, or the command refuses. With `--old-codes` too, the kill set must have been made from the same ref (its fingerprint says which). That is how a second slim candidate of an in-place fold reuses the first run.
- `--keep-work` — keep the throwaway copy (a private `mktemp`-style dir under `/private/tmp`, mode 0700).
- `--plan` — print the plan and stop. Nothing runs.
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` — show usage.

## Examples

- `bash scripts/commands/gbx-test-mutation-gate.sh --family pyvx --modules pyvx/_crs,pyvx/_validity --tests "test/pyvx/test_crs.py test/pyvx/test_validity.py" --slim-tests "test/pyvx/test_crs_core.py test/manager/test_family_cross_tier.py::test_family_oracle_l3_callables" --oracle-tables VECTOR_SCALAR,MVP_S`

## The verdict

The command runs the old selection, then the slim one, over every mutant of the old run.

- A mutant is killed when its test run exits non-zero (a failure, a timeout or a crash).
- The kill set `K` is what the old run kills. A mutant the slim run did not generate counts as not killed.
- It draws a sample of 50 and a sample of 100 mutants from the old run (seed 31, one per function in turn).
- PASS needs `|K_slim ∩ K| / |K|` of at least 95% for sample 50, sample 100 and all mutants.
- PASS also needs every module's kill rate to fall by no more than 5 points.
- The exit code is 0 for PASS and 1 for FAIL.

## What it writes

- `K_old.json`, `K_slim.json` — exit code per mutant.
- `sample50.json`, `sample100.json` — the sampled mutants.
- `survivors_old.json` — what the old run did not kill.
- `lost.json` — the old kills the slim run lost.
- `verdict.json`, `report.txt` — the numbers and the verdict.
- `mutmut_old.log`, `mutmut_slim.log` — the raw runs.

## Runs that prove nothing are refused

- If the old run killed no mutant, the command stops: "old run killed nothing: the old selection did not run". A test file that cannot collect in the throwaway copy causes this. Mutmut's stats phase aborts and every mutant stays unchecked. It used to read as a pass.
- If mutmut stops before running tests in either run (`failed to collect stats`, `Failed to run clean test`, `Stopping early ...`), the command names the log and stops.
- A verdict over an empty kill set is a FAIL.

## Limits

- Tier 0 only. Tests that use the `spark`, `jvm_spark` or `spark_connect_local` fixture are skipped. The command warns when a selected file imports `pyspark` at module level.
- Rows of an oracle table reach the kernels only through the `test_family_oracle_l3_callables` and `test_family_oracle_uc_handlers` legs. A table module opts its scalars into the first with `L3_CALLABLE = True`.
- A venv holds `mutmut==3.8.0` and `pytest==9.1.1`. It lives in `~/.cache/geobrix/mutation-venv` (`XDG_CACHE_HOME` is honoured); `GBX_MUTATION_VENV` moves it. mutmut 3.8.0 crashes under pytest 9.2.0.
- The command runs code from that venv, so it refuses one that is a link, is owned by another user, or is group- or world-writable. The message starts its fix with `Fix:`.
- It uses the host venv (`GBX_PYRX_VENV` or `.venv-pyrx`) for shapely, pyproj and the rest. Nothing is installed into it.
- It never touches the repo tree. The work dir is removed at the end.
- It does not use the container or its lock, so gates for several families can run at once.
