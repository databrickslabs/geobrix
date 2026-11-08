# gbx:test:mutation-gate

Check that a slimmer test selection still catches what the old one catches. It mutates a family's kernel modules with `mutmut` 3.8.0 in throwaway copies of the tree. It runs both selections on the host and prints PASS or FAIL. By default it runs a sample of the mutants, not all of them.

## Usage

`bash scripts/commands/gbx-test-mutation-gate.sh --family <name> --modules <a,b> --tests "<old>" --slim-tests "<new>" [OPTIONS]`

## Options

- `--family <name>` — names the work dir and the default output dir.
- `--modules <a,b,...>` — kernel modules under `src/databricks/labs/gbx`, for example `pygx/_bng,pygx/_quadbin`.
- `--tests "<paths>"` — the old tests, as pytest args relative to `python/geobrix`, in one quoted string. `--ignore=...` entries are allowed.
- `--slim-tests "<paths>"` — the slim tests, same form.
- `--oracle-tables <T1,T2>` — sets `GBX_ORACLE_TABLES` for the family harness. Default: all tables.
- `--out <dir>` — output dir. Default: `test-logs/mutation-gate/<family>`.
- `--max-children <n>` — the most mutmut workers this gate may use. Default: its share of the host budget (see Worker budget).
- `--seed <n>` — sample seed. Default: 31.
- `--full` — run every mutant in the old and the slim pass. Default: only a sample (see The verdict).
- `--old-codes <file>` — reuse the `K_old.json` of an earlier run, so the next slim candidate costs one run, not two. Every run writes `K_old.fingerprint` beside it (the kernel modules, the old selection and the old test files); a reuse whose fingerprint differs is refused with a `Fix:`. A kill set from before fingerprints is accepted unchecked.
- `--no-cache` — neither read nor write the cache of old kill sets (see Old kill set cache).
- `--old-ref <git-ref>` — run the old selection on the tests and the source as they were at that ref, not on the current tree. The oracle rows live in the source, so the old run sees the old rows too. Use it when the fold edits or deletes files in place. The old versions come back, deleted files too, and `conftest.py` and helpers as they were. Files added since are absent from the old run. The slim run uses the current tree. The kernel modules must be the same at the ref and in the tree, or the command refuses. With `--old-codes` too, the kill set must have been made from the same ref (its fingerprint says which). That is how a second slim candidate of an in-place fold reuses the first run.
- `--keep-work` — keep the throwaway copy (a private `mktemp`-style dir under `/private/tmp`, mode 0700).
- `--plan` — print the plan and stop. Nothing runs.
- `--log <path>` — write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` — show usage.

## Examples

- `bash scripts/commands/gbx-test-mutation-gate.sh --family pyvx --modules pyvx/_crs,pyvx/_validity --tests "test/pyvx/test_crs.py test/pyvx/test_validity.py" --slim-tests "test/pyvx/test_crs_core.py test/manager/test_family_cross_tier.py::test_family_oracle_l3_callables" --oracle-tables VECTOR_SCALAR,MVP_S`

## The verdict

The report names the mode that ran: `sample-first (50 of N mutants)`, `sample-first (100 of N mutants, extended from 50 after a loss)` or `full (every mutant)`.

Sample-first, the default:

- It generates the old mutants, then draws a sample of 50 (seed 31, one per function in turn).
- It runs only those 50 in the old pass and the slim pass. A pass costs one stats run plus 50 mutants.
- If the slim pass loses any sampled old kill, it draws 100 and judges on 100.
- PASS needs the slim run to keep at least 95% of the sample's old kills.
- PASS also needs every sampled module's kill rate to fall by no more than 5 points.

With `--full`:

- It runs every mutant in both passes and draws a sample of 50 and a sample of 100 from the old run.
- PASS needs 95% for sample 50, sample 100 and all mutants, and the same module rule over all mutants.

In both modes:

- A mutant is killed when its test run fails, times out or crashes.
- A mutant that no test ran against (mutmut exit 5 or 33), or that was skipped or interrupted, is not killed.
- The kill set `K` is what the old run kills. A mutant the slim run did not generate counts as not killed.
- The exit code is 0 for PASS and 1 for FAIL.

## What it writes

- `K_old.json`, `K_slim.json` — exit code per mutant (the sampled ones, unless `--full`).
- `K_old.mode` — `full` or `sample <n>`. A `--full` run refuses `--old-codes` from a sample.
- `sample50.json`, `sample100.json` — the sampled mutants (`sample100.json` only when it ran).
- `survivors_old.json` — what the old run did not kill.
- `lost.json` — the old kills the slim run lost.
- `verdict.json`, `report.txt` — the numbers and the verdict.
- `mutmut_old.log`, `mutmut_slim.log` — the raw runs, one section per mutmut call.

## Old kill set cache

- The old pass is the same for every slim candidate of a fold, so its kills are cached in `~/.cache/geobrix/mutation-kills/<key>.json`. `GBX_MUTATION_KILLS` moves it.
- The key covers the kernel modules, the old selection, the old test files (or the `--old-ref` sha), `--oracle-tables` and the pinned mutmut. Without `--old-ref` it also covers the `src` files and the test helpers (conftests and the like) of the tree.
- A hit skips the old pass. The report says so. An entry made from another fingerprint, or an unreadable one, is a miss.
- A sample-first run caches the universe and the sampled kills. A later run that needs more mutants (the 100-sample) runs only those. A `--full` run caches every kill and serves later full and sample-first runs.
- `--old-codes <file>` overrides the cache. A run with it does not touch the cache.

## Worker budget

- All gates on the host together use at most `GBX_MUTATION_MAX_WORKERS` workers. Default: the cores less two.
- Each mutmut call takes its fair share of the free slots (the budget divided by the gates running, at most `--max-children`). It takes at least one.
- It prints `using N of M workers (K other gates running)`. With no free slot it waits and prints progress. After `GBX_MUTATION_SLOT_WAIT` seconds (default 3600) it stops with a `Fix:`.
- The slots are lock files in `~/.cache/geobrix/mutation-slots` (`GBX_MUTATION_SLOTS` moves it). The kernel drops a lock when its process ends, so a killed gate frees its slots.

## Runs that prove nothing are refused

- If the old run killed no mutant, the command stops: "old run killed nothing: the old selection did not run". A test file that cannot collect in the throwaway copy causes this. Mutmut's stats phase aborts and every mutant stays unchecked. It used to read as a pass.
- If mutmut stops before running tests in either run (`failed to collect stats`, `Failed to run clean test`, `Stopping early ...`), the command names the log and stops.
- A pass where more than 20% of the judged mutants timed out is an invalid run, a FAIL. So is a module of 5 or more mutants where more than half did. A timeout still counts as a kill. The cause is an overloaded host or hung tests. The message says to rerun with fewer workers. The report lists timeouts per module and pass.
- A verdict over an empty kill set is a FAIL. In sample-first mode, an old pass that kills none of the sample stops the same way.

## Limits

- Tier 0 only. Tests that use the `spark`, `jvm_spark`, `spark_connect_local`, `spark_session` or `spark_delta` fixture are skipped. The command warns when a selected file imports `pyspark` at module level.
- Rows of an oracle table reach the kernels only through the `test_family_oracle_l3_callables` and `test_family_oracle_uc_handlers` legs. A table module opts its scalars into the first with `L3_CALLABLE = True`.
- A venv holds `mutmut==3.8.0` and `pytest==9.1.1`. It lives in `~/.cache/geobrix/mutation-venv` (`XDG_CACHE_HOME` is honoured); `GBX_MUTATION_VENV` moves it. mutmut 3.8.0 crashes under pytest 9.2.0.
- The command runs code from that venv, so it refuses one that is a link, is owned by another user, or is group- or world-writable. The message starts its fix with `Fix:`.
- It uses the host venv (`GBX_PYRX_VENV` or `.venv-pyrx`) for shapely, pyproj and the rest. Nothing is installed into it.
- It never touches the repo tree. The old and the slim pass each get a work dir, removed at the end.
- It does not use the container or its lock, so gates for several families can run at once.
