# gbx:test:mutation-databricks

Try every mutant of some kernel modules on one large Databricks node, and record what the tests did with each. It is guidance for the test-gap backlog and never blocks a fold. The local gate (`gbx:test:mutation-gate`) stays the pass or fail for a fold.

## Usage

`bash scripts/commands/gbx-test-mutation-databricks.sh --family <name> --modules <a,b> --tests "<paths>" --purpose "<why>" [OPTIONS]`

## Options

- `--family <name>` - names the run and its output folder.
- `--modules <a,b,...>` - kernel modules under `src/databricks/labs/gbx`, for example `pyvx/_validity,pyvx/_cleaning`.
- `--tests "<paths>"` - pytest args relative to `python/geobrix`, in one quoted string.
- `--purpose "<why>"` - required on a launch. It heads the runner notebook and rides on the run name.
- `--profile <name>` - the Databricks profile. Default: `DATABRICKS_CONFIG_PROFILE`.
- `--node-type <t>` - the node of a NEW cluster. Default: `m5d.16xlarge` (64 vCPU, local NVMe). A leased pool cluster keeps the node it was made with.
- `--pool <name>` - lease the single-node cluster of this pool. Default: `gbx-test-1n`. The command makes one when the pool has none.
- `--pool-wait-min <n>` - wait up to n minutes for a free pool cluster. Default: 30.
- `--fresh-cluster` - run on a new job cluster that ends with the run, not on the pool. Every run then pays a cold start.
- `--pip-extra "<reqs>"` - more pip requirements the selected tests import. Default: `mercantile>=1.2,<2` (the oracle tables).
- `--run-id <id>` - Default: `mut-<date>-<time>-<4 hex>`.
- `--sha <rev>` - the commit to test. Default: `HEAD`.
- `--wheel <path>` - the env6 wheel. Default: the one in `dist/env6/` (from `gbx:build:wheel --env 6`).
- `--table <c.s.t>` - the result table. Default: `<catalog>.<schema>.mutation_results` of the bundle Volume.
- `--oracle-tables <T1,T2>` - sets `GBX_ORACLE_TABLES` for the family harness.
- `--wall-cap-min <n>` - stop the mutants after n minutes. Default: 180.
- `--setup-cap-min <n>` - stop when no mutant was tried n minutes after the work started. Default: 30.
- `--stall-min <n>` - stop when no mutant finished for n minutes. Default: 15.
- `--start-cap-min <n>` - cancel when the node has not beaten n minutes after the submit. Default: 20.
- `--keep-stage` - keep the Volume folder and the runner notebook.
- `--explain-only` - print the plan and stop. No network, nothing staged.
- `--log <path>` - write output to `test-logs/<path>` (relative) or an absolute path.
- `--help`, `-h` - show usage.

## Examples

- `bash scripts/commands/gbx-test-mutation-databricks.sh --profile oauth-fe --family pyvx --modules pyvx/_validity,pyvx/_cleaning --tests test/pyvx --purpose "kill rates of the pyvx kernels"`
- Add `--explain-only` to see the plan first.

## What it does

- Builds a tarball of the committed tree at `--sha` (`git archive`: `python/geobrix/src`, `python/geobrix/test`, the pyproject, `scripts/mutation`). Uncommitted edits are not in the run; the command names them.
- Stages the tarball and the env6 wheel to an isolated Volume folder, `<volume>/mutation-runs/<run id>/`. It downloads each file back and compares sha256.
- Leases the pool's single-node cluster (DBR 19, dedicated, on demand). The pool keeps it warm across runs and agents, and it ends itself after 60 idle minutes. This command never ends it. No JAR.
- Runs the notebook as a job on that cluster. The job installs the wheel, `mutmut==3.8.0` and `pytest==9.1.1` (the gate's pins) in its own Python session, unpacks the tarball and runs `scripts/mutation/guidance_runner.py` with cores minus 2 workers. A later run restages the wheel and the tarball and installs them again; the cluster is not restarted.
- Runs the selected tests once with no mutant. A test that fails there (a data fixture the node lacks) is deselected and listed in `run.json`. A selection that does not run stops the run with the reason in `preflight.txt`.
- Writes one row per mutant to the Delta table, and the files to `test-logs/mutation-guidance/<run id>/`.
- Removes the Volume folder and the runner notebook after the download.

## Outputs

- Table `mutation_results`: `run_id`, `run_ts`, `git_sha`, `family`, `module`, `function`, `mutant`, `status`, `exit_code`, `killing_test`, `wall_s`, `diff`, `tests`, `node_type`, `purpose`. A rerun of the same run id replaces its rows. New columns are added by schema merge.
- `summary.md`: kill rate per module (with the change since the run before, when the table holds one), survivors grouped by function with the changed lines, functions no selected test reaches, and the tests that killed the most mutants.
- `rows.jsonl`, `run.json`, `K.json`, `killers.jsonl`, `survivors.json`, `progress.json`, `mutmut.log`.

## Status of a mutant

- `killed`: a test failed. `killing_test` is the first test that failed (mutmut runs with `-x`).
- `survived`: every selected test passed. The `diff` column holds the change.
- `timeout`, `segfault`: the test run hit its limit or crashed. Both count as detected.
- `absent`: no selected test reaches the mutant.
- `unchecked`: the run was stopped by a cap before the mutant was tried.
- Kill rate = (killed + timeout + segfault) / (those + survived).

## Limits

- Tier 0 only: tests that need a Spark session are skipped.
- It does not call the gate. The gate compares an old and a slim selection; this run needs one selection and one pass. It uses the gate's helpers (`scripts/mutation/gate.py`) and its pins.
- The wheel carries the dependencies and the native payload. The package that is mutated and tested is the tarball's.
- A stopped run (a cap) exits 3 and still writes the rows of the mutants tried; a cancelled notebook writes no table rows, and the files stay in the Volume folder until you remove them.
- Cost: one on-demand node while a run is on it and for 60 idle minutes after the last one. The first run of a phase pays a cold start (about 5 min).
- Make the pool by hand with `bash scripts/commands/gbx-bench-pool.sh up --name gbx-test-1n --n 1 --single-node --node-type m5d.8xlarge`; end it with `down --name gbx-test-1n --delete`.
