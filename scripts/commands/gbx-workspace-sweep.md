# gbx:workspace:sweep

Remove the runner notebooks and folders the launchers keep at run end, once they pass their window. A run page opens its notebook from the workspace, so the launchers no longer delete it when the run ends.

## Usage

`bash scripts/commands/gbx-workspace-sweep.sh --profile <name> [options]`

- `--profile <name>`: the Databricks profile. Required.
- `--older-than <age>`: remove items older than this, by modified time. Default `60m`; also `90s`, `2h`, `1d`, or bare minutes.
- `--scope <s[,s]>`: `bench`, `runners`, `uc`, `notebooks`. Repeat or comma-separate. Default all four.
- `--dry-run`: print what would be removed; remove nothing.
- `--log <path>`: write output to a log file.
- `--help`: show the help.

## Examples

- `bash scripts/commands/gbx-workspace-sweep.sh --profile oauth-fe --dry-run`
- `bash scripts/commands/gbx-workspace-sweep.sh --profile oauth-fe --scope bench --older-than 2h`

## What it removes

Only items under `/Users/<you>/jobs/geobrix/`, and only these:

- `bench`: `jobs/geobrix/bench/geobrix_bench_runner_*` and `geobrix_gen_corpus_*` (`gbx:bench:cluster`, `gbx:bench:gen-data`).
- `runners`: `tests_*`, `runner_*` and `mutation_*` notebooks in the runner folder. That folder is `GBX_RUNNER_DIR`, default `jobs/geobrix/runners`.
- `runners` also covers a failed mutation run's Volume stage (`.../mutation-runs/<run>`), which the launcher records.
- `uc`: the UC gate's `jobs/geobrix/uc-it-<sha>` folders, whole.
- `notebooks`: the notebooks `gbx:test:notebooks-serverless` imported, by the path it recorded. Other items in its `--ws-dir` folders stay.

## Legacy root

- Launchers used to put their notebooks under `/Users/<you>/GeoBrix/`. That folder also holds your own notebooks.
- The sweep applies the same prefixes and windows there, so old runs still go.
- It never removes any other item there.
- It then removes the empty runner folders (`bench`, `runners`, `serverless-run`, `python-task`) and the folders it emptied.
- It removes `/Users/<you>/GeoBrix/` itself only when it is empty.
- An empty folder of your own keeps the root.

## Rules

- An item goes when it is older than `--older-than` AND past its hold.
- A launcher sets the hold at run end: 60 min after a run that succeeded, 24 h after one that did not.
- The 24 h hold covers a repair run, which reads the notebook again, and a look at the failure the next day.
- An item an active run uses is kept. So is a folder that holds one, and a recorded import beside one.
- When the active runs cannot be read, nothing is deleted.
- Each line names an item: `DELETED`, `WOULD DELETE`, `KEPT <path>: <why>` or `SKIPPED <path>: outside`.
- It exits 1 when a delete failed or the active runs could not be read.

## Launch-time sweep

- Each launcher sweeps its own scope before it uploads, at most 25 deletes. It never fails the launch.
- `GBX_WS_SWEEP_AT_LAUNCH=0` turns that off.
- The record is a local JSON-lines file: `GBX_WS_SWEEP_LEDGER`, default `~/.cache/geobrix/workspace-sweep.jsonl`.
- An item with no record is judged by its modified time alone.

The code is `examples/tests/_workspace_sweep.py`; its tests are `scripts/ci/test_workspace_sweep.py`.
