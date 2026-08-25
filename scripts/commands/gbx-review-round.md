# gbx:review:round

Run [Isaac Review](https://github.com/databricks-eng) on a **scoped slice** of the branch
instead of the full branch-vs-main diff. On a release branch (e.g. `beta/0.5.0` → `main`)
that full diff is ~1000 files — an expensive, low-signal review. This command defaults to
reviewing only the commits you're about to push and refuses an oversized scope unless you
opt in, so the full-branch pass never fires by accident.

Isaac Review is incremental (session cache + reuse of the open PR's existing findings), so
per-round scoping is cheap and cumulative: each round's findings accrue on the PR.

## Usage

```bash
bash scripts/commands/gbx-review-round.sh [OPTIONS] [-- <extra isaac args>]
```

## Options

- `--base <ref>` — Base ref to review `HEAD` against. Default: the branch's upstream
  (`@{upstream}`) = your unpushed commits. Use a historical SHA for a back-fill pass, or
  `origin/main` for the full release sweep (requires `--allow-large`).
- `--full` — Ignore Isaac's incremental session cache (re-review every file).
- `--rule <set>` — Run a specific rule set or rule (isaac `--run-rule`, e.g. a platform).
- `--allow-large` — Proceed even when the scope exceeds 150 files. (isaac loads the full
  contents of every changed file, so a review overflows the 200k context window at roughly
  ~150–200 changed files — verified 2026-08-25. Larger scopes tend to fail with "Prompt is
  too long"; the command detects that silent failure and exits non-zero.)
- `--log <path>` — Tee output to a log (`filename` → `test-logs/<name>`).
- `-h`, `--help` — Show help.

Extra isaac flags after `--` pass through (e.g. `-- -f markdown -o report.md`). Isaac runs
via `dbexec repo run isaac` (the shell alias is not available in a non-interactive script).

## Examples

```bash
# Review what you're about to push, then push:
bash scripts/commands/gbx-review-round.sh

# Review a specific historical slice (workstream back-fill):
bash scripts/commands/gbx-review-round.sh --base 776b6ec7

# Full release sweep to a markdown report (large; must opt in):
bash scripts/commands/gbx-review-round.sh --base origin/main --allow-large -- -f markdown -o test-logs/isaac-pr.md
```
