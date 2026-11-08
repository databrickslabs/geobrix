# gbx:local:snapshot

Writes a rolling zip of the files git does not hold. These are local planning notes, editor and agent settings, local env files, and the commits no remote has yet. The zip goes to a local folder.

## Usage

```bash
bash scripts/commands/gbx-local-snapshot.sh [run | --dry-run | list | prune [--keep N] | restore <name> <dir> | status | install-hook | uninstall-hook] [--log <path>]
```

## Subcommands

- `run` — write `geobrix-snapshot-<UTC time>-<branch>-<sha>.zip`, then keep the newest N. This is the default.
- `--dry-run` — list the included and excluded roots, the file count, the size, and any skipped large files. It writes nothing.
- `list` — list the snapshots in the folder.
- `prune [--keep N]` — delete snapshots beyond the newest N. Files that do not match the snapshot name are never touched.
- `restore <name> <dir>` — unpack a snapshot into an empty directory and print the steps to restore the unpushed commits.
- `status` — show the last logged run, the newest snapshot, and whether the hook is installed.
- `install-hook` — add a marked block to the `pre-push` hook. After a successful push it starts a snapshot in the background and never fails the push.
- `uninstall-hook` — remove only that block.

## Options

- `--log <path>` — write output to a log file. A filename goes to `test-logs/<name>`. The hook logs to `test-logs/snapshot/`.
- `--help` / `-h` — show help and exit.

## Settings

Set these in the untracked `.gbx-local.env`, or in the environment, which wins.

- `GBX_SNAPSHOT_DIR` — destination folder. The default is `~/GeoBrix-Snapshots`.
- `GBX_SNAPSHOT_KEEP` — how many snapshots to keep. The default is 3.
- `GBX_SNAPSHOT_MAX_FILE_MB` — skip files larger than this, with a warning. The default is 500.
- `GBX_SNAPSHOT_GITLEAKS` — path to a gitleaks binary. The default is `gitleaks` on `PATH`.
- `GBX_SNAPSHOT_SCAN_ALLOW` — comma-separated path globs of files whose findings you have reviewed. The default is none.

## What is in the zip

- `worktree/` — the working tree, including untracked and gitignored files.
- `git-meta/unpushed.bundle` — every commit and stash no remote has. It is absent when there are none.
- `git-meta/` — `config`, `info/exclude`, and the non-sample hooks. Git objects are not copied.
- `SNAPSHOT-INFO.txt` — the branch, the commit, and how to restore.

## What is left out

The list is `EXCLUDES` in `scripts/snapshot/snapshot.py`. It covers run state that a command recreates.

- Test data, build output, and Maven, npm, Cargo, and Spark scratch.
- Docker state, the Maven cache at `scripts/docker/m2`, and tool caches.
- Virtual environments and `node_modules` at any depth, `__pycache__`, and `*.pyc`.
- Run logs, coverage output, and agent worktrees.
- `examples/tests/databricks_cluster_config.env` (your real cluster config, with secrets; recreate it from `databricks_cluster_config.example.env`) and `.superpowers/input/` (scratch input that may hold pasted tokens).

## Limits

- The secret scan runs before any file is written. A private key or token stops the run, and the output lists `file:line` and the rule, never the text.
- The scan uses built-in token patterns, and gitleaks when it is found.
- The gitleaks `generic-api-key` rule and vendor sample keys (containing `EXAMPLE`) do not count. Both are noise in local notes and logs.
- The home-folder credentials (`~/.databrickscfg`, the keychain) are outside the repository and never included.
- One snapshot runs at a time. A second run exits 75.
- Nothing is uploaded.
