# gbx:local:snapshot

Writes a rolling zip of the files git does not hold. These are local planning notes, editor and agent settings, local env files, and the commits no remote has yet. The zip goes to a local folder. It can also be copied to a Google Drive folder, which stays off until you turn it on (see Google Drive).

## Usage

```bash
bash scripts/commands/gbx-local-snapshot.sh [run [--upload [--respect-throttle]] [--dry-run] | list [--drive] | prune [--keep N] [--drive [--dry-run]] | restore [--from-drive] <name> <dir> | status | install-hook | uninstall-hook] [--log <path>]
```

## Subcommands

- `run` — write `geobrix-snapshot-<UTC time>-<branch>-<sha>.zip`, then keep the newest N. This is the default.
- `--dry-run` — list the included and excluded roots, the file count, the size, and any skipped large files. It writes nothing. With `--upload` it also checks the Drive folder and lists what a prune would trash (reads only).
- `--upload` — after the local write, copy the new zip to Google Drive and prune the Drive folder (see Google Drive). A command-line `--upload` always uploads; it ignores the throttle unless you add `--respect-throttle`.
- `list [--drive]` — list the snapshots in the folder, or in the Drive folder.
- `prune [--keep N] [--drive [--dry-run]]` — delete snapshots beyond the newest N. Files that do not match the snapshot name are never touched. With `--drive` it moves the extras in the Drive folder to the Drive trash, and `--dry-run` only prints them.
- `restore [--from-drive] <name> <dir>` — unpack a snapshot into an empty directory, rebuild the cluster config (see below), and print the steps to restore the unpushed commits.
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
- `GBX_SNAPSHOT_UPLOAD` — `1` to upload each new snapshot to Google Drive. The default is off.
- `GBX_SNAPSHOT_DRIVE_FOLDER_ID` — the Drive folder to use, from its URL. Any Drive step needs it.
- `GBX_SNAPSHOT_DRIVE_KEEP` — how many snapshots stay in the Drive folder. The default is 3.
- `GBX_SNAPSHOT_GCP_QUOTA_PROJECT` — the Google Cloud project that bills the Drive API calls. Most accounts need it.
- `GBX_SNAPSHOT_DRIVE_ALLOW_SHARED` — `1` to allow a Drive folder that anyone besides you can open. The default is off: the folder must be private.
- `GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN` — the least number of minutes between uploads that `GBX_SNAPSHOT_UPLOAD` starts, such as the ones from the push hook. The default is 60. `0` uploads on every run.

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

## Cluster config

The real `examples/tests/databricks_cluster_config.env` is never in the zip because it holds tokens. Instead, each `run` writes `examples/tests/databricks_cluster_config.nonsecret.env` (gitignored) just before the secret scan, and that copy goes in.

- It is the real file with every secret line removed. That means any key named like `*_TOKEN`, `*_SECRET`, or `*_PASSWORD` with a real value. It also means any line, commented or not, whose text looks like a token or JWT. Its header lists the dropped key names, never the values.
- Workspace values (profile, cluster and warehouse IDs, Volume paths) are kept. The file only lives in the private snapshot.
- On `restore`, if the restored tree has no real config, it is rebuilt as the example's defaults with the snapshot's values laid over them. The command then names the dropped keys to re-enter. Tokens normally come from the profile in `~/.databrickscfg`. An existing real config is never overwritten.

## Google Drive

Off by default. Turn it on with `--upload` or `GBX_SNAPSHOT_UPLOAD=1`, and set `GBX_SNAPSHOT_DRIVE_FOLDER_ID` in the untracked `.gbx-local.env`.

- The folder must be private. Before any upload or prune, the command reads the folder's permissions. The command stops with exit 4 if anyone besides you can open the folder (a person, a group, a domain, or a link). It also stops if you do not own it. The message names the sharing by type, role, and domain, or by a masked address. To fix it, make the folder private, or set `GBX_SNAPSHOT_DRIVE_ALLOW_SHARED=1` to accept it. If the permissions cannot be read, the folder counts as not confirmed private.
- Sign-in is the gcloud application-default login you already have. The command asks gcloud for an access token, keeps it in memory, and never prints it. It never starts a sign-in; with no usable login it stops and says so.
- It uploads only the zip that this `run` just wrote. It does so only when the secret scan found nothing and gitleaks ran cleanly. No `GBX_SNAPSHOT_SCAN_ALLOW` waiver may be in use, and a second scan of the finished zip must be clean. Otherwise the local zip stays and the run exits 4.
- The upload is chunked and resumable. Afterwards the command compares Drive's md5 and size with the local zip. On a mismatch it removes the bad copy and exits 4.
- After a verified upload it keeps the newest `GBX_SNAPSHOT_DRIVE_KEEP` snapshots, by the timestamp in the file name. It moves the rest to the Drive trash (recoverable for 30 days). It touches only files that match the exact snapshot name and are owned by you. It refuses to prune a folder that holds no snapshot, or when the listing fails. At most 25 files are trashed per run.
- The hook is throttled. An upload that `GBX_SNAPSHOT_UPLOAD=1` starts runs at most once per `GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN` minutes. The time of the last verified upload is kept in the snapshot folder (`.last-drive-upload`). Inside the interval the local snapshot still runs, the Drive step is skipped with one log line, and the result reads `drive=throttled`. A manual `--upload` ignores the throttle; `--upload --respect-throttle` keeps it.
- A Drive failure never fails the push. The hook ignores it, and the run logs to `test-logs/snapshot/`.
- Check it first with `gbx:local:snapshot --upload --dry-run`. It reports the folder name, who it is shared with, whether the private-folder check would refuse, the throttle state, and what a prune would trash. It changes nothing and exits 4 if the check would refuse.

## Limits

- The secret scan runs before any file is written. A private key or token stops the run, and the output lists `file:line` and the rule, never the text.
- The scan uses built-in token patterns, and gitleaks when it is found.
- The gitleaks `generic-api-key` rule and vendor sample keys (containing `EXAMPLE`) do not count. Both are noise in local notes and logs.
- The home-folder credentials (`~/.databrickscfg`, the keychain) are outside the repository and never included.
- One snapshot runs at a time. A second run exits 75.
- Nothing is uploaded unless you turn on the Drive upload.
