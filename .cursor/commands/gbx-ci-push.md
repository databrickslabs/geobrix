# Push and Run CI (build main)

Pushes the current branch to origin and watches the **build main** workflow run on GitHub Actions. Use this to initiate a remote build when working on a branch (e.g. `beta/0.2.0`).

## Usage

```bash
bash .cursor/commands/gbx-ci-push.sh
```

## What it does

1. Checks `gh` CLI is installed and authenticated.
2. Pushes the current branch to `origin`.
3. Waits for the **build main** workflow to start (triggered by the push).
4. Streams the workflow run until it completes.

## Prerequisites

- **GitHub CLI**: `gh` installed and logged in (`gh auth login`). Scopes: `repo`, `workflow`.
- **No uncommitted changes** (or confirm when prompted).

## When the build runs

- **Push** to any branch except `python/**` and `scala/**` triggers **build main**.
- To re-run without pushing (e.g. same ref): use **Actions** tab → **build main** → **Run workflow**, or run `gh workflow run "build main" --ref <branch>`.

## Related

- **Manual trigger only** (no push): `./scripts/ci/trigger-remote-tests.sh` then choose to trigger **build main**.
- **CI manager menu**: `./scripts/ci/ci-manager.sh` for status, push, trigger, watch, logs.
