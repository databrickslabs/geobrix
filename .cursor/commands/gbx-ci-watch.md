# Watch CI Run

Streams the latest GitHub Actions run for the current branch (or a specific run by ID) in real time.

## Usage

```bash
bash .cursor/commands/gbx-ci-watch.sh [RUN_ID]
```

## Options

- **RUN_ID** (optional) — Workflow run ID to watch. If omitted, watches the latest run for the current branch.

## Prerequisites

- GitHub CLI (`gh`) installed and authenticated.
