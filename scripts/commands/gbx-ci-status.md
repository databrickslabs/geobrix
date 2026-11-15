# CI Status

Shows recent GitHub Actions workflow runs for the current branch (build main, doc tests, etc.).

## Usage

```bash
bash scripts/commands/gbx-ci-status.sh [LIMIT] [--help]
```

## Options

- **LIMIT** (optional) — Number of recent runs to show (default: 10); must be a number.
- `--help`, `-h` — Show usage (an option is never taken as the limit).

## Prerequisites

- GitHub CLI (`gh`) installed and authenticated.
