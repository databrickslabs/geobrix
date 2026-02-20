# Fetch CI Logs

Downloads logs from the latest GitHub Actions run for the current branch (or a specific run by ID) into `ci-logs/`.

## Usage

```bash
bash .cursor/commands/gbx-ci-logs.sh [RUN_ID]
```

## Options

- **RUN_ID** (optional) — Workflow run ID to fetch. If omitted, fetches the latest run for the current branch.

## Output

- Logs are saved under `ci-logs/` with a timestamped filename.
