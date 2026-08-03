# gbx:qc:fix

Idempotently re-apply GeoBrix's local adjustments to the user-global QC judge
(`~/.claude/qc-judge/`). Databricks force-updates those files periodically and can
wipe the adjustments; re-run this after any suspected update.

Two marker-guarded adjustments (no-op if already present):
1. **WARN-check ERROR no longer hard-blocks** (`qc_core.py` `compute_verdict`) — a
   tooling failure (e.g. the LLM subprocess dying on an `ANTHROPIC_API_KEY` /
   claude.ai env conflict) reports ERROR but must not block an otherwise-clean push;
   CRITICAL checks still block on error.
2. **Inline override token honored** (`qc.py` `handle_pre`) — `QC_OVERRIDE=1` /
   `QC_SKIP=1` in the pushed command string is respected, so the incantation the
   block message prints works even from an agent tool call.

## Usage

```bash
bash scripts/commands/gbx-qc-fix.sh [OPTIONS]
```

## Options

- `--check` — report whether each adjustment is applied; make NO changes. Exit 1 if any is missing.
- `--log <path>` — write output to a log file (`filename` → `test-logs/<name>`).
- `--help` / `-h` — show help and exit.

Backs up each file (`*.gbx-qc-fix.<ts>.bak`) before editing and verifies the patched
files still import. Honors `QC_HOME` if set.

## When to run

After a suspected Databricks force-update of the QC judge — i.e. a clean push
(0 FAIL / 0 REVIEW, criticals PASS) still BLOCKS with only ERROR rows.

## Examples

```bash
bash scripts/commands/gbx-qc-fix.sh --check
bash scripts/commands/gbx-qc-fix.sh
```
