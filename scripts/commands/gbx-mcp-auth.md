# gbx:mcp:auth

Show which configured MCP servers are authenticated versus which need auth, at a glance.

Read-only: runs `claude mcp list` (a health-check that reports status but never launches a
browser/OAuth flow) and groups servers into **NEEDS AUTH**, **FAILED**, **PENDING APPROVAL**, and
**CONNECTED**. For each server that needs authenticating it prints the exact
`claude mcp login <name>` command to copy-paste. It also surfaces the "claude.ai connectors are
disabled" notice, which is gated by the managed-settings model gateway and is an org-admin lever,
not a per-server login.

## Usage

```bash
bash scripts/commands/gbx-mcp-auth.sh [OPTIONS]
```

## Options

- `--log <path>` — write output to a log file (`mcp-auth.log` → `test-logs/mcp-auth.log`; relative → under `test-logs/`; absolute → as-is)
- `--raw` — also print the raw `claude mcp list` output
- `--help`, `-h` — show help and exit

## Examples

```bash
bash scripts/commands/gbx-mcp-auth.sh
bash scripts/commands/gbx-mcp-auth.sh --raw --log mcp-auth.log
```

## Notes

- Does **not** trigger any authentication flow itself — it only reports status and prints the
  commands you would run.
- Exit code is `0` on success even when servers need auth; non-zero only if `claude mcp list` fails.
- Requires the `claude` CLI on `PATH`.
