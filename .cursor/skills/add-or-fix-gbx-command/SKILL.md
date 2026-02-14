---
name: add-or-fix-gbx-command
description: Add a new GeoBrix Cursor command or fix an existing one. Use when the user wants to create/change gbx:* commands or when a command fails and should be fixed (not worked around).
---

# Add or Fix a GeoBrix Cursor Command

Use this skill when adding a **new** `gbx:<category>:<action>` command or **fixing** an existing command (do not work around failures; fix the command).

## Command layout

- **Location**: `.cursor/commands/`
- **Pair**: Every command has two files:
  - `gbx-<category>-<action>.md` — Cursor registration (short description, usage, options). Shown in command palette.
  - `gbx-<category>-<action>.sh` — Bash implementation. Sourced from Cursor when the command runs.
- **Shared helpers**: `common.sh` — `check_docker`, `resolve_log_path`, `setup_log_file`, `show_banner`, `show_separator`, `open_report`. Source it in `.sh` when needed: `source "$SCRIPT_DIR/common.sh"`.

## Naming and ownership

- **Format**: `gbx:<category>:<action>` (e.g. `gbx:test:function-info`, `gbx:docker:exec`).
- **Category** must match the **subagent** that owns the topic (see `.cursor/rules/00-agent-context.mdc`):
  - `test` → Test Specialist
  - `coverage` → Coverage Analyst
  - `data` → Data Manager
  - `docs` → Documentation Manager (or Function-Info for function-info only)
  - `docker` → Docker Specialist
  - `gdal` → GDAL Expert
  - `rasterx` / `gridx` / `vectorx` → API specialists
- **Subagents** maintain and improve commands in their domain. After adding or changing a command, update the owning subagent’s `.cursor/agents/<name>.md` (e.g. document the new option or behavior).

## Steps to add a new command

1. **Decide category and action** from the task (e.g. `gbx:docker:logs` for tailing container logs). Confirm the owning subagent and that no duplicate command exists (check `.cursor/commands/` and `.cursor/rules/cursor-commands.mdc`).
2. **Create the `.md` file**:
   - Short title and 1–2 sentence description.
   - Usage: `bash .cursor/commands/gbx-<category>-<action>.sh [OPTIONS]`.
   - Options (e.g. `--log <path>`, `--help`).
   - One or two example invocations.
   - Notes (e.g. “Runs inside Docker”, “Requires geobrix-dev”).
3. **Create the `.sh` file**:
   - Shebang: `#!/bin/bash`.
   - Resolve `SCRIPT_DIR` and `PROJECT_ROOT` (see existing commands).
   - Source `common.sh` if you need Docker check, logging, or banner helpers.
   - Implement options (e.g. `--help` with `show_help`; `--log` with `setup_log_path`/`resolve_log_path`).
   - Run the actual logic (often `docker exec geobrix-dev ...` for tests/docs).
   - Exit with appropriate code and optional success/failure message.
4. **Register in cursor-commands.mdc**: Add the command to the list under the right category in `.cursor/rules/cursor-commands.mdc` with a one-line description and example if useful.
5. **Update the subagent**: In `.cursor/agents/<subagent>.md`, add or adjust the “Commands” section so the new/fixed command is documented there.

## Steps to fix an existing command

1. **Reproduce** the failure (run the command as the user would).
2. **Inspect** the `.sh` (and if relevant `.md`) under `.cursor/commands/`. Identify the bug (wrong path, missing check, bad option handling).
3. **Change** the script (or doc) to fix the behavior. Prefer minimal, clear fixes.
4. **Re-run** the command to confirm it succeeds.
5. **Update** the owning subagent file if the fix changes documented behavior or adds options.

## Conventions

- **Logging**: Support `--log <path>`. Use `resolve_log_path` and `setup_log_file` from `common.sh` so relative paths go under `test-logs/` when appropriate.
- **Help**: Support `--help` / `-h` and print usage and options; then exit 0.
- **Docker**: If the command needs the container, call `check_docker` early so the user gets a clear error if Docker or `geobrix-dev` is missing.
- **No placeholders**: Implement real behavior; do not leave TODOs that make the command a no-op.

## Reference

- Existing commands: `.cursor/commands/*.sh` and `*.md`
- Command list and categories: `.cursor/rules/cursor-commands.mdc`
- Topic → subagent (command ownership): `.cursor/rules/00-agent-context.mdc`
