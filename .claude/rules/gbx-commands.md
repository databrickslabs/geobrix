---
paths:
  - "scripts/commands/**"
---

# Adding or fixing a `gbx:*` command

When adding a new `gbx:<category>:<action>` command (or fixing an existing one — don't work around failures, fix the command):

1. **Pick category and action.** Categories in use: `test`, `coverage`, `data`, `docs`, `docker`, `ci`, `lint`, `security`, `versions`, `prompt`. Confirm no duplicate exists in `scripts/commands/`.
2. **Create the pair** under `scripts/commands/`:
   - `gbx-<category>-<action>.md` — short title, 1-2 sentence description, usage `bash scripts/commands/gbx-<category>-<action>.sh [OPTIONS]`, options (including `--log <path>` and `--help`), 1-2 example invocations.
   - `gbx-<category>-<action>.sh` — bash implementation. Source `common.sh` for `check_docker`, `resolve_log_path`, `setup_log_file`, `show_banner`. Resolve `SCRIPT_DIR` and `PROJECT_ROOT` (see existing commands).
3. **Conventions for the .sh:**
   - Support `--help` / `-h` and exit 0 after printing usage.
   - Support `--log <path>` via `resolve_log_path` (filename → `test-logs/<name>`, relative → `test-logs/<path>`, absolute → as-is).
   - If the command needs the dev container, call `check_docker` early so the user gets a clear error.
   - No placeholders or TODOs — implement real behavior.
   - Exit with a non-zero code on failure; let it propagate from Docker/Maven/pytest.
4. **Make executable**: `chmod +x scripts/commands/gbx-<category>-<action>.sh`.
5. **Fixing a broken command**: reproduce the failure, fix the script (or its `.md`), re-run to confirm, commit. Don't add fallback ad-hoc shell invocations elsewhere.

The `.md` files are legacy Cursor command-palette registrations; commands are now invoked directly
from any shell or via the Task tool (path math via `$SCRIPT_DIR/../..`).
