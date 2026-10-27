---
paths:
  - "scripts/commands/**"
---

# Adding or fixing a `gbx:*` command

When adding a new `gbx:<category>:<action>` command (or fixing an existing one — don't work around failures, fix the command):

1. **Pick category and action.** Categories in use: `test`, `coverage`, `data`, `docs`, `docker`, `dcs`, `ci`, `lint`, `security`, `versions`, `prompt`. Confirm no duplicate exists in `scripts/commands/`.
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
6. **Never detach a run with `&`, `nohup` or `( … &)`.** Use the harness's `run_in_background`, which is tracked: a detached run is invisible and the next run overlaps it (two Maven JVMs ended up on one `target/` this way). Commands that run Maven or a Spark test JVM in `geobrix-dev` take the container test lock (`gbx_acquire_test_lock` in `common.sh`: wait by default, `--no-wait` exits 75) and send that work through `$GBX_LOCK_RUN` so the work holds a lock of its own. Guarantee: gbx commands do not overlap, even after a killed run, because the container's Maven keeps running and keeps its lock, so the next run waits for it. Not covered: Maven started in the container outside a gbx command. A new such command must call the helper, wrap its exec, and document `--no-wait`.

The `.md` files are legacy Cursor command-palette registrations; commands are now invoked directly
from any shell or via the Task tool (path math via `$SCRIPT_DIR/../..`).
