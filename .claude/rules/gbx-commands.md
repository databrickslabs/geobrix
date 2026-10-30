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

7. **Never spell the container name.** A checkout's dev container comes from `scripts/docker/gbx-container.sh` (sourced by `common.sh`): `geobrix-dev` for the main checkout, the name in an untracked `.gbx-local.env` for a linked worktree. Write `"$GBX_DEV_CONTAINER"` in every `docker exec/start/stop/rm/inspect` and in messages, and call `check_docker` (or `gbx_require_local_env`) before the first one; a guard test fails on a literal `geobrix-dev` in `scripts/commands/*.sh` or `scripts/docker/**/*.sh`. A worktree without the file (or with one that names no container, or `geobrix-dev`) is refused, never defaulted to the main checkout's container, and a container that does not mount the worktree is never used or removed from it (`check_docker` and the start/rebuild commands call `gbx_require_owned_container`).

8. **Write the `.md` in STE-lite** (the rules in `.claude/rules/docs-and-doctests.md`). Agents read these files on almost every dispatch, so length costs tokens and causes misreads.
   - One option per bullet. A sentence that tells the reader what to do has at most 20 words; a sentence that describes has at most 25.
   - Order: what the command does (1-2 sentences), usage, options (one bullet each: what it does, its default), examples, then limits.
   - Put long detail (metric definitions, NULL rules, known limits) under its own heading as a list, never in an option's paragraph. Link to `docs/docs/` for the full reference.

The `.md` files are legacy Cursor command-palette registrations; commands are now invoked directly
from any shell or via the Task tool (path math via `$SCRIPT_DIR/../..`).
