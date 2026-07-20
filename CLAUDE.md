# CLAUDE.md

This file is the entry point for any Claude (or Cursor) session in this repo. User-global preferences live at `~/.claude/CLAUDE.md`; this file adds geobrix-specific facts and translates the user-global patterns into what they mean *here*.

## Project

**GeoBrix** is a high-performance spatial processing library — a modern successor to [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/), targeting Databricks Runtime (DBR 17.3 LTS or 18 LTS). Current version **0.4.2** (beta). APIs may break to stabilize, and there are **no function aliases** — one canonical name per function. See `docs/docs/beta-release-notes.mdx` for breaking changes.

Heavy code is Scala/Spark (JAR); lightweight bindings are Python (wheel) and SQL, both wrapping the Scala columnar expressions via Spark Connect.

Current branch: `beta/0.4.0`. Repo: `databrickslabs/geobrix`.

## Working patterns in this repo

These are the geobrix-specific translations of user-global preferences (`~/.claude/CLAUDE.md`):

- **`gbx:*` commands are authoritative.** They are the canonical entry points for tests, coverage, docs, lint, Docker, data, CI, and security in this repo. If a `gbx:*` command doesn't do what you need, **fix the command** — don't work around it with ad-hoc shell, and don't paper over it by augmenting with extra inline logic. The "Adding or fixing a `gbx:*` command" section below has the procedure. The whole point of the palette is that everyone (you, me, future contributors, CI) runs the same code path.
- **Orchestrator-master + per-task subagents** — Never run a `gbx:*` command inline if it touches the docker container, Maven, or the doc-test suite. Dispatch a Task subagent with the full task text and let it handle the long-running work in isolation. Test suites often take minutes; running inline blocks the main session.
- **Skills first** — Useful for adjacent work: `databricks-query` for SQL against the workspace, `databricks-workspace-files` for browsing notebooks, `databricks-lakeview-dashboard` for visualization, `databricks-authentication` before any databricks operation. The Field Engineering skills (`fevm`, `sage-context-catalog`) are unrelated to geobrix and shouldn't be invoked here.
- **Runtime judge** — Has already learned the common `gbx:*` scripts (`gbx-test-scala.sh`, `gbx-test-python.sh`, `gbx-docker-exec.sh`, etc.) from prior sessions. New patterns pay a 10-20s warmup; learned patterns are instant. Don't disable.
- **QC judge** — Project config at `.claude/qc-judge/config.json`. Wave-number regex (`wave\s*\d+`) blocks any user-facing doc that leaks the internal planning vocabulary (see "User-facing docs voice" below). `release_notes_path` points at `docs/docs/beta-release-notes.mdx` for the release-notes-current check.
- **gh account switch** — `gh auth switch --user mjohns-databricks` before **any** push, PR creation, PR comment, or `gh api` write to `databrickslabs/geobrix`. The default `mjohns_data` returns 403 for write operations on this repo.
- **Progress feedback on long-running ops** — Scala test suites, Maven builds, full doc tests, and coverage runs routinely take 1-10+ minutes. When you dispatch one of these, give the user a one-line progress update roughly every 30 seconds (tail the log, report the suite/file currently running). Don't go silent for minutes.

## Architecture

Three API packages, each with its own SQL prefix:

| Package | Scala root | Python | SQL prefix | Purpose |
|---|---|---|---|---|
| **RasterX** | `com.databricks.labs.gbx.rasterx` | `databricks.labs.gbx.rasterx` | `gbx_rst_*` | Raster ops (ported from Mosaic raster). Gap-filling — product has no built-in raster. |
| **GridX** | `com.databricks.labs.gbx.gridx.{bng,grid,h3}` | `databricks.labs.gbx.gridx.bng` | `gbx_bng_*` | Discrete global grids, primarily BNG (ported — preserve baseline behavior). |
| **VectorX** | `com.databricks.labs.gbx.vectorx` | `databricks.labs.gbx.vectorx` | `gbx_st_*` | Augments product built-in ST functions: vector-tile (MVT) encoding, TIN surface modeling, and legacy-geometry migration. |

Each package exposes `functions` with `register(spark)` to install SQL UDFs. Shared primitives (`expressions`, `ds`, `util`) live under `com.databricks.labs.gbx`. Spark data source registrations are in `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

**Readers** are namespace-suffixed:
- Raster (GDAL): `gdal`, `gtiff_gdal`
- Vector (OGR): `ogr`, `shapefile_ogr`, `geojson_ogr`, `gpkg_ogr`, `file_gdb_ogr`

Named readers extend generic readers and preset driver options via `dsExtraMap`. Pattern: `<format>_<engine>`. Generic readers (`ogr`, `gdal`) remain clean for flexibility.

Scala 2.13.16, Spark 4.0.0, Java 17. Python 3.12+.

## Development environment

All Maven/test/doc/coverage work runs inside the **`geobrix-dev` Docker container**:

- Project root mounted at `/root/geobrix`
- `sample-data/Volumes` mounted at `/Volumes`
- Maven uses a persistent local repo at `scripts/docker/m2/` (gitignored) to avoid re-downloading deps on restart
- Container commands set `MAVEN_OPTS=-Xmx4G -XX:+UseG1GC`

Use `gbx:docker:start` / `gbx:docker:exec` rather than `docker run` directly. The container has the corp-proxied Maven mirror (`db-maven-proxy`) configured via `scripts/docker/m2/settings.xml`; if proxy is missing, re-run `docker_maven_setup.sh` inside the container.

**`gbx:docker:start` is the canonical (re)create path** — it runs `scripts/docker/start_docker.sh` *and then* `docker_maven_setup.sh`, which copies the `db-maven-proxy` settings into the container's Maven conf. Recreating the container by calling `start_docker.sh` directly skips that step, so the fresh container falls back to blocked Maven Central and the first build dies on plugin resolution (`Connect to repo.maven.apache.org … Connection refused`). If you ever recreate it by hand, run `docker_maven_setup.sh` inside the container afterward. `start_docker.sh` itself resolves the bind mount from `git rev-parse --show-toplevel` (not `$PWD`) and refuses to mount a `.claude/worktrees/*` path — those get auto-cleaned and dangle the mount, making every `docker exec` fail with "current working directory is outside of container mount namespace root".

Default Maven profile is **`skipScoverage`** for fast compile/test (`mvn clean package -DskipTests`). Coverage commands explicitly trigger the `standard` profile.

## Commands (the `gbx:*` palette)

The repo has **50 `gbx:*` commands** in `scripts/commands/` (each is a `.md` registration + a `.sh` implementation). They handle Docker setup, env vars, log paths (`--log filename` → `test-logs/filename`), and profile selection. Originally registered for Cursor's command palette (hence the `.md` files), they're now invoked directly from any shell or via the Task tool.

**If a command fails, fix the command** — do not work around it. The commands are the canonical entry points; ad-hoc shell invocations diverge over time.

Most-used commands by category:

- **Tests**: `gbx:test:scala`, `gbx:test:python`, `gbx:test:scala-docs`, `gbx:test:python-docs`, `gbx:test:sql-docs`, `gbx:test:docs` (all), `gbx:test:function-info`, `gbx:test:notebooks`, `gbx:test:bindings`
  - Single Scala suite: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.*'` or `--suites 'A,B'`
  - Single Python path: `gbx:test:python --path python/geobrix/test/rasterx/`
- **Coverage**: `gbx:coverage:scala-package <pkg>` (1–3 min, use during dev), `gbx:coverage:gaps` (fast, uses existing data), `gbx:coverage:baseline` (weekly, ~10 min). Full `gbx:coverage:scala` runs ~10 min — use `--parallel` or `--report-only` to speed up.
- **Docs**: `gbx:docs:dev` (hot reload, port 3000), `gbx:docs:start` / `gbx:docs:stop`, `gbx:docs:function-info` (regenerate `function-info.json`)
- **Lint**: `gbx:lint:scalastyle` (matches CI — run before push), `gbx:lint:python` (isort/black/flake8; `--fix` on host)
- **Data**: `gbx:data:download --bundle {essential|complete}`, `gbx:data:generate-minimal-bundle`, `gbx:data:push-wheel`, `gbx:data:push-jar`
- **CI**: `gbx:ci:push`, `gbx:ci:status`, `gbx:ci:watch`, `gbx:ci:logs`, `gbx:ci:docs`
- **Docker**: `gbx:docker:start`, `gbx:docker:exec "<cmd>"`, `gbx:docker:attach`
- **Security**: `gbx:security:codeql`

**Log file paths**: `--log filename` resolves to `test-logs/filename`; relative paths resolve under `test-logs/`; absolute paths are used as-is. `test-logs/` is gitignored.

## Conventions

### Cross-language naming consistency

Maintain consistent naming between Scala implementations and Python bindings. Typos across languages silently break bindings.

```
Scala Class:      Component_OperationName     (e.g. BNG_EastNorthAsBNG)
Scala API:        component_operationname     (e.g. bng_eastnorthasbng)
SQL (registered): gbx_<scala-api>             (e.g. gbx_bng_eastnorthasbng)
Python API:       same as Scala API           (e.g. bng_eastnorthasbng)
Test function:    test_<component>_<op>       (e.g. test_bng_eastnorthasbng)
```

- SQL keeps the `gbx_` prefix; the rest mirrors Scala.
- Use `_geom` not `_geometry` (e.g. `bng_geomkring`, not `bng_geometrykring`).
- Keep `_agg` suffix for aggregators (aligns with Databricks geospatial docs).
- Quick check: `grep -r "def bng_" python/geobrix/src/` should match `grep -r "gbx_bng_" src/main/scala/.../register`.
- **Binding parity is enforced.** `gbx:test:bindings` (→ `docs/scripts/check-binding-parity.py`) asserts every name in `registered_functions.txt` exists as a Scala `override def name` literal, a Python `functions.py` binding, and a `function-info.json` key — a function missing from any binding fails (it would surface at runtime as `UNRESOLVED_ROUTINE`). The QC judge runs this on every push via the `binding-parity` command check in `.claude/qc-judge/config.json`. When adding a function, add all three bindings, not just the canonical list.

### BNG resolution

Only **integer indices ±1..±6** (1=100km, 2=10km, 3=1km, 4=100m, 5=10m, 6=1m; negatives = quadrants) or string keys from `BNG.resolutionMap` (e.g. `"1km"`, `"100m"`).

**Never** treat metres-as-Int (e.g. `1000`) as a resolution — that interpretation is not supported by `BNG.getResolution`.

`bng_pointascell` expects BNG eastings/northings (EPSG:27700), not WGS84 lon/lat. Use BNG coords in examples (e.g. `POINT(530000 180000)` for London). `gbx_bng_cellarea` returns **square kilometres**, not square metres.

### GDAL resource management

- **Prefer `rst_fromcontent` with `binaryFile` reader** over `rst_fromfile` when you already have bytes — avoids temp-file races on executors.
- `GetNoDataValue` requires an output array (returns void otherwise).
- `GetStatistics` only works on the MDArray, **not on `Band` directly**.
- Always release Dataset/Band resources via `RasterDriver.releaseDataset(ds)` in a `try/finally`.
- For tests that work with non-EPSG projections (e.g. ESRI:54008), mix in `SilenceProjError` to suppress expected PROJ warnings.
- **Thread-safety (REQUIRED): register GDAL/OGR only via the synchronized `GDALManager` guards** — `GDALManager.init(config)` for GDAL drivers, `GDALManager.initOgr()` for OGR drivers. NEVER call raw `gdal.AllRegister()` / `ogr.RegisterAll()` per task, and never set process-global `gdal.SetConfigOption` outside `GDALManager`'s guarded paths. The GDAL Java bindings hold process-global registry/config state; concurrent Spark tasks in one executor JVM that race registration get a null `GetDriverByName` (NPE) or a native SIGSEGV that kills the executor.

### Unity Catalog Volumes

On a Databricks cluster, `/Volumes/<catalog>/<schema>/<volume>/...` is **FUSE-mounted** — use `pathlib`/`os`, not the Databricks Files SDK.

- The Volume root **must pre-exist**; only paths under it can be created.
- `os.makedirs(volume_root, exist_ok=True)` is a no-op (idempotent).
- Avoid `seek` on volume files; use sequential I/O.
- For writes, prefer `shutil.copy` from a temp file.
- Sanitize env-derived strings (strip BOM/invisible Unicode) before building volume paths.

Env vars: `GBX_BUNDLE_VOLUME_CATALOG`, `GBX_BUNDLE_VOLUME_SCHEMA`, `GBX_BUNDLE_VOLUME_NAME`. Volume name must match Data Explorer exactly (hyphen vs underscore matters).

### Function-info / DESCRIBE FUNCTION

Single-source pattern: doc SQL examples in `docs/tests/python/api/{rasterx,gridx,vectorx}_functions_sql.py` (functions named `*_sql_example()`) feed `docs/scripts/generate-function-info.py`, which writes `src/main/resources/com/databricks/labs/gbx/function-info.json`. The canonical registered-function list is `docs/tests-function-info/registered_functions.txt`.

- **No aliases.** Beta = we break API to stabilize. Fix upstream (Scala registration + `registered_functions.txt`) to a single canonical name.
- Run regeneration via `gbx:docs:function-info` or `gbx:test:function-info` (which also runs pytest).
- Tests assert every function in `registered_functions.txt` has a non-empty example in `function-info.json`. If coverage fails, fix upstream — never add placeholder/empty usage.

### Doc tests are the documentation source (single source of truth)

Tests ARE the documentation source, not validators of it. Docs import code from tests via webpack raw-loader.

- Code lives in `docs/tests/python/` and `docs/tests/scala/`.
- MDX imports via: `import code from '!!raw-loader!../../tests/python/module/file.py';` (from `docs/docs/<subdir>/`).
- Tests **must execute real code with real assertions** — not just check structure or compilation. Use real sample data from `/Volumes/main/geobrix_samples/geobrix-examples/{nyc,london}/`.
- Run doc tests in Docker via `gbx:test:*-docs` commands. Doc tests **only run in Docker** (need full env + sample data).
- Do not mock Spark, GeoBrix, or file I/O. Mock only external APIs / very expensive ops / flaky deps.
- Doc-test iteration: **run per-package with its own log, narrow to failing test node IDs, rerun only those until green** — don't retest passing packages.

### User-facing docs voice (no internal vocabulary)

Anything under `docs/docs/` is read by end users — release notes, package pages, notebook walkthroughs, security/installation, etc. Never leak internal release-planning vocabulary into user-facing docs.

| ❌ Don't write | ✅ Write instead |
|---|---|
| "Composes with `gbx_pmtiles_agg` (Wave 6)" | "Composes with `gbx_pmtiles_agg`" |
| "the Wave 1 aggregator" | "the aggregator" or `gbx_st_asmvt` |
| references to internal subagents or dispatch sequencing | reference behavior, not the process |

**Wave numbers** are legitimate only in: `prompts/features/*.md` (internal plans), dispatch prompts (internal), git commit messages (internal), `input/` scoping drafts (gitignored).

Quick check before merging: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ 2>/dev/null` should print nothing. The QC judge enforces this automatically via the `internals-leak` check.

## Adding or fixing a `gbx:*` command

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

## Session artifacts

Two locations, by artifact class:

- **Design specs and implementation plans** (the `superpowers` workflow outputs) live under `docs/superpowers/` — specs (brainstorming-skill output, the `*-design.md` files) under `docs/superpowers/specs/YYYY-MM-DD-<kebab-topic>-design.md`, and plans (writing-plans-skill output) under `docs/superpowers/plans/YYYY-MM-DD-<kebab-topic>.md`. This tree is **version-controlled** — specs and plans are committed alongside the work they describe.
- **Everything else** (session summaries, analyses, progress notes, scoping drafts) goes under `prompts/<category>/YYYY-MM-DD-<kebab-topic>.md`. Categories include `features/`, `documentation/`, `refactoring/`, `testing/`, `bugfixes/`. **`/prompts/` is gitignored** — local scratch, not committed.

## What used to live under `.cursor/`

The project was originally driven through Cursor. That tree has been retired:

- `.cursor/rules/*.mdc` → **removed**; surviving content is in the "Conventions" section above.
- `.cursor/agents/*.md` → **removed**; Claude doesn't use Cursor's agent persona model. Dispatch via `Task` tool with `general-purpose` subagent and the relevant section of this file as context.
- `.cursor/skills/` → **removed**; the surviving procedure (add/fix a `gbx:*` command) is in the section of the same name above.
- `.cursor/commands/` → **moved to `scripts/commands/`** (same files, same path math via `$SCRIPT_DIR/../..`). Cursor's command-palette discovery no longer fires for these; invoke from any shell or via Task.

If you see old commit history, prompt files, or external references using `.cursor/commands/...`, treat them as historical — substitute `scripts/commands/...`.
