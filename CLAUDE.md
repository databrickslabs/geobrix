# CLAUDE.md

This file is the entry point for any Claude (or Cursor) session in this repo. User-global preferences live at `~/.claude/CLAUDE.md`; this file adds geobrix-specific facts and translates the user-global patterns into what they mean *here*.

## Project

**GeoBrix** is a high-performance spatial processing library — a modern successor to [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/), targeting Databricks Runtime (DBR 17.3 LTS or 18 LTS). Current version **0.5.0** (beta). APIs may break to stabilize, and there are **no function aliases** — one canonical name per function. See `docs/docs/beta-release-notes.mdx` for breaking changes.

Heavy code is Scala/Spark (JAR); lightweight bindings are Python (wheel) and SQL, both wrapping the Scala columnar expressions via Spark Connect.

Current branch: `beta/0.4.0`. Repo: `databrickslabs/geobrix`.

## Working patterns in this repo

These are the geobrix-specific translations of user-global preferences (`~/.claude/CLAUDE.md`):

- **`gbx:*` commands are authoritative.** They are the canonical entry points for tests, coverage, docs, lint, Docker, data, CI, and security in this repo. If a `gbx:*` command doesn't do what you need, **fix the command** — don't work around it with ad-hoc shell, and don't paper over it by augmenting with extra inline logic. The "Adding or fixing a `gbx:*` command" section below has the procedure. The whole point of the palette is that everyone (you, me, future contributors, CI) runs the same code path.
- **Orchestrator-master + per-task subagents** — Never run a `gbx:*` command inline if it touches the docker container, Maven, or the doc-test suite. Dispatch a Task subagent with the full task text and let it handle the long-running work in isolation. Test suites often take minutes; running inline blocks the main session. **Orient every subagent with the "Subagent orientation" section below** — an un-oriented subagent burns a run rediscovering repo basics, or reports a repo invariant as if it were a finding.
- **Check Databricks auth BEFORE dispatching, not after a browser tab appears.** The main agent owns auth readiness. Run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` (read-only, never opens a browser) before each dispatch block, and confirm the profiles the work needs are `VALID`. Most geobrix work is local (Docker/Maven/pytest/docs/git) and needs **no** profile — don't ask the user to re-auth a profile the work doesn't touch. Subagents must never fix auth; a `databricks auth login` is hook-blocked and only the user can run it.
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
- Raster (GDAL): `gdal`, `gtiff_gdal`, `netcdf_gdal`
- Vector (OGR): `ogr`, `shapefile_ogr`, `geojson_ogr`, `gpkg_ogr`, `file_gdb_ogr`, `netcdf_ogr`

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

#### Code examples are GENERATED — never hand-edit the JSON

`function-info.json` is a **build artifact**. Hand-editing it works until the next
`gbx:docs:function-info`, which silently overwrites your change. To fix what
`DESCRIBE FUNCTION EXTENDED` prints, edit the **source**, then regenerate:

| To change... | Edit this | Not this |
|---|---|---|
| the `Examples:` block | `docs/tests/python/api/*_functions_sql.py` → the function's `*_sql_example()` | ❌ `function-info.json` |
| `Usage:` / `Extended Usage:` | see "signature metadata" below | ❌ `function-info.json` |

How the example is extracted (`docs/scripts/generate-function-info.py`) — these
mechanics surprise people, so check them before wondering why your text vanished:

- Only the **first SQL statement** containing the package prefix is taken
  (`first_statement_containing`). A second query in the same `*_sql_example()` is
  ignored by `DESCRIBE FUNCTION` (it still renders in the docs page).
- `--` comments are **stripped**. Explanatory comments in the example never reach
  `DESCRIBE FUNCTION`; put that prose in the description metadata instead.
- One example can fill **several** functions: every registered name appearing in the
  statement inherits it, EXCEPT a name that has its own dedicated `*_sql_example()`
  (so `gbx_st_asmvt` and `gbx_st_asmvt_pyramid` don't cross-contaminate).
- Keys beginning `_` (e.g. `_package_rasterx`) are section markers, not functions.

#### Canonical `usageArgs` style

`DESCRIBE FUNCTION` prints `name(<usageArgs>) - <description>`, describing the **SQL** surface.

- **Optional arguments use Style B: `[param]`** — brackets wrap only the parameter name, the
  comma stays outside. `geom_wkb, attrs_struct, min_z, max_z, layer_name, [extent]`. Multiple
  trailing optionals: `a, b, [c], [d]`. Do **not** use `geom, target_crs [, source_crs]`
  (comma inside) — that form is being retired.
- **Parameter names are snake_case**, matching SQL — `geom_wkb`, `cell_id`, `size_in_mb`. Not
  the Scala camelCase (`geomWkb`, `cellId`) and not the internal `*Expr` field names.
- An argument is optional exactly when `builder()` has a shorter `case N =>` branch that
  injects a `Literal(...)` default. **34 functions** have optional args; rendering one as
  required is a bug, not a style nit.
- Don't parse the docs `**Signature:**` lines as truth — 63 of 173 use camelCase and at least
  one function has two conflicting lines. Validate against `builder()` arity instead.

#### Signature metadata derivation (automated from Scala)

As of v0.5.0, `usageArgs` and `description` are **derived from Scala case-class fields and builder
arity patterns**, not hand-maintained in `function-info.json`. This eliminates drift: parameter
names stay in sync with the actual Scala source, and optional parameter detection is validated
against real `builder()` branches.

**How it works:**

1. **`docs/scripts/extend-function-metadata.py`** (the parser):
   - Reads all Scala expression files under `src/main/scala/com/databricks/labs/gbx/{rasterx,vectorx,gridx}`.
   - For each function's case class, extracts field names and filters out internal state (e.g., `exprConfExpr`, aggregation buffer offsets).
   - Strips the `Expr` suffix from each field and converts to snake_case.
   - Inspects the `builder()` method: if `case N =>` and `case N+K =>` branches exist with `Literal(...)` defaults in the longer branch, marks args N+1…N+K as optional.
   - Outputs parsed metadata as JSON.

2. **`docs/scripts/generate-function-info.py`** (the generator):
   - Calls the parser to fetch `usage_args` for each function.
   - Merges parsed metadata into the JSON alongside examples (from `*_sql_example()` in docs).
   - Writes `src/main/resources/com/databricks/labs/gbx/function-info.json`.

3. **`WithExpressionInfo`** (the Scala consumer):
   - `getUsageArgs()` and `getDescription()` prefer JSON values (via `FunctionInfoLoader.get(name)`).
   - Fall back to Scala `usageArgs` / `description` overrides only if JSON is absent.
   - This allows legacy Scala overrides to coexist with generated metadata during migration.

**When adding or changing a function:**

- Update the **Scala case class** field names and `builder()` arity — the parser feeds from there.
- Run `gbx:docs:function-info` to regenerate the JSON (no manual edits needed).
- No Scala `usageArgs` override is normally required (it is derived). `description` still is — see below.
- If you must override (e.g., a builder arity is too irregular to parse), add `override def usageArgs` or `override def description` in the companion — the JSON loader respects it as a fallback, and the no-regression check will hold the derived value to it.

**Guardrails (these exist and are mutation-verified):**

- The parser **fails loudly** — it raises `SystemExit` rather than warning, and
  `generate-function-info.py` treats a parser failure as fatal instead of writing `{}`. A silent
  fallback is how an optional argument got published as required.
- **No-regression check** — a derived `usage_args` is compared against every hand-written
  `override def usageArgs`. Losing a bracket, or dropping a parameter the override listed, is a
  hard failure. Verified by mutating the bracket logic: the check caught all 5 override-backed
  functions and exited non-zero.
- **Multi-companion files are reported, not guessed.** When several companions share one SQL name
  (`ST_TransformCrs` + `ST_TransformCrs3` both register `gbx_st_transformcrs`), the parser
  describes the WIDEST case class so trailing optionals stay visible, and prints a note.
- Brace style must not matter: both `=> c.length match {` and `=> {` newline `c.length match {`
  are in use and parse identically.

Not yet wired: `gbx:test:function-info` does not assert usage coverage, and no lint checks bracket
syntax. `check-binding-parity.py` still compares **names only** — it cannot see a parameter list.

Currently **177 of 180** registered functions have derived `usage_args`. The 3 without
(`gbx_rst_fromfile`, `gbx_st_legacyaswkb`, `gbx_pmtiles_agg`) have irregular shapes and are left
absent so the Scala fallback applies. **`description` is still empty for all 180** — `DESCRIBE
FUNCTION` currently renders `name(args) - ` with a trailing dash. Populating descriptions, and
resolving whether derived parameter names should be published while R1/N9 naming debt is open
(the parser faithfully emits `points_array` where the docs say `points_geom`), are deferred.

#### Signature metadata (`usageArgs` / `description`) — a known drift area

`Usage:` is assembled in `WithExpressionInfo` as `name(usageArgs) - description`.
Historically each companion overrode these, but the convention was dropped along the
way: for a long stretch only 8 of ~179 companions had them, so most functions printed
`gbx_rst_foo() - ` — empty parens, no description. Treat blank metadata as a bug, not
a default. See `prompts/refactoring/2026-08-06-describe-function-metadata-drift-inventory.md`.

**A signature change must move every surface together.** Changing arity or a parameter's
meaning touches up to seven places, and the ones that fail *silently* are the dangerous
ones — SQL binds **positionally**, so a wrapper passing an arg the `builder()` doesn't
accept is discarded with no error (this is exactly how `rst_maketiles` advertised
`(tile, tileWidth, tileHeight)` while really taking `(tile, sizeInMB)` — callers set a
megabyte budget believing they set pixel dimensions):

1. the expression case-class fields + `builder()` arity
2. the public Scala wrapper overloads in `<pkg>/functions.scala` — **arg count must match `builder()`**
3. the heavy Python shim (`python/geobrix/src/databricks/labs/gbx/<pkg>/functions.py`)
4. the light Python binding (`.../pyrx|pyvx|pygx/functions.py`) + its registered UDF arity
5. the `**Signature:**` line in `docs/docs/api/*-functions.mdx`
6. the doc-test `*_sql_example()` (the generated example) — and its expected-output constant
7. signature metadata (`usageArgs`/`description`), then regenerate

Cross-check before declaring done: wrapper arg count vs `builder()` accepted range, and
whether each wrapper param name still denotes the quantity of the field it lands on
positionally. `check-binding-parity.py` compares **names only** and cannot see parameter
lists, so none of this is caught by CI today.

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

## Subagent orientation (paste the relevant parts into every dispatch)

A subagent starts with no repo knowledge. Left un-oriented it will rediscover basics on
your budget, work around a `gbx:*` command instead of fixing it, or — worst — report a
**repo invariant as a finding**. Include the applicable items below in the dispatch prompt
itself; don't tell an agent to "go read CLAUDE.md" when you can hand it the slice.

**Facts that are NOT findings.** Every one of these has been reported as a discovery by
some agent. State the relevant ones up front so the agent doesn't burn a run on them:

- **The heavy tier needs a built, staged JAR.** `mvn ... -DskipTests` leaves `target/classes/`
  but **no `*.jar`** unless `package` ran. If no JAR is present, heavy SQL registration
  cannot work and integration/parity tests fail with mass `UNRESOLVED_ROUTINE`. That is a
  missing build artifact, **not** a code defect — build/stage first, then test.
- **The light tier is pure Python and needs no JAR.** `pyrx`/`pyvx`/`pygx` never require the
  JAR; the wheel is always JAR-less.
- **Both tiers register the same `gbx_*` SQL names** and the last registration wins. Function
  metadata + builder are written to the registry as one atomic triple, so implementation and
  metadata cannot desync.
- **SQL binds positionally.** Heavy expressions register as plain Catalyst expressions with no
  named-argument support, so an extra wrapper argument is silently dropped rather than erroring.
- **Doc tests only run in Docker** (they need the full env + sample data under `/Volumes`).
  Corpus tests skip unless the container was started with the sample-data mounts.
- **`/prompts/` is gitignored** scratch; `docs/superpowers/` is version-controlled.
- Non-EPSG / authority-less CRS may render as different-but-equivalent strings across tiers.
  Parity means CRS-equivalence, not string equality.

**Standing instructions for any implementation subagent:**

1. Use `gbx:*` commands, never ad-hoc `docker`/`mvn`/`pytest`. If a command is broken, **fix
   the command** and say how it broke — never route around it.
2. Run **only the affected suites**; a full run is the orchestrator's call.
3. Never run `databricks auth login` (hook-blocked) and never try to fix auth.
4. Don't commit unless explicitly told to.
5. **Verify before reporting.** Read the source behind every claim. Regex sweeps over Scala
   produce false positives (`case Seq(...)`, `c.head`, overload chains that delegate) — mark
   findings CONFIRMED vs SUSPECTED and quote real source, never paraphrase a signature from
   memory. A fabricated parameter list is worse than no report.
6. If a precondition is missing (no JAR, no sample data, stale staged artifact), **say so
   plainly and stop** — do not report the consequence as a defect, and do not silently
   substitute a weaker test.
7. Exclude build artifacts from every search: `docs/build-static-zip/`,
   `docs/tests/coverage-report/`, `docs/tests/.pytest_cache/`, `target/`, `scripts/docker/m2/`,
   `*.pyc`. A naive grep for a Scala symbol otherwise hits minified JS in the docs build.

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

## Databricks authentication

Work that touches a workspace (staging the wheel/JAR to a Volume, running Serverless jobs, `databricks-query`) needs a valid profile. **Never auto-select one** — pass `--profile <name>` explicitly and let the user choose. In Claude Code each Bash call is a separate shell, so `export DATABRICKS_CONFIG_PROFILE=…` on its own line does NOT carry to the next command; use `--profile`, or chain with `&&`.

Profiles in `~/.databrickscfg` (check live status with `databricks auth profiles`):

| Profile | Workspace | Use for |
|---|---|---|
| `oauth-fe` | `e2-demo-field-eng` | The usual one for geobrix — Volumes, jobs, warehouses |
| `logfood` | `adb-2548836972759138` (Azure) | Internal metrics/logfood queries |
| `oauth` | `fevm-serverless-stable-vqr02h` | FEVM serverless workspace |
| `genie-map-env` | `fevm-serverless-stable-genie-map` | Genie Map app workspace |
| `DEFAULT` | `e2-demo-field-eng` | PAT-based; prefer `oauth-fe` instead |

**Why you get re-prompted, and what actually helps.** All the `oauth*` profiles use `auth_type = databricks-cli` — U2M OAuth. Access tokens last ~1 hour, but the CLI holds a **refresh token** and renews silently, so an expired access token is normal and not by itself a reason to log in again. Repeated browser prompts almost always mean one of:

- **The refresh token itself expired** (idle too long for that workspace). Fix: `databricks auth login --host <url> --profile <name>` for that ONE profile. Re-authenticating every profile is unnecessary.
- **A `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env var is shadowing the profile** — these take precedence over `--profile` and silently bypass cached OAuth. Check with `env | grep -i databricks`.
- **Genuinely idle-aged credentials across many workspaces.** Only fix the profile you need.

**Do not diagnose from `~/.databricks/token-cache.json`.** On macOS, CLI v1.10.0 keeps OAuth tokens in the **system keychain**; that JSON file is a stale leftover from an older CLI. Its timestamps do not update on login and reading them will tell you a profile is expired when it is actually valid. `databricks auth profiles` (the `Valid` column) plus a real call like `databricks current-user me --profile <name>` are the only trustworthy signals.

Token lifetimes are workspace/account-level policy and are **not** configurable per-profile from the CLI, so there is no local setting that extends them. Diagnose before re-authenticating: `databricks auth profiles` shows `Valid YES/NO` per profile, and only the `NO` ones need attention. A `Valid NO` on a profile you aren't using is harmless — don't fix it preemptively.

For unattended/CI work, U2M is the wrong credential: use an OAuth **M2M service principal** (client ID + secret, no browser). That's a separate identity, so it needs its own UC grants on the geobrix catalogs/Volumes/warehouses, and the secret belongs in a secrets manager or env var — never in `~/.databrickscfg` and never committed. Don't use PATs: they expire (~90 days) and are long-lived plaintext bearer secrets.

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
