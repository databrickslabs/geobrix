# CLAUDE.md

Entry point for any Claude (or Cursor) session in this repo. User-global preferences live at
`~/.claude/CLAUDE.md`; this file adds geobrix-specific facts and translates those patterns into
what they mean *here*. **Deep, task-specific conventions are split into `.claude/rules/*.md`**
(path-scoped — they load automatically when you touch matching files); this file is the always-on
core. The index of those rules is at the bottom.

## Project

**GeoBrix** is a high-performance spatial processing library — a modern successor to [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/), targeting Databricks Runtime (DBR 17.3, 18, or 19). Current version **0.5.2**. APIs may break to stabilize, and there are **no function aliases** — one canonical name per function. See `docs/docs/release-notes.mdx` for breaking changes.

Heavy code is Scala/Spark (JAR); lightweight bindings are Python (wheel) and SQL, both wrapping the Scala columnar expressions via Spark Connect.

Current branch: `beta/0.5.0`. Repo: `databrickslabs/geobrix`. Release fixes land on `beta/*` first; the user merges to `main` (docs deploy is main-only). A `branch/*` name is a red flag — confirm `beta/*`.

## Working patterns in this repo

Geobrix-specific translations of user-global preferences:

- **`gbx:*` commands are authoritative.** Canonical entry points for tests, coverage, docs, lint, Docker, data, CI, security. If a `gbx:*` command doesn't do what you need, **fix the command** — don't work around it with ad-hoc shell or extra inline logic. See `.claude/rules/gbx-commands.md`.
- **Orchestrator-master + per-task subagents** — Never run a `gbx:*` command inline if it touches the Docker container, Maven, or the doc-test suite. Dispatch a Task subagent with the full task text. Test suites take minutes; inline blocks the main session. **Orient every subagent** with the "Subagent orientation" section below.
- **Check Databricks auth BEFORE dispatching, not after a browser tab appears.** The main agent owns auth readiness. Run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` (read-only, never opens a browser) before each dispatch block, and confirm the profiles the work needs are `VALID`. Most geobrix work is local (Docker/Maven/pytest/docs/git) and needs **no** profile. Subagents must never fix auth; `databricks auth login` is hook-blocked and only the user can run it.
- **Skills first** — For adjacent work: `databricks-query` (SQL against the workspace), `databricks-workspace-files` (notebooks), `databricks-lakeview-dashboard`, `databricks-authentication`. The Field Engineering skills (`fevm`, `sage-context-catalog`) are unrelated to geobrix.
- **Runtime judge** — Has learned the common `gbx:*` scripts. New patterns pay a 10-20s warmup; learned patterns are instant. Don't disable.
- **QC judge** — Project config at `.claude/qc-judge/config.json`. Gates `git push`. When it blocks, read `~/.claude/qc-judge/reports/<latest>.md` and address findings — don't reflexively `QC_OVERRIDE=1` (and if you do, it must be *exported*, not an inline prefix). It runs binding-parity, doc-coverage, internals-leak (wave-number regex), python lint, secrets, and commit-message hygiene (subjects ≤72 chars + a WHY body).
- **gh account switch** — `gh auth switch --user mjohns-databricks` before **any** push, PR creation, PR comment, or `gh api` write to `databrickslabs/geobrix`. The default `mjohns_data` returns 403 for writes.
- **Progress feedback on long-running ops** — Scala suites, Maven builds, full doc tests, and coverage runs take 1-10+ min. Give a one-line progress update (tail the log) roughly every 30 seconds. Don't go silent for minutes.
- **Verify before reporting; hold pushes and batch.** Push on the user's go or at a clear stopping point, not per commit. Before pushing, `git status` for user hand-edits and commit them.

## Architecture

Three API packages, each with its own SQL prefix:

| Package | Scala root | Python | SQL prefix | Purpose |
|---|---|---|---|---|
| **RasterX** | `com.databricks.labs.gbx.rasterx` | `databricks.labs.gbx.rasterx` | `gbx_rst_*` | Raster ops (ported from Mosaic raster). Gap-filling — product has no built-in raster. |
| **GridX** | `com.databricks.labs.gbx.gridx.{bng,grid,h3}` | `databricks.labs.gbx.gridx.bng` | `gbx_bng_*` | Discrete global grids, primarily BNG (ported — preserve baseline behavior). |
| **VectorX** | `com.databricks.labs.gbx.vectorx` | `databricks.labs.gbx.vectorx` | `gbx_st_*` | Augments product built-in ST functions: vector-tile (MVT) encoding, TIN surface modeling, and legacy-geometry migration. |

Each package exposes `functions` with `register(spark)` to install SQL UDFs. Shared primitives (`expressions`, `ds`, `util`) live under `com.databricks.labs.gbx`. Spark data source registrations are in `src/main/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.

**Readers** are namespace-suffixed (`<format>_<engine>`), e.g. raster (GDAL): `gdal`, `gtiff_gdal`, `netcdf_gdal`; vector (OGR): `ogr`, `shapefile_ogr`, `geojson_ogr`, `gpkg_ogr`, `file_gdb_ogr`, `netcdf_ogr`. Light readers use the `*_gbx` suffix (pure Python, no JAR). Named readers extend generic readers and preset driver options via `dsExtraMap`; generic readers (`ogr`, `gdal`) stay clean.

Scala 2.13.16, Spark 4.0.0, Java 17. Python 3.12+. A **single wheel + single JAR** runs on DBR 17.3/18/19.

## Development environment

All Maven/test/doc/coverage work runs inside the **`geobrix-dev` Docker container** (project root at `/root/geobrix`, `sample-data/Volumes` at `/Volumes`, persistent Maven repo at `scripts/docker/m2/`, `MAVEN_OPTS=-Xmx4G -XX:+UseG1GC`).

- **`gbx:docker:start` is the canonical (re)create path** — it runs `start_docker.sh` *and then* `docker_maven_setup.sh`, which copies the `db-maven-proxy` settings into the container's Maven conf. Recreating by calling `start_docker.sh` directly skips that step, so the first build dies on plugin resolution (Maven Central blocked). If you recreate by hand, run `docker_maven_setup.sh` inside the container afterward.
- `start_docker.sh` resolves the bind mount from `git rev-parse --show-toplevel` and refuses to mount a `.claude/worktrees/*` path — those dangle the mount and make every `docker exec` fail with "current working directory is outside of container mount namespace root".
- Use `gbx:docker:start` / `gbx:docker:exec` rather than `docker run` directly.
- Default Maven profile is **`skipScoverage`** for fast compile/test (`mvn clean package -DskipTests`). Coverage commands explicitly trigger the `standard` profile.
- Corpus/doc tests skip unless the container was started with the sample-data mounts; heavy needs a built JAR.

## Commands (the `gbx:*` palette)

**50 `gbx:*` commands** in `scripts/commands/` (each a `.md` registration + a `.sh` implementation). They handle Docker setup, env vars, log paths (`--log filename` → `test-logs/filename`; relative → under `test-logs/`; absolute → as-is), and profile selection. **If a command fails, fix the command** — don't work around it (procedure in `.claude/rules/gbx-commands.md`).

- **Tests**: `gbx:test:scala`, `gbx:test:python`, `gbx:test:scala-docs`, `gbx:test:python-docs`, `gbx:test:sql-docs`, `gbx:test:docs`, `gbx:test:function-info`, `gbx:test:notebooks`, `gbx:test:bindings`
  - Single Scala suite: `gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.*'` or `--suites 'A,B'`
  - Single Python path: `gbx:test:python --path python/geobrix/test/rasterx/`
  - Cross-tier parity tests SKIP on plain `gbx:test:python` — re-run `--with-integration` after a JAR change.
- **Coverage**: `gbx:coverage:scala-package <pkg>` (1–3 min), `gbx:coverage:gaps` (fast), `gbx:coverage:baseline` (~10 min). Full `gbx:coverage:scala` ~10 min — use `--parallel` or `--report-only`.
- **Docs**: `gbx:docs:dev` (hot reload, port 3000 — reserved for the user; agents use `--port 3001`), `gbx:docs:start` / `gbx:docs:stop`, `gbx:docs:function-info` (regenerate `function-info.json` — in Docker, or it leaks host paths).
- **Lint**: `gbx:lint:scalastyle` (matches CI — run before push), `gbx:lint:python` (isort/black/flake8; `--fix` on host). Pre-push includes `gbx:lint:python --check`, not just scalastyle.
- **Data**: `gbx:data:download --bundle {essential|complete}`, `gbx:data:generate-minimal-bundle`, `gbx:data:push-wheel`, `gbx:data:push-jar`
- **CI**: `gbx:ci:push`, `gbx:ci:status`, `gbx:ci:watch`, `gbx:ci:logs`, `gbx:ci:docs`. Dev-branch CI is **not a gate** — batch and push; don't chase red→green on it.
- **Docker**: `gbx:docker:start`, `gbx:docker:exec "<cmd>"`, `gbx:docker:attach`
- **Review / Security**: `gbx:review:round` (Isaac — scope it; a full branch-vs-main run silently exits 0/0 findings), `gbx:security:codeql`

## Subagent orientation (paste the relevant parts into every dispatch)

A subagent starts with no repo knowledge. Left un-oriented it rediscovers basics, works around a
`gbx:*` command, or — worst — reports a **repo invariant as a finding**. Hand it the slice; don't
say "go read CLAUDE.md." The deep conventions in `.claude/rules/*.md` load for a subagent when it
edits matching files, but paste the relevant rule into the dispatch when the work needs it up front.

**Facts that are NOT findings** (each has been reported as a "discovery"):

- **The heavy tier needs a built, staged JAR.** `mvn ... -DskipTests` leaves `target/classes/` but **no `*.jar`** unless `package` ran. No JAR ⇒ heavy SQL registration can't work ⇒ mass `UNRESOLVED_ROUTINE`. That's a missing artifact, **not** a code defect — build/stage first.
- **The light tier is pure Python and needs no JAR.** `pyrx`/`pyvx`/`pygx` never require it; the wheel is JAR-less.
- **Both tiers register the same `gbx_*` SQL names** and the last registration wins. Metadata + builder write as one atomic triple — implementation and metadata cannot desync. One geobrix JAR per cluster (pkg + `gbx_*` collision).
- **SQL binds positionally** — an extra wrapper argument is silently dropped, not errored.
- **Doc tests only run in Docker** (full env + sample data under `/Volumes`).
- **`.superpowers/` is gitignored** scratch — all internal planning. The public representation of decisions lives in `docs/docs/`.
- Non-EPSG / authority-less CRS may render as different-but-equivalent strings across tiers. Parity means CRS-equivalence, not string equality.

**Standing instructions for any implementation subagent:**

1. Use `gbx:*` commands, never ad-hoc `docker`/`mvn`/`pytest`. If a command is broken, **fix the command** and say how it broke.
2. Run **only the affected suites**; a full run is the orchestrator's call.
3. Never run `databricks auth login` (hook-blocked) and never try to fix auth.
4. Don't commit unless explicitly told to.
5. **Verify before reporting.** Read the source behind every claim; mark findings CONFIRMED vs SUSPECTED and quote real source. Regex sweeps over Scala produce false positives — a fabricated parameter list is worse than no report.
6. If a precondition for a scoped check is missing (no JAR, no sample data, stale artifact), emit **one line** — `PRECONDITION MISSING: <what>; <check> not run` — and stop that check. Don't report the consequence as a defect.
7. Exclude build artifacts from every search: `docs/build-static-zip/`, `docs/tests/coverage-report/`, `docs/tests/.pytest_cache/`, `target/`, `scripts/docker/m2/`, `*.pyc`.

**Lead-agent responsibility (do NOT push onto the subagent):** decide the tier/JAR strategy *before* dispatching and state it in the prompt. Check `ls target/*.jar` yourself. Say explicitly which tiers to exercise, whether a staged JAR exists, and what to do if a precondition is absent. When heavy verification is wanted but no fresh JAR is staged, either (a) build+stage first, or (b) hand a JAR-free isolation path (register the pyrx UDF directly via `spark.udf.register`; do NOT call `rasterx.register()` — it loads the JAR). If neither is possible, tell the subagent heavy is out of scope.

## Databricks authentication

Work that touches a workspace (staging to a Volume, Serverless jobs, `databricks-query`) needs a valid profile. **Never auto-select one** — pass `--profile <name>` and let the user choose. Each Bash call is a separate shell, so `export DATABRICKS_CONFIG_PROFILE=…` on its own line does NOT carry forward; use `--profile` or chain with `&&`.

| Profile | Workspace | Use for |
|---|---|---|
| `oauth-fe` | `e2-demo-field-eng` | The usual one for geobrix — Volumes, jobs, warehouses |
| `logfood` | `adb-2548836972759138` (Azure) | Internal metrics/logfood queries |
| `oauth` | `fevm-serverless-stable-vqr02h` | FEVM serverless workspace |
| `genie-map-env` | `fevm-serverless-stable-genie-map` | Genie Map app workspace |
| `DEFAULT` | `e2-demo-field-eng` | PAT-based; prefer `oauth-fe` |

- The `oauth*` profiles use U2M OAuth: access tokens last ~1 h but the CLI refreshes silently, so an expired access token is normal and not by itself a reason to re-login. Repeated browser prompts mean the *refresh* token expired (`databricks auth login --host <url> --profile <name>` for that ONE profile), a `DATABRICKS_HOST`/`DATABRICKS_TOKEN` env var is shadowing the profile, or genuinely idle-aged creds.
- **Do not diagnose from `~/.databricks/token-cache.json`** — on macOS CLI v1.10+ tokens live in the keychain; that JSON is stale. Trust `databricks auth profiles` (the `Valid` column) and a real `databricks current-user me --profile <name>`.
- A `Valid NO` on a profile you aren't using is harmless — don't fix it preemptively. Don't auto-run CLI/SDK/MCP on OAuth profiles unless asked (fires browser tabs).
- For unattended/CI, use an OAuth **M2M service principal** (its own UC grants; secret in a manager, never in `~/.databrickscfg`). Don't use PATs.

## Session artifacts

All internal planning lives under the **gitignored `.superpowers/` tree** (this **overrides** the brainstorming/writing-plans skills' default `docs/superpowers/` location):

- **Design specs** → `.superpowers/specs/YYYY-MM-DD-<kebab-topic>-design.md`
- **Implementation plans** → `.superpowers/plans/YYYY-MM-DD-<kebab-topic>.md`
- **Everything else** (summaries, analyses, notes) → `.superpowers/prompts/<category>/YYYY-MM-DD-<kebab-topic>.md` (`features/`, `documentation/`, `refactoring/`, `testing/`, `bugfixes/`)
- **Scoping drafts / raw input** → `.superpowers/input/`. **SDD ledgers** → `.superpowers/sdd/<plan-basename>/`.

The project was originally driven through Cursor; that tree is retired. `.cursor/commands/` → **moved to `scripts/commands/`** (same path math via `$SCRIPT_DIR/../..`); `.cursor/rules|agents|skills` → **removed**, surviving content folded into this file and `.claude/rules/`. Old references to `.cursor/commands/...` are historical — substitute `scripts/commands/...`.

## Deep-dive rules (`.claude/rules/`)

Path-scoped — each loads automatically when you edit a matching file. Read the relevant one before doing that class of work; paste it into a subagent dispatch when the work needs it up front.

| Rule file | Loads when you touch | Covers |
|---|---|---|
| `functions-authoring.md` | Scala expressions, `functions.py`, `docs/docs/api/*.mdx`, `function-info.json`, `registered_functions.txt` | Cross-language naming, the 7-surface signature change, function-info/DESCRIBE generation, `usageArgs` style, param-name distinctions |
| `gdal-resources.md` | rasterx/ds Scala, `ds`/`pyrx` Python | GDAL/OGR `GDALManager` guards, resource release, materialize policy, pyrx Serverless constraints |
| `bng-resolution.md` | gridx Scala/Python | BNG resolution indices, `bng_pointascell` CRS, grid tessellation & geom-aware kring/kloop |
| `uc-volumes.md` | package Python, notebooks | UC Volume FUSE semantics, bare-path heavy reads, Serverless parallelism |
| `docs-and-doctests.md` | `docs/**` | Doc-tests-as-source, user-facing voice (no wave numbers), MDX/sidebar/deploy gotchas |
| `notebooks-on-databricks.md` | `notebooks/**`, notebook runner commands | Staging on dogfood, `%pip` wheel-install incantations, canonical runners, viz |
| `gbx-commands.md` | `scripts/commands/**` | How to add or fix a `gbx:*` command |
