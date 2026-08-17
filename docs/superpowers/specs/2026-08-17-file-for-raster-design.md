# FILE-for-raster — reader / writer / grouped-execution design

**Status:** design (post-spike). Empirical grounding: `input/file_type/2026-08-17-read-perf-matrix-and-handling-rules.md`
(+ the throwaway probes `prompts/testing/2026-08-17-*.py`) and `input/file_type/2026-08-15-file-type-for-raster-findings.md`.
**Scope:** light tier first (pyrx/pyvx); heavy (Scala) a support-to-the-extent-able fast-follow. Docs deferred (grounding captured).

---

## 1. Goal & framing

Add **governed, OOM-free, fast** virtual-tile storage/handling backed by the Databricks **FILE** type, **as an opt-in
accelerator layered over the portable Volume-path/FUSE behavior GeoBrix already has** — never a hard dependency.

The three tile representations (existing framing) are the spine:

| | 1. Materialized (BINARY) | 2. Virtual — EXTERNAL (Volume) | 3. Virtual — MANAGED (FILE) |
|---|---|---|---|
| storage | bytes inline in `tile.raster` | reference (path+window) to a Volume file | FILE ref; bytes in table-governed managed storage |
| lifecycle | in-row (bloat) | disconnected (orphans on row delete) | **unified** (GC with the table) |
| governance | table | Volume | **table (row/col policies apply to the FILE)** |
| OOM | high | low | low |
| availability | everywhere | everywhere | **GeoBrix treats FILE as a DBR 19+ capability (validated on DBR 19.5-snapshot classic). Platform docs say 18+ Beta, but its caveats + serverless-notebook/Spark-Connect display limits make DBR 18 a non-target.** |

FILE adds representation 3 (governed + fast reads); the reader/writer/functions must **degrade to 1/2 when FILE is absent**.

## 2. Empirical facts that drive the design (measured 2026-08-17, dogfood DBR 19.5)

1. **Cold random-window reads: `.open()` stream ≈ 0.66 ms vs FUSE ≈ 127 ms (~190×).** Stream is the fast read path where FILE exists.
2. **Read cost is dominated by the OPEN, not the pixel read.** Stream open scales with size/layout (COG → seconds at 7 GB; **striped → ~47–54 s** at 10 GB); FUSE open is flat-cheap (~7 ms). ⇒ **amortize opens.**
3. **Amortization works:** a per-partition LRU of open datasets + path-grouping cuts opens from N→n_files (spike: 3 vs 90; 15 vs 135).
4. **Per-row FileRef marshaling is a large tax** (per-row scalar UDF ~123–580 ms/tile). **`mapInPandas` (per-partition) is 6× faster (20.6 ms/tile) and pyrx-pure**, and **a FileRef survives into pandas/Arrow with `.open()` intact.** ⇒ the FILE fast path is a **partition-scoped executor**, not a per-row scalar UDF.
5. **MANAGED ≈ EXTERNAL for reads;** `.as_local_file()` is a zero-copy FUSE alias (never stages a copy).
6. **Write layout controls read-back grouping:** `ORDER BY path` on write → a *dumb* read amortizes; `CLUSTER BY` is durable but needs `OPTIMIZE` (not immediate on a fresh insert); a **smart read** (`repartition(n,'path').sortWithinPartitions('path')`) rescues *any* write and adds parallelism.
7. **Non-FILE (DBR 17/18, Serverless-today):** stage-once-per-partition → local read 0.57 ms (~220× over cold FUSE), **break-even ~17 windows/FILE**; and **FUSE worker reads scale with parallelism (no driver funnel).**
8. **FILE writes require a pre-declared typed column** (`FILE MANAGED`/`FILE EXTERNAL`) + `INSERT`; bare `saveAsTable`/CTAS fails `FILE_TYPE_UNKNOWN_WRITE`. MANAGED requires a `databricks.filespace-preview` `/Volumes` FileSpace.

## 3. Architecture

Three units + a capability gate.

```
WRITER (Unit 2)  →  Delta table: [other cols] + tile_file FILE(MANAGED|EXTERNAL), laid out FILE-aligned,
                    self-describing via TBLPROPERTIES(geobrix.version, geobrix.file.write_strategy)
        ↓ read
READER (Unit 1, file_gbx base)  →  v2 `tile` (+ pass-through cols), partitions aligned to source FILE,
                    FILE-first / Volume-FUSE fallback, capability-gated, cued by table properties
        ↓ compute
GROUPED EXECUTOR (Unit 3)  →  partition-scoped mapInPandas + per-partition capability-adaptive open-resource
                    LRU (FILE→stream · non-FILE-moderate→local-stage · non-FILE-huge→open-dataset/FUSE)
```

**Capability gate** (`pyrx/_file_ref.py:file_supported()`, exists): FILE path when supported, else representation 1/2 + FUSE.

### 3.1 Tile v2 `path_mode` (per-tile) **and** table properties (table-level) — both, different scopes

Add **`path_mode`** to tile v2: `None` (materialized — `raster` present) · `'external'` (path backed by a plain
Volume, usable as EXTERNAL FILE where supported, else FUSE) · `'managed'` (path backed by a governed **MANAGED FILE**
filestore). It **travels with the tile** through DataFrames/compute → the **per-tile ground truth** functions key off
(× `file_supported()`) to choose a read strategy. This is **not** replaced by the table property — they operate at
different scopes and are complementary:

| where the data came from | table props present? | how `path_mode` is set |
|---|---|---|
| **our writer wrote the table** | yes (`geobrix_writer_version`, `geobrix.file.write_strategy`) | writer **stamps `path_mode` per tile** at write; reader trusts it; table props add *table-level* planning (aligned? clustered? MANAGED?) |
| FILE-column table **not** written by us | no geobrix props | reader **infers** `path_mode` from the FILE column type (MANAGED→`'managed'`, EXTERNAL→`'external'`) |
| user-built DataFrame / foreign source | n/a | **inferred from tile structure**: `raster` present→`None`; FILE ref→its type; plain Volume path, no raster→`'external'` |

- **Absent `path_mode`** (older v2 tiles, or unset) → **inferred from structure** (raster→None; FILE-ref→type; path→external).
  So `path_mode` is **stored-when-known, inferred-when-absent** — backward-compatible with existing v2 tiles.
- **Per-tile read plan** = `path_mode` × `file_supported()`: managed+FILE→stream (MANAGED FileRef); external+FILE→
  EXTERNAL-FILE stream or FUSE; external+no-FILE→FUSE / local-stage; None→materialized bytes.
- **Cost:** a `V2_TILE_SCHEMA` field add (8→9), both tiers + serde + `v1→v2` default (`None`). It is a struct-field add,
  **not** a function-signature change; beta is the free window.
- **`path_mode` (per-tile) vs `geobrix.file.write_strategy` (table-level):** the former is required for correct per-tile
  handling anywhere the data flows; the latter is an *optional* table-level planning hint present only for our-writer tables.
  Keep both.

## 4. Unit 1 — Reader (`file_gbx` base)

- A **FILE-aware base reader** shared by raster and vector namespaced readers; resolves a MANAGED/EXTERNAL FILE column
  (or a plain path column) → **v2 `tile`** (path/window/crs/…) **plus pass-through of the DataFrame's other columns**.
- **FILE-first / Volume-FUSE fallback** at the existing decode chokepoint (`core/open_tile._to_virtual_tile`,
  `file_ref_arg`): when FILE is present + supported, keep the FileRef on the tile for stream reads; else derive
  `path = uri.removeprefix("dbfs:")` (validated FUSE-openable) / use the plain Volume path (today's behavior).
- **Output partition-aligned to source FILE** (see Unit 3 partitioning) so downstream functions amortize for free.
- **FILE column as native tile index** → no manual FUSE directory walk (kills the listing tax); `read_files`/`list_files`.
- **`_`-prefixed files:** explicit include/skip rule (Spark skips `_*` by default; the FILE-column index sidesteps most).
- **Reader cued by table properties** (`geobrix.file.write_strategy`) to pick its plan; absent props → conservative default.
- **Permissions flow (payoff):** a FILE column carries UC table governance to the bytes → authorized reads without
  separate Volume grants (enables heavy; standardizes light).

## 5. Unit 2 — Writer + conversions

- **Write a DataFrame (`tile` + other cols) to a table with a FILE column** — MANAGED (governed lifecycle) or EXTERNAL
  (no-delete-with-table). **Pre-declare the typed column then INSERT** (never bare `saveAsTable`):
  `CREATE TABLE t (… tile_file FILE MANAGED|EXTERNAL …) [TBLPROPERTIES 'databricks.filespace-preview'=/Volumes/… (MANAGED)] [CLUSTER BY (path)]`
  then `INSERT … SELECT … ORDER BY path`.
- **FILE-aligned layout (closes the loop):** `INSERT … ORDER BY path` (immediate grouping) and/or `CLUSTER BY (path)`
  (+ periodic `OPTIMIZE`, durable). **Never `partitionBy('path')`** (high-cardinality → partition explosion). **Avoid ZORDER**
  (liquid clustering supersedes it).
- **Materialize `raster` → MANAGED FILE** (`create_file(content => tile.raster)`) — the preferred handling where a
  filespace is available (kills inline-BINARY OOM, governed lifecycle).
- **Conversions are writer operations:** EXTERNAL/materialized → MANAGED = ingest into a MANAGED column (copies into the
  filespace); MANAGED → EXTERNAL = write bytes to a provided Volume + register EXTERNAL. No in-place adopt (`create_file` copies).
- **Self-describing `TBLPROPERTIES`:** stamp **`geobrix_writer_version`** (writer-FORMAT version, e.g. `1`) — the
  **primary reader cue**: the reader **branches its handling on it** (v1 → today's logic; a future v2+ → new logic), so
  the on-table format can evolve without breaking old-table reads. Plus `geobrix.version` (library version, informational)
  and `geobrix.file.write_strategy` (layout / MANAGED-EXTERNAL / filespace). The writer also **stamps `path_mode` per tile** (§3.1).
- **Portability default (decision, see §8):** consider defaulting to a **portable** representation (Volume-path/EXTERNAL) with
  MANAGED-FILE opt-in, since a FILE-column table may be a DBR 19+-only artifact.

## 6. Unit 3 — Grouped execution (the fast path)

- **Partition-scoped `mapInPandas` executor** with a **per-partition open-resource LRU** (maxsize ~2–4), keyed by uri/path.
  Validated pyrx-pure, 6× the per-row scalar path, FileRef-survives-pandas.
- **Capability-adaptive cached resource:**
  - FILE-capable → cache the **`.open()` stream** (fast cold reads ~0.66 ms).
  - Non-FILE, moderate file + ≥~17 windows → cache a **local-staged copy** (`/local_disk0`; local read ~0.57 ms).
  - Non-FILE, huge file / few windows → cache the **open dataset** (GDAL block/OS page-cache locality) or direct FUSE.
- **Partitioning contract:** `df.repartition(n, 'tile.path').sortWithinPartitions('tile.path')`; `n` = **parallelism-sized**
  (3–5× worker cores classic; a parallelism target on Serverless), **never `n_files`, never `sc`-derived** (Serverless has no `sc`).
  Hash-by-path never splits a FILE (saturated); sort makes each FILE contiguous so a small LRU suffices.
- **Function shapes:** scalars (`rst_memsize`, `rst_clip`, band math, reproject…) via the executor; **aggregators &
  1:n generators** (`rst_*_agg`, explode) are inherently grouped (group = FILE) — same open-once principle. The **per-row
  scalar `rst_*(tile)` form remains** (correct, amortizes opens via the module cache) as the slower convenience path.
- **User surface (decision, see §8):** automatic (reader emits aligned partitions → functions just work) vs an explicit
  `group_exec`-style entry point. Recommend: automatic on the happy path + a documented re-align recipe after reshapes.

## 7. Capability gating & cross-runtime

- **Strategy — attempt FILE with graceful fallback across ALL environments** (not a rigid runtime gate). Probe
  `file_supported()` and *try* the FILE path; on absence or failure, fall back to Volume-path/FUSE (representation 1/2) +
  the non-FILE staging amortization. Forward-compatible: **Serverless lights up automatically when it moves to DBR 19
  (DBR 19 is coming soon to Serverless with FILE support)** — no rewrite. GeoBrix's validated-safe FILE env today is **DBR 19.5-snapshot classic**.
- **Documented per-runtime behavior (as-is):**
  - **DBR 17 — CANNOT read a FILE-column table at all — CONFIRMED (2026-08-17):** every read (DESCRIBE, non-FILE columns,
    `SELECT *`, the FILE column; MANAGED + EXTERNAL) fails at **schema parse** — `[INVALID_JSON_DATA_TYPE] Failed to convert
    the JSON string 'file' to a data type`. The whole table is unreadable (not even the non-FILE columns). → **document it**;
    on DBR 17 GeoBrix stays on Volume/materialized (never FILE).
  - **DBR 18 — caveated FILE**; GeoBrix does **not** mitigate the caveats (not the immediate focus) — graceful attempt +
    fallback covers it.
  - **DBR 19 — full FILE path** (classic today; **DBR 19 coming soon to Serverless with FILE support**).
  - **Serverless GC (Spark Connect, pre-19) — CONFIRMED (2026-08-17):** `count(*)`, non-FILE-column projection, and lazy
    DataFrame build **work**; fetching the table **schema** (it contains a FILE field) and **collecting a FILE value**
    (`SELECT tile_file` / `SELECT *`) **fail** — a **client-side** type limit (`PySparkValueError: UNSUPPORTED_OPERATION`),
    identical for MANAGED + EXTERNAL. ⇒ **graceful fallback = the reader projects the plain `path` column (+ window), never
    the FILE column** → FILE-column tables stay consumable on Serverless GC today. (This is why the v2 tile carries a plain
    `path` string beside any FILE ref.)
- **FILE Beta caveats (per docs; some temporary):** Delta-only; must declare MANAGED/EXTERNAL; **a FILE column cannot be a
  partition / cluster / join / grouping / MAP key** — GeoBrix keys on `path` (string), never the FILE column, so our
  partition/cluster/group recipes are **already compatible**; cast to `FILE` (the target column decides MANAGED/EXTERNAL);
  writing an external ref into a MANAGED column ingests it. Treat caveats as current-Beta state, not permanent.
- **Forward-compatible / opt-in:** while FILE matures, **users opt in to FILE via the reader/writer options** (writer
  `managed_file_col` + MANAGED-vs-EXTERNAL; a reader FILE-preference flag) — the fast path is usable **now** by those who
  choose it, while **defaults stay conservative** (portable / graceful-fallback). **Defaults may flip** once the safe FILE
  env broadens + DBR 19 comes to Serverless with FILE support (coming soon) — decision #2.

## 8. Open decisions (to settle before/within writing-plans)

1. **[DECIDED 2026-08-17]** Increment-1 = **MANAGED bridge (light, with fallback)** — the strategic core (Q1/Q2 confirmed).
2. **[DECIDED 2026-08-17]** Writer default = **portable (Volume/EXTERNAL), MANAGED opt-in.** MANAGED-as-**default** is
   **deferred until DBR 19 (with FILE support) is the Serverless default — coming soon** — revisit the default flip then (forward-compat:
   the opt-in works now; only the default waits).
3. **Writer table scope:** new-table `CREATE` only, or also existing-table `ALTER`/upsize.
4. **Grouped-exec user surface:** automatic-only vs an explicit `group_exec` wrapper (+ how it composes with the columnar API).
5. **rst_*_agg / 1:n split:** which functions get partition-scoped FILE-cache forms; how 1:n generators separate from scalars.
6. **Heavy (Scala) parity:** scope + the GDAL dataset-cache honoring `GDALManager`; permissions-flow payoff.
7. **[SPIKE RUNNING 2026-08-17]** DBR 17/18 FILE-column-table read-compat — can a pre-FILE runtime read such a table
   (whole / non-FILE columns only / the FILE column / DESCRIBE)? Decides §7 portability + reinforces decision #2.
8. **LRU sizing / eviction / handle-close-at-partition-end**, and local-stage disk-budget guardrails (avoid the huge-file disk-fill).

## 9. Non-goals / deferred

- Custom tile container formats (protobuf/FlatBuffers/HDF5), ZARR/N-D datacube lane — a separate later lane.
- The read-default flip and `_stage_local_if_needed` probe-then-stage fix are **already shipped**.
- User-facing docs — grounding captured; write after the design ratifies (Virtual Tiles + Large Tiles handling-patterns).

## 10. Sequencing

Light: **(1) reader `file_gbx` base + writer typed-FILE + partition-scoped grouped executor** (they interlock), gated with
the portable FUSE fallback → **(2)** flip-in the non-FILE staging amortization + capability probes → **(3)** heavy fast-follow →
**(4)** docs. Then `writing-plans` for the build.
