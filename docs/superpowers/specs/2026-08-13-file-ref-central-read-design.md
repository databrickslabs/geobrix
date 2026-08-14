# Design: Central FILE / FILEREF read for light-tier virtual tiles

- **Date:** 2026-08-13
- **Status:** Design (approved direction; spec under review)
- **Scope:** GeoBrix **light tier** (`pyrx`), **virtual tiles only**. Heavy tier and materialized tiles are out of scope (heavy needs materialized bytes).
- **Related memory / prior art:** virtual tiling shipped in 0.5.0 (`light-virtual-tiling-by-reference`); FILE feasibility spike (`file-type-virtual-tiles`); Serverless config constraints (`pyrx-serverless-no-spark-config`).

## 1. Problem & motivation

GeoBrix light-tier **virtual tiles** carry a bytes-free `(path, window)` reference and read pixels
lazily. Today `path` is a plain UC Volume string, read via a whole-file FUSE stage +
`rasterio` windowed read.

Databricks' **FILE** data type (`FileRef`) wraps a governed file reference. It offers two things
we want:

1. **Governed access on compute without a FUSE mount** — notably future serverless-GC (light's
   target), where a bare `/Volumes/...` path is not directly readable but a `FileRef` is.
2. **Byte-range reads** — `FileRef.open()` returns a seekable stream, so a windowed read can fetch
   only the window's bytes instead of staging the whole file.

The catch: FILE is not in OSS Spark yet and is **not uniformly enabled**. So GeoBrix must use FILE
**when present and usable**, and **gracefully fall back** to today's path read everywhere else
(local dev, CI, non-FILE workspaces, and Serverless until its DB-Connect client is upgraded). The
aim is to ship **support-ready**: the plumbing is in now; the byte-range win lights up wherever FILE
becomes usable, with no user-facing change.

## 2. Feasibility — established by spike (2026-08-13)

Validated on **dogfood**, a **classic DBR 19.5-snapshot, single-user (dedicated, no Spark Connect)**
cluster, `fileReferenceCreationMode=MANAGED` in cluster `spark_conf`:

- FILE type recognized (`CAST(NULL AS FILE)` → `struct<f:file>`); **e2-demo does NOT have FILE
  enabled** (the gate is workspace enablement, not DBR version or cluster mode).
- `try_to_file('/Volumes/...')` mints a `pyspark.sql.types.FileRef` **column** (needs the
  creation-mode in the cluster `spark_conf` — a runtime `spark.conf.set` does **not** reach
  executors; that was the DBR-18 "Lost task" root cause).
- `try_to_file(tile.path)` composes on a **struct field**, and one UDF receives **both** the tile
  struct and the minted `fref`.
- `fref.open()` returns a **seekable** `_io.BufferedReader` (seek to offset 50000 + read the exact
  marker worked) → rasterio windowed reads are viable. `fref.as_local_file()` returns the `/Volumes`
  FUSE path (governance / whole-file fallback).

Hard constraints these findings impose on the design:

- **`try_to_file` is a plan-only Catalyst expression.** A `FileRef` can only be minted in the
  DataFrame plan, never inside a UDF, and there is **no `FileRef(path)` constructor**. So a
  `FileRef` must arrive at the read UDF as a **column argument** — it cannot be derived inside the
  UDF from a string path (this is why a `col:`-reference encoded into `path` buys nothing).
- **On the old serverless DB-Connect client, DML works but any *display* of a FILE result fails.**
  So a `FileRef` must never be stored in the tile struct or surfaced as a carried column that
  pipelines might `.show()`.

## 3. Goals / Non-goals

**Goals**
- Central, feature-detected read that uses FILE/FILEREF when usable, else today's path read.
- Byte-range windowed reads via `fref.open()` when FILE is usable (the perf win).
- Zero user-facing API/behavior change; tile struct surface unchanged.
- Ship support-ready: works (as fallback) on local/CI/Serverless today; lights up on FILE-enabled
  classic DBR 19.x now, and on serverless-GC once its Connect client is compatible.

**Non-goals**
- Heavy tier; materialized-tile paths.
- Any FILE *write* path (`from_bytes`) — deferred.
- Enabling FILE on a workspace (that is a Databricks admin/preview action).
- Changing the on-disk/serialized tile schema.

## 4. Design — option (a): plain path + under-the-hood FILE

Chosen over (b) `"file_type_col:<col>"` and (c) `"{<col>};volume"` because, given the plan-only
minting constraint, a string reference in `path` cannot be resolved to a `FileRef` inside the read
UDF — the FileRef must be a real column argument regardless, so a path-encoded reference adds
nothing. (a) keeps the struct clean and puts the logic in the binding.

### 4.1 Tile struct — unchanged
`V2_TILE_SCHEMA` (`pyrx/core/virtual_tile.py`) is unchanged; `path` stays a plain Volume string.
A `FileRef` is **never** stored in the struct.

### 4.2 Feature-detect — `_file_supported(spark)`
A cached (once-per-`SparkSession`) capability check that performs a **minimal end-to-end
roundtrip**, not just a plan/parse check — because on the old serverless client `try_to_file` can
plan-succeed yet be unusable:

1. Mint `try_to_file(<a tiny known Volume sentinel>)` and consume it in a one-row UDF that calls
   `fref.open().read(1)` / `fref.as_local_file()`.
2. If the roundtrip returns the expected byte → FILE **usable** (cache `True`).
3. Any exception (UNSUPPORTED_DATATYPE, creation-mode unset, display/consume failure, no sentinel)
   → cache `False`.

Serverless-safe: uses only `spark.sql` + a UDF (no `.rdd`/`_jvm`/`conf.set`). Result is memoized per
session (keyed on the session) so the roundtrip runs at most once. A cheap env override
(`GBX_DISABLE_FILE=1`) forces `False` for debugging.

> Open detail for the plan: the sentinel path. Prefer a caller-agnostic sentinel (e.g. the tile's
> own `path` on first read, guarded) over requiring a fixed Volume file, so detect needs no
> pre-provisioned fixture.

### 4.3 Minting "under the hood" — binding-level injection
A shared helper in the pyrx tile-consuming binding path (the `_header_accessor_udf` factory and the
tile-reading op bindings in `pyrx/functions.py`):

- When `_file_supported(spark)` is `True`, the binding builds its UDF call as
  `_udf(tile_col, try_to_file(tile_col['path']))` — `try_to_file` runs in the **plan**, per-op,
  just-in-time. The UDF receives `(tile, file_ref)`.
- When `False`, it builds `_udf(tile_col, F.lit(None))` (or the single-arg form) → `file_ref` is
  null.

This is **one central change point**, not per-function. The `FileRef` is **ephemeral per-op** — it
is never a carried DataFrame column, so nothing can accidentally `.show()` it. `try_to_file` metadata
minting is cheap; re-minting per op is acceptable (and simpler than carrying/validating a companion
column through a pipeline).

SQL surface: the registered `gbx_rst_*` functions bind positionally and cannot gain a hidden
companion arg, so the **SQL path uses the plain-path fallback** (no FILE acceleration). Acceptable —
the Python light API is the FILE target; SQL still works, just without byte-range.

### 4.4 Read chokepoint — `open_tile(tile, file_ref=None)`
`pyrx/core/open_tile.py::open_tile` gains an optional `file_ref` param (default `None`, so the
signature is back-compatible and all existing callers are unaffected):

```
if tile.raster is not None:            # materialized: bytes ARE the result (unchanged)
    yield from _serde.open_tile(tile.raster)
elif file_ref is not None:             # FILE path: byte-range windowed read
    with _open_windowed_via_fileref(file_ref, tile.window, pending_instructions) as ds:
        yield ds
else:                                  # fallback (unchanged): stage FUSE path + rasterio
    local, is_temp = _stage_local_if_needed(tile.path)
    ... rasterio.open(local) ...
```

`_open_windowed_via_fileref` opens `file_ref.open()` (seekable stream) as the rasterio source and
reads exactly `tile.window`. **Defensive degradation:** any failure in the FILE branch (open error,
non-seekable stream, rasterio incompatibility) is caught and falls through to
`file_ref.as_local_file()` (governed FUSE path) and then to the plain-path stage — so a FILE hiccup
never fails a read that the fallback could serve.

### 4.5 Guardrails
- pyrx product stays **Serverless-safe**: no `.rdd`/`_jvm`/`conf.set`; detect uses `spark.sql` +
  UDF; the `try_to_file` injection is a Connect-safe Column expression (it merely *resolves* only
  where FILE exists).
- `open_tile` signature change is additive/back-compatible.
- No change to the serialized tile schema or any public function signature.

## 5. MVP scope
Wire the central `open_tile` + the shared binding helper (this structurally covers *all* tile-reading
ops). **Validate and focus** on the eo-series surface first:

- Readers: `gtiff_gbx`, `raster_gbx`, `cog_gbx` (they keep emitting plain-path virtual tiles; FILE
  engages at **read** via the binding injection — generation is unchanged).
- Ops: the specific `rst_*` functions the eo-series notebooks exercise (e.g. `rst_metadata`,
  `rst_summary`, `rst_initnodata`, `rst_clip`, and the tessellate/merge path they use).

Expand coverage/validation to all remaining `rst_*` ops after the MVP lands (the mechanism already
covers them; this is about test/bench breadth).

## 6. Testing strategy
- **Local / CI (FILE absent):** `_file_supported` returns `False`; the fallback path is exercised
  exactly as today. New `open_tile(file_ref=None)` default keeps every existing test green. Add a
  unit test asserting the binding passes `lit(None)` when detect is `False`, and that `open_tile`
  ignores a `None` file_ref.
- **FILE path (integration, on a FILE-enabled classic DBR 19.x cluster):** mint → `(tile, fref)` UDF
  → windowed read; assert pixel-equality vs the plain-path read of the same tile/window.
- **Build-time integration:** confirm `rasterio` performs a true windowed read from `fref.open()`
  (reads only the needed strips/tiles), not a full read.
- **Guard test:** extend the existing pyrx "no Spark config" static guard to the new code.

## 7. Benchmarking — FILE vs Volume (new leg)
Goal: quantify the byte-range win. Mirror the recent **virtual-tile Volume** benchmark but with FILE.

- **Compute:** dogfood, **classic DBR 19.x dedicated (single-user), fixed 20 workers** (autoscale
  off, for stable measurement), `fileReferenceCreationMode=MANAGED` in `spark_conf`. Matches the
  20-worker sizing used for the recent Volume virtual-tile bench.
- **Comparison:** same corpus, same virtual tiles, same functions — measured **with FILE**
  (`fref.open()` windowed read) vs **without FILE** (today's Volume-path stage + rasterio window).
  The A/B is toggled by the feature-detect result (or the `GBX_DISABLE_FILE` override) so both legs
  run identical GeoBrix code.
- **Scope:** start with the **MVP functions**, expand to **all functions** once FILE support is
  implemented across them.
- **Hypothesis:** FILE (`open()` range reads) should beat whole-file FUSE staging for windowed
  reads of larger COGs, with the gap widening as file size grows and window fraction shrinks.
- **Reporting:** add FILE vs Volume columns to the benchmarking docs alongside the existing
  virtual-tile columns; keep the same disposition/QA-anomaly conventions.

> Sequencing: the bench leg runs **after** the MVP implementation (it measures the real GeoBrix FILE
> read path). An optional early **raw-mechanism micro-bench** (`fref.open()` windowed read vs
> whole-file stage, outside GeoBrix) can de-risk the hypothesis before the build if desired.

## 8. Rollout / support-readiness
- Ships in a 0.5.x release. On today's reachable compute it runs the **fallback** (plain path); the
  FILE branch is dormant until `_file_supported` returns `True`.
- Lights up automatically on FILE-enabled classic DBR 19.x now, and on **serverless-GC** once its
  DB-Connect client is upgraded (currently pinned to DBR 18.0 by DBRPINS — the blocker the product
  team is tracking; FILE hotfix is in DBR 19.2.5+).
- No user action or API change required for either state.

## 9. Risks & open questions
- **Feature-detect sentinel** (§4.2): pick a fixture-free sentinel so detect needs no
  pre-provisioned Volume file. (Plan detail.)
- **rasterio ↔ `fref.open()` integration**: seekability is proven; confirm rasterio consumes the
  stream for a true partial read (via opener / `MemoryFile` / fsspec) at build time; if a given
  rasterio/GDAL build can't, degrade to `as_local_file()` (still governed, whole-file).
- **Per-op re-mint cost**: `try_to_file` is a metadata fetch; if it proves non-trivial at scale,
  revisit caching a companion column for a bounded op-chain (weighed against the display-safety of
  keeping FILE ephemeral).
- **e2-demo not FILE-enabled**: development/validation of the FILE path happens on dogfood (or any
  FILE-enabled workspace) until e2-demo's preview is turned on.

## 10. Next step
On spec approval, proceed to the implementation plan (writing-plans skill): central `open_tile`
+ feature-detect + binding helper, MVP-scoped, TDD with the fallback path fully covered in CI and the
FILE path validated on a FILE-enabled classic DBR 19.x cluster; then the FILE-vs-Volume bench leg.
