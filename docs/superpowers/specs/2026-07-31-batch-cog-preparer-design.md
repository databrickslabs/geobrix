# Batch COG Preparer — Design Spec

**Date:** 2026-07-31
**Branch:** `feature/large-raster-reader`
**Status:** design approved; exploratory (non-wired) implementation to follow
**Related:**
- `docs/superpowers/specs/2026-07-31-serverless-cog-preparer-design.md` (the single-file
  `prepare_cog` core + the scalar-UDF experiment)
- Finding: `prompts/testing/2026-07-30-serverless-oom-rootcause.md` — driver-side `prepare_cog`
  SUCCEEDED on a 1.49 GiB source on Serverless (peak 2053 MiB, valid COG, 6 overviews) where
  the scalar UDF and DS-V2 writer both OOM'd at the ~1 GB per-PySpark-UDF cap.

## Problem

The single-file `prepare_cog(path, out_dir, ...)` core works, and driver-orchestrated
preparation is the proven way to COG-prep multi-GiB rasters on Serverless without a classic
cluster. But real corpora are directories or lists of files, and a long-running batch needs
**live per-file progress** (status after each file, how many done, how many remain) plus a
machine-readable summary. This spec adds a batch entry point over the existing core.

## Goal

One public function `prepare_cogs(sources, out_dir, ...)` that:
1. accepts **any** of: an existing directory, a single file, or a list/iterable freely mixing
   files and directories — and resolves all of them into a flat, deduped list of source files;
2. prepares one master COG per source (reusing the single-file core), driver-side, staging each
   `/Volumes` source to local disk first (GDAL cannot open a `/Volumes` striped TIFF directly,
   even on the driver);
3. prints a **live progress line per file** as it completes (index/total, status, output name,
   remaining count, elapsed, peak RSS);
4. returns a **summary dict** (counts + per-file records) suitable for
   `dbutils.notebook.exit(json)` capture on Serverless.

## Architecture

Build on the existing `python/geobrix/src/databricks/labs/gbx/pyrx/core/preparer.py`. Public
surface is `prepare_cogs`; `prepare_cog` becomes the internal per-file worker.

### Layers

- **`prepare_cogs(sources, out_dir, ...)`** — NEW public entry point. Resolves `sources` → flat
  deduped file list → for each file: stage `/Volumes`→local temp, call
  `prepare_cog(local, out_dir, out_name=<original basename>)`, clean up the temp, print a live
  progress line, accumulate a per-file record. Returns the summary dict. Owns staging and
  original-source naming.
- **`prepare_cog(path, out_dir, out_name=None, ...)`** — the single-file converter (existing),
  with ONE new optional param `out_name`. Still pure local-path (caller stages). When `out_name`
  is given, the output is `<out_dir>/<out_name>.cog`; when omitted, it derives from
  `os.path.basename(path)` (today's behavior — back-compatible).
- **`prepare_cog_measured(...)`** — unchanged; still wraps `prepare_cog` for the scalar-UDF
  record. (The UDF lane is ruled out for multi-GiB but the function stays for parity/small use.)
- **`_resolve_sources(sources, recursive, extensions) -> list[str]`** — NEW helper: normalizes
  the polymorphic input into a flat deduped file list.
- **`_peak_rss_mib()`** — existing; reused.

### Staging (load-bearing)

The driver-side experiment proved GDAL cannot open a `/Volumes` (FUSE) striped TIFF directly —
staging to worker/driver-local disk first (sequential `shutil.copyfileobj`, 8 MiB chunks) is
required. `prepare_cogs` stages **one file at a time**, converts, then deletes the temp — so the
disk/memory footprint is one source at a time, never the whole corpus. A source that is already a
plain local path (not `/Volumes`/`dbfs:`) is passed through without a redundant copy.

Naming wiring lesson (from the experiment, which produced `tmp*.tif.cog`): the output name MUST
derive from the **original** source basename, not the staged temp. `prepare_cogs` passes the
original basename via `prepare_cog(..., out_name=...)`.

## Input resolution

`_resolve_sources(sources, recursive=True, extensions=DEFAULT_RASTER_EXTS) -> list[str]`:

1. Normalize `sources` to a list (wrap a lone `str`/`os.PathLike`).
2. For each item: strip any scheme via `ds._listing.to_local_path`, then:
   - existing **directory** → list rasters under it (recursive by default; filtered to
     `extensions`);
   - existing **file** → `[item]` (a directly-named file is NOT extension-filtered — the caller
     asked for it explicitly);
   - **neither** (missing path) → yield a sentinel that `prepare_cogs` records as a resolution
     error (`status="error:not-found"`); do not abort.
3. Flatten all items' results into one list.
4. De-dup, preserving first-seen order (a file named directly AND found inside a listed dir
   converts once).

- `DEFAULT_RASTER_EXTS = (".tif", ".tiff", ".cog", ".nc", ".h5", ".hdf")` (lowercased match;
  case-insensitive). Directory listing uses the existing `ds._listing.list_files` /
  `list_files_recursive` machinery with an extension/regex filter.
- `extensions` is overridable; passing `None`/empty disables filtering (list everything).
- Directory listing applies only to directories; explicitly-named files bypass the filter.

## Error handling (two tiers, both non-fatal)

- **Resolution errors** — a listed path does not exist → summary record
  `{source, output_path: None, status: "error:not-found", ...}`; batch continues.
- **Conversion errors** — per file, `prepare_cog` already returns `(None, "error:<reason>")`
  without raising; captured per-file. One bad file never aborts the batch.
- **Batch-fatal only** for a truly unusable call (e.g. `out_dir` not creatable) — everything
  file-level is isolated and reported.
- **OOM caveat** unchanged: uncatchable; kills the process rather than returning `"error:"`.
  On the driver this is only a risk if a single source's transient exceeds driver memory — much
  larger headroom than the ~1 GB UDF cap.

## Progress reporting

### Live print (per file, as it completes)

```
[ 3/12] ok       scene3.tif → scene3.tif.cog     (9 left, 12.4s, peak 2053 MiB)
[ 4/12] skipped  scene4.tif                       (8 left)
[ 5/12] error    bad.tif — CPLE_AppDefinedError    (7 left)
```

Each line: `[index/total] status source [→ output_name] (remaining left, elapsed, peak RSS)`.
Printed with `flush=True` so it streams in notebook/stdout during the run. A final line:

```
done: 10 ok, 1 skipped, 1 error of 12  (peak 2101 MiB, 142.3s total)
```

Printing is unconditional (the user asked for live status). No callback/generator surface in this
pass (YAGNI — print + summary covers the stated need).

### Return summary (dict)

```python
{
  "total": 12,
  "ok": 10, "skipped": 1, "error": 1,
  "out_dir": "/Volumes/.../out",
  "peak_rss_mib": 2101.0,        # max across all files
  "elapsed_s": 142.3,
  "results": [                    # one record per file, in processed order
    {"index": 1, "source": "/Volumes/.../scene1.tif",
     "output_path": "/Volumes/.../out/scene1.tif.cog",
     "status": "ok", "peak_rss_mib": 2053.2, "elapsed_s": 12.4},
    # ... skipped: output_path set, status "skipped"
    # ... error:  output_path None, status "error:<reason>"
  ],
}
```

- `results` is the machine-readable record → feeds `dbutils.notebook.exit(json.dumps(summary))`
  for reliable Serverless capture (jobs API hides serverless stdout).
- Counts (`ok`/`skipped`/`error`/`total`) make "how many done / how many left" always derivable.
- `peak_rss_mib` is the max across files; per-file RSS lives in each record.

## `prepare_cogs` signature

```python
def prepare_cogs(
    sources,                    # str dir | str file | iterable of (dir|file), scheme-qualified OK
    out_dir: str,
    blocksize: int = 512,
    resampling: str = "AVERAGE",
    compression: str = "DEFLATE",
    subdataset: Optional[str] = None,   # applies to every source (multi-subdataset corpora)
    skip_if_exists: bool = True,
    recursive: bool = True,             # directory listing recursion
    extensions=DEFAULT_RASTER_EXTS,     # directory extension filter; None disables
    verbose: bool = True,               # live per-file print
) -> Dict[str, object]:
```

## `cog_gbx` `driverMode` integration

Add an opt-in `driverMode` option to the existing `cog_gbx` DS-V2 writer that routes conversion
to the **driver** via `prepare_cogs`, escaping the ~1 GB per-PySpark-UDF cap that OOMs the
default per-partition path on multi-GiB sources.

**Mechanism (keyed off which DS-V2 methods run where):**
- **`write(iterator)` — runs on EXECUTORS, per partition.** When `driverMode=true`, does **no
  conversion**: iterates the partition's rows, collects the `path` strings (tiny, no pixels),
  returns them in `CogCommitMessage(paths=[...])`. Cheap and cap-safe (no GDAL, no bytes on the
  worker). When `driverMode=false` (default), unchanged per-partition conversion.
- **`commit(messages)` — runs on the DRIVER.** When `driverMode=true`, flattens every partition's
  collected paths into one list and calls `prepare_cogs(all_paths, out_dir, blocksize=…,
  resampling=…, compression=…, skip_if_exists=…)`. Conversion happens on the driver → escapes the
  cap; `prepare_cogs` live progress prints surface in the notebook.
- **`abort(messages)`** — unchanged cleanup (removes any partial outputs).

**Why this is the only viable shape:** a DS-V2 writer's `write(iterator)` is invoked by Spark on
executors — there is no flag that relocates it to the driver. `commit`/`abort` are the driver-side
methods. So "driver mode" must be `write()` gathers references + `commit()` does the driver-side
work. This is the same "carry references, not pixels" discipline used elsewhere.

**Option surface:**
`df.write.format("cog_gbx").option("driverMode","true").save(out_dir)`. The existing
`cogBlockSize` / `cogOverviewResampling` / `cogCompression` / `nameCol` options thread straight
into the `prepare_cogs` call. Input schema is unchanged — still the path-bearing rows from
`file_gbx`; `assert_path_schema` still applies.

**Default = `false`** — clearly opt-in. The default preserves today's distributed per-partition
`write()` conversion (the moderate-file path). A user must explicitly set
`.option("driverMode","true")` to move conversion onto the driver (the large-file path).

**Honest caveats (documented in the writer docstring + `writers/cog.mdx`):**
- Heavy work is **driver-serial in `commit()`** — a `.write` that isn't distributed. Correct and
  OOM-safe, but unidiomatic; docs must say so plainly.
- **No per-partition Spark retry** for the conversion; if the driver dies mid-commit the whole
  batch re-runs — but `skip_if_exists=true` makes re-runs cheap (already-written `.cog`s skip).

## Testing strategy (local-first)

- **Local (Docker via `gbx:test:python`), unit tests** in
  `python/geobrix/test/pyrx/test_preparer.py` (append):
  - `_resolve_sources`: single dir (recursive + non-recursive), single file, list mixing files +
    dirs, de-dup (file named + inside a listed dir → once), missing path → not-found sentinel,
    extension filtering (sidecar `.json`/`.aux.xml` excluded; explicit file bypasses filter).
  - `prepare_cog` new `out_name` param: given → `<out_name>.cog`; omitted → basename behavior
    (back-compat).
  - `prepare_cogs` end-to-end over a temp dir of 3 small GeoTIFFs: summary counts correct
    (`total/ok`), each `results` record has status/output_path/index, outputs are valid COGs
    (`sniff_header`), a deliberately-bad entry yields `error` without aborting, a pre-existing
    output yields `skipped`.
  - progress `verbose=False` suppresses prints (assert via capsys); `verbose=True` prints one
    line per file + final summary line.
- **`cog_gbx` driverMode (local, in `python/geobrix/test/ds/test_cog_writer.py`):** with
  `driverMode=true`, `write(iterator)` returns a `CogCommitMessage` whose `paths` are the input
  source paths and produces NO `.cog` files by itself (no conversion on the worker path);
  `commit([...])` then produces valid COGs for every path (assert via `sniff_header`). With
  `driverMode=false` (default), behavior is unchanged (per-partition conversion). `assert_path_schema`
  still enforced in both modes.
- **Serverless (final gate):** extend/add a throwaway notebook that (a) calls `prepare_cogs` over
  the `large-raster/corpus` directory on the driver, and (b) exercises
  `df.write.format("cog_gbx").option("driverMode","true")` over the same corpus — both capturing the
  summary via `dbutils.notebook.exit`. Confirms multi-file driver-side batch prep AND the writer's
  driverMode both clear the ceiling on Serverless.
- **Non-wired throughout:** NO catalog registration, NO `registered_functions.txt` /
  `function-info.json` / `functions.py` binding / Scala entries. Keeps binding-parity + QC green.

## Non-goals

- No catalog registration / SQL surface (promotion is a separate plan).
- No callback or generator progress API (print + summary only).
- No distributed/UDF execution — the UDF lane is ruled out for multi-GiB; batch is
  driver-orchestrated. (Parallelism across files is a possible future follow-on, but driver-serial
  is correct and OOM-safe first.)
- No swath/warp support; no heavy-tier parity.
- No change to the DS-V2 `cog_gbx` writer's DEFAULT behavior (per-partition conversion stays the
  default; `driverMode` is opt-in and additive).
