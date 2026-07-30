# COG Lane + File-Preparation Writer — Design Spec

**Date:** 2026-07-30
**Branch:** `feature/large-raster-reader` (continues the 0.4.4 line)
**Status:** Design approved (pending written-spec review)

## Problem / motivation

The 0.4.4 large-raster work made the lightweight raster **reader** auto-split large rasters and emit COG tiles. Serverless validation proved this cannot work in the materialized-reader model: encoded tile bytes ride in Spark columns, and per-worker output accumulation scales with **file size** (a 1.5 GiB striped source → 25×64 MiB tiles → OOM on the 1 GB serverless UDF cap), regardless of per-tile budget or partition structure. See `prompts/testing/2026-07-30-serverless-oom-rootcause.md` for the full investigation.

**Lesson learned → reframe.** Stop trying to "split to survive" on read. Instead adopt a **halo (preferred) mode**: *prepare one master COG per source file, then windowed-read it.* Splitting returns to **opt-in**. Mastering COGs (internally tiled + overviews) — rather than treating rasters as generic GTiffs — is the strategic default, because a master COG supports cheap windowed and overview reads downstream.

This spec delivers the **prepare** half of that model plus the two-lane API reframe. The **consume-globally** half (virtual tiles over master COGs) is a committed follow-on (see Non-goals).

## Goals

1. **Two clean lanes** — separate the general `gtiff` lane from the optimized `cog` lane, pulling COG-specific options out of the gtiff surface.
2. **`file_gbx` reader** — a path-listing DataSource that emits file *references* (never content), the composable alternative to `binaryFile`.
3. **`cog_gbx` writer** — a file-preparation writer: reads each source file from the Volume, converts to a master COG, writes it back to the Volume. DataFrame carries references end-to-end → accumulation-proof.
4. **Reverse the forced-split default** — reader `splitStrategy` defaults back to `none`; retire reader-side `tileFormat`/COG-on-split.

## Non-goals (this spec)

- **Virtual tiles / global-processing mode (follow-on).** Enumerated window-set references `(cog_path, window, metadata)` over master COGs, lazy per-window reads, thousands-of-COGs scale in one columnar DataFrame. This is the "consume globally" half of the halo model and the real path past the single-file ceiling. Own brainstorm→spec. See `[[light-virtual-tiling-by-reference]]`.
- **Heavy-tier changes.** Light tier only.
- **Removing the materialized reader's opt-in split machinery.** It stays available (one-tile-per-partition + budget), just not the default.

## Approach summary

Two lanes, each with a reader and a writer:

| | **gtiff lane (general)** | **cog lane (optimized)** |
|---|---|---|
| Reader | `gtiff_gbx` — reads GTiff *or* COG | `cog_gbx` — COG-aware; efficient windowed/overview reads |
| Writer | `gtiff_gbx` — writes plain GTiff | `cog_gbx` — writes master COG (file preparation) |
| COG options | **none** (relocated to cog lane) | `cogBlockSize`, `cogOverviewResampling`, `cogCompression` |
| Splitting | opt-in | opt-in |

Plus a raster-agnostic `file_gbx` path-lister that feeds the `cog_gbx` writer for the on-Volume preparation pipeline:

```
spark.read.format("file_gbx").load("/Volumes/in")          # reference rows, NO content
     .write.format("cog_gbx").option("cogBlockSize","512").save("/Volumes/out")
# then, clip mode:
spark.read.format("cog_gbx").option("bbox","...").load("/Volumes/out")
```

---

## Section 1 — Two-lane architecture

Split the current mixed raster reader/writer into two lanes:

- **gtiff lane (general):** `gtiff_gbx` reader reads GTiff *or* COG (COG ⊂ GTiff, opens identically); `gtiff_gbx` writer writes plain GTiff. **No COG-creation options** on this lane.
- **cog lane (optimized):** `cog_gbx` reader is COG-aware (efficient windowed/overview reads; `bbox` AOI is the blessed clip path); `cog_gbx` writer writes master COGs. COG-creation options live here.

**Default reversal (breaking change, beta-acceptable).** The raster reader's `splitStrategy` default flips from `auto` back to **`none`** (one whole raster per read). The reader-side `tileFormat`/`cogBlockSize`/`cogOverviewResampling` options and the COG-on-split behavior added in 0.4.4 are **retired from the reader** — COG creation is a *writer* concern now, on the cog lane. The one-tile-per-partition + decoded-budget machinery remains available for **opt-in** splitting only (positive `sizeInMB`, or explicit `splitStrategy=serverless|classic`).

Documented in `beta-release-notes.mdx`: default reversal + COG options relocated reader→cog-writer.

---

## Section 2 — `file_gbx` reader (path lister)

Minimal DataSource emitting file **references, never content** — the deliberate contrast to `binaryFile`.

- **Format:** `file_gbx`. Raster-agnostic (pure lister; consumers decide what to do with paths).
- **Schema (one row per file):**
  `(path: string, name: string, extension: string, size: long, modificationTime: timestamp)`
  - `extension`: lowercased, no dot (`"tif"`, `"nc"`); **null** when the filename has none. Derived from `name` via `os.path.splitext` — no extra I/O.
  - `path`: scheme-qualified via `to_spark_uri` (joins cleanly against binaryFile/heavy conventions).
- **Options:** `filterRegex` (and/or `pathGlobFilter`), `recursiveFileLookup` — via the existing `list_files(path, filter_regex)` primitive.
- **Contract:** never opens files or reads bytes; `os.stat` for size/mtime, string-split for extension. Pure Python, Serverless-safe. References end-to-end.
- **Implementation:** `partitions()` → `list_files()`, one `InputPartition` per file; `read()` yields the metadata tuple.

---

## Section 3 — `cog_gbx` writer (master-COG preparer)

The halo-mode preparer. Accepts file-reference rows, produces master COGs on the Volume.

- **Format:** `cog_gbx`. `df.write.format("cog_gbx").option(...).save(outputPath)`.
- **Input contract (the key inversion):** validates a **`path` column** (the `file_gbx` output; extra columns ignored) — NOT the `(source, tile)` tile-struct the `gtiff_gbx` writer requires. Pixels never ride in a Spark column.
- **Per-row behavior (one task per file → no accumulation):**
  1. `to_local_path(path)` → bare FUSE path.
  2. `rasterio.open` → `analysis.cog_convert(ds, cogCompression, cogBlockSize, cogOverviewResampling)` (driver="COG") → **one master COG, no split** (internally tiled + overviews).
  3. FUSE-safe write to `outputPath`: build on local temp, sequential copy to Volume.
  4. Emit only writer commit messages downstream.
- **Options (cog lane's own):** `cogBlockSize` (512), `cogOverviewResampling` ("AVERAGE"), `cogCompression` ("DEFLATE"); `overwrite`; output naming (derive from source `name`, or a `nameCol`).
- **Memory:** peak = one file's `cog_convert` (~2.8× decoded) — the case that WORKS on serverless (no cross-tile accumulation). **Per-file ceiling:** a single very large source (e.g. >~5 GiB) can still strain one worker; that is the natural boundary for virtual tiling (follow-on) to cross. Common cases (many moderate files; or preparing on classic) work.

---

## Section 4 — Components & data flow

**New:**
- `ds/file.py` — `FileGbxDataSource`/`FileGbxReader` (`file_gbx`). `partitions()`→`list_files()`, one partition per file; `read()` yields `(path,name,extension,size,modificationTime)` via `os.stat`+`splitext`. No raster open.
- `ds/cog.py` — `CogGbxDataSource`/`CogGbxReader` (`cog_gbx`). Reader = COG-aware raster reader (subclass of the raster reader; `bbox` AOI is the blessed clip path). Hosts the `writer()` factory → `CogGbxWriter`.
- `ds/cog_writer.py` — `CogGbxWriter`: validates a `path` column; per row `to_local_path`→`rasterio.open`→`cog_convert`→FUSE-safe write. Cog-lane options.

**Changed:**
- `ds/gtiff.py` — strip COG options from gtiff reader/writer; gtiff stays general (reads GTiff or COG; writes plain GTiff).
- `ds/raster.py` — `splitStrategy` default `auto`→`none`; retire reader `tileFormat`/COG-on-split. Keep one-tile-per-partition + budget for opt-in split only.
- `ds/register.py` — add `FileGbxDataSource`, `CogGbxDataSource` to `_SOURCES`.
- `analysis.cog_convert` — reused unchanged (driver="COG").

**Data flow (halo prepare):**
```
file_gbx.load("/Volumes/in")  → (path,name,ext,size,mtime)  [references only]
  → .write.format("cog_gbx").option("cogBlockSize",512).save("/Volumes/out")
     per task: open path → cog_convert (master COG, no split) → write /Volumes/out/<name>.tif
consume (clip): cog_gbx.load("/Volumes/out").option("bbox", "...")
```

---

## Section 5 — Testing & docs

### Testing layers (local unit → Docker doc-test → **Serverless validation**)

**Principle: maximize the fast local loop first; Serverless is the final confirmation, not the workhorse.** Push as much correctness and memory-behavior verification as possible into fast local runs (unit + subprocess RSS) and Docker doc-tests — iterate there until green. Reserve Serverless for confirming ONLY what local cannot faithfully model (the 1 GB PySpark-UDF cap, Spark Connect worker semantics, Volume FUSE I/O, true worker RSS). The lesson from 0.4.4 is not "skip local" — it is "local green is necessary but NOT sufficient; a Serverless run must confirm before the memory-safety claim is considered validated." Aim for a single, well-instrumented Serverless run per real change, entered only after the local loop is exhausted (mirrors the [[iterate-spark-plans-locally]] discipline).

**Local unit (TDD, real sample data, no mocking Spark/GeoBrix/IO):**
- `file_gbx`: lists a dir → correct rows; `extension` null on an extensionless file; filterRegex/recursive; **asserts no content read** (works on a non-raster file, never opens it).
- `cog_gbx` writer: path-row DataFrame → output COGs exist, each `cog_validate=True` (strict), pixel-equivalent to source; cog options honored; per-file memory bounded (subprocess RSS ~2.8× one file, not Σ files).
- Reader default: `splitStrategy` defaults to `none` (reversal regression test); opt-in split still works.
- Two-lane: gtiff writer no longer accepts COG options; cog-lane options rejected on the gtiff surface.
- Binding/registration: `file_gbx` + `cog_gbx` registered; `gbx:test:bindings` green.

**Docker doc-test:** end-to-end `file_gbx`→`cog_gbx` on real sample GeoTIFFs → prepared COGs; then `cog_gbx.load(...).option("bbox",...)` clips correctly.

### Serverless validation (REQUIRED, gating — local/Docker green is NOT sufficient)

**Hard lesson from the 0.4.4 OOM investigation: local subprocess RSS and Docker doc-tests both passed while the feature OOM'd on real Serverless.** Memory behavior, the 1 GB PySpark-UDF cap, Volume FUSE I/O, and Spark Connect worker semantics only surface on an actual Serverless run. This feature's memory-safety claim (references-not-pixels; per-file — not per-dataset — encode peak) is therefore **not validated until it runs on Serverless**, and the plan MUST include a Serverless validation task gating "done."

Requirements for the Serverless validation (bake in the mechanics that worked this session):

- **Run via `gbx:test:notebooks-serverless`** (`jobs.submit` + env v5), not local/Docker. Notebook is a native `.ipynb` (no jupytext/`# MAGIC` double-encoding); install via the two-step `%pip` (force-reinstall `--no-deps geobrix` then `geobrix[light]`) — the job harness strips `%pip` and installs from the env spec, so the wheel MUST be staged first.
- **Wheel staleness guard:** rebuild + stage the wheel AFTER the last code commit, and **hash-verify** the staged Volume wheel matches the local build (and grep it for the expected changed symbols) before the run. A same-path overwritten wheel with stale bytes silently invalidates the whole run — this happened repeatedly in 0.4.4.
- **Persist results to the Volume, not stdout:** the jobs API does not expose notebook stdout for Serverless task runs, and an OOM crashes the worker before any return. Write a JSON marker/result to the Volume BEFORE and AFTER each stage so a worker crash still leaves evidence of exactly what was in flight. Use unique filenames per stage/run (markers from different runs must not clobber). Judge outcome from the persisted markers + the specific child-run error, not the harness exit code.
- **No `.rdd` / no driver-side monkeypatch of worker code:** Serverless is Spark Connect — `.rdd` does not exist, and a driver-side patch does not reach workers. Instrument via a real UDF or Volume markers.
- **Corpus:** the file-preparation path must be validated on a real striped multi-GiB source (VIIRS/UK-scale) staged to a Volume — generated FUSE-safe (build on local temp, sequential copy to Volume; row-band streamed so generation doesn't itself OOM the driver). Not just the small doc-test samples.
- **What to prove on Serverless:** (1) `file_gbx`→`cog_gbx` prepares a master COG for a multi-GiB source without OOM (per-file encode peak, not accumulation); (2) many-files preparation (e.g. a directory of moderate rasters) completes without per-worker accumulation; (3) `cog_gbx.load(...).option("bbox",...)` clips a prepared master COG. Establish the per-file size that succeeds vs strains (the documented single-file ceiling).
- **Notebooks + markers** live under `prompts/testing/` (gitignored); the persisted validation summary is linked when reported.

See `prompts/testing/2026-07-30-serverless-oom-rootcause.md` for the failure modes this validation exists to catch.

**Docs:**
- `beta-release-notes.mdx` — breaking: `splitStrategy` default reversal + COG options relocated reader→cog-writer; new file/cog lanes.
- New reader/writer pages: `file_gbx` (path lister), `cog_gbx` reader (COG-aware, bbox clip), `cog_gbx` writer (master-COG prep); the **halo-mode narrative** (prepare master COG → windowed-read; splitting is opt-in).
- Sidebar wiring (`sidebars.js`) for new pages.
- No internal/planning vocabulary (QC judge `internals-leak`).

---

## Roadmap / sequencing

1. **This spec:** two-lane reframe + `file_gbx` reader + `cog_gbx` writer + default reversal. Ships the "prepare master COGs on Volume" story and undoes the forced-split default.
2. **Virtual tiles / global-processing mode (follow-on brainstorm→spec):** enumerated window-set references over master COGs, lazy read, thousands-of-COGs columnar scale. Crosses the single-file ceiling; schema-parity is the crux. `[[light-virtual-tiling-by-reference]]`.
3. **Heavy-tier parity** for the two-lane model, as applicable.

## Open risks

- **Single very large source file** still peaks ~2.8× its decoded size in one `cog_gbx` writer task — documented per-file ceiling; virtual tiling is the eventual answer.
- **cog_gbx reader vs gtiff_gbx reader overlap** — both read COGs; the cog reader's distinct value is COG-aware windowing + being the blessed clip/AOI surface. Keep the boundary documented so users know which to reach for.
- **Output naming/overwrite semantics** for the writer — pin exact behavior in the plan (derive-from-name vs nameCol; overwrite vs error-if-exists).
