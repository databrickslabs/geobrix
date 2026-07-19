# Issue #59 — NULL for zero-valid-pixel raster reducers

**Date:** 2026-07-19
**Branch:** `issues/59` (from `beta/0.4.0`)
**Issue:** [databrickslabs/geobrix#59](https://github.com/databrickslabs/geobrix/issues/59) — *Value reducers return NaN/0.0 (not NULL) for all-nodata raster chips*
**Version:** 0.4.1 (beta)

## Problem

The raster value reducers `gbx_rst_max`, `gbx_rst_min`, `gbx_rst_avg`, `gbx_rst_median`
return a misleading in-array value — not a clear "no data" signal — when applied to a
raster chip band with **zero valid pixels** (e.g. an H3 covering-tessellation cell that
clips only nodata). The two tiers also disagree:

| Reducer | Heavy (Scala) all-nodata result | Light (pyrx) all-nodata result |
|---|---|---|
| `gbx_rst_max` / `gbx_rst_min` | `0.0` (zero-init array leaks through `ComputeRasterMinMax`) | `NaN` |
| `gbx_rst_avg` | `0.0` (`GetStatistics().getMean` on `valid_count == 0`) | `NaN` |
| `gbx_rst_median` | `0.0` (`getMax` of warped 1×1 empty raster) | `NaN` |

Two concrete harms (both correctly diagnosed in the issue):

1. **`NaN` (light)** silently passes `WHERE measure IS NOT NULL`, and **poisons `MAX()` in
   a `GROUP BY`**: because NaN sorts greater than everything, a cell with real data that
   also touches an all-nodata chip across a tile seam can have its true value overwritten
   with NaN during seam reconciliation (`GROUP BY h3 ... MAX(measure)`).
2. **`0.0` (heavy)** is indistinguishable from a genuine zero measurement.

Neither is a catchable, aggregation-safe empty marker.

## Convention (the fix)

> **A per-band element of `rst_max`/`rst_min`/`rst_avg`/`rst_median` is SQL `NULL` if and
> only if that band has zero valid pixels (mask fully nodata). This applies identically to
> both tiers.**

- Catchable via `WHERE measure IS NULL`; ignored by `MAX`/`MIN`/`AVG` aggregates.
- `gbx_rst_pixelcount` **stays `0`** for an empty band — a count of zero is meaningful and
  aggregation-safe; NULL would be wrong there.
- This adopts the pattern the light-tier `summary` already ships
  (`pyrx/core/accessors.py:261-269` emits `{"min": None, "max": None, "mean": None,
  "stdDev": None}` for a zero-valid-pixel band). The reducers simply opted out of it.

## Scope

**In scope:**
- The four reducers on both tiers.
- Light `isempty` parity: `pyrx/core/accessors.py:93-94` currently only checks
  width/height/count; align it to heavy's all-nodata-aware semantics
  (`RasterAccessors.isEmpty`) so a fully-masked raster returns `True`.

**Deferred to a new follow-up issue** (do NOT address here):
- Covering-mode tessellation divergence for geometrically-overlapping-but-all-nodata
  chips. Heavy covering-mode emits the chip; light drops it
  (`tessellate.py:183-184`). Making these consistent re-opens a *deliberate* past
  decision (`RasterTessellate.scala:26-28` documents that the covering keep-test was
  intentionally moved from a nodata-mask test to a geometric-overlap test to avoid fringe
  over-inclusion). This needs its own discussion and is out of scope for #59. File it as a
  separate issue and reference #59.

## Light tier (pyrx) — design

All reducer logic lives Spark-free in `python/geobrix/src/databricks/labs/gbx/pyrx/core/accessors.py`.

**Reducers.** Replace the empty-band sentinel `float("nan")` → `None` at the four call
sites, keyed off the existing `if vals.size` guard:
- `avg` — `accessors.py:125`
- `minimum` — `accessors.py:134`
- `maximum` — `accessors.py:143`
- `median` — `accessors.py:152`

No schema change: the UDFs already declare `ArrayType(DoubleType())`
(`functions.py:160-163`, `functions.py:3664-3667`), whose elements accept SQL NULL. A
Python `None` per band becomes a NULL array element.

**isempty.** `accessors.py:93-94` currently returns True only when width/height/count is 0.
Extend it to also return True when **every band has zero valid pixels**, reusing the
existing `_valid_values(ds, band_index)` masking helper (`accessors.py:113-117`). This
matches heavy `RasterAccessors.isEmpty` ("null, no size, or all bands have no valid
pixels", `operations/RasterAccessors.scala:58-62`).

**Docstrings.** Update the promise-of-NaN docstrings: `functions.py:2316, 2324, 2332, 2340`
and the accessor docstrings `accessors.py:121, 130, 139, 148`; and the `isempty` docstring.

## Heavy tier (rasterx) — design

Reducer expressions live under
`src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/`.

**Return type.** All four `execute` methods change return type `Array[Double]` →
`Array[java.lang.Double]`, emitting Java `null` for an empty band. Spark's
`ArrayType(DoubleType)` already accepts null elements; the expressions already declare
`dataType = ArrayType(DoubleType)` and `nullable = true`. The dense-primitive
`Array[Double]` is what currently makes a per-band NULL impossible — boxing fixes that.

- `RST_Max.scala` — `execute` + `getMinMax` usage (`:54-64`)
- `RST_Min.scala` — `execute` + `getMinMax` usage (`:54-64`)
- `RST_Avg.scala` — `execute` (`:54-64`)
- `RST_Median.scala` — `execute` (`:54-65`)

**Detection signal.** Use **`stats.getValid_count == 0`** uniformly, where
`stats = band.AsMDArray().GetStatistics()` — the same signal `RST_PixelCount` already uses
(`RST_PixelCount.scala:65`). Reuse `RST_PixelCount`'s shared-dataset + unique-description
tag technique (`RST_PixelCount.scala:42-44`) to avoid `AsMDArray` caching issues.
- `RST_Avg`/`RST_Median` already call `GetStatistics` → the check is near-free.
- `RST_Min`/`RST_Max` currently call `BandAccessors.getMinMax` (`ComputeRasterMinMax`);
  they add one `GetStatistics` call to read `getValid_count`. When `getValid_count == 0`,
  emit `null` instead of the leaked `0.0`.
- `RST_Median` also gains the currently-missing null-check on its `GetStatistics()`
  (`RST_Median.scala:61` calls `.getMax` directly today).

**Null-band vs empty-band.** Preserve the existing null-band-handle path (emit `null`, was
`Double.NaN`), so a null band and an all-nodata band both map to NULL — consistent.

**Caller audit (REQUIRED).** Grep for any internal Scala code that consumes these reducer
arrays and unboxes elements as primitive `Double` — a boxed `null` element would NPE.
Candidates: tessellation → reducer pipelines, aggregators, bench fingerprint code. The
implementation plan must enumerate and verify each caller before changing the signature.

## Tests (TDD — write first, watch fail, then implement)

**Light (`python/geobrix/test/pyrx/`):**
- **Replace** `test_stats_all_invalid_band_is_nan_zero`
  (`test_core_accessors_stats.py:70-78`, currently asserts NaN) with an assertion that
  `avg/minimum/maximum/median` return `None` (SQL NULL) and `pixelcount == [0]` for an
  all-invalid band.
- **Zero-not-null trap:** a band of genuine `0.0` valid pixels returns `0.0`, not None.
- **isempty:** an all-nodata raster returns `True`; a raster with ≥1 valid pixel returns
  `False`.

**Heavy (`src/test/scala/com/databricks/labs/gbx/rasterx/`):**
- New reducer-level assertion: all-nodata chip → NULL element for all four reducers (this
  assertion does not exist today).
- **Zero-not-null trap:** genuine `0.0`-pixel band returns `0.0`, not null.

**Cross-tier / integration:**
- **Seam-reconciliation regression** (the issue's core harm): tessellate a raster with an
  interior nodata region into H3 covering cells, run `GROUP BY h3 ... MAX(measure)` where a
  real-data cell is adjacent to an all-nodata chip, and assert the true value survives
  (NaN no longer poisons `MAX`). Run per convention in Docker (doc/integration tests need
  full env + sample data).

## Documentation (all four surfaces)

1. **Beta release notes** — `docs/docs/beta-release-notes.mdx`: breaking-change entry.
   Reducers now return `NULL` (was `NaN` on light / `0.0` on heavy) for zero-valid-pixel
   bands; light `isempty` is now all-nodata-aware (previously dimensions-only). User-facing
   voice — no internal vocabulary (no wave numbers, no subagent references); QC judge
   enforces via `internals-leak`.
2. **API docstrings + function-info** — update the Scala expression docstrings, the Python
   `functions.py`/`accessors.py` docstrings (which currently promise NaN), and regenerate
   `src/main/resources/com/databricks/labs/gbx/function-info.json` via
   `gbx:docs:function-info` so `DESCRIBE FUNCTION` reflects NULL-on-empty.
3. **Reducer doc pages** — update any `docs/docs/*.mdx` narrative/example pages that show
   reducer output or describe empty-band behavior.
4. **isempty note** — cover the light `isempty` semantic change in both the release notes
   and its docstring.

## Verification / commands

- Light: `gbx:test:python --path python/geobrix/test/pyrx/` (affected package).
- Heavy: Scala suite in Docker for the reducer + tessellate suites (Maven build required
  for the signature change).
- Bindings: `gbx:test:bindings` (element-type round-trips through Scala / Python /
  function-info).
- Docs: `gbx:test:docs` for doc tests; regenerate function-info via `gbx:docs:function-info`.
- Lint: `gbx:lint:python --check` and `gbx:lint:scalastyle` before push.

## Sequencing

1. Light reducers + isempty + tests (isolated, low-risk).
2. Heavy reducers (type change + `getValid_count` guard + `RST_Median` null-check) +
   caller audit + tests (Docker/Maven).
3. Binding-parity + function-info regeneration.
4. Docs: release notes, docstrings, reducer pages, isempty note.
5. Cross-tier seam-reconciliation integration test (Docker).

## Open risks

- **Heavy caller NPE:** boxed-null elements could NPE any unboxing Scala consumer — the
  caller audit is the gating step before the signature change.
- **`ComputeRasterMinMax` behavior on empty bands is GDAL-version-dependent** (can throw →
  caught by `safeEval` → whole column null). Switching min/max to the `getValid_count`
  guard makes the empty result deterministic (per-band NULL) regardless of that behavior —
  a secondary benefit to verify in the test.
