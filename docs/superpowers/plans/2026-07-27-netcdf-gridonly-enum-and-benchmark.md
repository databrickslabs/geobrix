# NetCDF grid-only enumeration + scale/offset parity + at-scale benchmark — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make heavy `netcdf_gdal` enumerate only true georeferenced grid variables (kill the 85-subdataset fan-out and match the light `classify()` set), make it apply CF `scale_factor`/`add_offset` so its values match light on scaled grids, add a regular-grid raster corpus downloader (NASA-NEX via anonymous Planetary Computer), and split the reader benchmark into an honest raster (heavy-vs-light) leg and a light-only vector leg.

**Architecture:** Three areas. (1) Scala reader: `NetCDF_Batch` gains a geotransform/CRS grid filter; `WindowedExtract` gains an opt-in `applyScale` (netcdf-only) that unscales via the proven `GDALTranslate` fallback; `NetCDF_Reader` opts in. (2) Python sample: a `NasaNexDownloader` mirroring `TropomiDownloader`. (3) Python bench: `run_format_read` passes `sizeInMB` to `netcdf_gdal`; the bench cell splits into `{CORPUS}/netcdf` (NASA-NEX raster) + `{CORPUS}/netcdf-swath` (S5P vector); the top-level `corpus.json` read is made lazy.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / Java 17 / GDAL Java bindings (heavy); Python 3.12 / PySpark / xarray / pystac-client + planetary-computer (light + downloader). Tests: ScalaTest (`PlanTest with SilentSparkSession`) in Docker; pytest (local Spark) for light + downloader; cluster jobs.submit for the at-scale bench (human-gated).

## Global Constraints

- **No aliases** — one canonical name per reader/function.
- **GDAL/OGR registration only via `GDALManager` guards** (`GDALManager.init`); never raw `gdal.AllRegister()` per task. The executor path must init `NodeFileManager.init(exprConfig.hConf)` before `readRemote` (already fixed in `7ba0d2dd`; do not regress).
- **`WindowedExtract` is shared by ALL raster readers.** The `applyScale` change MUST default to `false` and only `NetCDF_Reader` may set it true. GeoTIFF/other readers keep raw-copy behavior byte-for-byte.
- **Heavy raster schema is fixed:** `struct<source, tile:struct<cellid,raster,metadata>>`. Do not change.
- **Serverless-safe light tier + downloader:** no `spark.conf.set`, `_jvm`, `.rdd`, `cache`, `persist`. Downloaders discover on the driver (metadata-only) and fan out via `StacClient.download`.
- **Heavy work runs in the `geobrix-dev` Docker container** via `gbx:*` commands; dispatch long Scala/Maven suites to a subagent, never inline.
- **Bench discipline** (`benchmarking-preflight-discipline`): non-empty corpus, logged granule counts (no silent truncation), stamped worker count, `summary.md` link at end. Cluster is the user's `0519-143423-0jwqt79u`; it is TERMINATED — the build/stage/bench tasks must (re)start it and stage the fat JAR + tests.jar BEFORE start (`jar-stage-before-cluster-start`), then poll RUNNING + libs INSTALLED before submitting (`poll-cluster-start-and-libs`).
- **Verified data facts:** S5P `/PRODUCT/methane_mixing_ratio` = 215×3736, no geotransform, no CRS, GEOLOCATION array (swath). coral `bleaching_alert_area` = 7200×3600 with geotransform (regular grid). NASA-NEX GDDP-CMIP6 = `application/netcdf`, regular 0.25° grid, anonymous on PC.
- **`bench-changes-update-docs`:** any bench change reflected in `docs/docs/api/benchmarking.mdx` the same cycle. **No internal vocabulary** in `docs/docs/` (QC judge greps `wave\s*\d+`).

---

### Task 1: `NetCDF_Batch` grid-only enumeration filter

Replace the "keep any >1×1 subdataset" test with "keep only true georeferenced grids" so S5P's 85 subdatasets collapse to 0 (matching light) and gridded files keep their grid variables.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Batch.scala:50-59` (the `grids` filter)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala`

**Interfaces:**
- Consumes: `gdal.Open(selector, GA_ReadOnly)`, `Dataset.GetGeoTransform(Array[Double])`, `Dataset.GetProjectionRef`/`GetProjection`.
- Produces: enumeration keeps a subdataset iff `XSize>1 && YSize>1 && RasterCount>=1` AND (`GetProjectionRef` non-empty OR geotransform is non-identity). Same `(path, var)` partition shape as before.

- [ ] **Step 1: Write the failing test (swath enumerates 0, grid enumerates its vars)**

Add to `NetCDF_DataSourceTest.scala`. The coral fixture is a real grid (2 vars); there is no committed swath fixture, so build a tiny synthetic swath `.nc` in-test via a helper, OR assert on the existing grid fixture that the count is exactly its grid vars (2) and unchanged. Use the coral integration fixture for the positive case:

```scala
test("netcdf_gdal enumerates only georeferenced grid variables (coral grid = 2)") {
    import com.databricks.labs.gbx.rasterx.functions._
    rasterx.functions.register(spark)
    val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
    val df = spark.read.format("netcdf_gdal").option("sizeInMB", "-1")
        .option("filterRegex", ".*20220101\\.nc$").load(ncDir)
    val vars = df.select("source").collect().map(_.getString(0).split(":").last).toSet
    vars shouldBe Set("bleaching_alert_area", "mask")
}
```

For the swath-negative case, stage a synthetic swath fixture (Task 6 stages a real one; here use a small generated `.nc` with 2-D lat/lon and no geotransform). If generating in-test is impractical in Scala, defer the swath assertion to the Python cross-tier test (Task 4) and note it here. Minimum: the coral grid test above must pass and lock the georeferenced-only behavior.

- [ ] **Step 2: Run to verify (coral still 2; pre-change this passes, so add a guard the change preserves)**

Run (subagent, Docker): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-gridfilter.log`
Expected: PASS on coral (coral vars have geotransform, so both old and new filter keep them — this test guards against the change dropping real grids).

- [ ] **Step 3: Implement the georeferenced-grid filter**

In `NetCDF_Batch.scala`, replace the `grids` filter body:

```scala
val grids = vars.filter { v =>
    try {
        val sub = gdal.Open(s"""NETCDF:"$localPath":$v""", GA_ReadOnly)
        if (sub == null) false
        else {
            val bigEnough = sub.GetRasterXSize > 1 && sub.GetRasterYSize > 1 && sub.GetRasterCount >= 1
            // A true raster grid has real georeferencing: a CRS or a non-identity geotransform.
            // Swath subdatasets (e.g. S5P /PRODUCT/methane_mixing_ratio) report an empty
            // projection AND the identity transform [0,1,0,0,0,1] (their georeferencing lives in
            // a GEOLOCATION array), so they are correctly dropped here — matching the light
            // classify() which returns CURVILINEAR and excludes them from raster mode.
            val hasCrs = { val p = sub.GetProjectionRef; p != null && p.nonEmpty }
            val gt = new Array[Double](6); sub.GetGeoTransform(gt)
            val identity = gt(0) == 0.0 && gt(1) == 1.0 && gt(2) == 0.0 &&
                           gt(3) == 0.0 && gt(4) == 0.0 && gt(5) == 1.0
            val ok = bigEnough && (hasCrs || !identity)
            sub.delete(); ok
        }
    } catch { case _: Throwable => false }
}
```

- [ ] **Step 4: Run to green**

Run (subagent): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-gridfilter.log`
Expected: PASS (coral still enumerates its 2 grid vars; the georeference test now locks the behavior).

- [ ] **Step 5: Scalastyle + commit**

Run (subagent): `bash scripts/commands/gbx-lint-scalastyle.sh` (confirm no new violations in NetCDF_Batch).

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Batch.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala
git commit -m "fix(rasterx): netcdf_gdal enumerates only georeferenced grid variables

A subdataset is kept only when it has a CRS or a non-identity geotransform,
not merely >1x1. S5P swaths (85 subdatasets, no geotransform, GEOLOCATION
array) now enumerate to zero grid variables -- matching the light classify()
contract and eliminating the ~85-partitions-per-file fan-out that made the
reader unusable at scale. Regular grids (coral/CMIP/NASA-NEX) are unaffected.

Co-authored-by: Isaac"
```

---

### Task 2: opt-in `applyScale` in `WindowedExtract` (cross-tier value parity)

Make heavy apply CF scale/offset when the reader opts in, so `netcdf_gdal` values match light's decoded physical values. Use the proven `GDALTranslate` fallback with `-unscale -ot Float64` when any band has non-identity scale/offset — avoids hand-rolling per-dtype buffer math and reuses the correct path. Default OFF; GeoTIFF unchanged.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/operations/WindowedExtract.scala` (branch at top of `extract`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Reader.scala:35` (pass `Map("applyScale" -> "true")` into `splitRasterIter`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala` (GeoTIFF-unchanged regression)

**Interfaces:**
- Consumes: `options: Map[String,String]` already threaded `NetCDF_Reader → BalancedSubdivision.splitRasterIter → ReTile.reTileIter → getTile → WindowedExtract.extract`. `GDALTranslate.executeTranslate` (existing fallback). `Band.GetScale`/`GetOffset`.
- Produces: when `options.getOrElse("applyScale","false").toBoolean` AND some band has `GetScale != 1.0` or `GetOffset != 0.0`, the extracted tile carries **decoded Float64** values (raw*scale+offset), NoData mapped to the decoded fill; scale/offset NOT re-copied as metadata (already applied). Otherwise behavior is byte-for-byte unchanged.

- [ ] **Step 1: Write the failing GeoTIFF-unchanged regression test**

This proves `applyScale` defaults off — a GeoTIFF read is unaffected. Add to `NetCDF_DataSourceTest.scala` (or a WindowedExtract-focused suite):

```scala
test("gtiff_gdal read is unaffected by applyScale default (raw values preserved)") {
    import com.databricks.labs.gbx.rasterx.functions._
    rasterx.functions.register(spark)
    val tif = this.getClass.getResource("/modis/").toString
    val df = spark.read.format("gtiff_gdal").option("sizeInMB", "1").load(tif).limit(1)
    // Reading succeeds and produces a tile; no applyScale option is set anywhere.
    df.count() shouldBe 1L
}
```

(The stronger scaled-value parity assertion is the Python cross-tier test in Task 4; this Scala test guards the shared-code default.)

- [ ] **Step 2: Run to verify it passes pre-change (guard test)**

Run (subagent): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest' --log netcdf-applyscale.log`
Expected: PASS (no behavior change yet; this test must stay green after Step 3).

- [ ] **Step 3: Implement `applyScale` branch in `WindowedExtract.extract`**

At the top of `extract`, before `simpleEnough`, add the opt-in unscale path:

```scala
def extract(ds: Dataset, options: Map[String, String],
            xStart: Int, yStart: Int, xOffset: Int, yOffset: Int): (Dataset, Map[String, String]) = {
    val applyScale = options.getOrElse("applyScale", "false").toBoolean
    if (applyScale && hasNonIdentityScale(ds)) {
        // Decode CF scale_factor/add_offset to physical Float64 values, matching the light
        // netcdf_gbx reader (xarray mask_and_scale=True). Use the proven gdal_translate path
        // with -unscale -ot Float64 so per-dtype packing/nodata handling is GDAL's, not ours.
        return GDALTranslate.executeTranslate(
            ds,
            options + ("translateOptions" ->
                s"-srcwin $xStart $yStart $xOffset $yOffset -unscale -ot Float64"),
            /* match the existing fallback's call shape */ )
    }
    if (!simpleEnough(ds)) return fallback(ds, options, xStart, yStart, xOffset, yOffset)
    // ... existing fast path unchanged ...
}

private def hasNonIdentityScale(ds: Dataset): Boolean = {
    val n = ds.getRasterCount
    (1 to n).exists { b =>
        val sb = ds.GetRasterBand(b)
        val s = new Array[java.lang.Double](1); sb.GetScale(s)
        val o = new Array[java.lang.Double](1); sb.GetOffset(o)
        (s(0) != null && s(0).doubleValue() != 1.0) || (o(0) != null && o(0).doubleValue() != 0.0)
    }
}
```

NOTE: match `GDALTranslate.executeTranslate`'s actual signature — read `src/main/scala/com/databricks/labs/gbx/rasterx/operator/GDALTranslate.scala` and `WindowedExtract.fallback` to see exactly how the existing `-srcwin` fallback constructs its options/command, and mirror it (the existing `fallback` already builds a `-srcwin` translate — extend its options string with `-unscale -ot Float64` rather than inventing a new call). If `fallback` already accepts a srcwin, the cleanest implementation is: `if (applyScale && hasNonIdentityScale(ds)) return fallback(ds, options + ("unscale"->"true"), ...)` and have `fallback` append `-unscale -ot Float64` when that option is set.

- [ ] **Step 4: Wire `NetCDF_Reader` to opt in**

`NetCDF_Reader.scala:35` — pass the option into tiling:

```scala
private val tilesIter = BalancedSubdivision.splitRasterIter(ds, Map("applyScale" -> "true"), partition.sizeInMB)
```

- [ ] **Step 5: Run Scala suite to green**

Run (subagent): `bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.rasterx.ds.NetCDF_DataSourceTest,com.databricks.labs.gbx.rasterx.ds.GTiff_DataSourceTest,com.databricks.labs.gbx.rasterx.ds.GDAL_DataSourceTest' --log netcdf-applyscale.log`
Expected: PASS — netcdf tests green, GeoTIFF/GDAL readers unchanged (applyScale off).

- [ ] **Step 6: Scalastyle + commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/operations/WindowedExtract.scala \
        src/main/scala/com/databricks/labs/gbx/rasterx/ds/netcdf/NetCDF_Reader.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/ds/NetCDF_DataSourceTest.scala
git commit -m "feat(rasterx): opt-in applyScale so netcdf_gdal decodes CF scale/offset

WindowedExtract gains an opt-in applyScale (default false): when set and a
band carries non-identity scale_factor/add_offset, the tile is decoded to
physical Float64 via gdal_translate -unscale, matching the light netcdf_gbx
reader (xarray mask_and_scale). NetCDF_Reader opts in; all other raster
readers keep raw-copy behavior byte-for-byte (regression-tested on gtiff_gdal).

Co-authored-by: Isaac"
```

---

### Task 3: `NasaNexDownloader` (regular-grid raster corpus)

Mirror `TropomiDownloader` for `nasa-nex-gddp-cmip6` — anonymous PC, regular 0.25° grids.

**Files:**
- Create: `python/geobrix/src/databricks/labs/gbx/sample/nasanex.py`
- Modify: `python/geobrix/src/databricks/labs/gbx/sample/__init__.py` (export `NasaNexDownloader`, `download_nasanex_aoi`)
- Test: `python/geobrix/test/sample/test_nasanex.py`

**Interfaces:**
- Consumes: `databricks.labs.gbx.stac.StacClient` (same as tropomi), `_stac_client` injection seam.
- Produces: `NasaNexDownloader(catalog=PLANETARY_COMPUTER, collection="nasa-nex-gddp-cmip6", sign="planetary_computer").download(bbox, out_dir, temporal=None, variables=("tas",), spark=None) -> DataFrame` with columns `out_file_path, out_file_sz, is_out_file_valid`; saves `{item_id}_{asset}.nc`. `download_nasanex_aoi(spark, bbox, out_dir, **kw)` wrapper.

- [ ] **Step 1: Read the template**

Read `python/geobrix/src/databricks/labs/gbx/sample/tropomi.py` in full and `python/geobrix/test/sample/` (find the tropomi test) to copy the injection-seam + test pattern exactly.

- [ ] **Step 2: Write the failing downloader test (injected STAC client)**

`python/geobrix/test/sample/test_nasanex.py` — mirror the tropomi test: inject a fake `_stac_client` returning a canned item with `pr`/`tas` netcdf assets, assert `download` filters to the requested `variables` and calls `StacClient.download` with the right hrefs. No network.

```python
def test_nasanex_download_filters_to_requested_variables(monkeypatch, tmp_path):
    from databricks.labs.gbx.sample.nasanex import NasaNexDownloader
    dl = NasaNexDownloader()
    # inject fake client (mirror tropomi test's seam); assert only 'tas' asset is downloaded
    ...
    assert selected_assets == ["tas"]
```

- [ ] **Step 3: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/sample/test_nasanex.py`
Expected: FAIL — module not found.

- [ ] **Step 4: Implement `nasanex.py`**

Copy `tropomi.py`'s structure; swap `S5P_COLLECTION` → `"nasa-nex-gddp-cmip6"`, drop the `/PRODUCT` group and swath-specific bits, and filter assets by the `variables` tuple (asset names are the climate-var ids `tas`, `pr`, ...). Save `{item_id}_{asset}.nc`. Keep it Serverless-safe (driver discovery + `StacClient.download` fan-out; no spark config mutation). Add `download_nasanex_aoi`.

- [ ] **Step 5: Run to green + export**

Add exports to `sample/__init__.py`. Run: `gbx:test:python --path python/geobrix/test/sample/test_nasanex.py`
Expected: PASS.

- [ ] **Step 6: Lint + commit**

Run: `gbx:lint:python --fix` then verify Docker `--check`.

```bash
git add python/geobrix/src/databricks/labs/gbx/sample/nasanex.py \
        python/geobrix/src/databricks/labs/gbx/sample/__init__.py \
        python/geobrix/test/sample/test_nasanex.py
git commit -m "feat(sample): NasaNexDownloader for regular-grid NetCDF raster corpus

Mirrors TropomiDownloader for nasa-nex-gddp-cmip6 (anonymous Planetary
Computer): AOI/temporal-driven discovery + distributed StacClient.download
of the requested climate variables as {item_id}_{asset}.nc regular-grid
granules. Provides the at-scale gridded raster corpus for the heavy-vs-light
netcdf reader benchmark (S5P swaths cannot serve raster).

Co-authored-by: Isaac"
```

---

### Task 4: cross-tier parity — scaled-grid case

Extend the existing parity test so it gates the Task 2 unscaling on a scaled grid, and asserts swath → empty on both tiers.

**Files:**
- Modify: `python/geobrix/test/ds/test_netcdf_cross_tier.py`
- Test fixture: a synthetic packed-integer `.nc` with `scale_factor`/`add_offset` (built in-test with `netCDF4`, like the existing `_write_regular_grid` helpers).

**Interfaces:**
- Consumes: both registered readers; `_netcdf.readable_variables`. Produces: assertions only.

- [ ] **Step 1: Write the scaled-grid parity test**

```python
def _write_scaled_grid(path):
    from netCDF4 import Dataset
    import numpy as np
    with Dataset(path, "w") as ds:
        ds.createDimension("lat", 4); ds.createDimension("lon", 5)
        lat = ds.createVariable("lat","f8",("lat",)); lat.standard_name="latitude"
        lon = ds.createVariable("lon","f8",("lon",)); lon.standard_name="longitude"
        lat[:] = [50.0,49.5,49.0,48.5]; lon[:] = [10.0,10.5,11.0,11.5,12.0]
        v = ds.createVariable("t","i2",("lat","lon"), fill_value=-32768)
        v.scale_factor = 0.01; v.add_offset = 250.0
        v[:] = np.arange(20, dtype="i2").reshape(4,5)  # physical = raw*0.01 + 250

@pytest.mark.integration
def test_netcdf_gdal_applies_scale_matches_light(spark, tmp_path):
    if not _heavy_available(spark): pytest.skip("netcdf_gdal (heavy JAR) unavailable")
    f = tmp_path/"scaled.nc"; _write_scaled_grid(str(f))
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
    spark.dataSource.register(NetcdfGbxDataSource)
    light = _tile_values(spark.read.format("netcdf_gbx").load(str(f)))   # decoded physical
    heavy = _tile_values(spark.read.format("netcdf_gdal").load(str(f)))  # now decoded via applyScale
    np.testing.assert_allclose(light["t"], heavy["t"], rtol=1e-4, atol=1e-4, equal_nan=True)
```

(`_tile_values` = the existing helper that reads a tile's band into a numpy array, keyed by the `source` variable.)

- [ ] **Step 2: Run in Docker (needs heavy JAR)**

Run (subagent, after Task 1+2 JAR built/staged): `gbx:test:python --path python/geobrix/test/ds/test_netcdf_cross_tier.py --with-integration --log netcdf-parity-scaled.log`
Expected: PASS (ran, not skipped) — heavy now matches light on the scaled grid. If it fails, the Task-2 unscale path is wrong; fix there, not by loosening tolerance.

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/ds/test_netcdf_cross_tier.py
git commit -m "test(netcdf): cross-tier value parity on a scaled grid (applyScale gate)

Adds a packed-integer scale_factor/add_offset fixture and asserts heavy
netcdf_gdal (applyScale) tile values match light netcdf_gbx decoded values
within tolerance -- the correctness gate for the heavy unscaling path.

Co-authored-by: Isaac"
```

---

### Task 5: bench harness — raster/vector split + sizeInMB + lazy corpus.json

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/readers.py` (`run_format_read` sizeInMB passthrough; `stage_nasanex_corpus` + keep/rename swath stager)
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (`_CELL_NETCDF` → raster leg + `_CELL_NETCDF_SWATH` vector leg; lazy top-level `corpus.json` read)
- Modify: `notebooks/tests/push_and_run_bench_on_cluster.py` (if it also reads corpus.json unconditionally for netcdf-only — make it consistent)
- Test: extend `python/geobrix/test/bench/` smoke tests (imports, skip-clean-when-empty).

**Interfaces:**
- Consumes: `run_format_read(..., fmt, options, size_mib)`. Produces: `run_format_read` passes `sizeInMB` to `netcdf_gdal` (currently only `raster_gbx`); the netcdf raster leg calls both tiers over `{CORPUS}/netcdf` with `sizeInMB=-1` (one tile per grid var, fair granularity); the vector leg calls light `netcdf_gbx` mode=vector over `{CORPUS}/netcdf-swath`.

- [ ] **Step 1: Write failing test — `run_format_read` passes sizeInMB to netcdf_gdal**

In the bench test dir, assert (with a stub spark/reader) that `run_format_read(fmt="netcdf_gdal", size_mib=-1, ...)` sets `.option("sizeInMB","-1")`. Mirror any existing `run_format_read` test.

- [ ] **Step 2: Run to verify failure**

Run: `gbx:test:python --path python/geobrix/test/bench/` (the relevant test)
Expected: FAIL — sizeInMB only applied for `raster_gbx` today.

- [ ] **Step 3: Implement sizeInMB passthrough**

`readers.py:268` — broaden the condition:

```python
if fmt in ("raster_gbx", "netcdf_gdal", "gdal", "gtiff_gdal"):
    reader = reader.option("sizeInMB", str(size_mib))
```

(Include the heavy raster formats so the knob is available; netcdf raster leg passes `size_mib=-1`.)

- [ ] **Step 4: Split the bench cell (raster + vector legs) + lazy corpus.json**

In `cluster.py`: rename/duplicate `_CELL_NETCDF` into:
- **raster leg** — reads `{CORPUS}/netcdf` (NASA-NEX grids), both tiers, `options={"filterRegex": r".*\.nc$"}`, heavy `fmt="netcdf_gdal"` + light `fmt="netcdf_gbx"` (raster mode default), passing `size_mib=-1`.
- **vector leg** `_CELL_NETCDF_SWATH` — reads `{CORPUS}/netcdf-swath` (S5P), light only `fmt="netcdf_gbx"` with `options={"mode":"vector","filterRegex": r".*\.nc$"}`; a comment/log line states heavy has no swath path (light-only throughput, not a comparison).
Make the top-level `corpus = _m.Corpus.read(f"{CORPUS}/corpus.json")` (cluster.py:257) lazy: only read it when a function-bench leg actually runs (guard behind `not (netcdf_only or netcdf_swath_only or other reader-only flags)`), so a reader-only run does not require the function-corpus scaffold.

- [ ] **Step 5: Add `stage_nasanex_corpus` + swath stager**

In `readers.py`: `stage_nasanex_corpus(spark, corpus_dir, bbox=None, temporal=None, variables=("tas",), partitions=None)` calling `NasaNexDownloader().download(...)`; keep `stage_netcdf_corpus` (S5P) but point it at `{CORPUS}/netcdf-swath`. Both log granule counts (no silent truncation) and skip-clean when the pool is empty.

- [ ] **Step 6: Run bench smoke tests to green + commit**

Run: `gbx:test:python --path python/geobrix/test/bench/`
Expected: PASS (imports, sizeInMB passthrough, skip-clean).

```bash
git add python/geobrix/src/databricks/labs/gbx/bench/readers.py \
        python/geobrix/src/databricks/labs/gbx/bench/cluster.py \
        notebooks/tests/push_and_run_bench_on_cluster.py \
        python/geobrix/test/bench/
git commit -m "bench(netcdf): split raster (NASA-NEX) vs vector (S5P) legs

netcdf reader bench now has an honest split: a raster leg (heavy netcdf_gdal
vs light netcdf_gbx over NASA-NEX regular grids, sizeInMB=-1 for matching
one-tile-per-var granularity) and a light-only vector leg (netcdf_gbx vector
mode over S5P swaths -- heavy has no swath path). run_format_read now passes
sizeInMB to the heavy raster formats; the top-level corpus.json read is lazy
so reader-only runs do not require the function-bench scaffold.

Co-authored-by: Isaac"
```

---

### Task 6: docs + at-scale cluster run (human-gated)

**Files:**
- Modify: `docs/docs/api/benchmarking.mdx` (two corpora recipes + raster/vector split; NASA-NEX anonymous, S5P swath = vector-only; no internal vocabulary)
- Modify: `docs/docs/readers/netcdf.mdx` (reinforce: heavy = regular grids only; grid-only enumeration explicit; heavy now decodes scale/offset)
- Run: the actual bench on cluster `0519-143423-0jwqt79u` (rebuild+stage JAR/wheel, restart, stage a bounded NASA-NEX raster corpus + bounded S5P swath corpus, run both legs)

- [ ] **Step 1: Update benchmarking.mdx + netcdf.mdx**

Document the raster corpus (`stage_nasanex_corpus`, anonymous PC, regular 0.25° grids) and the swath vector corpus (S5P), the raster-vs-vector split, and that heavy `netcdf_gdal` now applies scale/offset (matches light). Reinforce in `netcdf.mdx` that heavy raster is regular-grid-only (swaths → light vector). Run `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` → nothing new.

- [ ] **Step 2: Commit docs**

```bash
git add docs/docs/api/benchmarking.mdx docs/docs/readers/netcdf.mdx
git commit -m "docs(netcdf): raster/vector bench corpora + heavy scale-decode

Co-authored-by: Isaac"
```

- [ ] **Step 3: Build + stage artifacts, (re)start cluster**

Dispatch a subagent: `set -a; source notebooks/tests/databricks_cluster_config.env; set +a` then `bash scripts/commands/gbx-data-push-jar.sh` (fat JAR + tests.jar) and `gbx-data-push-wheel.sh`; sync the wheel to `sample-data/` (per `bench-wheel-path-divergence`). Then `databricks clusters start 0519-143423-0jwqt79u --profile oauth-fe` and poll RUNNING + libs INSTALLED.

- [ ] **Step 4: Stage bounded corpora**

Raster: `stage_nasanex_corpus(spark, f"{CORPUS}/netcdf", bbox=..., temporal=<short>, variables=("tas",))` — bounded to ~20–50 grid granules; log the count. Vector: stage a bounded S5P subset (e.g. 20 granules from the existing pool) into `{CORPUS}/netcdf-swath`.

- [ ] **Step 5: Run both legs + capture summary**

`bash scripts/commands/gbx-bench-cluster.sh --netcdf-only --row-counts 1000` (raster + vector legs). Confirm it converges (grid-only enumeration → sane task count). Give the run's `summary.md` link (`bench-run-give-summary-link`). Record the heavy-vs-light raster throughput + light vector throughput in the ledger.

- [ ] **Step 6: Stop the cluster**

`databricks clusters delete 0519-143423-0jwqt79u --profile oauth-fe` (terminate) once the run is captured (`stop-clusters-you-start` — it's the user's cluster; terminate since we started it fresh, or leave per user preference).

---

## Sequencing note

Tasks 1→2 are the reader correctness core (Scala; one JAR build covers both). Task 3 (downloader) is independent Python. Task 4 needs Tasks 1+2's JAR. Task 5 is Python bench plumbing. Task 6 is docs + the human-gated at-scale run (needs everything). The one already-landed fix (`7ba0d2dd`, executor NPE) is a prerequisite that's done.

## Loose ends to surface at "done" (per report-loose-ends-after-spec-execution)

- Whether NASA-NEX GDDP variables actually carry scale/offset (if not, the scaled-parity test uses the synthetic fixture — the reader change ships regardless).
- The light-tier NetCDF **writer** remains a separate future cycle (from the prior spec).
- Wheel rebuild+restage after any light change (Tasks 3/5 touch light packages) per `whl-change-rebuild-and-stage`.
- The at-scale raster throughput number is only meaningful once Task 6 runs on-cluster; until then the raster bench is functionally-verified but un-numbered.
