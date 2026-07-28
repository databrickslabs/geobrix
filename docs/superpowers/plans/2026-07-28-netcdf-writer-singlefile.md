# NetCDF writer `singleFile` mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a symmetric opt-in `singleFile` option to BOTH light `netcdf_gbx` writers (default `false` = current sharded "parts" output, non-breaking): vector concatenates all points into one CF-DSG `.nc`; raster merges DISTINCT variables sharing one grid into one CF grid `.nc` (erroring clearly on incompatible grids / window-tile duplicates, redirecting to `gbx_rst_merge_agg`). Plus a parts-vs-single writer-bench variant and docs.

**Architecture:** Mirror the proven `VectorGbxWriter` two-phase pattern in `ds/vector.py`: when `singleFile=true`, `write(iterator)` on each executor writes an Arrow-IPC (feather) **fragment** into a shared `_scratch` dir and returns the fragment path in the commit message; `commit(messages)` on the driver reads all fragments, merges to ONE `.nc` written to a driver-local temp (random-access for `netCDF4.Dataset`), then `shutil.copyfile` to the target (FUSE-safe). Default (`singleFile=false`) keeps today's per-partition / per-tile scatter untouched.

**Tech Stack:** Python 3.12 / PySpark DataSource V2 / `netCDF4` / `pyarrow.feather` (fragments) / `rasterio` (tile decode) / `shapely` (point WKB) / `numpy`. Tests: pytest (local Spark) in Docker; cluster bench (human-gated).

## Global Constraints

- **Non-breaking:** `singleFile` defaults `false`. The existing parts-mode tests + output shape (`part-<uuid>.nc` vector, one-`.nc`-per-tile raster) must stay byte-for-byte unchanged. Regression-gate them.
- **Serverless-safe:** NO `spark.conf.set`, `_jvm`, `.rdd`, `cache`, `persist` in the writer paths.
- **FUSE discipline** (`volumes-cleanpath-bare-not-file`): `_listing.to_local_path(path)` scheme-strip; `netCDF4.Dataset` writes to a worker/driver-local `tempfile` then `shutil.copyfile` (NOT `copy`/`copy2` — they `chmod`; NOT `os.rename`). Scratch fragments live under `_scratch.new_scratch_dir(<parent>)` (dot-prefixed `.gbx_scratch/<uuid>`, invisible to the recursive reader, age-GC'd).
- **Two-phase contract:** `write(iterator)` returns a commit message carrying the fragment path (extend `NetcdfCommitMessage` or add a fragment-carrying message); `commit(messages)` merges on the driver; `abort(messages)` deletes fragments + any partial output. In parts-mode, `commit` stays no-op and `write` still writes final `.nc` files directly (current behavior).
- **Raster merge scope:** merges DISTINCT variables on ONE shared grid. Same grid = identical `(width, height)` + CRS EPSG exactly + geotransform within a float rtol. Incompatible grids OR duplicate varname (same-variable window-tiles) → raise `ValueError` naming the conflict AND pointing at `gbx_rst_merge_agg` / `gbx_rst_merge` for upstream mosaicking. NEVER silently mosaic or pick one.
- **Mosaic is upstream, not in the writer:** `tiles → rst_merge_agg → write(singleFile)`. Docs must state this.
- **Docker + synchronous tests:** `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py --log <name>.log` — run SYNCHRONOUSLY to completion (seconds), quote the pytest result line; do NOT background-monitor and return early. Lint: `gbx-lint-python.sh --fix` then Docker `--check` (pre-existing `test_vector_raster_bridge.py` flag is unrelated).
- **Commit mechanics** (repo pre-commit guard false-positives on piping + leading-dash tokens): write each message to a temp file and `git commit -F /tmp/<name>.txt`; no piping through `tail`/`2>&1`; no leading-dash tokens in the body; end with the `Co-authored-by: Isaac` trailer. No push, no account switch.
- **Wheel:** rebuild + restage after the light change (done in Task 5's cluster run).
- **No CI plumbing change:** `netcdf4`/`pyarrow` already deps; `test/ds/` + `test/bench/` already wired.

---

### Task 1: Vector `singleFile` (concat points → one CF-DSG `.nc`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` (`NetcdfVectorGbxWriter`: two-phase when `singleFile`)
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/netcdf.py` (thread `singleFile` into `writer()`)
- Test: `python/geobrix/test/ds/test_netcdf_writer.py`

**Interfaces:**
- Consumes: `_scratch.new_scratch_dir(parent)`; `pyarrow.feather` (write/read fragment tables); `_listing.to_local_path`; `netCDF4.Dataset`; the existing `_resolve_single_file_output(path, file_name, ext)` from `ds/vector.py` (import it) for the single-file target name.
- Produces: `NetcdfVectorGbxWriter(options, schema, overwrite)` honoring `options["singleFile"]` (default `"false"`). When true: `write` → feather fragment (cols `longitude`,`latitude`, attrs, + a `geom_0_srid` scalar carried in metadata) in scratch, returns a fragment-carrying commit message; `commit` → one CF-DSG `.nc` (obs-dimension, streamed fragment-by-fragment), copied to the resolved single target. When false: unchanged (per-partition `part-*.nc`, `commit` no-op).

- [ ] **Step 1: Write the failing vector singleFile test**

Add to `test_netcdf_writer.py`. A multi-partition points DataFrame + `singleFile=true` must yield EXACTLY ONE `.nc`, round-tripping all points/attrs.

```python
def test_vector_write_singlefile_one_nc(spark, tmp_path):
    import shapely, os
    from databricks.labs.gbx.ds.netcdf import NetcdfGbxDataSource
    from pyspark.sql.types import (StructType, StructField, FloatType, IntegerType,
                                   BinaryType, StringType)
    schema = StructType([
        StructField("ch4", FloatType(), True),
        StructField("qa_value", IntegerType(), True),
        StructField("geom_0", BinaryType(), True),
        StructField("geom_0_srid", StringType(), True),
        StructField("geom_0_srid_proj", StringType(), True),
    ])
    pts = [(float(i), i % 2,
            bytes(shapely.to_wkb(shapely.Point(10.0 + i*0.1, 50.0 + i*0.1))),
            "4326", "EPSG:4326") for i in range(12)]
    df = spark.createDataFrame(pts, schema).repartition(4)   # multiple partitions
    spark.dataSource.register(NetcdfGbxDataSource)
    out = tmp_path / "vout_single"
    (df.write.format("netcdf_gbx").option("mode", "vector")
       .option("singleFile", "true").mode("overwrite").save(str(out)))
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1, f"expected ONE .nc, got {ncs}"
    re = (spark.read.format("netcdf_gbx").option("mode", "vector")
          .option("variables", "ch4,qa_value").load(str(out)).orderBy("ch4").collect())
    assert len(re) == 12
    import pytest
    pt0 = shapely.from_wkb(bytes(re[0]["geom_0"]))
    assert pt0.x == pytest.approx(10.0) and pt0.y == pytest.approx(50.0)
```

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k singlefile --log nc-writer-sf.log`
Expected: FAIL — `singleFile` ignored, output is `part-*.nc` (multiple files), assert on `len(ncs)==1` fails.

- [ ] **Step 3: Implement vector two-phase singleFile**

In `NetcdfVectorGbxWriter.__init__`: read `self.single_file = str(options.get("singleFile", "false")).lower() == "true"`; when single, compute the scratch dir `self.scratch = _scratch.new_scratch_dir(to_local_path(path))`. In `write(iterator)`:
- If NOT single: current behavior (write `part-*.nc`), return `NetcdfCommitMessage(paths=[...])`.
- If single: buffer `(lons, lats, attrs, srids)` as today, but instead of a `.nc`, write a feather table (`pyarrow.Table` of `longitude`, `latitude`, each attr col; store the resolved single srid + attr dtypes in the table's schema metadata) into `self.scratch` (mkdir exist_ok). Return a message carrying the fragment path. Empty partition → message with empty frag.

Extend the commit message to carry a fragment path — simplest: add a field, e.g.
```python
@dataclass
class NetcdfCommitMessage(WriterCommitMessage):
    paths: List[str] = None            # parts-mode: final files written
    frag_path: str = ""                # singleFile-mode: this partition's scratch fragment
```
In `commit(messages)`:
- Parts-mode: no-op (as today).
- Single-mode: collect non-empty `frag_path`s; if none, write nothing. Else resolve the single target via `_resolve_single_file_output(self.path, self.name_col_value_or_None, "nc")`; write ONE CF-DSG `.nc` to a driver-local temp with an **unlimited `obs` dimension**, then **stream**: for each fragment, `feather.read_table`, append its rows to `obs` (grow `latitude`/`longitude`/attr vars via `var[start:start+k] = ...`). Reconcile srid across fragments (all-agree non-4326 → `crs` var). `shutil.copyfile` temp → target. Clean scratch.
`abort`: delete fragments + partial output + scratch.

- [ ] **Step 4: Thread `singleFile` through `writer()`**

`netcdf.py` `writer()` already passes `self.options` to the writer constructors — confirm `singleFile` is in `self.options` (it is, as a `.option`). No signature change needed; the writer reads it from options.

- [ ] **Step 5: Run vector singleFile test + parts-mode regression to green**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py --log nc-writer-sf.log`
Expected: the new singleFile test PASSES (one `.nc`, 12 points round-trip); ALL existing vector parts tests stay green (default still `part-*.nc`).

- [ ] **Step 6: Lint + commit**

Run `gbx-lint-python.sh --fix` then Docker `--check`.
```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py \
        python/geobrix/src/databricks/labs/gbx/ds/netcdf.py \
        python/geobrix/test/ds/test_netcdf_writer.py
git commit -F /tmp/sf-task1.txt   # "feat(netcdf): vector writer singleFile mode (concat points -> one CF-DSG .nc)"
```

---

### Task 2: Raster `singleFile` (merge distinct same-grid variables → one CF `.nc`)

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` (`NetcdfRasterGbxWriter`: two-phase when `singleFile`)
- Test: `python/geobrix/test/ds/test_netcdf_writer.py`

**Interfaces:**
- Consumes: same `_scratch`, feather, `to_local_path`, `_resolve_single_file_output`, `rasterio.MemoryFile` (decode tile), `netCDF4.Dataset`.
- Produces: `NetcdfRasterGbxWriter` honoring `singleFile`. Single-mode `write` → per-row fragment capturing `{varname, array (feather 2-D as a column or a small .npy sidecar), width, height, transform(6), crs_epsg, nodata}`; `commit` → grid-compat gate then ONE CF `.nc` with shared `lat`/`lon` + one data var per distinct varname. Incompatible grid or duplicate varname → `ValueError` with `rst_merge_agg` pointer.

- [ ] **Step 1: Write failing raster singleFile tests (merge + error cases)**

Add three tests:
```python
def test_raster_write_singlefile_multivar(spark, tmp_path):
    # two DISTINCT vars (tas, pr) on the SAME grid -> one .nc with both, sharing lat/lon
    ...  # build two (source, tile) rows via netCDF4 fixtures with source NETCDF:"f":tas / :pr
    (df.write.format("netcdf_gbx").option("singleFile","true").mode("overwrite").save(str(out)))
    ncs = [f for f in os.listdir(str(out)) if f.endswith(".nc")]
    assert len(ncs) == 1
    from netCDF4 import Dataset
    with Dataset(os.path.join(str(out), ncs[0])) as nc:
        assert "tas" in nc.variables and "pr" in nc.variables
        assert nc.variables["tas"].dimensions == ("lat","lon")

def test_raster_write_singlefile_incompatible_grid_errors(spark, tmp_path):
    # two tiles, different grid sizes/CRS + singleFile -> ValueError mentioning rst_merge_agg
    with pytest.raises(Exception) as e:
        df.write.format("netcdf_gbx").option("singleFile","true").mode("overwrite").save(str(out))
    assert "rst_merge_agg" in str(e.value)

def test_raster_write_singlefile_duplicate_var_errors(spark, tmp_path):
    # two tiles, SAME varname + same grid dims (window-tiles) + singleFile -> ValueError -> rst_merge_agg
    ...
```

(Building `(source, tile)` rows: read a netCDF4-written grid via `netcdf_gbx` raster to get real tiles, or construct GTiff tiles with `source = 'NETCDF:"f":tas'`. Reuse the Task-1-cycle raster fixture helpers.)

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py -k "singlefile_multivar or singlefile_incompatible or singlefile_duplicate" --log nc-writer-rsf.log`
Expected: FAIL — singleFile ignored (multiple `.nc`), no error raised.

- [ ] **Step 3: Implement raster two-phase singleFile**

`write(iterator)` single-mode: per row, decode the tile (array, transform, crs epsg, nodata, varname — reuse the existing decode + `_var_from_source`), write a fragment (feather table with the flattened array + shape + a metadata blob for transform/crs/nodata/varname, or a compact container). Return frag-carrying message.
`commit` single-mode: read all fragments. **Grid-compat gate:** collect `(width,height,crs_epsg)` + transform per fragment; require identical `(width,height)`, identical `crs_epsg`, transforms equal within rtol (e.g. `1e-9`). If any differ → `raise ValueError(f"netcdf_gbx singleFile: tiles have incompatible grids ...; to mosaic window-tiles of one variable into a single grid, use gbx_rst_merge_agg / gbx_rst_merge before writing.")`. Detect duplicate varname across fragments → same message (window-tiles are a mosaic, not a multi-var merge). If compatible + distinct varnames: write ONE CF grid `.nc` (shared `lat`/`lon` from the common transform, one data var per varname with its `_FillValue`, shared `crs` grid_mapping for non-4326) to driver-local temp → `shutil.copyfile` to the resolved single target. `abort`: clean.

- [ ] **Step 4: Run raster singleFile tests + parts regression to green**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/ds/test_netcdf_writer.py --log nc-writer-rsf.log`
Expected: multivar merges to one `.nc`; incompatible + duplicate raise with the `rst_merge_agg` pointer; existing parts raster tests stay green.

- [ ] **Step 5: Serverless-safety grep + lint + commit**

`grep -nE "spark\\.conf\\.set|_jvm|\\.rdd|\\.cache\\(|\\.persist\\(" python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py` → nothing. Lint.
```bash
git add python/geobrix/src/databricks/labs/gbx/ds/_write_netcdf.py \
        python/geobrix/test/ds/test_netcdf_writer.py
git commit -F /tmp/sf-task2.txt   # "feat(netcdf): raster writer singleFile merges same-grid vars (errors -> rst_merge_agg)"
```

---

### Task 3: Writer-bench `singleFile` variant

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/bench/cluster.py` (`_CELL_NETCDF_WRITER`: add a `singleFile` measured leg per mode)
- Test: `python/geobrix/test/bench/` smoke tests

**Interfaces:** consumes `run_format_write(... options={... "singleFile":"true"})` (the single options dict flows `singleFile` to the writer). Produces two additional writer ResultRows (raster-single, vector-single) distinguished from the parts rows via a note/fn tag so they don't collide in the store.

- [ ] **Step 1: Write failing smoke test**

Assert the writer cell emits BOTH a parts leg and a singleFile leg per mode (grep the emitted cell for `"singleFile": "true"`), and rows are distinguishable. Mirror the existing writer-bench smoke tests.

- [ ] **Step 2: Run to verify failure**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/bench/ -k netcdf_writer --log bench-sf.log`
Expected: FAIL — no singleFile leg emitted.

- [ ] **Step 3: Add the singleFile legs to `_CELL_NETCDF_WRITER`**

After each existing parts-mode `run_format_write` leg (raster, vector), add a parallel measured leg with `options={... "singleFile": "true"}` (raster keeps `filterRegex`; vector keeps `mode=vector`+`group=/PRODUCT`+`variables=...`+`singleFile`). Tag the row so parts vs single are distinct in the store (append a suffix to the note, or a distinct out-dir like `{CORPUS}/netcdf-out-single`). Keep the skip-clean + row-count>0 guards. Distinct out-dirs per leg so overwrite doesn't clobber.

- [ ] **Step 4: Run smoke tests to green + commit**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/bench/ --log bench-sf.log`
```bash
git add python/geobrix/src/databricks/labs/gbx/bench/cluster.py python/geobrix/test/bench/
git commit -F /tmp/sf-task3.txt   # "bench(netcdf): parts-vs-single writer throughput legs"
```

---

### Task 4: Docs (`singleFile` + the mosaic pattern pointer)

**Files:**
- Modify: `docs/docs/readers/netcdf.mdx` (writer section: `singleFile` on both modes; the `rst_merge_agg` mosaic pattern; memory tradeoff)
- Modify: `docs/docs/api/benchmarking.mdx` (parts-vs-single writer legs)
- Modify: `docs/docs/beta-release-notes.mdx` (the new `singleFile` option)

- [ ] **Step 1: Write the docs**

Document `.option("singleFile","true")` on both writer modes (default sharded parts; opt-in single). Vector = one CF-DSG `.nc` (all points). Raster = one CF `.nc` merging DISTINCT variables that share a grid. **Call out the mosaic pattern prominently:** to combine many spatial-window tiles of ONE variable into a single grid, use `gbx_rst_merge_agg` (or `gbx_rst_merge`) BEFORE the writer — show the `SELECT rst_merge_agg(tile) ... GROUP BY ...` → `.option("singleFile","true")` flow. Note the single-file memory tradeoff (driver-funneled; sharded parts safer at very large scale). USER-FACING VOICE — no internal vocabulary; run `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` → nothing new.

- [ ] **Step 2: Commit**

```bash
git add docs/docs/readers/netcdf.mdx docs/docs/api/benchmarking.mdx docs/docs/beta-release-notes.mdx
git commit -F /tmp/sf-task4.txt   # "docs(netcdf): document writer singleFile mode + rst_merge_agg mosaic pattern"
```

---

### Task 5: Wheel rebuild + at-scale parts-vs-single writer bench (human-gated)

- [ ] **Step 1: Rebuild + stage the wheel**

`set -a; source notebooks/tests/databricks_cluster_config.env; set +a` then `GBX_BUNDLE_SKIP_JAR_UPLOAD=1 bash scripts/commands/gbx-data-push-wheel.sh` (light-only change — JAR unchanged this cycle); sync the wheel to `sample-data/` (`bench-wheel-path-divergence`).

- [ ] **Step 2: (Re)start cluster + run the parts-vs-single writer bench**

Start `0519-143423-0jwqt79u`, poll RUNNING + libs INSTALLED. Corpora already staged (33 NASA-NEX grids at `{CORPUS}/netcdf`, 15 S5P swaths at `{CORPUS}/netcdf-swath`). Run `bash scripts/commands/gbx-bench-cluster.sh --netcdf-writer-only --row-counts 1000`. Confirm all four legs (raster parts/single, vector parts/single) report rows>0 (the 0-row guard fails loud otherwise). Verify vector-single produces ONE `.nc` in its out-dir. Give the run's `summary.md` link. Record parts-vs-single throughput in the ledger.

- [ ] **Step 3: Stop the cluster**

`databricks clusters delete 0519-143423-0jwqt79u --profile oauth-fe` after capture.

---

## Sequencing note

Tasks 1 (vector) → 2 (raster) build the feature (one wheel covers both). Task 3 (bench) + Task 4 (docs) are independent Python/docs. Task 5 is the human-gated wheel-rebuild + at-scale run. Tasks 1-4 are local; only Task 5 needs the cluster.

## Loose ends to surface at "done" (per report-loose-ends-after-spec-execution)

- Raster singleFile is a DISTINCT-variable merge, not a window-mosaic (mosaic → `rst_merge_agg` upstream; documented + error-guarded).
- Wheel rebuild+restage after Tasks 1-2 (done in Task 5).
- The 18 prior unpushed writer-cycle commits + these — a push (and the fixed CI `__all__` test) is still pending the user's go.
