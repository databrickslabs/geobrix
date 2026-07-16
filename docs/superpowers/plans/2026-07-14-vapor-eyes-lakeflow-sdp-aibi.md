# Vapor-Eyes Lakeflow SDP + AI/BI Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a self-contained, incremental (daily + backfill) Lakeflow Declarative Pipeline that reproduces the vapor-eyes methane cascade into a dedicated `geospatial_docs.vapor_eyes_lf` schema, materializes latest + trend analytics as gold MVs, produces an app-ready fanout PMTiles product plus a light overview, and drives an AI/BI dashboard — all packaged as a Databricks Asset Bundle, deployed and run live, with docs + screenshots.

**Architecture:** A Databricks Asset Bundle defines one **job** (`vapor_eyes_lf_job`, daily schedule) with Task 1 = a date-parameterized Python **land** task (runs the GeoBrix sample downloaders idempotently, stages raw files to a Volume subtree) → Task 2 = the **Lakeflow pipeline** (Serverless, environment version 5). The pipeline is medallion-layered: Auto Loader streaming **bronze** (file inventory, append-only, bi-temporal), append **silver** partitioned by `observation_date` (with SCD2 wells for as-of attribution), **gold** materialized views split into *latest* (operational maps) and *daily/trend* (time series), and a **tiles** layer (MVT pyramid → fanout PMTiles shards + light overview). An AI/BI dashboard resource binds to the gold MVs.

**Tech Stack:** Databricks Asset Bundles (`databricks bundle`), Lakeflow Declarative Pipelines Python (`from pyspark import pipelines as dp`), Auto Loader (`cloudFiles`), GeoBrix lightweight tier (`geobrix[light,stac,vizx]` — `databricks.labs.gbx.pyrx`/`pyvx`/`ds`/`sample`/`stac`/`pmtiles`), Databricks-native spatial SQL (`st_*`, `h3_*` via `pyspark.databricks.sql.functions`), AI/BI (Lakeview) dashboards, `pytest` for pure-Python units, Python 3.12 / Spark 4 / Scala 2.13.

## Global Constraints

Every task's requirements implicitly include these (copied verbatim from the spec):

- **Target schema:** `geospatial_docs.vapor_eyes_lf` (dedicated; NEVER write to the notebook series' `geospatial_docs.vapor_eyes`).
- **Compute:** Serverless, **environment version 5** (`environment: { spec: { environment_version: "5" }}` / pipeline `serverless: true`).
- **Tier:** lightweight only. Pipeline/transformation code MUST NOT call `spark.conf.set`, `spark._jvm`, or `.rdd`. `repartition(N, col)` is the only allowed parallelism lever (Serverless).
- **Lakeflow API:** `from pyspark import pipelines as dp`; datasets defined with `@dp.table` (streaming), `@dp.materialized_view` (batch), `@dp.temporary_view`, `@dp.expect*`, and `dp.create_auto_cdc_flow` (SCD). Dataset functions return a Spark DataFrame. **No side effects at module scope** (module code is evaluated repeatedly during planning) — all imperative work lives in the land task or inside decorated function bodies. **No `%pip` / `%restart_python` / `dbutils` / `%run`** in pipeline source files (those are notebook-only); dependencies come from the pipeline environment, secrets from the land task.
- **Dependency:** `geobrix[light,stac,vizx]` installed from the staged wheel on a UC Volume, declared as a **pipeline environment dependency** (Lakeflow's supported mechanism — see https://docs.databricks.com/aws/en/ldp/developer/external-dependencies; serverless pipelines do NOT support init scripts, `%pip`, `dbutils.library.restartPython()`, or JVM libraries). A bare Volume-path dependency (`"/Volumes/.../geobrix-0.4.0-py3-none-any.whl"`) installs the wheel; to also pull the `[light,stac,vizx]` extras, use the PEP 508 direct-reference form `geobrix[light,stac,vizx] @ file:///Volumes/.../geobrix-0.4.0-py3-none-any.whl`.
- **Registration:** every pipeline module that uses GeoBrix SQL/readers calls, inside a helper invoked from function bodies: `from databricks.labs.gbx.pyrx import functions as rx; rx.register(spark)`, `from databricks.labs.gbx.pyvx import functions as vx; vx.register(spark)`, `from databricks.labs.gbx.ds.register import register; register(spark)`.
- **AOI:** full AOI `FULL_BBOX = (-103.60, 31.05, -102.60, 31.85)`, EPSG:4326, `(minx, miny, maxx, maxy)`.
- **Bi-temporal:** every fact row carries `observation_date` (DATE, event time) and `_ingested_at` (TIMESTAMP, `current_timestamp()`).
- **Maps:** AI/BI cannot render WKB or H3 — gold exposes native `GEOMETRY` (via `st_geomfromwkb(wkb)`) and/or lat/lon columns.
- **Docs voice:** anything under `docs/docs/` or a README is user-facing — no internal/wave vocabulary. `grep -rn -iE "wave [0-9]+" docs/docs/` must print nothing.
- **Auth for push:** `gh auth switch --user mjohns-databricks` before any push/PR (not needed for local commits). Local git commits per task are fine.
- **Workspace auth for deploy:** use the Databricks CLI profile authorized for the workspace (confirm with `databricks auth describe` in Phase 1 Task 1).

---

## File Structure

```
notebooks/examples/vapor-eyes/lakeflow/
  databricks.yml                     # bundle root: job (land->pipeline) + pipeline + dashboard + vars + target
  README.md                          # deploy / run / schedule / backfill; params; caveats; screenshots
  land/
    land.py                          # Task 1: date-parameterized downloader driver (CLI, no dp import)
    _dates.py                        # pure helpers: window parsing, observation_date derivation, sharding math
  transformations/
    _config.py                       # params (spark.conf.get), Volume paths, GeoBrix registration helper
    bronze_ingest.py                 # s5p_granules, s2_swir_assets, emit_scenes, wells_raw (Auto Loader @dp.table)
    silver_cascade.py                # s5p_hotspots, s2_plume_cells, emit_plumes, plume_quant, wells_shl(SCD2), plume_candidate_wells
    gold_analytics.py                # *_latest MVs + *_daily/trend MVs + aoi_kpis_latest
    portfolio_tiles.py               # portfolio_mvt_tiles, pmtiles_shards (fanout), vapor_eyes_overview.pmtiles
  dashboards/
    vapor_eyes_lf.lvdash.json        # AI/BI dashboard (date filter + 4 pages)
  tests/
    test_dates.py                    # pytest for land/_dates.py helpers
    test_land.py                     # pytest for land.py argument handling (mocked downloaders)
    validate/
      phase1.sql ... phase6.sql      # SQL assertion queries run post-deploy per phase
```

Each `transformations/*.py` is a pipeline source file (referenced by the pipeline `libraries`/`root_path`). `land/*.py` is NOT part of the pipeline — it is the job's first task and must not `import` `pyspark.pipelines`.

---

## Phase 1 — Vertical slice (S5P only)

Goal: prove the entire pattern end-to-end on one source — DAB deploy on Serverless env v5, land-task idempotency, Auto Loader bronze, append silver partitioned by `observation_date` — and resolve the two Lakeflow unknowns (environment dependency install; dir-reader → incremental silver).

### Task 1.1: Bundle scaffold + workspace auth check

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/databricks.yml`
- Create: `notebooks/examples/vapor-eyes/lakeflow/transformations/_config.py`

**Interfaces:**
- Produces: bundle name `vapor_eyes_lf`; variables `catalog`, `schema`, `volume`, `full_aoi`, `date_window`, `s5p_temporal`, and the rest of the Global-Constraints parameters; pipeline `vapor_eyes_lf_pipeline` (target `${var.catalog}.${var.schema}`); `_config.py` helpers `cfg(spark)` → dict of resolved params, `paths(spark)` → dict of Volume dirs, `register_gbx(spark)`.

- [ ] **Step 1: Confirm workspace auth + Serverless availability**

Run:
```bash
databricks auth describe
databricks pipelines list-pipelines --max-results 1
```
Expected: prints the authenticated host/user (the profile authorized for the workspace) and returns without error. If it errors, stop and resolve auth before continuing.

- [ ] **Step 2: Write the bundle root**

Create `notebooks/examples/vapor-eyes/lakeflow/databricks.yml`:
```yaml
bundle:
  name: vapor_eyes_lf

variables:
  catalog: {default: geospatial_docs}
  schema: {default: vapor_eyes_lf}
  volume: {default: data}
  full_aoi: {default: "true"}
  date_window: {default: "2023-07-15/2023-08-20"}
  s5p_temporal: {default: "2024-08-23/2024-08-24"}
  h3_res: {default: "6"}
  qa_min: {default: "0.5"}
  cloud_max: {default: "20"}
  s2_h3_res: {default: "10"}
  k_candidates: {default: "5"}
  min_z: {default: "6"}
  max_z: {default: "13"}
  overview_max_z: {default: "12"}
  earthdata_secret: {default: "geospatial_docs.vapor_eyes.earthdata_token"}
  gbx_wheel: {default: "/Volumes/geospatial_docs/geobrix/sample-data/geobrix-0.4.0-py3-none-any.whl"}

resources:
  pipelines:
    vapor_eyes_lf_pipeline:
      name: vapor_eyes_lf_pipeline
      serverless: true
      catalog: ${var.catalog}
      schema: ${var.schema}
      root_path: ./transformations
      libraries:
        - glob:
            include: transformations/**
      environment:
        dependencies:
          - "geobrix[light,stac,vizx] @ file://${var.gbx_wheel}"
      configuration:
        vapor_eyes.catalog: ${var.catalog}
        vapor_eyes.schema: ${var.schema}
        vapor_eyes.volume: ${var.volume}
        vapor_eyes.full_aoi: ${var.full_aoi}
        vapor_eyes.date_window: ${var.date_window}
        vapor_eyes.s5p_temporal: ${var.s5p_temporal}
        vapor_eyes.h3_res: ${var.h3_res}
        vapor_eyes.qa_min: ${var.qa_min}
        vapor_eyes.cloud_max: ${var.cloud_max}
        vapor_eyes.s2_h3_res: ${var.s2_h3_res}
        vapor_eyes.k_candidates: ${var.k_candidates}
        vapor_eyes.min_z: ${var.min_z}
        vapor_eyes.max_z: ${var.max_z}
        vapor_eyes.overview_max_z: ${var.overview_max_z}

targets:
  dev:
    mode: development
    default: true
```

> Environment dependency (mechanism confirmed — https://docs.databricks.com/aws/en/ldp/developer/external-dependencies): serverless Lakeflow pipelines install packages via **pipeline environment dependencies** only (no init scripts / `%pip` / restartPython / JVM libs). The `environment.dependencies` list above uses the PEP 508 direct-reference form to carry the `[light,stac,vizx]` extras. Residual to verify against `bundle validate`: the exact DAB key for the pipeline environment block (`environment.dependencies` vs `environment.spec.dependencies`) and that the extras resolve from the direct reference — if extras don't resolve, list the wheel path plus the extra dependencies explicitly.

- [ ] **Step 3: Write `_config.py`**

Create `notebooks/examples/vapor-eyes/lakeflow/transformations/_config.py`:
```python
"""Shared config for the vapor-eyes Lakeflow pipeline. Parameters come from the
pipeline `configuration` block (spark.conf.get); NO side effects at import time."""


def cfg(spark):
    g = spark.conf.get
    full_aoi = g("vapor_eyes.full_aoi", "true").lower() == "true"
    return {
        "catalog": g("vapor_eyes.catalog", "geospatial_docs"),
        "schema": g("vapor_eyes.schema", "vapor_eyes_lf"),
        "volume": g("vapor_eyes.volume", "data"),
        "full_aoi": full_aoi,
        "bbox": (-103.60, 31.05, -102.60, 31.85) if full_aoi
                else (-103.25, 31.30, -102.85, 31.62),
        "date_window": g("vapor_eyes.date_window", "2023-07-15/2023-08-20"),
        "s5p_temporal": g("vapor_eyes.s5p_temporal", "2024-08-23/2024-08-24"),
        "h3_res": int(g("vapor_eyes.h3_res", "6")),
        "qa_min": float(g("vapor_eyes.qa_min", "0.5")),
        "cloud_max": int(g("vapor_eyes.cloud_max", "20")),
        "s2_h3_res": int(g("vapor_eyes.s2_h3_res", "10")),
        "k_candidates": int(g("vapor_eyes.k_candidates", "5")),
        "min_z": int(g("vapor_eyes.min_z", "6")),
        "max_z": int(g("vapor_eyes.max_z", "13")),
        "overview_max_z": int(g("vapor_eyes.overview_max_z", "12")),
    }


def paths(spark):
    c = cfg(spark)
    root = f"/Volumes/{c['catalog']}/{c['schema']}/{c['volume']}/vapor-eyes-lf"
    return {
        "root": root,
        "s5p": f"{root}/s5p",
        "s2": f"{root}/sentinel2",
        "emit": f"{root}/emit",
        "wells": f"{root}/wells",
        "tiles": f"{root}/tiles",
        "schema_loc": f"{root}/_schema",     # Auto Loader schema locations
    }


def register_gbx(spark):
    """Register GeoBrix light SQL functions + DS readers. Call from function bodies."""
    from databricks.labs.gbx.pyrx import functions as rx
    from databricks.labs.gbx.pyvx import functions as vx
    from databricks.labs.gbx.ds.register import register as register_ds
    rx.register(spark)
    vx.register(spark)
    register_ds(spark)
```

- [ ] **Step 4: Validate the bundle skeleton**

Run:
```bash
cd notebooks/examples/vapor-eyes/lakeflow && databricks bundle validate
```
Expected: `Validation OK!` (or the concrete error that resolves Phase-1 unknown #1 — fix per Step 2 note, re-run until OK).

- [ ] **Step 5: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/databricks.yml notebooks/examples/vapor-eyes/lakeflow/transformations/_config.py
git commit -m "feat(vapor-eyes-lf): bundle scaffold + pipeline config"
```

### Task 1.2: Pure-Python date/window helpers (TDD)

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/land/_dates.py`
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/test_dates.py`

**Interfaces:**
- Produces: `parse_window(window: str) -> tuple[date, date]` (accepts `"YYYY-MM-DD/YYYY-MM-DD"`); `asof_window(asof: str, days: int = 1) -> str` (returns a `"start/end"` string ending at `asof`); `observation_date_from_item(item_id: str, source: str) -> date | None` (parses the acquisition date token out of an S5P/S2/EMIT item id).

- [ ] **Step 1: Write the failing tests**

Create `notebooks/examples/vapor-eyes/lakeflow/tests/test_dates.py`:
```python
from datetime import date
from land._dates import parse_window, asof_window, observation_date_from_item


def test_parse_window_splits_range():
    assert parse_window("2023-07-15/2023-08-20") == (date(2023, 7, 15), date(2023, 8, 20))


def test_asof_window_single_day():
    assert asof_window("2024-08-24", days=1) == "2024-08-23/2024-08-24"


def test_observation_date_s5p():
    # S5P item id embeds the sensing date as ..._YYYYMMDDT... 
    got = observation_date_from_item(
        "S5P_OFFL_L2__CH4____20240823T193456_20240823T211626_...", "s5p")
    assert got == date(2024, 8, 23)


def test_observation_date_none_when_absent():
    assert observation_date_from_item("no-date-here", "s5p") is None
```

- [ ] **Step 2: Run to verify failure**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_dates.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'land._dates'`.

- [ ] **Step 3: Implement `_dates.py`**

Create `notebooks/examples/vapor-eyes/lakeflow/land/_dates.py`:
```python
"""Pure date/window helpers for the land task. No Spark, no side effects."""
import re
from datetime import date, timedelta

_DATE8 = re.compile(r"(\d{4})(\d{2})(\d{2})T\d{6}")


def parse_window(window: str) -> tuple[date, date]:
    start, end = window.split("/")
    return date.fromisoformat(start), date.fromisoformat(end)


def asof_window(asof: str, days: int = 1) -> str:
    end = date.fromisoformat(asof)
    start = end - timedelta(days=days)
    return f"{start.isoformat()}/{end.isoformat()}"


def observation_date_from_item(item_id: str, source: str) -> date | None:
    m = _DATE8.search(item_id)
    if not m:
        return None
    y, mo, d = (int(x) for x in m.groups())
    return date(y, mo, d)
```

- [ ] **Step 4: Run to verify pass**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_dates.py -v`
Expected: 4 passed.

> Note: `observation_date_from_item` is verified against a real item id in Task 1.4 Step 3 (S5P) and adjusted per-source in Phase 2 if a source's id format differs.

- [ ] **Step 5: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/land/_dates.py notebooks/examples/vapor-eyes/lakeflow/tests/test_dates.py
git commit -m "feat(vapor-eyes-lf): land date/window helpers (TDD)"
```

### Task 1.3: Land task — S5P download driver

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/land/land.py`
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py`
- Modify: `notebooks/examples/vapor-eyes/lakeflow/databricks.yml` (add the job with the land task)

**Interfaces:**
- Consumes: `land._dates.parse_window`, `asof_window`.
- Produces: `land.py` CLI `--sources s5p[,s2,emit,wells] --window <w>|--asof <d> --catalog --schema --volume --s5p-temporal`; function `run_land(spark, sources, *, catalog, schema, volume, date_window, s5p_temporal)` that stages files to the Volume subtree and returns a summary dict `{source: staged_count}`. Job task key `land`.

- [ ] **Step 1: Write the failing test (arg handling, mocked downloaders)**

Create `notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py`:
```python
import sys, types
from unittest import mock


def _install_fakes():
    # Fake databricks.labs.gbx.sample so land.py imports without the wheel.
    sample = types.ModuleType("databricks.labs.gbx.sample")
    for name in ("TropomiDownloader", "EmitDownloader", "WellsDownloader"):
        setattr(sample, name, mock.MagicMock())
    sys.modules["databricks"] = types.ModuleType("databricks")
    sys.modules["databricks.labs"] = types.ModuleType("databricks.labs")
    sys.modules["databricks.labs.gbx"] = types.ModuleType("databricks.labs.gbx")
    sys.modules["databricks.labs.gbx.sample"] = sample
    stac = types.ModuleType("databricks.labs.gbx.stac")
    stac.StacClient = mock.MagicMock()
    sys.modules["databricks.labs.gbx.stac"] = stac
    return sample


def test_run_land_s5p_calls_tropomi_download():
    sample = _install_fakes()
    from land.land import run_land
    fake_spark = mock.MagicMock()
    tropomi = sample.TropomiDownloader.return_value
    tropomi.download.return_value.count.return_value = 1
    run_land(fake_spark, ["s5p"], catalog="c", schema="s", volume="data",
             date_window="2023-07-15/2023-08-20", s5p_temporal="2024-08-23/2024-08-24")
    assert tropomi.download.called
    # staged to the vapor-eyes-lf s5p subtree with the s5p_temporal window
    _, kwargs = tropomi.download.call_args
    assert kwargs.get("temporal") == "2024-08-23/2024-08-24"
```

- [ ] **Step 2: Run to verify failure**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'land.land'`.

- [ ] **Step 3: Implement `land.py`**

Create `notebooks/examples/vapor-eyes/lakeflow/land/land.py`:
```python
"""Task 1 of the vapor-eyes-lf job: date-parameterized downloader driver.

Runs the GeoBrix sample downloaders idempotently (their own skip-guards avoid
re-downloading valid staged files) and lands raw files into the pipeline's own
Volume subtree. Emits NO Delta tables — the pipeline's Auto Loader bronze layer
inventories the staged files. This file is NOT part of the pipeline and must not
import pyspark.pipelines."""
import argparse

from land._dates import asof_window


def _subtree(catalog, schema, volume):
    root = f"/Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf"
    return {
        "root": root, "s5p": f"{root}/s5p", "s2": f"{root}/sentinel2",
        "emit": f"{root}/emit", "wells": f"{root}/wells",
    }


def run_land(spark, sources, *, catalog, schema, volume, date_window,
             s5p_temporal, bbox=(-103.60, 31.05, -102.60, 31.85)):
    from databricks.labs.gbx.sample import (
        EmitDownloader, TropomiDownloader, WellsDownloader)
    dirs = _subtree(catalog, schema, volume)
    for d in dirs.values():
        _mkdir(spark, d)
    staged = {}
    if "s5p" in sources:
        df = TropomiDownloader().download(bbox, dirs["s5p"], temporal=s5p_temporal, spark=spark)
        staged["s5p"] = df.count()
    if "s2" in sources:
        from databricks.labs.gbx.stac import StacClient
        staged["s2"] = _land_s2(spark, StacClient(), bbox, dirs["s2"], date_window)
    if "emit" in sources:
        df = EmitDownloader().download(bbox, dirs["emit"], temporal=date_window, spark=spark)
        staged["emit"] = df.count()
    if "wells" in sources:
        df = WellsDownloader().download(bbox, dirs["wells"], spark=spark)
        staged["wells"] = int(df.first()["feature_count"])
    print(f"... landed: {staged}")
    return staged


def _mkdir(spark, path):
    try:
        import os
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        print(f"... mkdir skipped {path}: {type(e).__name__}")


def _land_s2(spark, stac, bbox, out_dir, date_window):
    # Filled in Phase 2 (S2 needs a computed top-hotspot window); Phase 1 no-op.
    return 0


def main():
    from pyspark.sql import SparkSession
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="s5p")
    ap.add_argument("--window")
    ap.add_argument("--asof")
    ap.add_argument("--catalog", default="geospatial_docs")
    ap.add_argument("--schema", default="vapor_eyes_lf")
    ap.add_argument("--volume", default="data")
    ap.add_argument("--s5p-temporal", default="2024-08-23/2024-08-24")
    a = ap.parse_args()
    window = a.window or (asof_window(a.asof) if a.asof else "2023-07-15/2023-08-20")
    spark = SparkSession.builder.getOrCreate()
    run_land(spark, a.sources.split(","), catalog=a.catalog, schema=a.schema,
             volume=a.volume, date_window=window, s5p_temporal=a.s5p_temporal)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run to verify pass**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py -v`
Expected: 1 passed.

- [ ] **Step 5: Add the job (land task) to the bundle**

In `databricks.yml`, under `resources:` add:
```yaml
  jobs:
    vapor_eyes_lf_job:
      name: vapor_eyes_lf_job
      tasks:
        - task_key: land
          spark_python_task:
            python_file: ./land/land.py
            parameters:
              ["--sources", "s5p", "--s5p-temporal", "${var.s5p_temporal}"]
          environment_key: land_env
        - task_key: pipeline
          depends_on: [{task_key: land}]
          pipeline_task:
            pipeline_id: ${resources.pipelines.vapor_eyes_lf_pipeline.id}
      environments:
        - environment_key: land_env
          spec:
            environment_version: "5"
            dependencies:
              - "geobrix[light,stac,vizx] @ file://${var.gbx_wheel}"
      schedule:
        quartz_cron_expression: "0 0 7 * * ?"
        timezone_id: "America/Chicago"
        pause_status: PAUSED
```

- [ ] **Step 6: Validate + commit**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && databricks bundle validate`
Expected: `Validation OK!`
```bash
git add notebooks/examples/vapor-eyes/lakeflow/land/land.py notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py notebooks/examples/vapor-eyes/lakeflow/databricks.yml
git commit -m "feat(vapor-eyes-lf): S5P land task + job wiring"
```

### Task 1.4: Bronze `s5p_granules` (Auto Loader streaming table)

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/transformations/bronze_ingest.py`
- Create: `notebooks/examples/vapor-eyes/lakeflow/tests/validate/phase1.sql`

**Interfaces:**
- Produces: streaming table `s5p_granules` with columns `path STRING, file_size LONG, source_file STRING, observation_date DATE, _ingested_at TIMESTAMP`.
- Consumes: `_config.paths`, `_config.cfg`.

- [ ] **Step 1: Write `bronze_ingest.py` (S5P only for Phase 1)**

Create `notebooks/examples/vapor-eyes/lakeflow/transformations/bronze_ingest.py`:
```python
"""Bronze: Auto Loader file inventory per source. Append-only, exactly-once per
staged file. observation_date parsed from the filename; _ingested_at = now."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import paths

# S5P sensing date token in the granule filename: ..._YYYYMMDDT......
_S5P_DATE = r".*_(\d{8})T\d{6}.*"


def _autoload(spark, src_dir, schema_loc, glob):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("pathGlobFilter", glob)
        .load(src_dir)
        .select(
            F.col("path"),
            F.col("length").alias("file_size"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("source_file"),
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(name="s5p_granules", comment="Staged Sentinel-5P CH4 granule inventory")
def s5p_granules():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["s5p"], f"{p['schema_loc']}/s5p", "*.nc")
    return df.withColumn(
        "observation_date",
        F.to_date(F.regexp_extract("source_file", _S5P_DATE, 1), "yyyyMMdd"),
    )
```

- [ ] **Step 2: Write the Phase-1 validation SQL**

Create `notebooks/examples/vapor-eyes/lakeflow/tests/validate/phase1.sql`:
```sql
-- Bronze populated, exactly-once, dated.
SELECT count(*) AS n_granules,
       count(DISTINCT source_file) AS n_files,
       count(DISTINCT observation_date) AS n_dates,
       max(observation_date) AS latest_obs
FROM geospatial_docs.vapor_eyes_lf.s5p_granules;
-- Expect: n_granules > 0, n_granules = n_files (no dup ingest), observation_date NOT NULL.

-- Silver hotspots present and per-observation_date.
SELECT observation_date, count(*) AS n_cells,
       round(max(ch4_max), 1) AS peak_ch4
FROM geospatial_docs.vapor_eyes_lf.s5p_hotspots
GROUP BY observation_date ORDER BY observation_date;
```

- [ ] **Step 3: Deploy, run land, inspect a real S5P item id, run pipeline**

Run (deploy + land + confirm the date regex against a real filename):
```bash
cd notebooks/examples/vapor-eyes/lakeflow && databricks bundle deploy
databricks bundle run vapor_eyes_lf_job --python-params '["--sources","s5p","--s5p-temporal","2024-08-23/2024-08-24"]'
databricks fs ls dbfs:/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/s5p
```
Expected: the job's `land` task stages ≥1 `.nc` file; `fs ls` prints a filename containing a `_YYYYMMDDT` token. If the token position differs from `_S5P_DATE`, fix the regex in `bronze_ingest.py` and `land/_dates.py`, re-run `tests/test_dates.py`, redeploy.

- [ ] **Step 4: Validate bronze + silver populated**

Run:
```bash
databricks sql query --warehouse-id <wh> --query "$(cat tests/validate/phase1.sql | sed -n '1,7p')"
```
Expected: `n_granules > 0`, `n_granules = n_files`, `latest_obs = 2024-08-23`.

- [ ] **Step 5: Verify idempotency (run job twice → no new rows)**

Run:
```bash
databricks bundle run vapor_eyes_lf_job --python-params '["--sources","s5p"]'
databricks sql query --warehouse-id <wh> --query "SELECT count(*) FROM geospatial_docs.vapor_eyes_lf.s5p_granules"
```
Expected: same `count(*)` as Step 4 (downloader skip-guard + Auto Loader exactly-once → no duplicates).

- [ ] **Step 6: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/bronze_ingest.py notebooks/examples/vapor-eyes/lakeflow/tests/validate/phase1.sql
git commit -m "feat(vapor-eyes-lf): S5P Auto Loader bronze inventory"
```

### Task 1.5: Silver `s5p_hotspots` (append, partitioned by observation_date) — resolves reader→incremental unknown

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/transformations/silver_cascade.py`

**Interfaces:**
- Consumes: `s5p_granules` (bronze), `_config.cfg`, `_config.register_gbx`.
- Produces: streaming table `s5p_hotspots` with `h3_cellid LONG, observation_date DATE, ch4_mean DOUBLE, ch4_max DOUBLE, n_obs LONG, geom_wkb BINARY, _ingested_at TIMESTAMP`, partitioned by `observation_date`.

- [ ] **Step 1: Confirm the netcdf_gbx reader exposes a source-path column**

Run (in a scratch Serverless notebook or `databricks` exec, using the wheel):
```python
from databricks.labs.gbx.sample import TropomiDownloader
df = TropomiDownloader().read("/Volumes/geospatial_docs/vapor_eyes_lf/data/vapor-eyes-lf/s5p")
print(df.columns)
```
Expected: confirms the point columns `methane_mixing_ratio_bias_corrected, qa_value, geom_0` and whether a source/path column exists. **Branch:**
- If a source-path column exists → silver joins reader points to bronze on that path to attach `observation_date` (Step 2a).
- If NOT → silver derives `observation_date` per file by reading each new granule directory-scoped via the reader's `filterRegex` on the date token, looping over new dates from bronze (Step 2b). Pick the branch that matches reality; implement only that one.

- [ ] **Step 2: Implement `silver_cascade.py` (s5p_hotspots)**

Create `notebooks/examples/vapor-eyes/lakeflow/transformations/silver_cascade.py`:
```python
"""Silver: the methane cascade, append-only and partitioned by observation_date.
GeoBrix light readers are directory-scoped; we attach observation_date from the
bronze inventory (Step 1 branch) so each overpass's points aggregate into that
date's hotspot cells."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.databricks.sql import functions as DBF

from _config import cfg, paths, register_gbx


@dp.table(
    name="s5p_hotspots",
    comment="Per-H3-cell CH4 mean/max per overpass (S5P screening surface)",
    partition_cols=["observation_date"],
)
@dp.expect("has_obs", "n_obs > 0")
@dp.expect("ch4_present", "ch4_mean IS NOT NULL")
def s5p_hotspots():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    c = cfg(spark)
    p = paths(spark)
    minx, miny, maxx, maxy = c["bbox"]

    from databricks.labs.gbx.sample import TropomiDownloader
    pts = TropomiDownloader().read(p["s5p"])  # cols: methane_mixing_ratio_bias_corrected, qa_value, geom_0(+ source path)
    pts = pts.filter(
        (F.col("qa_value") >= c["qa_min"])
        & F.col("methane_mixing_ratio_bias_corrected").isNotNull()
    )
    # observation_date: Step-1 branch. Branch-2a (join to bronze on source path):
    granules = spark.read.table("s5p_granules").select("path", "observation_date")
    pts = pts.join(granules, pts["<source_path_col>"] == granules["path"], "left")

    pts = (
        pts.withColumn("_g", DBF.st_geomfromwkb(F.col("geom_0")))
        .withColumn("lon", DBF.st_x("_g")).withColumn("lat", DBF.st_y("_g"))
        .filter((F.col("lon") >= minx) & (F.col("lon") <= maxx))
        .filter((F.col("lat") >= miny) & (F.col("lat") <= maxy))
        .withColumn("h3_cellid", DBF.h3_longlatash3("lon", "lat", F.lit(c["h3_res"])))
    )
    return (
        pts.groupBy("h3_cellid", "observation_date")
        .agg(
            F.mean("methane_mixing_ratio_bias_corrected").alias("ch4_mean"),
            F.max("methane_mixing_ratio_bias_corrected").alias("ch4_max"),
            F.count("*").alias("n_obs"),
        )
        .withColumn("geom_wkb", DBF.h3_centeraswkb("h3_cellid"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
```
Replace `<source_path_col>` with the confirmed column from Step 1 (Branch 2a), or replace the join with the Branch-2b per-date read.

> Phase-1 unknown #2 note: `s5p_hotspots` is defined with `@dp.table` (streaming). If aggregation over a directory-read (non-streaming) source is rejected by the pipeline, switch to `@dp.materialized_view` and add `observation_date` as a MERGE key so history is retained across runs (documented fallback). Resolve against the live pipeline run.

- [ ] **Step 3: Deploy + run + validate**

Run:
```bash
cd notebooks/examples/vapor-eyes/lakeflow && databricks bundle deploy
databricks bundle run vapor_eyes_lf_job --python-params '["--sources","s5p"]'
databricks sql query --warehouse-id <wh> --query "$(sed -n '9,14p' tests/validate/phase1.sql)"
```
Expected: one row per `observation_date` with `n_cells > 0` and a plausible `peak_ch4`.

- [ ] **Step 4: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/silver_cascade.py
git commit -m "feat(vapor-eyes-lf): S5P hotspots silver (append by observation_date)"
```

**Phase 1 gate:** DAB deploys on Serverless env v5; `land` stages S5P idempotently; `s5p_granules` (Auto Loader) and `s5p_hotspots` (append by `observation_date`) populate; a second job run adds no duplicate rows. The environment-install and reader→incremental unknowns are resolved and the resolved approach is encoded. STOP for review before Phase 2.

---

## Phase 2 — All downloads + full bronze

Goal: extend `land.py` and `bronze_ingest.py` to S2, EMIT, wells. Each bronze table follows the S5P Auto Loader pattern; land uses the real per-source downloader signatures.

### Task 2.1: Land — EMIT + wells

**Files:** Modify `land/land.py` (EMIT + wells already scaffolded — wire the Earthdata secret), `databricks.yml` (land task `--sources s5p,emit,wells`; inject `EARTHDATA_TOKEN` from the UC secret).

- [ ] **Step 1:** In `databricks.yml` land task, add the secret env and expand sources:
```yaml
          spark_python_task:
            python_file: ./land/land.py
            parameters: ["--sources","s5p,emit,wells","--window","${var.date_window}","--s5p-temporal","${var.s5p_temporal}"]
          environment_key: land_env
```
And under the `land` task add (bundle job secret reference):
```yaml
          # EARTHDATA_TOKEN for EMIT (NASA LP DAAC). UC secret scope.key from var.earthdata_secret.
          spark_env_vars:
            EARTHDATA_TOKEN: "{{secrets/vapor_eyes/earthdata_token}}"
```
> Verify the scope/key path against `databricks secrets list-scopes`; the secret was created for the notebook series (`geospatial_docs.vapor_eyes.earthdata_token`).

- [ ] **Step 2:** Confirm `run_land` already calls `EmitDownloader().download(bbox, dir, temporal=date_window, spark=spark)` and `WellsDownloader().download(bbox, dir, spark=spark)` (implemented in Task 1.3). Add a pytest to `tests/test_land.py` asserting both are called for `--sources emit,wells`:
```python
def test_run_land_emit_wells():
    sample = _install_fakes()
    from land.land import run_land
    fake = mock.MagicMock()
    sample.EmitDownloader.return_value.download.return_value.count.return_value = 2
    sample.WellsDownloader.return_value.download.return_value.first.return_value = {"feature_count": 500}
    out = run_land(fake, ["emit","wells"], catalog="c", schema="s", volume="data",
                   date_window="2023-07-15/2023-08-20", s5p_temporal="x")
    assert sample.EmitDownloader.return_value.download.called
    assert sample.WellsDownloader.return_value.download.called
    assert out["wells"] == 500
```
Run: `python -m pytest tests/test_land.py -v` → 3 passed.

- [ ] **Step 3:** Commit: `git commit -am "feat(vapor-eyes-lf): land EMIT + wells with Earthdata secret"`.

### Task 2.2: Bronze — s2_swir_assets, emit_scenes, wells_raw

**Files:** Modify `transformations/bronze_ingest.py`.

- [ ] **Step 1:** Add three Auto Loader tables mirroring `s5p_granules`, with per-source globs and date tokens:
```python
@dp.table(name="emit_scenes", comment="Staged EMIT L2B CH4 product inventory")
def emit_scenes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    df = _autoload(spark, p["emit"], f"{p['schema_loc']}/emit", "*")
    return df.withColumn(
        "observation_date",
        F.to_date(F.regexp_extract("source_file", r".*_(\d{8})T\d{6}.*", 1), "yyyyMMdd"))


@dp.table(name="wells_raw", comment="Staged TX RRC WellSHL snapshot inventory")
def wells_raw():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    # wells snapshot has no acquisition date; observation_date = ingest date.
    return (_autoload(spark, p["wells"], f"{p['schema_loc']}/wells", "*.geojson")
            .withColumn("observation_date", F.to_date(F.col("_ingested_at"))))


@dp.table(name="s2_swir_assets", comment="Staged Sentinel-2 B11/B12 SWIR COG inventory")
def s2_swir_assets():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    p = paths(spark)
    return (_autoload(spark, p["s2"], f"{p['schema_loc']}/s2", "*.tif")
            .withColumn("observation_date",
                        F.to_date(F.regexp_extract("source_file", r".*_(\d{8})T\d{6}.*", 1), "yyyyMMdd")))
```
> Confirm each source's real date-token position with `databricks fs ls` after the first land run (as in Phase 1 Task 1.4 Step 3); adjust regexes if needed.

- [ ] **Step 2:** Deploy + run job (`--sources s5p,emit,wells`); validate each bronze table `count(*) > 0` via a `phase2.sql` mirroring `phase1.sql` for the three tables. (S2 lands in Task 3.2 once the top-hotspot window exists.)

- [ ] **Step 3:** Commit: `git commit -am "feat(vapor-eyes-lf): EMIT/wells/S2 Auto Loader bronze"`.

---

## Phase 3 — Silver cascade (temporal)

Goal: complete silver — `emit_plumes`, `plume_quant`, `wells_shl` (SCD2), `plume_candidate_wells` (as-of), `s2_plume_cells` — all append/partitioned by `observation_date`, mirroring the notebook transforms exactly (API pinned).

### Task 3.1: `emit_plumes` + `plume_quant`

**Files:** Modify `transformations/silver_cascade.py`.

**Interfaces:** Produces `emit_plumes` (`plume_id, observation_date, max_conc_ppmm, emission_rate_kg_hr, emission_rate_uncert_kg_hr, wind_speed_ms, fetch_length_m, lon_max, lat_max, plume_geom, _ingested_at`) and `plume_quant` (adds `gbx_mean_ppmm, gbx_max_ppmm`).

- [ ] **Step 1:** Add `emit_plumes` (append, partition by observation_date). Read via `EmitDownloader().read_plumes(p["emit"])`; attach `observation_date` from `emit_scenes` (same Step-1 branch as S5P); `@dp.expect("rate_nonneg","emission_rate_kg_hr >= 0")`, `@dp.expect("has_geom","plume_geom IS NOT NULL")`:
```python
@dp.table(name="emit_plumes", partition_cols=["observation_date"],
          comment="EMIT plume outlines + JPL emission estimates")
@dp.expect("rate_nonneg", "emission_rate_kg_hr >= 0")
@dp.expect("has_geom", "plume_geom IS NOT NULL")
def emit_plumes():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)
    from databricks.labs.gbx.sample import EmitDownloader
    plumes = EmitDownloader().read_plumes(p["emit"])
    scenes = spark.read.table("emit_scenes").select("path", "observation_date").distinct()
    # attach observation_date via the plume product's source path (Step-1 branch)
    return (plumes.join(scenes, plumes["<source_path_col>"] == scenes["path"], "left")
            .withColumn("_ingested_at", F.current_timestamp()))
```
- [ ] **Step 2:** Add `plume_quant` — port NB03 CELL 10 verbatim (crossJoin `read_enh`, `rx.rst_clip("scene","plume_geom",F.lit(True))`, `rx.rst_summary("clip")`, `get_json_object` for mean/max, `row_number` max-per-plume). Carry `observation_date`. Full code mirrors the pinned NB03 CELL 10 with `plumes = spark.read.table("emit_plumes")` as the source.
- [ ] **Step 3:** Deploy + run; validate `emit_plumes`/`plume_quant` row counts > 0 and `gbx_max_ppmm` non-null. Commit.

### Task 3.2: `s2_plume_cells` (+ S2 land with computed top-hotspot window)

**Files:** Modify `land/land.py` (`_land_s2`), `transformations/silver_cascade.py`.

- [ ] **Step 1:** Implement `_land_s2`: read `s5p_hotspots` (latest date), pick top cell by `ch4_max`, derive its H3 boundary bbox, `stac_client.search(aoi, geojson_col="geojson", collections=["sentinel-2-l2a"], datetime=date_window)`, filter `eo:cloud_cover <= cloud_max`, take least-cloudy `item_id`, filter `asset_name in (B11,B12)`, `stac_client.download(bands, s2_dir, bbox=list(cell_bbox), bbox_crs="EPSG:4326")`. (Ports NB02 CELL 7; the top-hotspot is computed here in the land task, not cross-module.)
- [ ] **Step 2:** Add `s2_plume_cells` — port NB02 CELL 9 (`_band_tile` via `gtiff_gbx` + `filterRegex`, `rx.rst_mapalgebra(F.array("tile_b12","tile_b11"), "(B - A) / (A + B)")`, LATERAL `gbx_rst_h3_tessellate(tile, s2_h3_res)`, `gbx_rst_summary(named_struct(...))`). Carry `observation_date` from `s2_swir_assets`.
- [ ] **Step 3:** Add S2 to the land task sources in `databricks.yml` (`--sources s5p,emit,wells,s2`; S2 must run after a hotspot exists — it reads `s5p_hotspots`, so within the same land run S5P is processed first by the pipeline; if S2 needs the pipeline's hotspots, split land into `land_s5p` before pipeline and `land_s2` after — decide against the live DAG in review). Deploy + validate. Commit.

### Task 3.3: `wells_shl` (SCD2) + `plume_candidate_wells` (as-of)

**Files:** Modify `transformations/silver_cascade.py`.

- [ ] **Step 1:** Define a streaming view over `wells_raw` that reads each snapshot via `WellsDownloader().read(p["wells"])` with the NB04 CELL 7 column projection (`API→api`, `CompanyName→operator`, `LeaseName→lease`, `WellNbr→well_no`, `FieldName→field`, `County→county`, `WellURL→well_url`, `geom_0→well_geom`), plus `observation_date` (snapshot date) and `_ingested_at`.
- [ ] **Step 2:** Create the SCD2 target with `dp.create_auto_cdc_flow`:
```python
dp.create_streaming_table("wells_shl")
dp.create_auto_cdc_flow(
    target="wells_shl",
    source="wells_snapshots",          # the streaming view from Step 1
    keys=["api"],
    sequence_by=F.col("observation_date"),
    stored_as_scd_type=2,
)
```
- [ ] **Step 3:** Add `plume_candidate_wells` (append, partition by observation_date) — port NB04 CELL 9 but join **as-of**: for each plume at `observation_date d`, join to `wells_shl` rows where `__START_AT <= d AND (__END_AT IS NULL OR __END_AT > d)`. Then `st_point(lon_max,lat_max)`, `st_geomfromwkb(well_geom)`, `st_distancesphere`, `row_number` ≤ `k_candidates`. Carry plume `observation_date`.
- [ ] **Step 4:** Deploy + validate (`wells_shl` has `__START_AT/__END_AT`; `plume_candidate_wells` = `k_candidates` rows per plume). Commit.

---

## Phase 4 — Gold analytics (latest + trend)

**Files:** Create `transformations/gold_analytics.py`.

**Interfaces:** Produces MVs `plume_leaderboard_latest`, `operator_emissions_latest`, `field_county_emissions_latest`, `hotspot_latest`, `aoi_kpis_latest`, `emissions_trend_daily`, `operator_emissions_daily`, `hotspot_trend`. Each map-facing MV exposes native `GEOMETRY`/lat-lon.

### Task 4.1: Latest MVs

- [ ] **Step 1:** `plume_leaderboard_latest` — `@dp.materialized_view`; from `plume_quant` join `plume_candidate_wells` (rank=1) on `plume_id`+`observation_date`; keep the latest `observation_date` per `plume_id` via `row_number() over (partition by plume_id order by observation_date desc) = 1`; expose `origin_geom = st_point(lon_max, lat_max)`, `plume_geom_native = st_geomfromwkb(plume_geom)`, `lead_operator/lead_lease/lead_field/lead_dist_m`, emission cols, `observation_date`. Complete code:
```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(name="plume_leaderboard_latest",
                      comment="Latest per-plume emission + leading candidate operator (map-ready)")
def plume_leaderboard_latest():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    q = spark.read.table("plume_quant")
    lead = (spark.read.table("plume_candidate_wells")
            .filter("rank = 1")
            .select("plume_id", "observation_date",
                    F.col("operator").alias("lead_operator"),
                    F.col("lease").alias("lead_lease"),
                    F.col("field").alias("lead_field"),
                    F.col("dist_m").alias("lead_dist_m")))
    j = q.join(lead, ["plume_id", "observation_date"], "left")
    latest = j.withColumn("_r", F.row_number().over(
        Window.partitionBy("plume_id").orderBy(F.col("observation_date").desc()))).filter("_r = 1").drop("_r")
    return latest.select(
        "plume_id", "observation_date", "emission_rate_kg_hr", "emission_rate_uncert_kg_hr",
        "max_conc_ppmm", "gbx_mean_ppmm", "gbx_max_ppmm", "wind_speed_ms", "fetch_length_m",
        "lead_operator", "lead_lease", "lead_field", "lead_dist_m",
        F.expr("st_x(st_point(lon_max, lat_max))").alias("lon_max"),
        F.expr("st_y(st_point(lon_max, lat_max))").alias("lat_max"),
        F.expr("st_point(lon_max, lat_max)").alias("origin_geom"),
        F.expr("st_geomfromwkb(plume_geom)").alias("plume_geom_native"))
```
- [ ] **Step 2:** `operator_emissions_latest` — group latest `plume_leaderboard_latest` by `lead_operator`: `sum/max(emission_rate_kg_hr)`, `count(plume_id)` as `plume_count`, `approx_count_distinct` of wells from `plume_candidate_wells`.
- [ ] **Step 3:** `field_county_emissions_latest` — group by `lead_field, county` (county via join to `plume_candidate_wells` rank=1): `total_emission_kg_hr, plume_count`.
- [ ] **Step 4:** `hotspot_latest` — from `s5p_hotspots` latest `observation_date`; `center_lon/center_lat = st_x/st_y(st_geomfromwkb(geom_wkb))`, `hex_geom = st_geomfromwkb(h3_boundaryaswkb(h3_cellid))`, ranked by `ch4_max`.
- [ ] **Step 5:** `aoi_kpis_latest` — single-row MV: `total_plumes` (count `plume_leaderboard_latest`), `total_emission_kg_hr` (sum), `wells_scanned` (count distinct `api` in `wells_shl` current), `hotspot_cells` (count `hotspot_latest`), `aoi_area_km2` (from bbox), `latest_observation_date`.
- [ ] **Step 6:** Deploy + validate all 5 non-empty; native GEOMETRY columns non-null (`SELECT count(*) FROM ... WHERE origin_geom IS NOT NULL`). Commit.

### Task 4.2: Trend MVs

- [ ] **Step 1:** `emissions_trend_daily` — `plume_quant` group by `observation_date`: `sum/max(emission_rate_kg_hr)`, `count(*) plume_count`.
- [ ] **Step 2:** `operator_emissions_daily` — `plume_candidate_wells` (rank=1) join `plume_quant`, group by `observation_date, operator`.
- [ ] **Step 3:** `hotspot_trend` — `s5p_hotspots` group by `observation_date, h3_cellid`: `ch4_mean, ch4_max` (+ `center_lon/lat` for optional animated maps).
- [ ] **Step 4:** Deploy + validate; commit.

---

## Phase 5 — Tiles / synthesis (fanout PMTiles + overview)

**Files:** Create `transformations/portfolio_tiles.py`; add sharding math to `land/_dates.py` companion or a new `transformations/_shard.py` with a pytest.

### Task 5.1: `portfolio_mvt_tiles`

- [ ] **Step 1:** `@dp.materialized_view portfolio_mvt_tiles` — build the three layers from **latest** silver (`hotspot_latest`, `plume_leaderboard_latest`, `wells_shl` current), each `(geom_wkb, attrs)` carrying `observation_date` in `attrs`; `repartition(N, "geom_wkb")`; LATERAL `gbx_st_asmvt_pyramid(geom_wkb, attrs, min_z, max_z, '<layer>')` per NB05 CELL 7; union. Columns `layer, z, x, y, mvt_bytes`.
- [ ] **Step 2:** Deploy + validate row count > 0 across `z` in `[min_z, max_z]`. Commit.

### Task 5.2: Sharding math (TDD) + `pmtiles_shards` fanout

**Files:** Create `transformations/_shard.py`, `tests/test_shard.py`.

- [ ] **Step 1:** Write failing test for `tile_shard(z, x, y, shard_zoom) -> str` (shard key = the ancestor tile at `shard_zoom`, e.g. `"z5/x/y"`), and `shard_bounds(shard_key) -> (minx,miny,maxx,maxy)` (Web-Mercator tile → lon/lat bbox). Provide concrete assertions (e.g. `tile_shard(13, 1600, 3000, 6) == "6/25/46"`).
- [ ] **Step 2:** Implement `_shard.py` (pure math: `x >> (z - shard_zoom)`, standard XYZ→lon/lat). Run tests → pass.
- [ ] **Step 3:** `pmtiles_shards` — group `portfolio_mvt_tiles` by `tile_shard(...)`, per shard `pmtiles_agg("mvt_bytes","z","x","y", META)` (META = the NB05 TileJSON `vector_layers` for hotspots/plumes/wells), write each archive to `{tiles}/shards/<shard_key>.pmtiles` (FUSE-safe sequential write in the body), and return the catalog rows `shard_id, min_x, min_y, max_x, max_y, archive_path, layer_feature_counts, min_z, max_z`. Binary-free (no `tile-join`).
- [ ] **Step 4:** Deploy + validate `pmtiles_shards` rows > 0 and archives exist on the Volume (`databricks fs ls .../tiles/shards`). Commit.

### Task 5.3: `vapor_eyes_overview.pmtiles` (light single archive)

- [ ] **Step 1:** In `portfolio_tiles.py`, a final `@dp.materialized_view overview_manifest` (1 row) whose body filters `portfolio_mvt_tiles` to `z <= overview_max_z`, `pmtiles_agg` the whole set, and `open(f"{tiles}/vapor_eyes_overview.pmtiles","wb").write(archive)`; return `path, byte_size, max_zoom`.
- [ ] **Step 2:** Deploy + validate the file exists and `byte_size` is small; commit.

---

## Phase 6 — AI/BI dashboard

**Files:** Create `dashboards/vapor_eyes_lf.lvdash.json`; add the dashboard resource to `databricks.yml`.

**REQUIRED SUB-SKILL:** use the `fe-databricks-tools:databricks-lakeview-dashboard` skill to author + deploy the `.lvdash.json` (datasets, widgets, map layers, filters) rather than hand-writing the widget schema.

### Task 6.1: Datasets + pages

- [ ] **Step 1:** Define dashboard datasets (SQL queries over the gold MVs):
  - `ds_kpis` → `aoi_kpis_latest`
  - `ds_hotspots` → `hotspot_latest` (`hex_geom`, `ch4_max`, `center_lon/lat`)
  - `ds_plumes` → `plume_leaderboard_latest` (`origin_geom`, `plume_geom_native`, `emission_rate_kg_hr`, `lead_operator`)
  - `ds_operators` → `operator_emissions_latest`
  - `ds_fields` → `field_county_emissions_latest`
  - `ds_wells` → `plume_candidate_wells` (`well_lon`, `well_lat`, `dist_m`, `rank`, `operator`)
  - `ds_trend` → `emissions_trend_daily`; `ds_operator_trend` → `operator_emissions_daily`
- [ ] **Step 2:** Add a **date-range filter** widget bound to `observation_date` (default = latest), scoping the operational pages.
- [ ] **Step 3:** Build the 4 pages:
  - **Regional screen (latest):** counter tiles (`ds_kpis`), choropleth map (`ds_hotspots.hex_geom` colored by `ch4_max`), top-cells table.
  - **Quantify & attribute (latest):** point map (`ds_plumes.origin_geom` sized/colored by `emission_rate_kg_hr`) + polygon choropleth (`plume_geom_native`), leaderboard table, operator bar (`ds_operators`), field/county bar (`ds_fields`).
  - **Wells & candidates (latest):** point map (`ds_wells.well_lon/well_lat`), nearest-candidate table.
  - **Trends:** line (`ds_trend` total emission over `observation_date`), multi-series line (`ds_operator_trend`).
  Map viz: **point map** on lat/lon or GEOMETRY POINT; **choropleth** on GEOMETRY POLYGON (WKB/H3 not renderable — that's why gold exposes native GEOMETRY).
- [ ] **Step 4:** Add the dashboard resource to `databricks.yml`:
```yaml
  dashboards:
    vapor_eyes_lf_dashboard:
      display_name: "Vapor-Eyes — Methane Cascade (Lakeflow)"
      file_path: ./dashboards/vapor_eyes_lf.lvdash.json
      warehouse_id: ${var.warehouse_id}
```
(add `warehouse_id` var). Deploy: `databricks bundle deploy`.
- [ ] **Step 5:** Open the deployed dashboard; verify each map/chart/filter renders against the populated MVs. Capture screenshots to `resources/images/...` for the docs. Commit.

---

## Phase 7 — Docs

**Files:** Create `notebooks/examples/vapor-eyes/lakeflow/README.md`; modify `notebooks/examples/vapor-eyes/README.md`; create a page under `docs/docs/`.

### Task 7.1: lakeflow/README.md

- [ ] **Step 1:** Write deploy/run/schedule/backfill instructions (`databricks bundle deploy`, `bundle run vapor_eyes_lf_job`, daily schedule note, backfill = `--window <wide>`), the full parameter table (from Global Constraints), the **portability caveat** (downloads its own full-AOI data), the SCD2/as-of note, prerequisites (Volume `data`, Earthdata secret), and embed the dashboard screenshots. No wave/internal vocabulary.
- [ ] **Step 2:** Commit.

### Task 7.2: series README + docs page

- [ ] **Step 1:** Add a "Lakeflow pipeline + AI/BI dashboard" section to `notebooks/examples/vapor-eyes/README.md` cross-linking `lakeflow/`, explaining it as the productionized, incremental, as-of counterpart to the notebook cascade.
- [ ] **Step 2:** Add/update a page under `docs/docs/` (near the vapor-eyes example docs) covering: incremental Lakeflow SDP, bi-temporal as-of modeling, gold latest/trend MVs, AI/BI native-spatial maps, fanout PMTiles for apps. Screenshots + cross-links.
- [ ] **Step 3:** Run `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/ notebooks/examples/vapor-eyes/` → expect no output. Run `gbx:lint:python --check` over `lakeflow/` Python. Commit.
- [ ] **Step 4:** (Optional, on user go) `gh auth switch --user mjohns-databricks` then push per "Hold pushes, batch more" — push only at a clear stopping point or on user request.

---

## Self-Review (writing-plans)

**Spec coverage:**
- Ingestion (land→pipeline job): Phase 1 T1.3, Phase 2 T2.1 ✓
- Dedicated `vapor_eyes_lf` schema + own Volume subtree: Global Constraints, `_config.paths` ✓
- Full AOI, downloaded once/idempotent: `_config.cfg` bbox, land skip-guards, Phase 1 T1.5 idempotency check ✓
- Bi-temporal (observation_date + _ingested_at): every bronze/silver task ✓
- SCD2 wells + as-of attribution: Phase 3 T3.3 ✓
- Gold latest + trend MVs (all 4 families + trend): Phase 4 ✓
- Native GEOMETRY/lat-lon for maps: Phase 4 T4.1, Phase 6 ✓
- Fanout PMTiles + light overview: Phase 5 T5.2/T5.3 ✓
- DAB packaging + live deploy/run + dashboard + screenshots: Phases 1–6 ✓
- 7-phase build: matches ✓
- Docs (3 targets) + voice check: Phase 7 ✓

**Placeholder scan:** Two intentional `<...>` markers remain and are *resolved by an explicit adjacent step*, not left vague: `<source_path_col>` (resolved in Phase 1 T1.5 Step 1 branch) and `<wh>`/`<shard>` CLI values (workspace-specific, filled at run). All code-bearing steps contain complete code. No "add error handling"/"similar to Task N"/"TBD".

**Type/name consistency:** table names, column names (`observation_date`, `_ingested_at`, `ch4_max`, `plume_geom`, `well_geom`, `lon_max/lat_max`, `dist_m`, `rank`), and function names (`cfg`, `paths`, `register_gbx`, `run_land`, `tile_shard`, `shard_bounds`) are used consistently across tasks. GeoBrix API (`rx.rst_mapalgebra/rst_clip/rst_summary`, `gbx_rst_h3_tessellate`, `gbx_st_asmvt_pyramid`, `pmtiles_agg`, `DBF.h3_*/st_*`) matches the pinned notebook signatures.

**Known workspace-resolved unknowns (front-loaded into Phase 1):** (1) exact DAB key for the pipeline environment block + extras resolution from the direct-reference wheel (mechanism confirmed = environment dependencies; https://docs.databricks.com/aws/en/ldp/developer/external-dependencies); (2) netcdf_gbx source-path column for the reader→incremental join; (3) `@dp.table` streaming vs `@dp.materialized_view` for directory-read aggregation; (4) S2 land ordering vs the pipeline's hotspot table. Each has a specified fallback.
