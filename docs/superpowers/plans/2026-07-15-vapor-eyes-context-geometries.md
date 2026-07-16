# Vapor-Eyes Context Geometries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add EIA Permian shale-play and TIGER county context geometries to the Vapor-Eyes Lakeflow SDP, roll up Carbon Mapper plumes by play and county via point-in-polygon, and surface both as choropleths on a new AI/BI "Regional Context" dashboard page.

**Architecture:** Two static reference sources land once per run to `context/` on the Volume; two reference MVs read them with the GeoBrix light vector reader (`geojson_gbx` / `shapefile_gbx`) emitting native `GEOMETRY` at SRID 4326; two gold MVs join `cm_plume_attributed` points into the polygons with native `st_contains`; a fourth dashboard page renders the two rollup choropleths.

**Tech Stack:** Databricks Lakeflow Declarative Pipelines (`from pyspark import pipelines as dp`), GeoBrix light tier (`pyrx`/`pyvx`/`ds`), Databricks native ST functions, Databricks Asset Bundle, AI/BI (Lakeview) dashboards, pyogrio-backed light vector readers.

## Global Constraints

- **Light tier only.** Product/transform paths never call `spark.conf.set` / `_jvm` / `.rdd`. `repartition(N, col)` only if keyed (memory `serverless-fanout-repartition-by-column`). These rollups are tiny (≤ a few hundred polygons) — no repartition needed.
- **GEOMETRY at SRID 4326.** Every map-facing geometry column is native `GEOMETRY` tagged SRID 4326.
- **Choropleth render contract (memory `aibi-custom-geometry-choropleth`):** the widget's `encodings.region.fieldName` = `"geo(<col>)"` AND the dataset query MUST include a field `{"name":"geo(<col>)","expression":"ST_ASGEOJSON(`<col>`)"}`. Both halves required or the map is blank.
- **Ranking metric = detection count + MEAN kg/hr, never summed** (memory `operator_emissions_leaderboard`).
- **Guarded downloads:** a context download failure logs a WARNING and continues (context is additive; core demo unaffected) — same pattern as EMIT/CM in `land.py`.
- **No aliases; user-facing docs voice** — nothing under `docs/docs/` leaks internal vocabulary.
- **No push until user go** (memory `hold-pushes-batch-more`); PR via `gh auth switch --user mjohns-databricks` (memory `gh_account_for_geobrix`).
- **Verification reality:** land helpers (Tasks 1–2) are locally unit-tested with pytest. The SDP transforms and dashboard (Tasks 3–5) run only on Databricks Serverless — their verification is *bundle deploy + pipeline run + live row query* (and chrome-devtools for the dashboard), driven by the orchestrator, not local pytest. Implementer subagents author the code and run the local unit tests that exist; the orchestrator runs the cluster verification between tasks.

## Key interfaces (verified against current code)

- **Light vector reader** (`from databricks.labs.gbx.ds.register import register`): `spark.read.format("geojson_gbx").load(path)` / `format("shapefile_gbx").load(path.zip)`. Output schema = source attribute columns (preserved by name) + `geom_0` (WKB `BinaryType`, `asWKB` default true) + `geom_0_srid` (String) + `geom_0_srid_proj` (String). Convert with `st_setsrid(st_geomfromwkb(geom_0), 4326)`.
- **`_config.paths(spark)`** returns a dict of Volume dirs under `/Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf`. **`_config.cfg(spark)`** returns config incl. `bbox`. **`register_gbx(spark)`** registers pyrx + pyvx + ds readers.
- **Point layer** `cm_plume_attributed` (gold, ~3724 rows): columns include `plume_id`, `emission_rate_kg_hr`, `lead_operator`, `lon`, `lat`, `plume_geom` (native GEOMETRY 4326), `observation_date`. Build point via `st_setsrid(st_point(lon, lat), 4326)`.
- **EIA plays GeoJSON** attributes: `Shale_play`, `Basin` (filter `='Permian'` → 7 rows), `Lithology`, `Age_shale`, `Area_sq_km`.
- **TIGER counties** attributes: `STATEFP` (TX=`48`, NM=`35`), `GEOID` (FIPS), `NAME`.

---

### Task 1: Config + Volume subtree for the `context` source

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/transformations/_config.py` (add `context` to `paths`)
- Modify: `notebooks/examples/vapor-eyes/lakeflow/land/land.py:22` (`_subtree` add `context`)
- Modify: `notebooks/examples/vapor-eyes/lakeflow/databricks.yml` (job `--sources` add `context`)
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py` (extend), or `tests/test_dates.py` sibling for config

**Interfaces:**
- Produces: `paths(spark)["context"]` → `f"{root}/context"`; `_subtree(...)` dict includes `"context"`.

- [ ] **Step 1: Write the failing test** in `tests/test_land.py` (add a test):

```python
def test_subtree_includes_context():
    from land.land import _subtree
    dirs = _subtree("cat", "sch", "vol")
    assert dirs["context"].endswith("/vapor-eyes-lf/context")
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py::test_subtree_includes_context -q`
Expected: FAIL (KeyError: 'context').

- [ ] **Step 3: Add `context` to `_subtree`** in `land/land.py` — locate the dict returned by `_subtree` (around line 22) and add a `"context"` entry mirroring the others, e.g. `"context": f"{root}/context",`.

- [ ] **Step 4: Add `context` to `_config.paths`** — in `_config.py`, add `"context": f"{root}/context",` to the returned dict.

- [ ] **Step 5: Add `context` to the job sources** — in `databricks.yml`, change the `land` task `parameters` `"--sources", "s5p,emit,wells,s2,cm"` → `"s5p,emit,wells,s2,cm,context"`.

- [ ] **Step 6: Run test to confirm it passes**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py::test_subtree_includes_context -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/_config.py notebooks/examples/vapor-eyes/lakeflow/land/land.py notebooks/examples/vapor-eyes/lakeflow/databricks.yml notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py
git commit -m "feat(vapor-eyes): add context Volume subtree + source toggle"
```

---

### Task 2: `context` land source — download EIA plays + TIGER counties

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/land/land.py` (add `_land_context`, `_dl_eia_plays`, `_dl_tiger_counties`; dispatch from `run_land`)
- Test: `notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py`

**Interfaces:**
- Consumes: `_subtree(...)["context"]`.
- Produces: `run_land(..., sources=["context"])` writes `context/plays/plays.geojson` and `context/counties/cb_2024_us_county_500k.zip` (+ unzipped `.shp` set); returns `staged["context"]` = count of files landed. Guarded: on any download error, logs `WARNING` and sets `staged["context"] = 0`.

- [ ] **Step 1: Write the failing tests** in `tests/test_land.py`:

```python
def test_eia_plays_url_is_permian_geojson():
    from land.land import _EIA_PLAYS_URL
    assert _EIA_PLAYS_URL.startswith("https://hub.arcgis.com/api/download/v1/items/")
    assert "geojson" in _EIA_PLAYS_URL

def test_tiger_counties_url():
    from land.land import _TIGER_COUNTIES_URL
    assert _TIGER_COUNTIES_URL == (
        "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"
    )

def test_land_context_guarded_on_download_error(tmp_path, monkeypatch):
    """A download failure must not raise — it logs and returns 0."""
    import land.land as L
    def boom(*a, **k):
        raise RuntimeError("network down")
    monkeypatch.setattr(L, "_http_get_to_file", boom)
    n = L._land_context(str(tmp_path))
    assert n == 0
```

- [ ] **Step 2: Run to confirm failure**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py -q -k context or url`
Expected: FAIL (ImportError / AttributeError).

- [ ] **Step 3: Implement the download helpers** in `land/land.py` (near `_land_cm`):

```python
_EIA_PLAYS_URL = (
    "https://hub.arcgis.com/api/download/v1/items/"
    "3f001fba00dc4add8dbd00542d61e4da/geojson?redirect=true&layers=0"
)
_TIGER_COUNTIES_URL = (
    "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"
)


def _http_get_to_file(url, dst, timeout=180):
    """Stream an HTTP GET to a local/Volume file path. Raises on non-200."""
    import requests
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    with open(dst, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                fh.write(chunk)


def _land_context(context_dir):
    """Download the two static Permian context geometry sources into
    context_dir/{plays,counties}. Returns the number of files landed.

    Guarded: any download failure logs a WARNING and is skipped — context is
    additive, so a failure must not abort the (already-valuable) core demo."""
    import os
    landed = 0
    plays_dir = os.path.join(context_dir, "plays")
    counties_dir = os.path.join(context_dir, "counties")
    os.makedirs(plays_dir, exist_ok=True)
    os.makedirs(counties_dir, exist_ok=True)
    try:
        dst = os.path.join(plays_dir, "plays.geojson")
        _http_get_to_file(_EIA_PLAYS_URL, dst)
        print(f"... context: EIA plays -> {dst}")
        landed += 1
    except Exception as e:  # noqa: BLE001 - guarded, additive source
        print(f"... WARNING: EIA plays download failed ({e}); skipping")
    try:
        dst = os.path.join(counties_dir, "cb_2024_us_county_500k.zip")
        _http_get_to_file(_TIGER_COUNTIES_URL, dst)
        print(f"... context: TIGER counties -> {dst}")
        landed += 1
    except Exception as e:  # noqa: BLE001 - guarded, additive source
        print(f"... WARNING: TIGER counties download failed ({e}); skipping")
    return landed
```

- [ ] **Step 4: Dispatch from `run_land`** — add, alongside the other `if "<src>" in sources:` blocks:

```python
    if "context" in sources:
        staged["context"] = _land_context(dirs["context"])
        _list_dir(dirs["context"], "context")
```

- [ ] **Step 5: Run tests to confirm pass**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -m pytest tests/test_land.py -q`
Expected: PASS (all, including existing).

- [ ] **Step 6: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/land/land.py notebooks/examples/vapor-eyes/lakeflow/tests/test_land.py
git commit -m "feat(vapor-eyes): land EIA plays + TIGER counties context geometries"
```

---

### Task 3: Reference tables `ref_shale_plays` + `ref_counties`

**Files:**
- Create: `notebooks/examples/vapor-eyes/lakeflow/transformations/context_reference.py`

**Interfaces:**
- Consumes: `context/plays/plays.geojson`, `context/counties/cb_2024_us_county_500k.zip` on the Volume; `_config.paths`, `register_gbx`.
- Produces MVs: `ref_shale_plays(play_name STRING, area_sq_km DOUBLE, play_geom GEOMETRY)`, `ref_counties(county_name STRING, state_fp STRING, geoid STRING, county_geom GEOMETRY)`. Both geometries SRID 4326.

- [ ] **Step 1: Create the transform file**

```python
"""Context reference geometries: EIA Permian shale plays + TIGER counties.

Static reference (not observations) — read once per run from the Volume with the
GeoBrix light vector reader (geojson_gbx / shapefile_gbx, pyogrio-backed, no JAR)
and emit native GEOMETRY at SRID 4326 for the AI/BI choropleths and the gold
point-in-polygon rollups. The reader emits geometry as WKB in `geom_0`."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

from _config import paths, register_gbx


@dp.materialized_view(
    name="ref_shale_plays",
    comment="EIA tight-oil/shale plays for the Permian basin (named play polygons)",
)
def ref_shale_plays():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)
    src = f"{p['context']}/plays/plays.geojson"
    return (
        spark.read.format("geojson_gbx").load(src)
        .filter(F.col("Basin") == "Permian")
        .select(
            F.col("Shale_play").alias("play_name"),
            F.col("Area_sq_km").cast("double").alias("area_sq_km"),
            F.expr("st_setsrid(st_geomfromwkb(geom_0), 4326)").alias("play_geom"),
        )
    )


@dp.materialized_view(
    name="ref_counties",
    comment="US Census TIGER counties (TX + NM) clipped to the AOI",
)
def ref_counties():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    register_gbx(spark)
    p = paths(spark)
    src = f"{p['context']}/counties/cb_2024_us_county_500k.zip"
    return (
        spark.read.format("shapefile_gbx").load(src)
        .filter(F.col("STATEFP").isin("48", "35"))
        .select(
            F.col("NAME").alias("county_name"),
            F.col("STATEFP").alias("state_fp"),
            F.col("GEOID").alias("geoid"),
            F.expr("st_setsrid(st_geomfromwkb(geom_0), 4326)").alias("county_geom"),
        )
    )
```

- [ ] **Step 2: Local syntax check** (no local Spark for DLP):

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -c "import ast; ast.parse(open('transformations/context_reference.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/context_reference.py
git commit -m "feat(vapor-eyes): ref_shale_plays + ref_counties reference tables"
```

- [ ] **Step 4: Cluster verification (orchestrator-driven, after commit)** — the orchestrator lands the context source and runs the pipeline, then confirms:
  - `ref_shale_plays` has 7 rows, all `ST_SRID(play_geom)=4326`, `play_name` ∈ {Delaware, Bone Spring, Wolfcamp, Wolfcamp - Midland, Spraberry, Abo-Yeso, Glorieta-Yeso}.
  - `ref_counties` non-empty (TX+NM AOI counties), all `ST_SRID(county_geom)=4326`.
  - If `geom_0` is not the emitted column name at runtime, adjust the `select` to the actual reader geometry column (the reader default is `geom_0`; confirm from the failure message and fix in this file, not downstream).

---

### Task 4: Gold rollups `emissions_by_play` + `detections_by_county`

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py` (append two MVs)

**Interfaces:**
- Consumes: `cm_plume_attributed` (lon, lat, emission_rate_kg_hr, lead_operator, plume_id), `ref_shale_plays`, `ref_counties`.
- Produces MVs (map-ready, geometry SRID 4326):
  - `emissions_by_play(play_name, plume_count LONG, mean_emission_kg_hr DOUBLE, max_emission_kg_hr DOUBLE, active_operators LONG, play_geom GEOMETRY)`
  - `detections_by_county(county_name, state_fp, geoid, plume_count LONG, mean_emission_kg_hr DOUBLE, max_emission_kg_hr DOUBLE, county_geom GEOMETRY)`

- [ ] **Step 1: Append the two MVs** to `gold_analytics.py`:

```python
@dp.materialized_view(
    name="emissions_by_play",
    comment="Carbon Mapper plume detections rolled up to EIA shale plays (map-ready)",
)
def emissions_by_play():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    plumes = spark.read.table("cm_plume_attributed").select(
        "plume_id", "emission_rate_kg_hr", "lead_operator",
        F.expr("st_setsrid(st_point(lon, lat), 4326)").alias("pt"),
    )
    plays = spark.read.table("ref_shale_plays")
    joined = plays.join(
        plumes, F.expr("st_contains(play_geom, pt)"), "left"
    )
    return joined.groupBy("play_name", "play_geom").agg(
        F.count("plume_id").alias("plume_count"),
        F.avg("emission_rate_kg_hr").alias("mean_emission_kg_hr"),
        F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
        F.countDistinct("lead_operator").alias("active_operators"),
    )


@dp.materialized_view(
    name="detections_by_county",
    comment="Carbon Mapper plume detections rolled up to TX/NM counties (map-ready)",
)
def detections_by_county():
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    plumes = spark.read.table("cm_plume_attributed").select(
        "plume_id", "emission_rate_kg_hr",
        F.expr("st_setsrid(st_point(lon, lat), 4326)").alias("pt"),
    )
    counties = spark.read.table("ref_counties")
    joined = counties.join(
        plumes, F.expr("st_contains(county_geom, pt)"), "left"
    )
    return joined.groupBy("county_name", "state_fp", "geoid", "county_geom").agg(
        F.count("plume_id").alias("plume_count"),
        F.avg("emission_rate_kg_hr").alias("mean_emission_kg_hr"),
        F.max("emission_rate_kg_hr").alias("max_emission_kg_hr"),
    )
```

- [ ] **Step 2: Local syntax check**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -c "import ast; ast.parse(open('transformations/gold_analytics.py').read()); print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/transformations/gold_analytics.py
git commit -m "feat(vapor-eyes): emissions_by_play + detections_by_county rollups"
```

- [ ] **Step 4: Cluster verification (orchestrator-driven)** — run pipeline; confirm both MVs non-empty, `plume_count` sums are plausible vs the 3724 attributed plumes (points outside all plays/counties simply don't join), geometry SRID 4326, mean/max populated. Grouping by the geometry column is required so each row carries its polygon for the choropleth (grouping a small reference set by geometry is safe).

---

### Task 5: "Regional Context" dashboard page

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/dashboards/vapor_eyes_lf.lvdash.json`

**Interfaces:**
- Consumes: `emissions_by_play`, `detections_by_county`.
- Produces: a fourth page `page_regional_context` with two choropleth widgets + their datasets, following the proven render contract.

- [ ] **Step 1: Add two datasets** to the `datasets` array — `ds_emissions_by_play` (`SELECT * FROM emissions_by_play`) and `ds_detections_by_county` (`SELECT * FROM detections_by_county`), mirroring the existing dataset entries' shape.

- [ ] **Step 2: Add the page + two choropleth widgets.** Each choropleth widget MUST use the render contract verbatim (memory `aibi-custom-geometry-choropleth`):
  - `encodings.region` = `{"regionType": "custom", "fieldName": "geo(play_geom)"}` (county widget: `"geo(county_geom)"`).
  - The widget query `fields` MUST include `{"name": "geo(play_geom)", "expression": "ST_ASGEOJSON(`play_geom`)"}` (county: `geo(county_geom)` / `ST_ASGEOJSON(`county_geom`)`), plus the metric/tooltip fields (`plume_count`, `mean_emission_kg_hr`, `max_emission_kg_hr`, `play_name`/`county_name`, `active_operators`).
  - `encodings.color` = `plume_count` (both, primary metric).
  - Page title "Regional Context"; description noting the play/county rollups; a Carbon Mapper attribution text widget (copy the attribution string used on the other CM pages).
  - Copy an existing working choropleth widget from `page_regional_screen` as the structural template and swap dataset/field names — do NOT hand-write the widget JSON from scratch.

- [ ] **Step 3: Validate the JSON**

Run: `cd notebooks/examples/vapor-eyes/lakeflow && python -c "import json; json.load(open('dashboards/vapor_eyes_lf.lvdash.json')); print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/dashboards/vapor_eyes_lf.lvdash.json
git commit -m "feat(vapor-eyes): Regional Context dashboard page (play + county choropleths)"
```

- [ ] **Step 5: Cluster+browser verification (orchestrator-driven)** — `databricks bundle deploy --force -p oauth-fe`, publish, then chrome-devtools screenshot to confirm BOTH choropleths paint polygons over the Permian (not a blank world map). If blank, re-check the geo()/ST_ASGEOJSON field pairing before anything else.

---

### Task 6: Docs + dependency/tier verification

**Files:**
- Modify: `notebooks/examples/vapor-eyes/lakeflow/README.md`
- Modify: `docs/docs/notebooks/vapor-eyes-lakeflow.mdx`
- Capture: `resources/images/diagrams/vapor-eyes/lakeflow-dashboard-regional-context.png`

**Interfaces:** none (docs only).

- [ ] **Step 1: Verify pyogrio is pinned** in the light extra and does not need a new pin. Confirm the light vector reader dep (`pyogrio`) is already present in the geobrix `[light]`/`[stac,vizx]` resolution used by the pipeline env (memory `pyogrio-for-pyvx-vector`, `light-ci-lock-completeness`). Record the finding; add a pin only if genuinely missing.

Run: `grep -rn "pyogrio" python/geobrix/pyproject.toml python/geobrix/requirements*/ 2>/dev/null`
Expected: pyogrio appears in the light dependency set. If absent, STOP and escalate (do not silently add).

- [ ] **Step 2: Add a "Regional Context" section** to `README.md` (after the Regional Screen section) describing the two rollup choropleths and the two sources (EIA plays, TIGER counties) with the `![...](../../../resources/images/diagrams/vapor-eyes/lakeflow-dashboard-regional-context.png)` image reference. Note the data sources + licenses.

- [ ] **Step 3: Mirror the section** in `docs/docs/notebooks/vapor-eyes-lakeflow.mdx`, keeping user-facing voice (no internal vocabulary; `grep -rn -iE "wave [0-9]+" docs/docs/` stays empty).

- [ ] **Step 4: Capture the screenshot (orchestrator-driven)** from the published Regional Context page (same method as the other three: hide nav, hide Ask Genie, expand scroll container, fullPage).

- [ ] **Step 5: Commit**

```bash
git add notebooks/examples/vapor-eyes/lakeflow/README.md docs/docs/notebooks/vapor-eyes-lakeflow.mdx resources/images/diagrams/vapor-eyes/lakeflow-dashboard-regional-context.png
git commit -m "docs(vapor-eyes): document Regional Context page + sources"
```

---

## Final review + wrap

- [ ] Whole-branch review of the context-geometry commits (spec compliance + quality).
- [ ] Confirm nothing pushed; summarize for the user and await go before push/PR (squash the redundant S2 commit `47785515` at that point, run `gbx:lint`, PR via `mjohns-databricks`).
