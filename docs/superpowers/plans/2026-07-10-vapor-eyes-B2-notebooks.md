# vapor-eyes Plan B2 — config_nb + notebook series + README/diagrams

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Notebook tasks are **author + run against real staged data**, not pre-written cell-by-cell; each ends with a rendered payoff from real data (the verification).

**Goal:** Build the five-notebook vapor-eyes methane series (`config_nb` + NB01–05) plus its `README.md` and five per-notebook diagrams, consuming the Plan B1 downloaders + the `netcdf_gbx` reader, on the lightweight/Serverless tier.

**Architecture:** Mirrors `eo-series`/`helios`: `%run ./config_nb` from each NB establishes catalog/schema + the Volume ETL tree + toggles + downloaders + `finalize_delta`. Each NB adds one source, writes managed Delta tables (path-bearing metadata tables → Volume assets) per the Spec B §6 inventory, and ends in a payoff visualization. The additive cascade: S5P hotspots → S2 MBMP plume → EMIT IME+rate → nearest-well attribution → portfolio PMTiles synthesis. Design ref: `docs/superpowers/specs/2026-07-10-vapor-eyes-series-design.md`.

**Tech Stack:** GeoBrix light tier (`pyrx` `rx.rst_*`, `pyvx`, `gbx.ds` readers, `gbx.vizx`), the B1 downloaders (`TropomiDownloader`/`EmitDownloader`/`WellsDownloader`) + `StacClient`, Databricks-native ST/H3, Delta, MapLibre (`plot_interactive`). Serverless (env v5, Python 3.12).

## Global Constraints

- **Location:** `notebooks/examples/vapor-eyes/` — `config_nb.ipynb`, `01. S5P Screening.ipynb`, `02. Sentinel-2 Detection.ipynb`, `03. EMIT Quantification.ipynb`, `04. Facility Attribution.ipynb`, `05. Portfolio Synthesis.ipynb`.
- **UC layout (Spec B §6):** `catalog_name="geospatial_docs"`, `schema_name="vapor_eyes"`; Volume `data`; `VAPOR_EYES_DIR=/Volumes/geospatial_docs/vapor_eyes/data/vapor-eyes` with `s5p/ sentinel2/ emit/ wells/ tiles/`. Managed Delta tables (unqualified) per the §6 inventory; metadata tables carry `*_path` columns → Volume assets.
- **Toggles in `config_nb`:** `FULL_AOI` (False default → SMALL bbox `[-103.90,31.65,-103.40,32.15]`; True → FULL `[-104.4,31.3,-103.0,32.7]`) drives bbox + datetime across downloaders; `FORCE_REBUILD`; `INTERACTIVE_PLOTS`.
- **EMIT credential:** `config_nb` reads an Earthdata token from a Databricks secret into `EARTHDATA_TOKEN` (e.g. `os.environ["EARTHDATA_TOKEN"] = dbutils.secrets.get(scope, key)`), documented; discovery stays anonymous.
- **Serverless discipline:** no runtime `spark.conf.set` (use `set_conf_safe` + `repartition(N, col)`); no `.cache()` (write managed tables via `finalize_delta`); ~1 GB per-UDF cap (one item/tile per task); sequential Volume I/O.
- **Notebook hygiene:** `.ipynb` cell last source line must not end in `\n`; every NB code edit updates its section's markdown narrative in the same stroke; no internal vocabulary (wave numbers etc.) in any markdown.
- **Commit hygiene:** subject ≤72 chars + WHY body; end with `Co-authored-by: Isaac`. Branch: `examples/vapor-eyes`.

---

### Task 0: Real SMALL-AOI data pull (grounding — do FIRST)

**Deliverable:** real staged data under `VAPOR_EYES_DIR` on a dev workspace so every subsequent NB is authored against actual data shapes. No repo files.

- [ ] Provision `EARTHDATA_TOKEN` (user supplies the NASA Earthdata token).
- [ ] On a Serverless notebook (or the dev cluster), for the SMALL bbox: `TropomiDownloader().download(...)` (S5P `.nc`), `StacClient` Sentinel-2 B11/B12 (COG), `EmitDownloader().download(...)` (ENH COG + PLM GeoJSON), `WellsDownloader().download(...)` (wells GeoJSON).
- [ ] Eyeball each: S5P `netcdf_gbx` vector output columns (`methane_mixing_ratio_bias_corrected`, `qa_value`, geom); S2 SWIR value ranges; EMIT ENH raster + PLM GeoJSON properties (emission-rate field name, plume-origin coords); wells GeoJSON fields (`API`, `CompanyName`, …). Record the exact field/column names — they parameterize the NB cells below.

---

### Task 1: `config_nb.ipynb`

**Files:** Create `notebooks/examples/vapor-eyes/config_nb.ipynb`.
**Interfaces:** Produces the shared state every NB uses: `spark`, `rx`, registered readers, `catalog_name`/`schema_name`, `VAPOR_EYES_DIR` (+ subdir consts), `SMALL_BBOX`/`FULL_BBOX`/`AOI_BBOX`, `DATE_WINDOW`, `FULL_AOI`/`FORCE_REBUILD`/`INTERACTIVE_PLOTS`, `finalize_delta`, `set_conf_safe`, and instantiated `tropomi`/`emit`/`wells`/`stac_client`.

- [ ] **Cells (mirror `helios/config_nb`):**
  1. 2-step wheel install (`--force-reinstall --no-deps` then `[light,stac,vizx]`) from the sample-data Volume wheel; `%restart_python`.
  2. Imports (`os`, delta, `DBF`, `F`, types); `from databricks.labs.gbx.pyrx import functions as rx` (option-1 default; option-2 heavyweight commented); `rx.register(spark)`; `from databricks.labs.gbx.ds.register import register; register(spark)`.
  3. USER SETTINGS: `catalog_name="geospatial_docs"`, `schema_name="vapor_eyes"`; toggles `FULL_AOI=False`, `FORCE_REBUILD=False`, `INTERACTIVE_PLOTS=False`; `SMALL_BBOX`/`FULL_BBOX`; `AOI_BBOX = FULL_BBOX if FULL_AOI else SMALL_BBOX`; `DATE_WINDOW` (anchored near an EMIT overpass — pick from Task 0, e.g. `"2024-08-01/2024-09-30"`).
  4. Earthdata secret → `os.environ["EARTHDATA_TOKEN"]` (guarded: only if the scope/key exist; print a clear note if absent so NB03 fails with guidance not a stack trace).
  5. `set_conf_safe(...)` (as helios).
  6. `USE CATALOG`/`CREATE DATABASE IF NOT EXISTS`/`USE DATABASE`.
  7. `VAPOR_EYES_DIR` + subdir consts; `dbutils.fs.mkdirs` each; `os.environ` exports.
  8. `finalize_delta(df, tbl, do_display=True)` (copy verbatim from `helios/config_nb`).
  9. Downloader instantiation: `tropomi = TropomiDownloader()`, `emit = EmitDownloader()`, `wells = WellsDownloader()`, `stac_client = StacClient()`; import vizx helpers (`plot_raster`, `plot_pmtiles`, `plot_interactive`, `pmtiles_layer`).
- [ ] **Verify:** `%run ./config_nb` in a scratch notebook prints the catalog/schema/dir banner and the AOI bbox; `spark.catalog.currentDatabase() == "vapor_eyes"`.
- [ ] **Commit** (`feat(vapor-eyes): config_nb (catalog/schema/Volume/toggles/downloaders)`).

---

### Task 2: `01. S5P Screening.ipynb`

**Files:** Create the notebook. **Tables:** `s5p_granules` (item_id, date, **ch4_path**, is_out_file_valid), `s5p_hotspots` (h3_cellid, ch4_mean, ch4_max, n_obs, geom_wkb).

- [ ] **Cells:** `%run ./config_nb`; `tropomi.download(AOI_BBOX, S5P_DIR, ...)` → `finalize_delta(..., "s5p_granules")`; `tropomi.read(S5P_DIR)` → `netcdf_gbx` vector points (`methane_mixing_ratio_bias_corrected`, `qa_value`, geom); **quality filter** on `qa_value` (threshold from Task 0, document it); `h3_longlatash3` bin points → group → mean/max CH4 per H3 cell → `finalize_delta(..., "s5p_hotspots")`; flag top-N hotspot cells (the AOIs NB02 consumes).
- [ ] **Payoff:** H3 choropleth of CH4 enhancement over the AOI (matplotlib/`plot_interactive`), super-emitter cells highlighted. Assert the table is non-empty + a hotspot cell exists.
- [ ] **Narrative markdown** per section; **commit** (`feat(vapor-eyes): NB01 S5P screening -> H3 hotspots`).

---

### Task 3: `02. Sentinel-2 Detection.ipynb`

**Files:** Create. **Tables:** `s2_swir_assets` (item_id, date, **b11_path**, **b12_path**), `s2_plume_cells` (h3_cellid, mbmp_frac, geom_wkb).

- [ ] **Cells:** `%run ./config_nb`; scope to a hotspot cell's bbox from NB01 (`spark.table("s5p_hotspots")`); `StacClient` search+download `sentinel-2-l2a` B11/B12 for that sub-AOI (low cloud) → `finalize_delta(..., "s2_swir_assets")`; `gtiff_gbx` read → **MBMP band ratio** via `rx.rst_*` map-algebra (B12/B11 fractional absorption; document the illustrative nature — R3); `rst_h3_tessellate` the fraction raster → `s2_plume_cells`.
- [ ] **Payoff:** the MBMP plume-fraction raster over the hotspot (`plot_raster`), a candidate plume visible. Assert a plume-fraction cell exceeds a documented threshold.
- [ ] Narrative; **commit** (`feat(vapor-eyes): NB02 Sentinel-2 SWIR MBMP detection`).

---

### Task 4: `03. EMIT Quantification.ipynb`

**Files:** Create. **Tables:** `emit_scenes` (plume_id, date, **enh_cog_path**, **plm_geojson_path**, href, is_out_file_valid), `plume_quant` (plume_id, ime, emission_rate, rate_uncertainty, outline_wkb, origin_lon, origin_lat).

- [ ] **Cells:** `%run ./config_nb`; **EMIT token guard** (clear message if `EARTHDATA_TOKEN` absent); `emit.download(AOI_BBOX, EMIT_DIR, temporal=DATE_WINDOW)` → `finalize_delta(..., "emit_scenes")` (persist `href` for `repair`); `emit.read_enh(EMIT_DIR)` (ENH COG tiles) + `emit.read_plumes(EMIT_DIR)` (PLM GeoJSON); compute **IME** = zonal sum of enhancement × pixel-area within each PLM plume polygon via `rx.rst_clip` + `rst_summary` (GeoBrix raster); read the **emission rate** + origin from the PLM GeoJSON properties (field names from Task 0) → `plume_quant`.
- [ ] **Payoff:** high-res EMIT plume raster + its quantified rate label (`plot_raster` + the `plume_quant` row). Assert a plume with a positive emission_rate + IME.
- [ ] Narrative; **commit** (`feat(vapor-eyes): NB03 EMIT COG IME + PLM emission rate`).

---

### Task 5: `04. Facility Attribution.ipynb`

**Files:** Create. **Tables:** `wells` (api, operator, lease, county, geom_wkb), `plume_attribution` (plume_id, origin_lon, origin_lat, nearest_well_api, operator, dist_m).

- [ ] **Cells:** `%run ./config_nb`; `wells.download(AOI_BBOX, WELLS_DIR)` → `wells.read(WELLS_DIR)` → `finalize_delta(..., "wells")` (geom + `CompanyName`/`API`/…); join `plume_quant` origin points to `wells` via **native ST nearest** (`st_distancesphere`/`st_distance` + window `row_number` over ascending distance = 1) → `plume_attribution`.
- [ ] **Payoff:** attribution map — plume origin(s), the nearest well highlighted, operator labeled (`plot_interactive`). Assert every plume maps to exactly one well with a finite distance.
- [ ] Narrative; **commit** (`feat(vapor-eyes): NB04 nearest-well facility attribution`).

---

### Task 6: `05. Portfolio Synthesis.ipynb`

**Files:** Create. **Volume:** `tiles/*.pmtiles`, `tiles/mosaic.json`. **Tables:** `super_emitters` (operator, total_rate, plume_count, well_count), `pmtiles_catalog` (layer, **pmtiles_path**, bounds).

- [ ] **Cells:** `%run ./config_nb`; rank operators by summed `emission_rate` (`plume_attribution` ⋈ `plume_quant`) → `super_emitters`; build the **multi-layer PMTiles map**: encode hotspots/plumes/wells/attribution to MVT (`gbx_st_asmvt` + `gbx_st_asmvt_pyramid`) → `gbx_pmtiles_agg` per layer → `pmtiles_catalog` + a `mosaic.json`; render with `plot_pmtiles`/`plot_interactive([...])`.
- [ ] **Payoff:** the super-emitter leaderboard table + the unified interactive map; a short ESG-style summary cell.
- [ ] Narrative; **commit** (`feat(vapor-eyes): NB05 portfolio synthesis + PMTiles map`).

---

### Task 7: `README.md`

**Files:** Create `notebooks/examples/vapor-eyes/README.md`, mirroring `eo-series`/`helios`.

- [ ] Sections: intro + narrative; lightweight-tier/Serverless note; data-source note (S5P/S2 anon PC, EMIT via earthaccess + Earthdata token, TX RRC wells); **"Notebooks at a glance"** with an embedded diagram + 2-3 bullets per NB; a Files table; Prerequisites (incl. the Earthdata secret + Volume `data`); Run order; a Data-flow ASCII block; Serverless execution-strategy; Gotchas (EMIT opportunistic coverage, MBMP illustrative, TX-only wells, EMIT token); "Key GeoBrix / Databricks functions shown".
- [ ] **Commit** (`docs(vapor-eyes): series README`).

---

### Task 8: Per-notebook diagrams (5)

**Files:** `resources/images/diagrams/vapor-eyes/vapor-eyes-0{1..5}.png` (+ `.svg`); Modify `resources/images/generators/example-diagrams.py`.

- [ ] Extend `example-diagrams.py` with a `vapor-eyes` series (5 diagrams in the established visual style: source → GeoBrix path → table/asset → payoff, one per NB). Generate PNG+SVG.
- [ ] Embed each in the README ("Notebooks at a glance").
- [ ] Run `docs/scripts/check-diagram-coverage.py` → passes for `vapor-eyes`.
- [ ] **Commit** (`docs(vapor-eyes): per-notebook diagrams`).

---

### Task 9: Validation

- [ ] Run the series end-to-end on Serverless (SMALL default) via `gbx:test:notebooks` where applicable + a manual pass; each NB's payoff cell renders from real staged data.
- [ ] `FULL_AOI=True` smoke (larger counts resolve; no code path breaks).
- [ ] Diagram-coverage check + README link audit ([[docs-link-audit-pending]]).
- [ ] Lint any helper `.py`; ensure no internal vocabulary in markdown (`grep -rn -iE "wave [0-9]+" docs/ notebooks/`).

---

## Notes for the implementer

- **Author against Task 0 data.** The exact S5P `qa_value` threshold, the PLM GeoJSON emission-rate + origin field names, the S2 MBMP threshold, and the wells operator field are read from the real pull, not guessed — fill them into NB01/02/03/04 cells from Task 0.
- **`fsspec`/`s3fs` on Serverless (from B1):** the `[stac]` install now pulls `fsspec`/`s3fs==2026.6.0`; on env v5 verify no "core package changed" hard-fail — pin to the base if needed (same discipline as the `[light]` idna/rio-tiler pins). Check during Task 0's first Serverless install.
- **MBMP is illustrative** (R3) — frame NB02 as a demonstrative SWIR proxy, not an operational retrieval.
- **EMIT is opportunistic** (R2) — anchor `DATE_WINDOW` near a real EMIT overpass over the SMALL cluster (2023-06-08 / 2023-10-08 / 2023-12-24 / 2024-08-23 from coverage verification).
- **Netcdf raster mode** is intentionally not used here (Spec B §2/R5) — S5P uses vector mode; raster mode is documented/tested in Spec A.
- Notebooks are built + validated against real data, so tasks are "author + run", not pre-written cells — the payoff render is the per-task verification.
