# Vapor-Eyes Context Geometries — Design

**Status:** proposed (awaiting user review)
**Extends:** [2026-07-14-vapor-eyes-lakeflow-sdp-aibi-design.md](2026-07-14-vapor-eyes-lakeflow-sdp-aibi-design.md)
**Date:** 2026-07-15

## Goal

Add **Permian context geometries** to the Vapor-Eyes Lakeflow SDP + AI/BI dashboard so
detections and emissions can be seen against the basin's real geography and rolled up
by region. Three public-good sources (all verified links/licenses in
`prompts/features/2026-07-15-permian-context-geometries.md`):

1. **USGS Permian Basin Province** boundary (CC0) — basin / sub-basin outline.
2. **US Census TIGER counties** (2024, 1:500k, public domain) — TX + NM, AOI-clipped.
3. **EIA tight-oil / shale plays** (public use) — Wolfcamp / Bone Spring / Spraberry /
   Delaware play polygons.

## Constraint that shapes the design

AI/BI map widgets render **one geometry dataset per widget** — confirmed against the
current Databricks maps docs. There is **no layer stacking**: you cannot draw the basin
outline beneath the hexagon choropleth or the plume points in the same map. Therefore
context geometries are surfaced as **rollup-dimension choropleths** — each geometry set
becomes its own map, colored by an aggregated metric. This both renders the geography
and adds new analytics, instead of a decorative outline.

Per user decision, the **primary** basin expression is a **sub-basin metric choropleth**
(sub-basin polygons colored by detections / mean emission), with county and play rollups
alongside.

## Sources → ingestion

New `context` source in `land/land.py` (dispatched from `run_land`, added to the job's
`--sources`). Unlike the observation sources these are **static reference geometries** —
downloaded once per run to a new Volume subtree `context/`, not time-windowed and not
bi-temporal. `_subtree`/`paths` gain a `context` dir.

- **USGS basin**: direct download of the ScienceBase data release zip (DOI
  `10.5066/P13P5ZGT`); unzip the shapefile set into `context/basin/`.
- **TIGER counties**: direct download `cb_2024_us_county_500k.zip`; unzip into
  `context/counties/`. (Filtered to TX=48 / NM=35 at read time.)
- **EIA plays**: query the ArcGIS FeatureServer as GeoJSON (bbox-filtered to the AOI) and
  write `context/plays/plays.geojson`.

All three are small (single-digit MB). Downloads are guarded like EMIT/CM — a failure
logs and skips (context is additive; the core demo is unaffected).

> **Implementation risk (resolve in Task 1, not now):** the USGS *Permian Basin Province*
> release may expose *assessment units*, not the structural sub-basins
> (Delaware / Midland / Central Basin Platform). Task 1 downloads and **inspects the
> actual attribute schema first**. Fallbacks, in order: (a) use a sub-basin field if
> present; (b) derive sub-basin membership by grouping the EIA plays (Bone Spring /
> Wolfcamp-Delaware → Delaware; Spraberry / Wolfcamp-Midland → Midland); (c) if neither
> yields clean sub-basins, fall back to the single basin-province polygon + assessment
> units as the "basin" layer and note it. The choropleth design is unchanged either way —
> only the polygon set differs.

## Bronze → reference tables

Three small **reference materialized views** (re-materialized each run; not streaming —
they are static reference, read once). Read each file with the **GeoBrix light vector
reader** (pyogrio-backed `pyvx`/`ds` reader — on-brand for the light-tier story) and emit
native `GEOMETRY` at SRID 4326:

- `ref_basin_regions` — sub-basin (or fallback) polygons + `region_name`.
- `ref_counties` — county polygons, `county_name`, `state_fp`, `geoid` (FIPS); filtered
  to TX+NM and clipped/intersected to the AOI bbox.
- `ref_shale_plays` — play polygons + `play_name`.

Geometry column convention: a WKB/native `GEOMETRY` column tagged SRID 4326, plus the
`geo(<col>)` / `ST_ASGEOJSON(<col>)` choropleth query-field contract already proven for
the hex maps (see memory `aibi-custom-geometry-choropleth`).

## Gold — rollups (point-in-polygon)

Join the per-plume point layer (`cm_plume_attributed`, 3724 rows: lon/lat + native
`plume_geom` + `emission_rate_kg_hr` + `operator`) to each polygon set via native
`st_contains(polygon, plume_point)` (equivalently `st_intersects`). Three new gold MVs:

- `emissions_by_subbasin` — per sub-basin: plume_count, mean/max emission (kg/hr),
  active operators; plus the sub-basin `GEOMETRY`.
- `detections_by_county` — per county: plume_count, mean/max emission; plus county
  `GEOMETRY` and FIPS (joins cleanly to the RRC wells' county field).
- `emissions_by_play` — per play: plume_count, total/mean emission; plus play `GEOMETRY`.

Each carries the metric columns AND the geometry so the choropleth is a single-dataset
map. Ranking metric = detection count and **mean** kg/hr (defensible; matches the
leaderboard's "don't sum emission rate" rule, memory `operator_emissions_leaderboard`).

## Dashboard — new "Regional Context" page

A fourth page `page_regional_context` with three choropleths:

1. **Leakiest Sub-Basins** — `emissions_by_subbasin`, colored by plume_count (primary,
   per user's basin-priority choice), tooltip mean/max kg/hr + operators.
2. **Leakiest Counties** — `detections_by_county`, colored by plume_count.
3. **Leakiest Plays** — `emissions_by_play`, colored by mean emission (kg/hr).

Carbon Mapper attribution note on the page (all three derive from CM plumes). Uses the
same `geo(<col>)` region-field + `ST_ASGEOJSON` query-field contract as the working hex
choropleths.

## Cross-cutting (per standing checklists)

- **Deps / tiers** (memories `new-feature-dep-and-tier-checklist`, `light-ci-lock-completeness`,
  `pyogrio-for-pyvx-vector`): the light vector reader path uses `pyogrio` — confirm it is
  already pinned in the light extra + all resolving envs; if the download needs `requests`
  it is already used by CM. No new pins expected, but verify before finishing.
- **Config**: add `context` toggle + AOI county/state filters to `_config.cfg` and the
  pipeline `configuration` block; add `context` to the job `--sources`.
- **Tests**: unit-test the land download helper (URL construction / guarded skip) under
  `tests/`; the reference-table + rollup transforms are doc/integration-verified against
  live rows (same pattern as the existing silver/gold — verify non-empty counts + SRID
  4326 post-build).
- **Docs**: extend `README.md` + `docs/docs/notebooks/vapor-eyes-lakeflow.mdx` with the
  new page + a screenshot; keep user-facing voice (no internal vocab).
- **No push** until user go (memory `hold-pushes-batch-more`); PR via `mjohns-databricks`.

## Out of scope

- TX RRC district polygons (portal-only, unverified download — research-flagged).
- NM OCD lease/field polygons.
- True multi-layer map overlays (not supported by AI/BI).
