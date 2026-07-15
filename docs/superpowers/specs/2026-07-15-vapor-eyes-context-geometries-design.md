# Vapor-Eyes Context Geometries — Design

**Status:** proposed (awaiting user review)
**Extends:** [2026-07-14-vapor-eyes-lakeflow-sdp-aibi-design.md](2026-07-14-vapor-eyes-lakeflow-sdp-aibi-design.md)
**Date:** 2026-07-15

## Goal

Add **Permian context geometries** to the Vapor-Eyes Lakeflow SDP + AI/BI dashboard so
detections and emissions can be seen against the basin's real geography and rolled up
by region. **Two** public-good sources (verified links/licenses in
`prompts/features/2026-07-15-permian-context-geometries.md`):

1. **EIA tight-oil / shale plays** (public use) — the 7 named Permian play polygons
   (Delaware, Bone Spring, Wolfcamp, Wolfcamp - Midland, Spraberry, Abo-Yeso,
   Glorieta-Yeso). These authoritative named plays ARE the recognizable Permian
   sub-geography (Delaware play ≈ Delaware sub-basin, Spraberry / Wolfcamp - Midland ≈
   Midland sub-basin), so they serve as the primary geography choropleth.
2. **US Census TIGER counties** (2024, 1:500k, public domain) — TX + NM, AOI-clipped.

> **USGS dropped (decided 2026-07-15 after live inspection):** there is no clean, free,
> authoritative Delaware / Midland / Central Basin Platform *structural* sub-basin
> polygon. DOI `10.5066/P13P5ZGT` resolves to a *Woodford/Barnett* AU release (wrong
> subject); the per-sub-basin USGS releases are play-level AUs and omit the Central Basin
> Platform. Rather than a geologically debatable dissolve of plays into sub-basins, the
> named EIA plays carry the geography authoritatively. No sub-basin dissolve is built.

## Constraint that shapes the design

AI/BI map widgets render **one geometry dataset per widget** — confirmed against the
current Databricks maps docs. There is **no layer stacking**: you cannot draw the basin
outline beneath the hexagon choropleth or the plume points in the same map. Therefore
context geometries are surfaced as **rollup-dimension choropleths** — each geometry set
becomes its own map, colored by an aggregated metric. This both renders the geography
and adds new analytics, instead of a decorative outline.

Per user decision, the **primary** geography expression is the **EIA play choropleth**
(named Permian play polygons colored by mean emission / detections), with a county rollup
alongside.

## Sources → ingestion

New `context` source in `land/land.py` (dispatched from `run_land`, added to the job's
`--sources`). Unlike the observation sources these are **static reference geometries** —
downloaded once per run to a new Volume subtree `context/`, not time-windowed and not
bi-temporal. `_subtree`/`paths` gain a `context` dir.

- **EIA plays**: GeoJSON from the agency-owned ArcGIS item (verified working):
  `https://hub.arcgis.com/api/download/v1/items/3f001fba00dc4add8dbd00542d61e4da/geojson?redirect=true&layers=0`
  Lower-48; filter to `Basin='Permian'` (7 features) at read time. Write to
  `context/plays/plays.geojson`. Attributes: `Shale_play`, `Basin`, `Lithology`,
  `Age_shale`, `Area_sq_km`.
- **TIGER counties**: direct download `https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip`;
  unzip into `context/counties/`. Filter to TX=`48` / NM=`35` and AOI-intersect at read
  time. Attributes: `STATEFP`, `GEOID` (FIPS), `NAME`.

Both are small (single-digit MB). Downloads are guarded like EMIT/CM — a failure logs and
skips (context is additive; the core demo is unaffected).

## Bronze → reference tables

Two small **reference materialized views** (re-materialized each run; not streaming —
they are static reference, read once). Read each file with the **GeoBrix light vector
reader** (pyogrio-backed `pyvx`/`ds` reader — on-brand for the light-tier story) and emit
native `GEOMETRY` at SRID 4326:

- `ref_shale_plays` — the 7 Permian play polygons + `play_name` (`Shale_play`),
  `area_sq_km`.
- `ref_counties` — county polygons, `county_name` (`NAME`), `state_fp` (`STATEFP`),
  `geoid` (FIPS); filtered to TX+NM and AOI-intersected.

Geometry column convention: a WKB/native `GEOMETRY` column tagged SRID 4326, plus the
`geo(<col>)` / `ST_ASGEOJSON(<col>)` choropleth query-field contract already proven for
the hex maps (see memory `aibi-custom-geometry-choropleth`).

## Gold — rollups (point-in-polygon)

Join the per-plume point layer (`cm_plume_attributed`, 3724 rows: lon/lat + native
`plume_geom` + `emission_rate_kg_hr` + `operator`) to each polygon set via native
`st_contains(polygon, plume_point)` (equivalently `st_intersects`). Two new gold MVs:

- `emissions_by_play` — per play: plume_count, mean/max emission (kg/hr), active
  operators; plus the play `GEOMETRY`.
- `detections_by_county` — per county: plume_count, mean/max emission; plus county
  `GEOMETRY` and FIPS (joins cleanly to the RRC wells' county field).

Each carries the metric columns AND the geometry so the choropleth is a single-dataset
map. Ranking metric = detection count and **mean** kg/hr (defensible; matches the
leaderboard's "don't sum emission rate" rule, memory `operator_emissions_leaderboard`).

## Dashboard — new "Regional Context" page

A fourth page `page_regional_context` with two choropleths:

1. **Leakiest Plays** — `emissions_by_play`, colored by plume_count (primary geography
   view, per user's decision), tooltip mean/max kg/hr + active operators.
2. **Leakiest Counties** — `detections_by_county`, colored by plume_count.

Carbon Mapper attribution note on the page (both derive from CM plumes). Uses the same
`geo(<col>)` region-field + `ST_ASGEOJSON` query-field contract as the working hex
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

- USGS sub-basin / structural-boundary polygons (no clean free authoritative source).
- TX RRC district polygons (portal-only, unverified download — research-flagged).
- NM OCD lease/field polygons.
- True multi-layer map overlays (not supported by AI/BI).
