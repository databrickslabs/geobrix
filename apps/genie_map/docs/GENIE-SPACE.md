# Genie Space — context fed to the space

This documents everything the Genie Map app's Genie Space is given: the tables it
can query, the description/instructions it carries, and the geometry contract that
lets natural-language answers render on the map. Keep this in sync with the space
whenever its context changes.

## Space identity

| Deployment | Workspace | Space id | Warehouse |
|---|---|---|---|
| Reference (e2-demo) | `e2-demo-field-eng` | `01f18180a9a4192ea9669e77da489e62` | `82e587bd93c6cbcf` |
| Full-stack demo | `fevm-serverless-stable-genie-map` | `01f18192f19c14c89ca05c62111a68b4` | `13e2ed4a49e74f6c` |

Title: **Vapor-Eyes — Permian Methane (Genie Map)**.

## Tables fed to the space (the `data_sources`)

The space is scoped to nine gold tables in the vapor-eyes gold schema
(`<catalog>.vapor_eyes_lf.*`, where `<catalog>` is `geospatial_docs` on the
reference deployment and `serverless_stable_genie_map_catalog` on the full-stack
demo env). The serialized space uses `{"version":2,"data_sources":{"tables":[...]}}`
with table identifiers **sorted alphabetically** (the API rejects an unsorted list):

1. `detections_by_county` — Carbon Mapper detections rolled up to county (map-ready polygon).
2. `emissions_by_play` — detections rolled up to shale play (map-ready polygon).
3. `hotspot_latest` — latest-overpass S5P CH₄ hotspot H3 cells (map-ready hex geometry).
4. `operator_intensity_latest` — concentration intensity aggregated by leading operator.
5. `plume_candidate_wells` — k-nearest wells per plume (operator attribution).
6. `plume_leaderboard_latest` — per-plume peak concentration + leading candidate operator (map-ready points).
7. `ref_counties` — TX/NM county polygons (`county_name`, `state_fp`, `geoid`, `county_geom`).
8. `ref_shale_plays` — shale play polygons (`play_name`, `play_geom`, `area_sq_km`).
9. `wells_enriched_latest` — current well inventory tagged with play + county/state (map-ready points).

These cover the map-facing layers (hotspots, plumes, enriched wells), the attribution
join (`plume_candidate_wells`), and the reference geometries (counties, plays) so a
question can cross **wells ↔ plumes ↔ basin ↔ county**.

## Description fed to the space

> Natural-language querying over the Permian methane gold tables for the Genie Map
> app. For geometry answers, alias geometry as `*_geojson` via `ST_ASGEOJSON` so the
> map renders them.

## The geometry render contract (why the description says what it says)

The app's Genie path renders a result as a map layer **only when the result carries a
geometry column**. The client recognizes any column whose name contains `geojson`
(plus exact `geometry`/`geom` fallbacks). So the space is steered to return geometry as
an `ST_ASGEOJSON(...)`-aliased `*_geojson` column. This works in practice: a smoke test
question ("well density by county…") had Genie generate a correct wells↔counties join
and, unprompted beyond the description, alias the geometry as `county_geom_geojson` via
`ST_AsGeoJSON` — exactly the column the app detects.

The gold geometry columns are native `GEOMETRY` at SRID 4326, so `ST_ASGEOJSON` on them
yields WGS84 GeoJSON that kepler renders directly.

## Curation status & follow-ups

- **Fed via API:** the table set + the description above (this is what the
  `create-space` call carried).
- **UI-side follow-up (not yet done):** richer per-table/column instructions, explicit
  join hints, and saved example NL→SQL pairs are best authored in the Genie Space editor
  UI. The concentration-led framing (rank by `max_conc_ppmm`, never sum emission rates)
  and the "always alias geometry as `*_geojson`" rule should be added there as standing
  instructions. Any context added in the UI must be reflected back into this document.
