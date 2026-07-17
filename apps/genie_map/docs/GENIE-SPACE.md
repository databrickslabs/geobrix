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

## How the space is created / curated (what's automated vs manual)

- **Automated by the DAB bundle** (`resources.genie_spaces.vapor_eyes_genie` in
  `apps/genie_map/databricks.yml`): the space itself, its warehouse, the **table set**
  (from `genie_space.geniespace.json`), and a short **description** carrying the key
  guardrails. `databricks bundle deploy` creates/updates it — no manual space id.
- **Manual, in the Genie Space UI** (the rich curation below): the Databricks Genie API
  does not reliably accept detailed instructions + saved example SQL in the serialized
  body, so paste the two blocks below into the space editor. This is the one manual step
  in the setup runbook (`docs/SETUP.md`); `gbx:app:setup` prints a reminder.

> Keep this document as the single source of truth. If you change the space's instructions
> or examples in the UI, update the blocks below to match.

---

### Paste block A — General instructions (Genie Space → Instructions tab)

```
This Space answers questions about methane monitoring over the Permian Basin (West
Texas / SE New Mexico). ALL tables are already spatially scoped to the Permian Basin —
there is no "Permian" filter to apply and no "Permian" value anywhere.

IMPORTANT — do NOT filter by a "Permian" play. ref_shale_plays.play_name contains only
sub-basin plays/formations: Delaware, Wolfcamp, Wolfcamp - Midland, Spraberry, Bone
Spring, Abo-Yeso, Glorieta-Yeso. For "Permian Basin" questions, query the whole table
(it is already Permian). Only filter by play_name for a specific named play, and match
those exact names. Counties (ref_counties.county_name, e.g. Loving, Reeves, Eddy, Lea,
Midland, Ector) are the other geographic filter.

Ranking: rank plumes by max_conc_ppmm (peak methane concentration, ppm·m), highest first.
emission_rate_kg_hr is frequently NULL (no wind at overpass) — never rank or filter by it;
report it only as an optional secondary column.

Attribution: to link a plume to its operator/well, join plume_candidate_wells on plume_id
and keep rank = 1 (the nearest well). operator lives on the well tables.

MAP RESULTS — when a question asks to show/map/plot something, return the geometry as a
column aliased with the suffix _geojson using ST_ASGEOJSON(<geometry column>). The app
renders any *_geojson column as a map layer. The map-ready geometry columns (all SRID
4326) are:
- hotspot_latest.hex_geom            (H3 hotspot hexagons; also center_lon/center_lat)
- plume_leaderboard_latest.plume_geom_native  (EMIT plumes; also lon_max/lat_max)
- wells_enriched_latest.well_geom_native      (wells; also longitude/latitude)
- ref_shale_plays.play_geom          (play polygons)
- ref_counties.county_geom           (county polygons)

Table roles:
- hotspot_latest — latest-overpass S5P CH4 hotspot H3 cells (ch4_max, ch4_mean, n_obs).
- plume_leaderboard_latest — one row per EMIT plume, peak concentration + leading operator.
- wells_enriched_latest — current well inventory, tagged with operator, lease, field,
  play_name, county_name/state_fp.
- plume_candidate_wells — nearest-K wells per plume (rank=1 = closest); operator attribution.
- operator_intensity_latest — per-operator plume counts + peak concentration + well_count.
- detections_by_county / emissions_by_play — plume rollups to county / shale play (map-ready).
```

### Paste block B — Example SQL queries (Genie Space → Example SQL queries)

Add each as a named example. All were run against the gold schema and return rows
(counts noted). Replace the catalog/schema prefix if you deployed to a different one.

**Latest EMIT plumes as a map layer** — *"Show the latest methane plumes on the map, strongest first"* (72 rows)
```sql
SELECT plume_id, max_conc_ppmm, lead_operator, lead_county,
       ST_ASGEOJSON(plume_geom_native) AS plume_geojson
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.plume_leaderboard_latest
ORDER BY max_conc_ppmm DESC
```

**CH4 hotspot hexagons as a map layer** — *"Map the CH4 hotspots"* (48 rows)
```sql
SELECT h3_h3tostring(h3_cellid) AS hex, ch4_max, ch4_mean, n_obs,
       ST_ASGEOJSON(hex_geom) AS hex_geojson
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.hotspot_latest
ORDER BY ch4_max DESC
```

**Operators nearest the strongest plumes** — *"Which operators have wells nearest the strongest plumes?"* (72 rows)
```sql
SELECT p.plume_id, p.max_conc_ppmm, w.operator, w.county, w.dist_m
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.plume_candidate_wells w
JOIN serverless_stable_genie_map_catalog.vapor_eyes_lf.plume_leaderboard_latest p
  ON w.plume_id = p.plume_id
WHERE w.rank = 1
ORDER BY p.max_conc_ppmm DESC
```

**Wells in a specific county, as a map layer** — *"Show the wells in Loving County"* (15 rows)
```sql
SELECT api, operator, field, play_name,
       ST_ASGEOJSON(well_geom_native) AS well_geojson
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.wells_enriched_latest
WHERE county_name = 'Loving'
```

**Plumes by shale play** — *"How many plumes are in each shale play?"* (7 rows)
```sql
SELECT play_name, plume_count, max_emission_kg_hr
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.emissions_by_play
ORDER BY plume_count DESC
```
