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
- **Automated by `gbx:app:seed-genie`** (the rich curation below): the instructions
  (block A) and example SQL queries (block B) are written to the space via the Genie
  spaces update API (`serialized_space.instructions.text_instructions` +
  `.example_question_sqls`), parsed straight from this file. **No manual UI pasting.**
  The DAB `genie_space` resource re-applies only the table set on every deploy and wipes
  these, so `gbx:app:deploy` runs the seed automatically at the end (for the
  `genie-map-env` profile); you can also run `gbx:app:seed-genie` any time after editing
  the blocks below.

> Keep this document as the single source of truth: `gbx:app:seed-genie` reads blocks A
> and B directly from here, so editing them here and re-running the command is the whole
> workflow. (Undocumented API constraints the seeder handles: each instruction/example
> needs a 32-hex-lowercase `id`, and `example_question_sqls` must be sorted by id — the
> seeder derives ids from a content hash, so both are automatic.)

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

Attribution / proximity: to link a plume to its operator/well — or to answer any
"near", "nearest", "within N km", "co-located", "attributed to" question — ALWAYS use the
precomputed plume_candidate_wells table (join on plume_id; rank = 1 is the nearest well,
and it carries the distance). operator lives on the well tables. Do NOT compute proximity
yourself with ST_Distance/ST_Point/buffers: the geometry columns are lon/lat degrees
(SRID 4326), so ST_Distance returns DEGREES not metres and a "<= 1000" metre threshold is
wrong; a cross-join distance is also needlessly slow. plume_candidate_wells already
encodes the nearest-well relationship correctly.

Query robustness: prefer a single aggregation with LEFT JOINs over UNION-ing multiple
CTEs. Never UNION/UNION ALL subqueries that have different column counts (that is a hard
error). Default to PERMISSIVE thresholds so results aren't empty: do not gate an answer
behind a large minimum (e.g. "operators with >= 50 wells") unless the user asked for it —
if a strict filter would return no rows, drop or lower it and note that you did.

MAP RESULTS — when a question asks to show/map/plot something, return the geometry as a
column aliased with the suffix _geojson using ST_ASGEOJSON(<geometry column>). The app
renders any *_geojson column as a map layer. The map-ready geometry columns (all SRID
4326) are:
- hotspot_latest.hex_geom            (H3 hotspot hexagons; also center_lon/center_lat)
- plume_leaderboard_latest.plume_geom_native  (EMIT plumes; also lon_max/lat_max)
- wells_enriched_latest.well_geom_native      (wells; also longitude/latitude)
- ref_shale_plays.play_geom          (play polygons)
- ref_counties.county_geom           (county polygons)

CHART + MAP CROSS-FILTER — the app can render an interactive chart
(histogram/box plot of a NUMERIC measure) whose selection filters the map to the matching
features. For this to work the result must be ONE ROW PER MAP FEATURE and carry BOTH:
  (a) an ST_ASGEOJSON(...) *_geojson geometry column, AND
  (b) the chartable attributes on the SAME rows — at least one numeric column
      (e.g. max_conc_ppmm, ch4_max, well_count) and useful categoricals
      (e.g. lead_operator, lead_county, play_name).
The charts plot NUMERIC values only — chart a number (max_conc_ppmm, ch4_max), never a
category on a chart axis (a string like lead_operator produces NaN). To slice by a
category, keep it as a column and filter on it in the map, not as a chart axis.
So DO NOT pre-aggregate away the individual features when the user wants to explore or
filter (e.g. for "chart plume concentration and let me filter the map", return one row per
plume with plume_geojson + max_conc_ppmm + lead_operator, NOT a GROUP BY operator rollup).
Keep a stable per-feature id column too (plume_id, api, geoid). A pure aggregate/rollup
result (one row per group, no per-feature geometry) can be charted but will NOT cross-filter
the map — only per-feature-with-geometry results produce the cross-filter.

Table roles:
- hotspot_latest — latest-overpass S5P CH4 hotspot H3 cells (ch4_max, ch4_mean, n_obs).
- plume_leaderboard_latest — one row per EMIT plume, peak concentration + leading operator.
- wells_enriched_latest — current well inventory, tagged with operator, lease, field,
  play_name, county_name/state_fp.
- plume_candidate_wells — nearest-K wells per plume (rank=1 = closest); operator attribution.
- operator_intensity_latest — per-operator plume counts + peak concentration + well_count.
- detections_by_county / emissions_by_play — plume rollups to county / shale play (map-ready).

H3 GRID ANALYSIS — the map's "Well Density (H3)" layer bins wells into H3 cells at a
resolution that changes with zoom/density (it is a rendering aid, NOT a fixed grid), so
never treat it as a single resolution or ask the user which resolution it is. For any
question that grids, joins, or compares by H3 cell (e.g. "join plumes to the well-density
hex they fall in", "is concentration higher where well density is higher"), compute cells
yourself in SQL at ONE fixed resolution. Default to res 7 for this dataset: plumes and
wells rarely co-locate finely, so a plume→well cell join yields only ~1 overlapping cell
at res 8, ~5 at res 7, ~10 at res 6. Use res 6 when you need more overlapping cells for a
statistical comparison, res 8 only for the densest areas. Use the native function
h3_longlatash3(lon, lat, res) to derive a cell from
coordinates and h3_h3tostring(...) for a readable hex id — do NOT rely on any client-side
or DuckDB H3 helper. Plume coordinates are plume_leaderboard_latest.lon_max/lat_max; well
coordinates are wells_enriched_latest.longitude/latitude. To join plumes to wells by cell,
grid both to the same resolution on h3_longlatash3(...) and join on the cell id.
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

**Plumes vs well density by H3 cell** — *"Is methane concentration higher where well density is higher?"* (~8 overlapping cells at res 6) — grids plumes and wells to the SAME fixed H3 resolution and joins on the cell id (never uses the map's dynamic Well Density layer resolution).
```sql
WITH plume_cells AS (
  SELECT h3_longlatash3(lon_max, lat_max, 6) AS cell, max_conc_ppmm
  FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.plume_leaderboard_latest
  WHERE lon_max IS NOT NULL AND lat_max IS NOT NULL
),
well_cells AS (
  SELECT h3_longlatash3(longitude, latitude, 6) AS cell,
         COUNT(*) AS well_count, COUNT(DISTINCT operator) AS operator_count
  FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.wells_enriched_latest
  WHERE longitude IS NOT NULL AND latitude IS NOT NULL
  GROUP BY 1
)
SELECT h3_h3tostring(p.cell) AS hex, w.well_count, w.operator_count,
       COUNT(*) AS plume_count, ROUND(AVG(p.max_conc_ppmm), 1) AS avg_conc_ppmm
FROM plume_cells p JOIN well_cells w ON p.cell = w.cell
GROUP BY p.cell, w.well_count, w.operator_count
ORDER BY w.well_count DESC
```

**Plumes for a chart that filters the map** — *"Chart plume concentration and let me filter the map"* (72 rows) — one row per plume with geometry AND chartable attributes, so a HISTOGRAM/box plot of the numeric max_conc_ppmm cross-filters the plume layer on the map. Chart the numeric column (not lead_operator — a category on a chart axis yields NaN); to slice by operator, add a kepler filter on lead_operator. Do NOT roll up to one row per operator (that breaks the map cross-filter).
```sql
SELECT plume_id, max_conc_ppmm, lead_operator, lead_county,
       ST_ASGEOJSON(plume_geom_native) AS plume_geojson
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.plume_leaderboard_latest
WHERE plume_geom_native IS NOT NULL
ORDER BY max_conc_ppmm DESC
```

**Wells for a chart that filters the map** — *"Show wells and let me filter the map by operator"* (996 rows) — one row per well with geometry AND categorical attributes (operator, play_name, county_name). Render the wells layer, then slice with a kepler filter on operator/play_name/county_name (these are categorical, so use a filter, not a chart axis).
```sql
SELECT api, operator, play_name, county_name,
       ST_ASGEOJSON(well_geom_native) AS well_geojson
FROM serverless_stable_genie_map_catalog.vapor_eyes_lf.wells_enriched_latest
WHERE well_geom_native IS NOT NULL
```
