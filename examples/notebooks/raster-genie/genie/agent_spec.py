"""The raster-genie slice Genie Agent: its tables, GeoBrix functions, instructions and example SQL.

`build_serialized_space(TABLES, FUNCTIONS, TEXT, EXAMPLES)` (databricks.labs.gbx.genie.space) turns these
constants into the live space; `seed_agent.py` applies it. The diagram of the series reads `FUNCTIONS`.

There are two spaces over the same data and the same attached functions. The **live** space computes every
answer from the COGs with the GeoBrix functions. The **precomputed** space also holds the table
`burn_severity_summary` (the saved answer of the severity chain), prefers it for severity, and uses the
functions for anything the table does not hold. Constants of the second space end in `_PRECOMPUTED`.

Counts against the Genie limit of 100 instructions: each attached function, each example and the one text
block. Tables do not count.

Every function and table is written with its full name: the warehouse that runs a Genie query resolves a bare
`gbx_rst_*` or a bare table name against its own default schema, not this one.
"""

CATALOG = "geospatial_catalog"
DATA_SCHEMA = "raster_genie"  # the tables
UC_SCHEMA = "geobrix_uc"  # the GeoBrix Unity Catalog functions
_T = f"{CATALOG}.{DATA_SCHEMA}."
_G = f"{CATALOG}.{UC_SCHEMA}."

#: The tables and view the agent may read (fully qualified).
TABLES = [
    f"{_T}rasters",
    f"{_T}aois",
    f"{_T}s2_scene_assets",
    f"{_T}coverage",
]

#: The tables of the precomputed space: the live ones plus the saved severity answer.
TABLES_PRECOMPUTED = [*TABLES, f"{_T}burn_severity_summary"]

#: The GeoBrix functions attached to the space (fully qualified). Genie grants EXECUTE on an attached
#: function only, so every function the text or an example uses is listed.
FUNCTIONS = [
    f"{_G}gbx_rst_{name}"
    for name in (
        "fromfile",
        "clip",
        "merge",
        "width",
        "height",
        "pixelwidth",
        "pixelheight",
        "srid",
        "boundingbox",
        "numbands",
        "metadata",
        "avg",
        "min",
        "max",
        "median",
        "summary",
        "pixelcount",
        "histogram",
        "stats_partial",
        "ndvi",
        "nbr",
        "savi",
        "index",
        "mapalgebra",
    )
]

_TEXT_DATA = """- rasters: one row per master COG. product is s2_l2a (Sentinel-2 L2A imagery, 10 m, 7 bands in this order: B02, B03, B04, B08, B11, B12, SCL) or population (WorldPop 2020, people per pixel). footprint is a GEOMETRY. file is a FILE column. bands lists the band names; a band number is its 1-based position, so use CAST(array_position(bands, 'B08') AS INT), never a guessed number. resolution_m is the native pixel size.
- aois: named areas with a GEOMETRY geom. kind is fire, county or state. The seven counties are the ones the fire touches (Carson, Gray, Hemphill, Hutchinson, Lipscomb, Roberts, Wheeler).
- coverage: what we hold per product, tier and area (best_resolution_m, first_date, last_date, assets, mean_valid_pct). Use it for what data exists, at what resolution and how recent. It holds imagery and population only: no elevation, land cover or wells. Say so when asked about them.
- s2_scene_assets: the Sentinel-2 scenes behind the imagery: tile, date, relative orbit, cloud cover. The relative orbit is the digits after _R in item_id."""

_TEXT_DATA_PRECOMPUTED = """- burn_severity_summary: the saved result of the burn severity query for the Smokehouse Creek fire (dNBR classes at 80 m, relative to each tile's unburned ring), one row per class (1 Unburned, 2 Low, 3 Moderate-low, 4 Moderate-high, 5 High) with pixels, km2 and acres. Burned area is low severity or worse (class_id >= 2)."""

_TEXT_READ = f"""HOW TO READ RASTERS
1. Pick rasters rows first: join aois with st_intersects(rasters.footprint, aois.geom) and filter on product, tier and acquired_date.
2. Read with {_G}gbx_rst_fromfile(file, 'GTiff', <geom>, <clip_crs>, <resolution>). Pass the rasters.file column and the aois.geom column directly. NEVER wrap them in CAST(... AS FILE), and never convert it to WKB or WKT. The read fetches only the COG blocks the geometry needs. Never pass the string 'NULL'; use NULL.
3. resolution is the pixel size in the raster's units: metres for Sentinel-2. Use 80 or coarser for the whole fire perimeter and 40 or coarser for a county; a read larger than 64 MiB is refused with an error that carries a Fix, so follow the Fix. For population, omit the resolution.
4. The Sentinel-2 reflectance is already cloud-masked when the COG is written. Do not read or use the SCL band to mask. Nodata is 0 and the statistics skip it.
5. Then apply an index (gbx_rst_nbr, gbx_rst_ndvi, gbx_rst_savi) or gbx_rst_mapalgebra, then a statistic (gbx_rst_avg, gbx_rst_pixelcount, gbx_rst_histogram). People inside an area = the population pixel mean times its valid pixel count.
6. An area that spans several assets: read each one, then combine with gbx_rst_merge(collect_list(tile)).
7. Return geometry as ST_ASGEOJSON(...) when the answer is a map."""

_TEXT_SEVERITY_LIVE = """BURN SEVERITY
- Compute it live from the COGs; no table holds a ready answer. "Burn severity" and "how badly it burned" mean dNBR: the NBR of a pre-fire scene minus the NBR of a post-fire scene (gbx_rst_nbr with the B08 and B12 band numbers), for each MGRS tile that meets the area. Scenes of one tile come from the same relative orbit where possible. Pre-fire scenes are 2024-01-15 to 2024-02-25, post-fire scenes 2024-03-05 to 2024-03-31.
- Each tile's dNBR is taken relative to that tile's own unburned ring: read the same pre and post scenes over the ring (1 to 5 km outside the Smokehouse Creek perimeter, kind = 'fire' in aois, intersected with both footprints), take the mean ring dNBR as the tile's offset (0 when the ring holds fewer than 1000 valid pixels) and subtract it from the tile's dNBR with gbx_rst_mapalgebra. The offset removes the difference between scenes; the mean of the offset dNBR is the "average burn severity".
- Classes of the offset dNBR (USGS, Key and Benson 2006): 1 Unburned below 0.1, 2 Low 0.1 to 0.27, 3 Moderate-low 0.27 to 0.44, 4 Moderate-high 0.44 to 0.66, 5 High 0.66 and above, made with gbx_rst_mapalgebra. Burned area is low severity or worse (classes 2 to 5); "moderate-low or worse" is classes 3 to 5. Count the classes with gbx_rst_histogram(tile, 5, 0.5, 5.5, false), merge the tiles first with gbx_rst_merge(collect_list(tile)), and turn pixels into acres with the pixel area (gbx_rst_pixelwidth times gbx_rst_pixelheight, divided by 4046.8564224).
- To clip to any area, read each scene with gbx_rst_fromfile(file, 'GTiff', <aois.geom of that area>, 'EPSG:4326', 80.0) and keep only the tiles whose footprint intersects it: a county is a row of aois with kind = 'county' (match its name), the fire is the row with aoi_id = 'smokehouse_creek'. The ring stays the ring of the fire perimeter whatever the area.
- The example "How much land burned in the Smokehouse Creek fire, and how severe was it?" is that whole chain for the fire perimeter, read at 80 m: adapt the example's chain to the area asked about, changing only the area (and the statistic or class limit the question asks for). It takes about 1.5 to 2.5 minutes. Its column low_or_worse_acres is the burned total.
- The people inside an area come from the WorldPop raster (example "How many people live inside the Smokehouse Creek fire perimeter?"): the population pixel mean times its valid pixel count, for the geometry asked about. They are counted on the whole area, burned or not. Say so. For a question about both land and people, run both statements."""

_TEXT_SEVERITY_PRECOMPUTED = """BURN SEVERITY
- For how much burned, how severe and the area per class of the Smokehouse Creek fire, read burn_severity_summary: it is the saved answer of the full chain and it takes seconds. Its column low_or_worse_acres is the burned total. Burn severity is the dNBR (NBR before minus NBR after, gbx_rst_nbr) of a pre-fire and a post-fire scene of each MGRS tile, relative to that tile's unburned ring (1 to 5 km outside the perimeter, the tile's mean ring dNBR subtracted), in the USGS classes (Key and Benson 2006): 1 Unburned below 0.1, 2 Low, 3 Moderate-low from 0.27, 4 Moderate-high from 0.44, 5 High from 0.66. Burned area is low severity or worse (classes 2 to 5).
- Use the GeoBrix functions for anything the table does not hold: another area, window, resolution or statistic, or when the user asks to recompute. Clip to any area with gbx_rst_fromfile(file, 'GTiff', <aois.geom>, 'EPSG:4326', 80.0) (a county is a row of aois with kind = 'county'), then apply the chain. The example "Recompute the burn severity of the Smokehouse Creek fire from the Sentinel-2 imagery" is the whole chain for the perimeter at 80 m: adapt its chain to the area asked about. It takes about 1.5 to 2.5 minutes.
- The people inside an area come from the WorldPop raster (example "How many people live inside the Smokehouse Creek fire perimeter?"): the population pixel mean times its valid pixel count. They are counted on the whole area, burned or not. Say so. For a question about both land and people, run both statements."""

_TEXT_ANSWERS = """ANSWERS
- Give the numbers with units (acres and km2; people), name the source (Sentinel-2 L2A, WorldPop 2020) and the dates used, and keep it short.
- For what data we hold, report product, tier, resolution in metres, first and last date, number of assets and valid percent from coverage, and confirm the pixel size with one live gbx_rst_fromfile read of one asset, as in the example for Carson County. Do not cite the pixel size of a coarse read as the resolution."""

_TEXT_HEAD = f"""You answer questions about raster data held as Cloud Optimized GeoTIFFs (COGs) for the 2024 Smokehouse Creek fire in the Texas Panhandle. Write one self-contained SQL statement for each part of a question (a question about land and people takes one statement for each) and run each. Compute every number in SQL: never add, subtract or convert numbers yourself. Always write full names: tables as {_T}<table> and GeoBrix functions as {_G}gbx_rst_<name>.
"""


_SQL_SEVERITY_SUMMARY = f"""SELECT class_id, severity, round(acres) AS acres, round(km2, 1) AS km2, pct_of_valid_pixels,
       round(sum(CASE WHEN class_id >= 2 THEN acres END) OVER (), 0) AS low_or_worse_acres
FROM {_T}burn_severity_summary
ORDER BY class_id"""


def _text(severity: str, extra_data: str = "") -> str:
    """The instruction text: head, data, how to read, the severity section, answers."""
    data = _TEXT_DATA + ("\n" + extra_data if extra_data else "")
    return (
        "\n\n".join([_TEXT_HEAD, "DATA\n" + data, _TEXT_READ, severity, _TEXT_ANSWERS])
        + "\n"
    )


#: The instruction text of the live space.
TEXT = _text(_TEXT_SEVERITY_LIVE)
#: The instruction text of the precomputed space.
TEXT_PRECOMPUTED = _text(_TEXT_SEVERITY_PRECOMPUTED, _TEXT_DATA_PRECOMPUTED)

_SQL_SEVERITY = f"""WITH perimeter AS (
  SELECT geom FROM {_T}aois WHERE aoi_id = 'smokehouse_creek'
),
ring AS (
  SELECT st_transform(st_difference(st_buffer(st_transform(geom, 5070), 5000),
                                    st_buffer(st_transform(geom, 5070), 1000)), 4326) AS rg
  FROM perimeter
),
meta AS (
  SELECT DISTINCT mgrs_tile, to_date(substr(date, 1, 10)) AS d, regexp_extract(item_id, '_R([0-9]+)_', 1) AS orbit
  FROM {_T}s2_scene_assets
),
scenes AS (
  SELECT r.asset_id, m.mgrs_tile, m.orbit, r.acquired_date, r.bands, r.file, r.footprint
  FROM {_T}rasters r
  JOIN meta m ON r.asset_id = concat('s2_', m.mgrs_tile, '_', date_format(m.d, 'yyyyMMdd'))
  WHERE r.product = 's2_l2a' AND r.aoi_id = 'smokehouse_creek'
),
pairs AS (
  SELECT pre.mgrs_tile, pre.acquired_date AS pre_date, pre.orbit AS pre_orbit,
         post.acquired_date AS post_date, post.orbit AS post_orbit,
         pre.bands, pre.file AS pre_file, post.file AS post_file, pre.footprint AS fp, post.footprint AS fp_post, p.geom
  FROM scenes pre
  JOIN scenes post ON post.mgrs_tile = pre.mgrs_tile
  CROSS JOIN perimeter p
  WHERE pre.acquired_date  BETWEEN DATE'2024-01-15'  AND DATE'2024-02-25'
    AND post.acquired_date BETWEEN DATE'2024-03-05' AND DATE'2024-03-31'
    AND st_intersects(pre.footprint, p.geom)
    AND st_intersects(post.footprint, p.geom)
),
reads AS (
  SELECT mgrs_tile, pre_date, pre_orbit, post_date, post_orbit,
         CAST(array_position(bands, 'B08') AS INT) AS nir,
         CAST(array_position(bands, 'B12') AS INT) AS swir,
         {_G}gbx_rst_fromfile(pre_file,  'GTiff', geom, 'EPSG:4326', 80.0) AS pre_tile,
         {_G}gbx_rst_fromfile(post_file, 'GTiff', geom, 'EPSG:4326', 80.0) AS post_tile
  FROM pairs
),
parts AS (
  SELECT mgrs_tile, pre_date, pre_orbit, post_date, post_orbit,
         {_G}gbx_rst_nbr(pre_tile,  nir, swir) AS pre_nbr,
         {_G}gbx_rst_nbr(post_tile, nir, swir) AS post_nbr
  FROM reads
  WHERE pre_tile IS NOT NULL AND post_tile IS NOT NULL
),
dnbr_raw AS (
  SELECT mgrs_tile, pre_date, pre_orbit, post_date, post_orbit, pre_nbr, post_nbr,
         {_G}gbx_rst_mapalgebra(array(pre_nbr, post_nbr), 'A - B') AS raw_tile
  FROM parts
),
ring_pairs AS (
  SELECT p.mgrs_tile, p.pre_file, p.post_file,
         CAST(array_position(p.bands, 'B08') AS INT) AS nir, CAST(array_position(p.bands, 'B12') AS INT) AS swir,
         st_intersection(st_intersection(r.rg, p.fp), p.fp_post) AS rtg
  FROM pairs p CROSS JOIN ring r
),
ring_reads AS (
  SELECT mgrs_tile, nir, swir,
         {_G}gbx_rst_fromfile(pre_file,  'GTiff', rtg, 'EPSG:4326', 80.0) AS pre_r,
         {_G}gbx_rst_fromfile(post_file, 'GTiff', rtg, 'EPSG:4326', 80.0) AS post_r
  FROM ring_pairs
  WHERE NOT st_isempty(rtg)
),
ring_dnbr AS (
  SELECT mgrs_tile,
         {_G}gbx_rst_mapalgebra(array({_G}gbx_rst_nbr(pre_r, nir, swir), {_G}gbx_rst_nbr(post_r, nir, swir)), 'A - B') AS rtile
  FROM ring_reads
  WHERE pre_r IS NOT NULL AND post_r IS NOT NULL
),
offsets AS (
  SELECT mgrs_tile, {_G}gbx_rst_pixelcount(rtile)[0] AS ring_px, {_G}gbx_rst_avg(rtile)[0] AS ring_mean,
         CASE WHEN {_G}gbx_rst_pixelcount(rtile)[0] >= 1000 THEN {_G}gbx_rst_avg(rtile)[0] ELSE 0.0 END AS dnbr_offset
  FROM ring_dnbr
),
dnbr AS (
  SELECT d.mgrs_tile, d.pre_date, d.pre_orbit, d.post_date, d.post_orbit, d.pre_nbr, d.post_nbr, d.raw_tile,
         coalesce(o.ring_px, 0) AS ring_px, coalesce(o.dnbr_offset, 0.0) AS dnbr_offset,
         {_G}gbx_rst_mapalgebra(array(d.raw_tile), concat('A - (', printf('%.6f', coalesce(o.dnbr_offset, 0.0)), ')')) AS tile
  FROM dnbr_raw d LEFT JOIN offsets o ON o.mgrs_tile = d.mgrs_tile
)
,
severity AS (
  SELECT {_G}gbx_rst_merge(collect_list(
           {_G}gbx_rst_mapalgebra(array(tile), 'A < 0.1 ? 1 : (A < 0.27 ? 2 : (A < 0.44 ? 3 : (A < 0.66 ? 4 : 5)))')
         )) AS tile
  FROM dnbr
)

SELECT pos + 1 AS class_id,
       CASE pos + 1 WHEN 1 THEN 'Unburned' WHEN 2 THEN 'Low' WHEN 3 THEN 'Moderate-low'
                    WHEN 4 THEN 'Moderate-high' ELSE 'High' END AS severity,
       n AS pixels,
       round(n * abs({_G}gbx_rst_pixelwidth(tile) * {_G}gbx_rst_pixelheight(tile)) / 1e6, 1) AS km2,
       round(n * abs({_G}gbx_rst_pixelwidth(tile) * {_G}gbx_rst_pixelheight(tile)) / 4046.8564224, 0) AS acres,
       round(100.0 * n / sum(n) OVER (), 1) AS pct_of_valid_pixels,
       round(sum(CASE WHEN pos + 1 >= 2 THEN n END) OVER ()
             * abs({_G}gbx_rst_pixelwidth(tile) * {_G}gbx_rst_pixelheight(tile)) / 4046.8564224, 0) AS low_or_worse_acres
FROM (SELECT tile, {_G}gbx_rst_histogram(tile, 5, 0.5, 5.5, false)['band_1'] AS counts FROM severity)
LATERAL VIEW posexplode(counts) h AS pos, n
ORDER BY class_id"""

_SQL_PEOPLE = f"""SELECT round(avg_values[0] * valid_counts[0]) AS people_in_perimeter
FROM (
  SELECT {_G}gbx_rst_avg(t) AS avg_values, {_G}gbx_rst_pixelcount(t) AS valid_counts
  FROM (
    SELECT {_G}gbx_rst_fromfile(r.file, 'GTiff', a.geom, 'EPSG:4326') AS t
    FROM {_T}rasters r CROSS JOIN {_T}aois a
    WHERE r.product = 'population' AND a.aoi_id = 'smokehouse_creek'
      AND st_intersects(r.footprint, a.geom)
  )
)"""

_SQL_COVERAGE = f"""WITH cov AS (
  SELECT product, tier, aoi_name, best_resolution_m, first_date, last_date, assets, mean_valid_pct
  FROM {_T}coverage
  WHERE aoi_kind = 'county' AND aoi_name = 'Carson'
),
newest AS (
  SELECT r.asset_id, r.file, st_buffer(st_centroid(st_intersection(a.geom, r.footprint)), 0.005) AS window_geom
  FROM {_T}rasters r JOIN {_T}aois a ON st_intersects(r.footprint, a.geom)
  WHERE a.kind = 'county' AND a.name = 'Carson' AND r.product = 's2_l2a'
  ORDER BY r.acquired_date DESC LIMIT 1
),
live AS (
  SELECT asset_id,
         {_G}gbx_rst_pixelwidth(t) AS pixel_size_m,
         {_G}gbx_rst_pixelcount(t)[0] AS valid_pixels_in_window
  FROM (SELECT asset_id, {_G}gbx_rst_fromfile(file, 'GTiff', window_geom, 'EPSG:4326', 10.0) AS t FROM newest)
)
SELECT cov.*, live.asset_id AS checked_asset, live.pixel_size_m, live.valid_pixels_in_window
FROM cov LEFT JOIN live ON cov.product = 's2_l2a'
ORDER BY cov.product"""

_SQL_AREAS = f"""SELECT aoi_id, name, kind, description
FROM {_T}aois
ORDER BY kind, name"""

_SQL_SCENES = f"""SELECT r.asset_id, s.mgrs_tile, s.orbit, r.acquired_date, r.cloud_pct, r.valid_pct, r.resolution_m
FROM {_T}rasters r
JOIN (SELECT DISTINCT mgrs_tile, orbit, to_date(substr(date, 1, 10)) AS d FROM {_T}s2_scene_assets) s
  ON r.asset_id = concat('s2_', s.mgrs_tile, '_', date_format(s.d, 'yyyyMMdd'))
WHERE r.product = 's2_l2a' AND r.aoi_id = 'smokehouse_creek'
ORDER BY r.acquired_date, s.mgrs_tile"""

_SQL_NDVI = f"""WITH county AS (
  SELECT geom FROM {_T}aois WHERE kind = 'county' AND name = 'Gray'
),
scenes AS (
  SELECT r.asset_id, r.acquired_date, r.file, c.geom,
         CAST(array_position(r.bands, 'B04') AS INT) AS red, CAST(array_position(r.bands, 'B08') AS INT) AS nir
  FROM {_T}rasters r JOIN county c ON st_intersects(r.footprint, c.geom)
  WHERE r.product = 's2_l2a'
),
per_scene AS (
  SELECT asset_id, acquired_date,
         CASE WHEN acquired_date < DATE'2024-02-26' THEN 'before the fire' ELSE 'after the fire' END AS period,
         {_G}gbx_rst_ndvi({_G}gbx_rst_fromfile(file, 'GTiff', geom, 'EPSG:4326', 160.0), red, nir) AS ndvi_tile
  FROM scenes
)
SELECT period, count(*) AS scenes, round(avg({_G}gbx_rst_avg(ndvi_tile)[0]), 3) AS mean_ndvi
FROM per_scene
GROUP BY period
ORDER BY period DESC"""

#: (question, SQL) pairs: plain GeoBrix SQL, no wrappers.
EXAMPLES = [
    (
        "How much land burned in the Smokehouse Creek fire, and how severe was it?",
        _SQL_SEVERITY,
    ),
    ("How many people live inside the Smokehouse Creek fire perimeter?", _SQL_PEOPLE),
    (
        "What imagery and elevation do we have for Carson County, at what quality, and how recent?",
        _SQL_COVERAGE,
    ),
    ("List the areas", _SQL_AREAS),
    ("What Sentinel-2 scenes do we have for the fire?", _SQL_SCENES),
    (
        "How did vegetation health in Gray County change from before to after the fire?",
        _SQL_NDVI,
    ),
]

#: The examples of the precomputed space: the saved severity answer first, the chain for a recompute.
EXAMPLES_PRECOMPUTED = [
    (
        "How much land burned in the Smokehouse Creek fire, and how severe was it?",
        _SQL_SEVERITY_SUMMARY,
    ),
    (
        "Recompute the burn severity of the Smokehouse Creek fire from the Sentinel-2 imagery",
        _SQL_SEVERITY,
    ),
    *EXAMPLES[1:],
]

#: The functions of the precomputed space: the same as the live space.
FUNCTIONS_PRECOMPUTED = FUNCTIONS

#: What counts against the 100-instruction limit of a Genie space.
INSTRUCTION_COUNT = len(FUNCTIONS) + len(EXAMPLES) + 1
INSTRUCTION_COUNT_PRECOMPUTED = (
    len(FUNCTIONS_PRECOMPUTED) + len(EXAMPLES_PRECOMPUTED) + 1
)

#: The two spaces by variant: (tables, functions, text, examples).
VARIANTS = {
    "live": (TABLES, FUNCTIONS, TEXT, EXAMPLES),
    "precomputed": (
        TABLES_PRECOMPUTED,
        FUNCTIONS_PRECOMPUTED,
        TEXT_PRECOMPUTED,
        EXAMPLES_PRECOMPUTED,
    ),
}
