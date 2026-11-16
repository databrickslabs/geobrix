# raster-genie — Raster in Genie and SQL warehouses

A notebook series that lands master Cloud Optimized GeoTIFFs in **FILE** columns, queries them with the `gbx_rst_*` functions in plain SQL, and then asks a **Genie Agent** the same questions in plain English, using [GeoBrix](https://databrickslabs.github.io/geobrix/) on Databricks.

The first notebook is a vertical slice on one event: the **2024 Smokehouse Creek fire** in the Texas Panhandle. It answers *how much land burned, how severe was it, and how many people live there* from Sentinel-2 and WorldPop rasters.

> **Runs on Serverless by default.** `config_nb` installs the `geobrix[stac,vizx,databricks]` env6 wheel, so the notebook runs on Serverless environment 6 with no JAR. The `gbx_rst_*` functions run in the session; the same calls run in Unity Catalog once the GeoBrix UC functions are installed (see [Query FILE columns from Unity Catalog](https://databrickslabs.github.io/geobrix/docs/readers/file#query-file-columns-from-unity-catalog)).

> **Data sources.** Sentinel-2 L2A from Microsoft Planetary Computer (`StacClient`), the incident perimeter from NIFC WFIGS (`FirePerimeterDownloader`), population from WorldPop (`WorldPopDownloader`), and county and state boundaries from the US Census TIGER cartographic files. All are fetched over HTTPS, online only.

---

## Notebooks at a glance

### 01 — Smokehouse Creek

- **FILE + COG** — each Sentinel-2 scene becomes one seven-band master COG (`stack_scene`: 512-pixel blocks, average overviews 2/4/8/16, GeoBrix compression, nodata 0). The scene is cloud-masked once, when the COG is written: SCL classes 0, 1, 3, 8, 9, 10, 11 become nodata in the reflectance bands at 10 m, so no query needs an SCL mask. SCL stays as band 7, valid at full resolution only, registered in the `rasters` table by a `FILE EXTERNAL` column. Nothing is copied.
- **Windowed reads** — `gbx_rst_fromfile(file, 'GTiff', <perimeter>, 'EPSG:4326', <resolution>)` fetches only the COG blocks the perimeter needs, at a suitable overview.
- **Burn severity in SQL** — the notebook first sizes the perimeter's window against the 64 MiB a Unity Catalog function can return and picks the overview to read. `gbx_rst_nbr`, `gbx_rst_mapalgebra` (dNBR, and each tile's offset from an unburned ring), `gbx_rst_merge` and `gbx_rst_histogram` turn a pre-fire and a post-fire scene of each tile (same relative orbit where possible) into dNBR severity classes (Key & Benson, 2006; USGS) and an area per class. WorldPop gives the people inside the perimeter.
- **Map and Genie** — the severity classes over an Esri basemap, then the same question asked of a Genie Agent in Agent mode, with the SQL it wrote.

---

## Files

| File | Purpose |
|---|---|
| `config_nb.ipynb` | Shared setup (`%run ./config_nb`). Installs the env6 wheel (two-step: `--no-deps` first, then with extras), registers the functions with `gbx.register(spark)`, holds the USER SETTINGS (`catalog_name`, `schema_name`, the fire, the pre and post windows, `MAX_CLOUD`, `MAX_NODATA`, `BURN_RES_M`, `GENIE_SPACE_ID`), creates the schema, the `data` Volume and its directory tree, and defines `finalize_delta`. |
| `01_smokehouse_creek.ipynb` | The Smokehouse Creek slice: `aois`, Sentinel-2 master COGs, WorldPop, the `rasters` FILE table, the `coverage` view, the burn severity query, a map and the Genie Agent. |
| [`lakeflow/land/scenes.py`](lakeflow/land/scenes.py) | `stack_scene`: per-band Sentinel-2 files in, one master COG out. Tested in `lakeflow/tests/`. |

Tables in `geospatial_catalog.raster_genie`: `aois` (the perimeter, the counties it touches and the Texas outline), `rasters` (one row per master COG, with a `FILE EXTERNAL` column), the `coverage` view, and `burn_severity_summary`.

---

## Prerequisites

- **Serverless environment 6** (or Databricks Runtime 19), Python 3.12. The series needs no JAR.
- **GeoBrix 0.6.0.** `config_nb` installs the env6 wheel from the Hub Volume: `/Volumes/geospatial_catalog/geobrix/sample-data/env6/geobrix-0.6.0+env6-py3-none-any.whl`.
- **The GeoBrix UC functions installed** in your catalog, if you want to query the `rasters` table from a SQL warehouse or Genie. The notebook itself calls the session functions.
- **Unity Catalog.** `config_nb` creates the schema and a Volume named `data` under it (`CREATE ... IF NOT EXISTS`), so you need `CREATE SCHEMA` and `CREATE VOLUME` on the catalog. Edit `catalog_name` and `schema_name` to change them.
- **The whole folder.** Open the series from a Git folder, or copy the `lakeflow/` folder next to the notebooks, so that `lakeflow/land/scenes.py` sits beside notebook 01: it imports `stack_scene` from `./lakeflow` and stops with a Fix if the file is missing.
- **A Genie space (last step only).** Paste the space id into `GENIE_SPACE_ID` in `config_nb`; until then the Genie step prints a message and skips.
- **Network access.** Planetary Computer, NIFC, WorldPop and Census are all fetched over HTTPS.

---

## Run order

1. Open `config_nb.ipynb`, check `catalog_name` / `schema_name`, and (optionally) set `GENIE_SPACE_ID`.
2. Run `01_smokehouse_creek.ipynb` top to bottom. It starts with `%run ./config_nb`.

The first run downloads the national WorldPop file once (about 0.5 GB) and the Sentinel-2 windows (a few GB). Every step is guarded: files already on the Volume are not downloaded again and existing tables are not rewritten. Set `FORCE_REBUILD = True` in the cell right after `%run ./config_nb` to rebuild everything.

---

## Tests

`stack_scene` has unit tests that write real small rasters and check the COG layout (blocks, overviews, compression, nodata, no mask band, band descriptions, resampling):

```bash
bash scripts/commands/gbx-test-python.sh --path examples/notebooks/raster-genie/lakeflow/tests
```

---

## Learn more

- [Raster functions](https://databrickslabs.github.io/geobrix/docs/api/raster-functions): `rst_fromfile`, `rst_nbr`, `rst_mapalgebra`, `rst_merge`, `rst_histogram`
- [File reader](https://databrickslabs.github.io/geobrix/docs/readers/file): FILE columns and querying them from Unity Catalog
- [STAC client](https://databrickslabs.github.io/geobrix/docs/api/stac): search and windowed download
- [COG writer](https://databrickslabs.github.io/geobrix/docs/writers/cog) and [Large rasters](https://databrickslabs.github.io/geobrix/docs/api/large-rasters): master COG preparation
