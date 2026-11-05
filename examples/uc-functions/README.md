# GeoBrix Unity Catalog SQL examples

Ready-made queries for the GeoBrix [Unity Catalog functions](https://databrickslabs.github.io/geobrix/docs/api/uc-functions). Each file covers one category. Each query uses small inputs and returns rows.

## Install the functions

Run `gbx.uc.install` once from an env 6 notebook or job. Use Serverless environment 6 or DBR 19. Add `procedures=True` for the procedures in `11_procedures.sql`.

```python
import databricks.labs.gbx as gbx

gbx.uc.install("geospatial_docs", "geobrix_uc", "/Volumes/<catalog>/<schema>/<volume>/<folder>", procedures=True)
```

The folder holds one env 6 wheel of the same version as the running wheel. It may also hold the GeoBrix JAR. See the [install guide](https://databrickslabs.github.io/geobrix/docs/api/uc-functions#install).

## Use another catalog or schema

The files use the catalog `geospatial_docs` and the schema `geobrix_uc`. To use others, change both names:

- in the two `USE` lines at the top of each file;
- in the table names inside the `CALL` statements of `11_procedures.sql`;
- in the `gbx:examples:uc-sql` options `--catalog` and `--schema`.

## Run the examples

1. Import the files into your workspace, or run `gbx:examples:uc-sql --upload` (below).
2. Open `00_setup.sql` and run it. It builds the sample tables (`ex_*`) the other files read.
3. Open any other file and run one statement at a time.

A statement that calls a GeoBrix Python function takes about 10 seconds on a SQL warehouse. A SQL-kernel function is fast.

To run every file and check the results, use the command:

```bash
bash scripts/commands/gbx-examples-uc-sql.sh --profile <profile> --upload
```

It fails when a statement errors or a query returns no rows. With `--upload` it imports the files into `/Workspace/Users/<you>/geobrix/uc-function-examples/`, where they open in the SQL editor.

## The files

| File | Category |
|---|---|
| `00_setup.sql` | Sample tables: two small rasters, parcels, sensor points, roads and legacy geometries |
| `01_grids_bng.sql` | British National Grid cells, rings, polyfill, tessellation, chips and table functions |
| `02_grids_quadbin_custom.sql` | Quadbin cells and custom grids |
| `03_grids_h3.sql` | H3 rings of a geometry, cell bounding boxes and cell fill |
| `04_vector.sql` | Geometry cleaners, coverage checks, vector tiles and legacy geometry migration |
| `05_raster_accessors.sql` | Raster size, CRS, geotransform, metadata, coordinates and statistics |
| `06_raster_operations.sql` | Warp, resample, clip, filters, combine, overviews, contours and web tiles |
| `07_raster_indices_and_terrain.sql` | Spectral indices, terrain, canopy height and land cover |
| `08_raster_generators.sql` | Cut a tile into tiles, and build tiles from points and polygons |
| `09_raster_to_grid.sql` | Summarize pixels into H3, quadbin, BNG and custom cells |
| `10_aggregation_recipes.sql` | Array and partial forms that replace aggregates |
| `11_procedures.sql` | Whole ETL steps that run inside Unity Catalog |

## Functions not covered

The files call every Unity Catalog function except the ones below.

| Function | Why it is left out |
|---|---|
| `gbx_rst_getsubdataset` | It reads one subdataset of a NetCDF or HDF file. The sample tiles are GeoTIFFs, which have none. |
| `gbx_zarr_tag_version`, `gbx_zarr_untag_version`, `gbx_zarr_version_history` | They work on the manifest table of a Zarr store, which the Zarr writer makes. |

A function that has no Unity Catalog form is not in these files. The [availability matrix](https://databrickslabs.github.io/geobrix/docs/api/availability) lists the surfaces of each function.
