<img src="resources/images/brand/GeoBriX.png" width="50%" />

[![build](https://github.com/databrickslabs/geobrix/actions/workflows/build_main.yml/badge.svg)](https://github.com/databrickslabs/geobrix/actions/workflows/build_main.yml)
[![documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://databrickslabs.github.io/geobrix/)
[![scala](https://img.shields.io/badge/scala-2.13-red.svg)](https://www.scala-lang.org/)
[![python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/)
[![license](https://img.shields.io/badge/license-Databricks-blue.svg)](LICENSE)

<!--
  SQL function counts — keep verbatim with docs/tests-function-info/registered_functions.txt
  (the canonical source: Functions = total lines; RasterX = gbx_rst_*, GridX =
  gbx_bng_*+gbx_custom_*+gbx_h3_*+gbx_quadbin_*, VectorX = gbx_st_*, PMTiles = gbx_pmtiles_*).
  Run `gbx:test:bindings` after changes to confirm binding parity.
  VizX is NOT a SQL package — its badge counts the public helpers in
  python/geobrix/src/databricks/labs/gbx/vizx/__init__.py __all__, and is excluded from the total.
  Update these badges if functions are added or removed.
-->
![Functions](https://img.shields.io/badge/functions-192-2e7d32)
![RasterX](https://img.shields.io/badge/RasterX-129-1565c0)
![GridX](https://img.shields.io/badge/GridX-41-1565c0)
![VectorX](https://img.shields.io/badge/VectorX-21-1565c0)
![VizX](https://img.shields.io/badge/VizX-20-6a1b9a)
![PMTiles](https://img.shields.io/badge/PMTiles-1-1565c0)

**GeoBrix** is a high-performance spatial library for Databricks that delivers the next generation of *product-augmenting* capabilities — raster, discrete global grids, and vector format I/O — and is built to drive you *deeper* into Databricks-native [`GEOMETRY`/`GEOGRAPHY` and ST/H3 functions](https://databrickslabs.github.io/geobrix/docs/databricks-spatial), not replace them. It is the modern successor to [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/) (now in maintenance).

> **Full docs:** **https://databrickslabs.github.io/geobrix/** — this README is the 2-minute tour.

<img src="resources/images/brand/geobrix_vision.png" width="70%" />

## Tiers

- **Lightweight tier** — pure Python (+ SQL bindings) on [rasterio](https://rasterio.readthedocs.io/)/[pyogrio](https://pyogrio.readthedocs.io/)/[shapely](https://shapely.readthedocs.io/), **no JAR, no init script, no native GDAL bundle**. Runs on **Serverless**, standard (shared), Lakeflow pipelines, and **ARM** — where the heavyweight tier can't.
- **Heavyweight tier** — Scala (Python and SQL bindings) + native GDAL for distributed processing on classic (x86) clusters. **Same function names across tiers** — switching is a one-line import change.

## Packages

<img src="resources/images/brand/RasterX.png" width="18%" /> <img src="resources/images/brand/GridX.png" width="18%" /> <img src="resources/images/brand/VectorX.png" width="18%" /> <img src="resources/images/brand/VizX.png" width="18%" />

- **[RasterX](https://databrickslabs.github.io/geobrix/docs/api/raster-functions)** — full-spectrum raster I/O and analytics (gap-filling; the platform has no built-in raster): reprojection, terrain, spectral indices, XYZ/PMTiles tiling, and H3/quadbin/BNG aggregation, plus **virtual tiles** and a **COG-preparation lane** for memory-safe processing of multi-gigabyte rasters. **Both tiers** — lightweight `pyrx` and heavyweight Scala.
- **[GridX](https://databrickslabs.github.io/geobrix/docs/api/gridx-functions)** — discrete global grids: British National Grid, CARTO quadbin, and custom user-defined grids — cell math, k-ring/k-loop, polyfill, tessellation, and grid-aware aggregation (pairs with native H3 for global hex). **Both tiers** — lightweight `pygx` and heavyweight Scala.
- **[VectorX](https://databrickslabs.github.io/geobrix/docs/api/vectorx-functions)** — vector operations that augment the native ST functions: MVT tile encoding, TIN elevation surfaces, authority-string CRS transforms, antimeridian handling, and distributed-shapely geometry validity, cleaning & coverage-validity, plus legacy-geometry migration. **Both tiers** — lightweight `pyvx` and heavyweight Scala.
- **[VizX](https://databrickslabs.github.io/geobrix/docs/api/vizx)** — tier-agnostic notebook visualization: render raster tiles/files, whole VRT mosaics (`plot_mosaic`), and PMTiles/COG archives, and turn Spark geometry/H3-cell/grid DataFrames into GeoPandas for static & interactive maps. **Python-only** (no SQL); works with either tier.

All SQL functions register with a `gbx_` prefix (e.g. `gbx_rst_clip`, `gbx_bng_cellarea`, `gbx_st_asmvt`) so usage is clearly attributable to GeoBrix on classic compute. Python/Scala bindings mirror the names. See [benchmarks](https://databrickslabs.github.io/geobrix/docs/api/benchmarking) for light-vs-heavy timings. 

## Supported Databricks Runtimes

GeoBrix supports the following Databricks Runtime releases:

| DBR | Ubuntu | Spark | Python | Scala | Java | GeoBrix |
|---|---|---|---|---|---|---|
| **17.3** | 24.04 | 4.0.0 | 3.12.3 | 2.13.16 | 17 | ✅ Supported |
| **18** | 24.04 | 4.1.0 | 3.12.3 | 2.13.16 | 21 | ✅ Supported |
| **19** | 24.04 | 4.2.0 | 3.12.3 | 2.13.18 | 21 | ✅ Supported |

A **single wheel + single JAR** runs on 17.3, 18, and 19: Scala 2.13 minor versions are binary-compatible, the JAR is compiled to Java-17 bytecode so it loads on all three JVMs, and Spark is a `provided` dependency.

GeoBrix Light uses **explicit, runtime-pinned extras** — each install names the target runtime. On Serverless use `[light_env6]` (environment v6, recommended) or `[light_env5]` (env 5); on classic clusters use `[light_dbr17]`, `[light_dbr18]`, or `[light_dbr19]`. See [Installation](https://databrickslabs.github.io/geobrix/docs/installation?tier=lightweight) for the full extras table.

## Quick start (lightweight)

Stage the wheel (a [Releases](https://github.com/databrickslabs/geobrix/releases) artifact, not on PyPI) in a Unity Catalog Volume, then install the `[light_env6]` extra (Serverless env 6, recommended):

```python
%pip install "geobrix[light_env6] @ file:///Volumes/<catalog>/<schema>/<volume>/geobrix-<version>-py3-none-any.whl"
```

> **Use the quoted `geobrix[light_env6] @ file://…` form** (PEP 508, one argument). Don't put the extra on the path (`'/Volumes/…/…whl[light_env6]'`) — on Serverless, `%pip` keeps the surrounding quotes and pip reads `[light_env6]` as part of the filename, failing with *"Expected package name at the start of dependency specifier."* The named form installs cleanly on Serverless, standard/shared, and ARM.

```python
from databricks.labs.gbx.ds.register import register   # *_gbx readers/writers
from databricks.labs.gbx.pyrx import functions as rx   # gbx_rst_* functions

register(spark)
rx.register(spark)   # optional — only to call the gbx_rst_* SQL functions

# Read a GeoTIFF and compute with RasterX
rasters = spark.read.format("gtiff_gbx").load("/Volumes/<catalog>/<schema>/<volume>/*.tif")
rasters.select(rx.rst_width("tile"), rx.rst_srid("tile")).show()

# Vector read -> write (round-trips with the matching reader)
boroughs = spark.read.format("geojson_gbx").load("/Volumes/.../boroughs.geojson")
boroughs.write.format("geojson_gbx").mode("overwrite").save("/Volumes/.../out.geojson")
```

**Heavyweight** is the same code with `from databricks.labs.gbx.rasterx import functions as rx`, plus the JAR and a GDAL init script — see [Installing & Choosing a Tier](https://databrickslabs.github.io/geobrix/docs/api/execution-tiers).

## Readers & writers

Lightweight (`*_gbx`) formats are pure-Python (no JAR); each pairs with a heavyweight counterpart (`*_ogr` / `gdal` / `gtiff_gdal`). Both tiers emit the **same schema** and are held to row-count / byte parity, so they are drop-in swaps. Full options, output schemas, and examples: [Readers](https://databrickslabs.github.io/geobrix/docs/readers/overview) · [Writers](https://databrickslabs.github.io/geobrix/docs/writers/overview).

**Readers**

| Format | Lightweight | Heavyweight |
|---|---|---|
| Raster (generic) | `raster_gbx` | `gdal` |
| GeoTIFF | `gtiff_gbx` | `gtiff_gdal` |
| COG | `cog_gbx` | — (light-only) |
| NetCDF | `netcdf_gbx` | `netcdf_gdal` / `netcdf_ogr` |
| PMTiles | `pmtiles_gbx` | — (light-only) |
| File lister | `file_gbx` | — (light-only) |
| Vector (generic) | `vector_gbx` | `ogr` |
| Shapefile | `shapefile_gbx` | `shapefile_ogr` |
| GeoJSON | `geojson_gbx` | `geojson_ogr` |
| GeoJSONL | `geojsonl_gbx` | — (light-only reader) |
| GeoPackage | `gpkg_gbx` | `gpkg_ogr` |
| File Geodatabase | `file_gdb_gbx` | `file_gdb_ogr` |

**Writers**

| Format | Lightweight | Heavyweight |
|---|---|---|
| Raster (generic) | `raster_gbx` | `gdal` |
| GeoTIFF | `gtiff_gbx` | `gtiff_gdal` |
| COG | `cog_gbx` | — (light-only) |
| PMTiles | `pmtiles_gbx` | `pmtiles` |
| NetCDF | `netcdf_gbx` | — (light-only) |
| Vector (generic) | `vector_gbx` | — (light-only) |
| Shapefile | `shapefile_gbx` | — (light-only) |
| GeoJSON | `geojson_gbx` | — (light-only) |
| GeoJSONL — *sharded, multi-file* | `geojsonl_gbx` | `geojsonl_ogr` |
| GeoPackage | `gpkg_gbx` | — (light-only) |
| File Geodatabase | `file_gdb_gbx` | — (hybrid; needs native GDAL) ¹ |

Single-file vector writes are lightweight-only; the **sharded GeoJSONL** writer (multi-file, one shard per partition, no driver merge — the recommended writer at any scale) is available in **both** tiers.

¹ `file_gdb_gbx` write is a **hybrid**: it encodes the `.gdb` via the native GDAL (`osgeo`) from the heavyweight GDAL init script, because pyogrio's bundled GDAL ships a read-only OpenFileGDB driver. On compute with those natives it writes natively; otherwise it raises a clear error (use `gpkg_gbx` / `geojson_gbx`). FileGDB *reading* is lightweight-only.

Light vector readers/writers exchange geometry as **WKB/WKT** with companion `*_srid` columns — convert to/from Databricks `GEOMETRY` with `st_geomfromwkb` / `st_aswkb` (see [Databricks Spatial](https://databrickslabs.github.io/geobrix/docs/databricks-spatial)).

## Known limitations

- Native Databricks `GEOMETRY`/`GEOGRAPHY` are not produced directly yet — geometries are exchanged as **WKB/WKT** (+ `*_srid`); convert with the native ST functions ([Databricks Spatial](https://databrickslabs.github.io/geobrix/docs/databricks-spatial)).
- Spatial KNN is not yet ported; nor is H3 for geometry-based k-ring / k-loop.

## Building, deploying, releasing

See the [`scripts`](./scripts) folder and the [docs](https://databrickslabs.github.io/geobrix/docs/developers).

## Support

Databricks Labs projects are provided **AS-IS**, for exploration only, and are **not** covered by Databricks SLAs. Please file issues as [GitHub Issues](https://github.com/databrickslabs/geobrix/issues); they are reviewed as time permits. Do not file Databricks support tickets for these projects.
