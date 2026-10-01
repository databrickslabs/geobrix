---
paths:
  - "src/main/scala/com/databricks/labs/gbx/rasterx/**"
  - "src/main/scala/com/databricks/labs/gbx/ds/**"
  - "src/main/scala/com/databricks/labs/gbx/util/**"
  - "python/geobrix/src/databricks/labs/gbx/ds/**"
  - "python/geobrix/src/databricks/labs/gbx/pyrx/**"
---

# GDAL / OGR resource management

- **Serverless-safe materialize policy (REQUIRED):** any new code that reads a whole file or tile into executor RAM (a materialize) MUST route through `materialize_decision` in `ds/file_gbx.py` — never a raw unbounded `.read()` or `materialize_to_bytes` without it (Serverless per-task RAM ~1 GB; a mis-sized read silently OOMs).
- **Prefer `rst_fromcontent` with `binaryFile` reader** over `rst_fromfile` when you already have bytes — avoids temp-file races on executors. (Light `rst_fromfile` defaults to VIRTUAL; `materialize=True` forces bytes.)
- `GetNoDataValue` requires an output array (returns void otherwise).
- `GetStatistics` only works on the MDArray, **not on `Band` directly**.
- Always release Dataset/Band resources via `RasterDriver.releaseDataset(ds)` in a `try/finally`.
- For tests that work with non-EPSG projections (e.g. ESRI:54008), mix in `SilenceProjError` to suppress expected PROJ warnings.
- **Thread-safety (REQUIRED): register GDAL/OGR only via the synchronized `GDALManager` guards** — `GDALManager.init(config)` for GDAL drivers, `GDALManager.initOgr()` for OGR drivers. NEVER call raw `gdal.AllRegister()` / `ogr.RegisterAll()` per task, and never set process-global `gdal.SetConfigOption` outside `GDALManager`'s guarded paths. The GDAL Java bindings hold process-global registry/config state; concurrent Spark tasks in one executor JVM that race registration get a null `GetDriverByName` (NPE) or a native SIGSEGV that kills the executor.

## Light-tier pyrx specifics

- **HARD REQ: pyrx never `spark.conf.set` / `_jvm` / `.rdd`** — Serverless-safe. `repartition` is bench-only.
- **pyrx GDAL bundling is rejected** — don't bundle `osgeo.gdal`; reach GDAL via `rasterio`'s `libgdal` per-op.
- **Byte-heavy slowness is the scalar `@f.udf` boundary tax** — an Arrow UDF is the lever; robustness (rio-tiler/xrspatial swap-out) over a partial stub.
- pyrx diverges from heavy on NoData/edge behavior (terrain border, band-math, focal). Expected.
- gdal writer enforces exact `(source, tile)` schema; `nameCol` overwrites `source`.
- `JTS.toWKB` is 2D-only and drops Z — use `JTS.toWKB3`. (PySpark sends ints as Long.)
