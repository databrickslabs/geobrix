# GeoBrix — Lakeflow Connect connector

Ingest geospatial files from Unity Catalog Volumes into Unity Catalog tables with a
Lakeflow Connect ingestion pipeline: vector data (GeoPackage, GeoJSON, GeoJSONSeq,
Shapefile, File Geodatabase), LiDAR point clouds, EXIF photo metadata, GeoTIFF and COG
rasters, NetCDF files, and the tiles of PMTiles archives. Each run appends only files that
are new since the last run.

Use this connector for the **Add data** page and managed ingestion pipelines, with access
governed by a Unity Catalog connection. For large or fast-growing folders, where Auto Loader
tracks the files, or when overwritten files must be kept as versions (`scd2`) or replaced
(`replace`), use the Auto Loader route instead:
[Auto Loader ingestion](https://databrickslabs.github.io/geobrix/docs/ingestion/auto-loader).

The published documentation for this connector is
[Lakeflow connector](https://databrickslabs.github.io/geobrix/docs/ingestion/lakeflow-connector),
and [Ingestion Pipelines](https://databrickslabs.github.io/geobrix/docs/ingestion/overview)
compares it with the other ingestion routes.

Files are read by the same code as the GeoBrix `*_gbx` readers, so rows carry the same
columns: attributes, the geometry column (`geom_0`, or the layer's geometry name such as
`geom` for GeoPackage) with its `*_srid` / `*_srid_proj` companions for vector data, plus
two lineage columns:

| Column | Type | Meaning |
|---|---|---|
| `_source_path` | STRING | Volume path of the file the row came from |
| `_source_mtime` | TIMESTAMP | That file's modification time |

## Formats

The reader for a table is the `format` table option, or is inferred from the file
extension when every file in the dataset has the same one. A folder that mixes formats
needs an explicit `format`.

| `format` | Extensions |
|---|---|
| `vector` | any vector extension below (generic) |
| `gpkg` | `.gpkg` |
| `geojson` | `.geojson` (for GeoJSON files named `.json`, set `format: geojson` and `file_glob: *.{geojson,json}`) |
| `geojsonl` | `.geojsonl`, `.geojsons` |
| `shapefile` | `.shp`, `.shz`, `.shp.zip` |
| `file_gdb` | `.gdb`, `.gdb.zip` |
| `lidar` | `.las`, `.laz` |
| `exif` | `.jpg`, `.jpeg` |
| `raster` | `.tif`, `.tiff` (another raster file, such as `.jp2`, is read only when `path` names that file, or `file_glob` names it, and `format` is `raster`) |
| `gtiff` | `.tif`, `.tiff` |
| `cog` | `.tif`, `.tiff`, read with the COG reader, which splits rasters into tile windows like `raster`. By extension a `.tif` is a `gtiff` table, so set `format: cog`. |
| `netcdf` | `.nc`. The `mode` option picks what a file becomes: `raster` (default), one GeoTIFF tile per grid variable; `vector`, one point feature per observation or grid cell. |
| `pmtiles` | `.pmtiles`: one row per tile stored in the archive (`z`, `x`, `y`, `bytes`; the reader's `source="archive"`). Rendering tiles from GeoTIFFs (`source="raster"`) is not a connector table; use `read_paths` or `declare_ingest`. |

`format` names a reader the same way as `read_paths` does: the `_gbx` suffix is optional
and case is ignored, so `gpkg`, `GPKG` and `gpkg_gbx` all select the `gpkg_gbx` reader.
The lightweight `file` reader (it lists files) and the heavyweight JVM readers are not
connector formats.

## Install

The pipeline's environment installs three wheels (no GeoBrix install is needed on the
compute that queries the resulting tables):

```text
geobrix[light_dbr18]@file:///Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix-<version>-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/lakeflow_community_connectors-<version>-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix_lakeflow_connectors-<version>-py3-none-any.whl
```

- Write the GeoBrix line **without spaces around `@`** — pipelines install each line with an
  unquoted `%pip install`, so spaces split the requirement.
- Match the extra to the pipeline's channel runtime (`light_dbr18` for a DBR 18 channel,
  `light_dbr19` for DBR 19).
- Keep the wheels in a folder of their own.

## Configure

**1. Connection (once).** A Unity Catalog connection of type `COMMUNITY`:

| Option | Required | Meaning |
|---|---|---|
| `sourceName` | yes | `geobrix` |
| `externalOptionsAllowList` | yes | `path,format,layer,where,bbox,chunk_size,file_glob,mode` |
| `root_path` | no | Volume prefix. Enables discovery (every file or folder directly under it is a table) and confines every table path to it. Access is still governed by Volume grants. |

Grant `USE CONNECTION` to the people who build pipelines on it.

**2. Pipeline (per ingestion).** Each table names a dataset and may set these options:

| Option | Meaning |
|---|---|
| `path` | File or folder, relative to `root_path` or absolute. A folder is one dataset; its files are read recursively, except files and folders whose names start with `_` or `.`. Optional when the table name matches an entry under `root_path`. |
| `format` | A reader from the table above. Default: inferred from the file extension. |
| `layer` | Vector only. Layer name for multi-layer sources (default: the first layer) |
| `where` | Vector only. OGR SQL filter, e.g. `borough = 'Queens'` |
| `bbox` | Vector only. `xmin,ymin,xmax,ymax` in the layer's CRS |
| `chunk_size` | Vector only. Features per Arrow batch (default 10000) |
| `file_glob` | File-name pattern for folder members, e.g. `zones_*.gpkg`, in Spark `pathGlobFilter` syntax (see below) |
| `mode` | `lidar`, `exif` and `netcdf` only. Reader mode: `points` (default) or `metadata` for LiDAR, `metadata` (default) or `qc` for EXIF, `raster` (default) or `vector` for NetCDF |

"Vector" means the `vector`, `gpkg`, `geojson`, `geojsonl`, `shapefile` and `file_gdb`
readers. `path`, `format` and `file_glob` apply to every table; setting one of the other
options on a table whose reader does not take it fails with an error that names the option
and the readers it applies to. Raster (`raster`, `gtiff`, `cog`) and `pmtiles` tables take
none of them.

`file_glob` uses the same syntax as the `glob` of `declare_ingest`: a Spark
`pathGlobFilter` (Hadoop glob) pattern matched against the file name. It is case-sensitive,
`{a,b}` matches either alternative, and a backslash escapes a pattern character (`\*`
matches a literal `*`). With `format` set, `file_glob` alone picks the folder's members, as
`glob` replaces the default pattern; so for shapefiles end it in `.shp` (`zones_*.shp`),
because `zones_*` would also pick each shapefile's `.dbf`, `.shx` and `.prj` sidecars.
Without `format`, it narrows the files that have a recognized extension.

Folders and files whose names start with `_` or `.` are ignored, as Spark's file sources
ignore them: use `_archive/` or `_staging/` to keep files out of a table.

The ingestion type is append-only, with `_source_mtime` as the cursor. For `scd2` and
`replace` ingestion, see [Auto Loader ingestion](https://databrickslabs.github.io/geobrix/docs/ingestion/auto-loader); the managed Add
data route supports append only.

## Deploy

Create a **serverless** pipeline whose source is [`pipeline/ingest.py`](pipeline/ingest.py)
(edit its `pipeline_spec`), with the three wheels above as the environment's dependencies.
Each pipeline update creates or appends to one streaming table per spec entry.

The Add data UI and the managed-ingestion pipeline API (`ingestion_definition` with
`source_type: COMMUNITY`) are the other deployment route for community connectors; use the
source-file pipeline above when that route is not available in your workspace.

## Behavior

- **Incremental by file.** The offset records the newest modification time seen and
  the files at that time; a run reads files newer than that (or at that time but not
  yet seen). A file replaced in place gets a new modification time and is read again.
- **One Spark partition per file**, read as Arrow batches (no row-by-row conversion).
- The table schema comes from the first file; stage at least one file before creating
  the table.

## Develop

Tests run in an isolated venv: `bash scripts/commands/gbx-test-connectors.sh`.
