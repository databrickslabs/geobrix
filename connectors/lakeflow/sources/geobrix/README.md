# GeoBrix — Lakeflow Connect connector

Ingest geospatial files from Unity Catalog Volumes into Unity Catalog tables with a
Lakeflow Connect ingestion pipeline: vector data (GeoPackage, GeoJSON, GeoJSONSeq,
Shapefile, File Geodatabase), LiDAR point clouds, and EXIF photo metadata. Each run
appends only files that are new since the last run.

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

| Reader | `format` aliases | Extensions |
|---|---|---|
| `vector_gbx` | `vector` | any vector extension below (generic) |
| `gpkg_gbx` | `gpkg` | `.gpkg` |
| `geojson_gbx` | `geojson` | `.geojson`, `.json` |
| `geojsonl_gbx` | `geojsonl` | `.geojsonl`, `.geojsons` |
| `shapefile_gbx` | `shapefile` | `.shp`, `.shz`, `.shp.zip` |
| `file_gdb_gbx` | `filegdb` | `.gdb`, `.gdb.zip` |
| `lidar_gbx` | `lidar` | `.las`, `.laz` |
| `exif_gbx` | `exif` | `.jpg`, `.jpeg` |

The reader name itself (for example `gpkg_gbx`) is also accepted for `format`.

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
| `path` | File or folder, relative to `root_path` or absolute. A folder is one dataset; its files are read recursively. Optional when the table name matches an entry under `root_path`. |
| `format` | A reader alias or name from the table above. Default: inferred from the file extension. |
| `layer` | Layer name for multi-layer vector sources (default: the first layer) |
| `where` | OGR SQL filter, e.g. `borough = 'Queens'` |
| `bbox` | `xmin,ymin,xmax,ymax` in the layer's CRS |
| `chunk_size` | Features per Arrow batch (default 10000) |
| `file_glob` | Basename filter for folder members, e.g. `zones_*.gpkg` |
| `mode` | Reader mode for `lidar` (default `points`) and `exif` (default `metadata`); passed through to the reader |

The ingestion type is append-only, with `_source_mtime` as the cursor. For `scd2` and
`replace` ingestion, see the incremental-ingestion docs page; the managed Add data route
supports append only.

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
