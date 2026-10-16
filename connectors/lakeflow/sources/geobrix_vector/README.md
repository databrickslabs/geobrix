# GeoBrix Vector — Lakeflow Connect connector

Ingest vector files (GeoPackage, GeoJSON, GeoJSONSeq, Shapefile, File Geodatabase, or any
OGR format) from Unity Catalog Volumes into Unity Catalog tables with a Lakeflow Connect
ingestion pipeline. Each run appends only files that are new since the last run.

Rows carry the same columns as the GeoBrix `*_gbx` readers — attributes, the geometry
column (`geom_0`, or the layer's geometry name such as `geom` for GeoPackage) with its
`*_srid` / `*_srid_proj` companions — plus two lineage columns:

| Column | Type | Meaning |
|---|---|---|
| `_source_path` | STRING | Volume path of the file the row came from |
| `_source_mtime` | TIMESTAMP | That file's modification time |

## Install

The pipeline's environment installs three wheels (no GeoBrix install is needed on the
compute that queries the resulting tables):

```text
geobrix[light_dbr19] @ file:///Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix-<version>-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/lakeflow_community_connectors-<version>-py3-none-any.whl
/Volumes/<catalog>/<schema>/<volume>/<dir>/geobrix_lakeflow_connectors-<version>-py3-none-any.whl
```

Match the GeoBrix extra to the pipeline's runtime channel (`light_dbr19` for a DBR 19
channel, `light_dbr18` for DBR 18). Keep the wheels in a folder of their own.

## Configure

**1. Connection (once).** A Unity Catalog connection of type `COMMUNITY`:

| Option | Required | Meaning |
|---|---|---|
| `sourceName` | yes | `geobrix_vector` |
| `externalOptionsAllowList` | yes | `path,format,layer,where,bbox,chunk_size,file_glob` |
| `root_path` | no | Volume prefix. Enables discovery (every file or folder directly under it is a table) and confines every table path to it. Access is still governed by Volume grants. |

Grant `USE CONNECTION` to the people who build pipelines on it.

**2. Pipeline (per ingestion).** Each table names a dataset and may set these options:

| Option | Meaning |
|---|---|
| `path` | File or folder, relative to `root_path` or absolute. A folder is one dataset; its files are read recursively. Optional when the table name matches an entry under `root_path`. |
| `format` | `gpkg`, `geojson`, `geojsonl`, `shapefile`, `filegdb`, or `vector` (any OGR driver; default) |
| `layer` | Layer name for multi-layer sources (default: the first layer) |
| `where` | OGR SQL filter, e.g. `borough = 'Queens'` |
| `bbox` | `xmin,ymin,xmax,ymax` in the layer's CRS |
| `chunk_size` | Features per Arrow batch (default 10000) |
| `file_glob` | Basename filter for folder members, e.g. `zones_*.gpkg` |

The ingestion type is append-only, with `_source_mtime` as the cursor.

## Behavior

- **Incremental by file.** The offset records the newest modification time seen and
  the files at that time; a run reads files newer than that (or at that time but not
  yet seen). A file replaced in place gets a new modification time and is read again.
- **One Spark partition per file**, read as Arrow batches (no row-by-row conversion).
- The table schema comes from the first file; stage at least one file before creating
  the table.

## Develop

Tests run in an isolated venv: `bash scripts/commands/gbx-test-connectors.sh`.
