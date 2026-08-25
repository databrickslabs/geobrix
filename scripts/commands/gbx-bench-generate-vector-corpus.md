# gbx:bench:generate-vector-corpus

Generate the scaled vector benchmark corpus (polygon seed → per-format transcoding → ×N replicas) using the pyrx venv. Runs locally for small-scale validation; use the bench cluster for the full 1M×100 corpus.

**Usage:** `bash scripts/commands/gbx-bench-generate-vector-corpus.sh [OPTIONS]`

**Options:**
- `--rows <n>` — number of polygon rows in the seed (default `1000000`)
- `--copies <n>` — number of per-format replicas (default `100`)
- `--formats <list>` — comma-separated list of `*_gbx` format names (default `geojson_gbx,shapefile_gbx,gpkg_gbx,file_gdb_gbx`; note: `file_gdb_gbx` requires the heavyweight GDAL natives — cluster only)
- `--out <dir>` — output root directory (default `/Volumes/main/default/bench-corpus/vector-scale`)
- `--log <path>` — tee output to a log file under `test-logs/`
- `--help`, `-h` — show help and exit
- `--gpkg-bench` — GeoPackage bench preset: stages one copies-dir per copies value via `stage_gpkg_bench_corpus` (few files × many rows, swept `copies`). Uses `--rows`, `--copies`, and `--out`; all other options are ignored.
  - `--rows <n>` — (with `--gpkg-bench`) rows per `.gpkg` file (default `100000`)
  - `--copies <list>` — (with `--gpkg-bench`) comma-separated copies ladder (default `10,80,160`)
  - `--out <dir>` — (with `--gpkg-bench`) output root directory (default `/Volumes/main/default/bench-corpus/gpkg-bench`)

**Examples:**
- Small local validation (geojson + gpkg only; no native osgeo required):
  `bash scripts/commands/gbx-bench-generate-vector-corpus.sh --rows 500 --copies 2 --formats geojson_gbx,gpkg_gbx --out /tmp/vc_smoke`
- Full cluster corpus (all 4 formats, 1M rows × 100 copies):
  `bash scripts/commands/gbx-bench-generate-vector-corpus.sh --rows 1000000 --copies 100 --log vector-corpus.log`
- GeoPackage bench preset (100k rows/file, copies ladder 10/80/160):
  `bash scripts/commands/gbx-bench-generate-vector-corpus.sh --gpkg-bench --out /Volumes/main/default/bench-corpus/gpkg-bench`
- GeoPackage bench preset, local smoke (tiny rows/copies):
  `bash scripts/commands/gbx-bench-generate-vector-corpus.sh --gpkg-bench --rows 500 --copies 2,3 --out /tmp/gpkg_bench_smoke`
