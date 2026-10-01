---
paths:
  - "notebooks/**"
  - "scripts/commands/gbx-test-notebooks*.sh"
  - "docs/docs/notebooks/**"
---

# Running notebooks on Databricks (staging, install, runners) — READ BEFORE hand-rolling anything

These facts have been painfully rediscovered by multiple agents. Follow them; don't reinvent.

- **Use the canonical commands — do NOT hand-roll `jobs.submit`/`workspace.upload` drivers.**
  - `gbx:test:notebooks-serverless` — imports a local `.ipynb` to the workspace, **strips `%pip`/`%restart_python` cells** (they fail in Serverless JOB compute), injects deps via the **Serverless environment spec** (`--extras`, `--wheel`, `--env-version`, `--profile`), submits via `jobs.submit`, polls. This sidesteps the whole notebook-install saga.
  - `gbx:test:notebooks` — runs notebooks **cell-by-cell inside the `geobrix-dev` Docker container** (fully local, `/Volumes` mounted, no workspace). Best for a quick "does it render/run" check.
  - If a command lacks a capability, **fix the command** (add an option) — don't write a one-off script.
- **On-cluster work is a job** (`jobs.submit`, visible in Runs), NOT Command-Exec.
- **Don't auto-retry a failed job.** Report `result_state` / `state_message` / `run_page_url` and stop; a failure is almost always structural.
- **Staging on dogfood** (a non-account-admin identity is assumed): wheels/data → the Volume **`/Volumes/geospatial_docs/geobrix/sample-data/`** via SDK `files.upload` (streaming). The configured `GBX_ARTIFACT_VOLUME` default (`…/gdal_artifacts/noble/geobrix`) **does not exist on dogfood** and returns a *misleading* `PermissionDenied: … not account admin` — that's a missing-schema error, not a real block. Notebooks → WSFS **`/Users/<you>/GeoBrix/<fresh dated subfolder>`** (dogfood aggressively GCs old notebooks). **Import a notebook with `w.workspace.import_(path, format=ImportFormat.JUPYTER, content=base64(nb_bytes), overwrite=True)` after `w.workspace.mkdirs(parent)`** — this classic `/api/2.0/workspace/import` endpoint WORKS for a non-admin (it errors `ResourceDoesNotExist` if the parent folder is absent — hence the mkdirs). Do **NOT** use the `w.workspace.upload()` SDK mixin — *that* routes through the gated path and returns the misleading "not account admin" error. `gbx:test:notebooks-serverless` already does the `import_` for you. `files.download`/`files.list` are gated — read job results via `jobs.get_run_output(task_run_id).notebook_output.result`, never `files.download`.
- **push-wheel SDK vs CLI**: SDK can exit 0 while bytes didn't land — verify. On dogfood use direct `files.upload`.
- **Notebook `%pip` install of the wheel** — ALWAYS `@ file:///Volumes/…/geobrix-<ver>-py3-none-any.whl` **with the extra** (`light_env6` for Serverless env 6 / `light_env5` for env 5; `light_dbr17` / `light_dbr18` / `light_dbr19` for classic DBR 17.3 / 18 / 19):
  - **INTERACTIVE** (refresh a live session) — two steps: `--no-deps --force-reinstall "geobrix[EXTRA] @ file://…"` then the same line with no flags, then `restartPython()`. (`--force-reinstall` is needed to swap fresh bytes of an already-installed *same-version* wheel — `--no-cache-dir` alone won't; `--no-deps` is what keeps force-reinstall from touching pyspark/preinstalled deps.)
  - **JOB** (non-interactive, fresh kernel) — a single **plain** install, NO flags.
  - **Never `--force-reinstall` WITHOUT `--no-deps`** — that reinstalls pyspark/other preinstalled packages (serverless hard-fails `violate preinstalled package pyspark==…`; classic `Failure starting repl`). **Never a bare `geobrix[EXTRA]`** without `@ file://` — it resolves from PyPI and downgrades idna/protobuf → the kernel won't restart.
  - No bare `[light]` extra: Serverless → `[light_env6]`/`[light_env5]`; classic → `[light_dbr17/18/19]`; each also has an `_all`.
- **WHL change → rebuild wheel + stage to Volume** (package/pyproject change). Keep only the current WHL/JAR version in the artifact Volume.
- **Min DBR 17.3 for Volumes**; supported 17.3 / 18 (+ 19.5-snap FILE). 16.x is INVALID.

## Rendering / viz

- **h3 mosaic (`plot_mosaic`):** match `gridResolution` to the scene scale or hexes render mostly empty (edge lengths res-6≈3.7 km, 7≈1.2 km, 8≈0.46 km, 9≈0.17 km). `plot_mosaic` is a **static** matplotlib render — pan/zoom is `plot_interactive`/`plot_cog`. A VRT ComplexSource+NODATA closes honeycomb seams.
- `plot_raster`: `bands=`/`stretch=`/`fill=`; true-color = `bands=(1,2,3), stretch="shared"`. XYZ uint16→8bit washes out; opt-in stretch.
- **Sample raster data:** the usable one under `sample-data/Volumes/.../london/` is `sentinel2/london_sentinel2_red.tif` (388×385 @10 m, EPSG:32630); `elevation/srtm_n51w001.tif` is a degenerate 2×3-px placeholder. No NYC *raster*. For bigger scenes use `gbx:data:download`, the STAC light API, or the DEM-3DEP / NAIP / NASANEX / TROPOMI / EMIT downloaders. `DemDownloader.lidar_dtm` tiles are UTM (not 4326) — `rst_transform(tile, 4326)` for lon/lat H3.
- Notebook narrative must track the code: when you edit a notebook, update its narrative and announce which notebooks changed; force-compute validation cells.
- Paste-into-notebook code snippets go to `.superpowers/prompts/` (md) with the path handed back, not inline.
