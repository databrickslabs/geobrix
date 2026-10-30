---
paths:
  - "examples/**"
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
- **Submit through `examples/tests/_job_submit.submit_once`**, never a bare `w.jobs.submit(...)`: it sends one `idempotency_token` per logical run on every attempt, so a lost response cannot create a second run (a structural test fails on any direct `jobs.submit` in `examples/tests/`).
- **Don't auto-retry a failed job.** Report `result_state` / `state_message` / `run_page_url` and stop; a failure is almost always structural.
- **Staging on dogfood** (a non-account-admin identity is assumed): wheels/data → the Volume **`/Volumes/geospatial_docs/geobrix/sample-data/`** via SDK `files.upload` (streaming). The configured `GBX_ARTIFACT_VOLUME` default (`…/gdal_artifacts/noble/geobrix`) **does not exist on dogfood** and returns a *misleading* `PermissionDenied: … not account admin` — that's a missing-schema error, not a real block. Notebooks → WSFS **`/Users/<you>/GeoBrix/<fresh dated subfolder>`** (dogfood aggressively GCs old notebooks). **Import a notebook with `w.workspace.import_(path, format=ImportFormat.JUPYTER, content=base64(nb_bytes), overwrite=True)` after `w.workspace.mkdirs(parent)`** — this classic `/api/2.0/workspace/import` endpoint WORKS for a non-admin (it errors `ResourceDoesNotExist` if the parent folder is absent — hence the mkdirs). Do **NOT** use the `w.workspace.upload()` SDK mixin — *that* routes through the gated path and returns the misleading "not account admin" error. `gbx:test:notebooks-serverless` already does the `import_` for you. `files.download`/`files.list` are gated — read job results via `jobs.get_run_output(task_run_id).notebook_output.result`, never `files.download`.
- **stage-wheel SDK vs CLI**: SDK can exit 0 while bytes didn't land — verify. On dogfood use direct `files.upload`.
- **Notebook `%pip` install of the wheel** — ALWAYS the **env wheel for the runtime**, by file:
  `@ file:///Volumes/…/geobrix-<ver>+env6-py3-none-any.whl` (Serverless env 6, DBR 19, DBR 19 ML) or
  `…+env5-py3-none-any.whl` (Serverless env 5, DBR 17.3 / 18, Lakeflow on DBR 18), with **feature extras only**:
  `[stac]`, `[vizx]`, `[databricks]`, and `[zarr]` on env6. **Never a `light_*` extra**: since 0.6.0 every
  `light_*` / `light_*_all` name is a tripwire (pip `ResolutionImpossible`, naming the env wheel to install), and the
  other family's env wheel refuses to import. The plain `geobrix-<ver>-py3-none-any.whl` is for off Databricks only.
  - **INTERACTIVE** (refresh a live session) — two steps: step 1 **bare, no extra**
    `--no-deps --force-reinstall "geobrix @ file://…+env6…"`, then step 2 the plain install **with the feature
    extras** `"geobrix[stac,vizx] @ file://…+env6…"` (no flags), then `restartPython()`. (`--force-reinstall` swaps
    fresh bytes of an already-installed same-version wheel; `--no-deps` keeps it from touching preinstalled deps;
    step 2 restores the dependencies.)
  - **JOB** (non-interactive, fresh kernel) — a single **plain** install, NO flags.
  - **What the env wheels have no extra for is a separate `%pip` line after the wheel**, with its cost:
    `"overturemaps>=1.0.2,<2"` (moves click and tqdm, installs PyPI shapely), `"earthaccess>=0.11,<1" "botocore<1.44.0"`
    (moves fsspec), `"pycolmap==4.2.1"` / `"pycolmap-cuda12==4.2.1"`, the GeoSAM GPU pins
    (`torch==2.9.0 torchvision==0.24.0 segment-geospatial opencv-python-headless==4.11.0.86`).
  - **Never `--force-reinstall` WITHOUT `--no-deps`** (serverless hard-fails `violate preinstalled package …`; classic
    `Failure starting repl`). **Never a bare `geobrix[EXTRA]`** without `@ file://` (it resolves from PyPI).
  - `gbx:test:notebooks-serverless` builds the Serverless environment spec from the env wheel's own extra files:
    `--wheel` (default the env6 wheel on the sample-data Volume), `--extras` = that wheel's extras (no `light_*`),
    `--extra-deps` for the separate steps, `--wheel …+env5… --env-version 5` for an env-5 notebook, and
    `--new-cluster 19.x-scala2.13` for a classic DBR 19 job cluster that ends with the run.
- **WHL change → rebuild + stage**: `gbx:build:wheel --env 5|6` (both), then `gbx:data:stage-wheel --env both` (the
  bundle volroot, by each wheel's own `+env<N>` name). Keep only the current version of each wheel there.
- **Min DBR 17.3 for Volumes**; supported 17.3 / 18 (+ 19.5-snap FILE). 16.x is INVALID.

## Rendering / viz

- **h3 mosaic (`plot_mosaic`):** match `gridResolution` to the scene scale or hexes render mostly empty (edge lengths res-6≈3.7 km, 7≈1.2 km, 8≈0.46 km, 9≈0.17 km). `plot_mosaic` is a **static** matplotlib render — pan/zoom is `plot_interactive`/`plot_cog`. A VRT ComplexSource+NODATA closes honeycomb seams.
- `plot_raster`: `bands=`/`stretch=`/`fill=`; true-color = `bands=(1,2,3), stretch="shared"`. XYZ uint16→8bit washes out; opt-in stretch.
- **Sample raster data:** the usable one under `sample-data/Volumes/.../london/` is `sentinel2/london_sentinel2_red.tif` (388×385 @10 m, EPSG:32630); `elevation/srtm_n51w001.tif` is a degenerate 2×3-px placeholder. No NYC *raster*. For bigger scenes use `gbx:data:download`, the STAC light API, or the DEM-3DEP / NAIP / NASANEX / TROPOMI / EMIT downloaders. `DemDownloader.lidar_dtm` tiles are UTM (not 4326) — `rst_transform(tile, 4326)` for lon/lat H3.
- Notebook narrative must track the code: when you edit a notebook, update its narrative and announce which notebooks changed; force-compute validation cells.
- Paste-into-notebook code snippets go to `.superpowers/prompts/` (md) with the path handed back, not inline.
